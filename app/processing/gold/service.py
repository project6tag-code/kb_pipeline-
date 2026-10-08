"""Psycopg-native Gold Publisher V2 service (no model inference, insert-only Gold).

V2 (docs/cc_fact_kb_v2.md): ``public.metadata.source_id`` is the original source
document (``app.processing.gold.source_document``), registered as one
``KB_SOURCE_DOCUMENT`` row in ``public.datasets``; ``published_at``,
``updated_at`` and ``crawled_at`` are always set. Per chunk, one transaction:
datasets (once per document) -> metadata -> kb -> gold_publications.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import socket
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime, timezone
from typing import Any

import psycopg

from app.config import target

from app.processing.chunk_dedup import eligibility, method
from app.processing.embedding import model as embedding_model
from app.processing.gold_bridge import bootstrap as gold_bridge
from app.processing.gold import repository as sql
from app.processing.gold import source_document
from app.queue.retry import RetryPolicy, jittered_backoff
from app.queue.states import RunKind, RunStatus

REQUIRED_DATABASE = target.DEFAULT_DATABASE  # default only; the enforced name is KB_EXPECTED_DATABASE
REQUIRED_REVISION = "kbp_001e"
RUNTIME_ROLE = "kb_pipeline_app"
MIGRATOR_ROLE = "kb_pipeline_migrator"
MODEL_ID = 2
SOURCE_CHANNELS = frozenset({"common_crawl", "github"})
MIN_UUID = uuid.UUID(int=0)
TRANSIENT_RETRIES = RetryPolicy(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=5.0)


class GoldPublisherError(RuntimeError):
    """A target, model, eligibility, provenance, or vector invariant failed."""


class GoldConflict(GoldPublisherError):
    """Existing Gold content disagrees with immutable canonical inputs."""


class GoldInjectedFailure(RuntimeError):
    """Test-only failure injected inside a publication transaction."""


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row)


def verify_target(conn: Any, expect_database: str, *, role: str) -> dict[str, Any]:
    try:
        expected = target.expected_database()
    except target.TargetConfigError as exc:
        raise GoldPublisherError(str(exc)) from None
    if expect_database != expected:
        raise GoldPublisherError(f"--expect-database {expect_database!r} differs from the configured expected "
                                 f"database {expected!r} ({target.ENV_NAME})")
    row = conn.execute(sql.IDENTITY_SQL).fetchone()
    actual = _mapping(row)
    if actual["database"] != expected:
        raise GoldPublisherError(f"connected to database {actual['database']!r}; expected {expected!r}")
    if actual["role"] != role:
        raise GoldPublisherError(f"connected as {actual['role']!r}; expected {role}")
    if actual["revision"] != REQUIRED_REVISION:
        raise GoldPublisherError(f"database revision is {actual['revision']!r}; expected {REQUIRED_REVISION!r}")
    if actual["superuser"] and role != "postgres":
        raise GoldPublisherError("runtime/migrator must not connect as superuser")
    return actual


def _json_object(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


def validate_model_row(row: Mapping[str, Any], *, allowed_statuses: set[str]) -> None:
    expected = embedding_model.registration_row()
    checks = {
        "embedding_model_id": MODEL_ID,
        "model_name": embedding_model.MODEL_NAME,
        "model_revision": embedding_model.MODEL_REVISION,
        "embedding_version": embedding_model.EMBEDDING_VERSION,
        "embedding_dimension": embedding_model.DIMENSION,
        "vector_type": embedding_model.VECTOR_TYPE,
        "distance_metric": embedding_model.DISTANCE_METRIC,
        "is_normalized": True,
        "max_input_tokens": embedding_model.MAX_SEQ_LENGTH,
    }
    for field, wanted in checks.items():
        if row[field] != wanted:
            raise GoldPublisherError(f"model 2 {field} is {row[field]!r}; expected {wanted!r}")
    if _json_object(row["config"]) != embedding_model.CONFIG:
        raise GoldPublisherError("model 2 registered configuration differs from the frozen MPNet V1 config")
    if row["status"] not in allowed_statuses:
        raise GoldPublisherError(f"model 2 status is {row['status']!r}; expected one of {sorted(allowed_statuses)}")


def read_model(conn: Any) -> dict[str, Any]:
    row = conn.execute(sql.MODEL_SQL, (MODEL_ID,)).fetchone()
    if row is None:
        raise GoldPublisherError(f"embedding_model_id {MODEL_ID} is missing")
    return _mapping(row)


def preflight_promotion(conn: Any, expect_database: str) -> dict[str, Any]:
    identity = verify_target(conn, expect_database, role=MIGRATOR_ROLE)
    model_row = read_model(conn)
    validate_model_row(model_row, allowed_statuses={"candidate", "active_gold"})
    active = [int(row["embedding_model_id"]) for row in conn.execute(sql.MODELS_ACTIVE_SQL).fetchall()]
    if any(model_id != MODEL_ID for model_id in active):
        raise GoldPublisherError(f"incompatible active_gold model(s) already exist: {active}")
    return {"database": identity["database"], "role": identity["role"], "revision": identity["revision"],
            "embedding_model_id": MODEL_ID, "status": model_row["status"], "other_active_gold": active}


def public_gold_counts(conn: Any, expect_database: str) -> dict[str, int]:
    """Runtime-role precondition: public Gold tables are readable by kb_pipeline_app only."""
    verify_target(conn, expect_database, role=RUNTIME_ROLE)
    preflight_public_schema(conn)
    return {"public_kb": int(conn.execute("SELECT count(*) AS count FROM public.kb").fetchone()["count"]),
            "public_metadata": int(conn.execute("SELECT count(id) AS count FROM public.metadata").fetchone()["count"])}


def promote_model(conn: Any, expect_database: str) -> dict[str, Any]:
    """Promote only the exact frozen model 2 row; caller supplies transaction."""
    checked = preflight_promotion(conn, expect_database)
    if checked["status"] == "active_gold":
        return {**checked, "action": "reused"}
    method_id = resolve_method_id(conn)
    shadow = audit_shadow_state(conn, method_id, allowed_statuses={"candidate"})
    bridges = _validate_bridges(conn)
    # The migrator cannot read public.kb/public.metadata; their emptiness is verified by the
    # runtime role (public_gold_counts) before this call, so only the kbp ledger is checked here.
    empty = {"gold_publications": int(conn.execute("SELECT count(*) AS count FROM kbp.gold_publications").fetchone()["count"])}
    if any(empty.values()):
        raise GoldPublisherError(f"candidate model promotion requires empty Gold ledger; found {empty}")
    updated = conn.execute(sql.PROMOTE_MODEL_SQL, (MODEL_ID,)).fetchone()
    row = read_model(conn)
    validate_model_row(row, allowed_statuses={"active_gold"})
    if updated is None:
        raise GoldPublisherError("model 2 changed concurrently; promotion did not update the candidate row")
    return {**checked, "status": row["status"], "action": "promoted", "eligibility": shadow,
            "bridge": bridges, "gold_before": empty}


def resolve_method_id(conn: Any) -> int:
    row = conn.execute(sql.METHOD_SQL, (method.ALGORITHM, method.VERSION)).fetchone()
    if row is None:
        raise GoldPublisherError(f"chunk dedup method {method.ALGORITHM}:{method.VERSION} is not registered")
    return int(row["dedup_method_id"])


def _decision_pages(conn: Any, method_id: int, batch_size: int) -> Iterator[list[dict[str, Any]]]:
    after = MIN_UUID
    while True:
        rows = [_mapping(row) for row in conn.execute(sql.DECISION_PAGE_SQL,
                                                       (method_id, after, batch_size)).fetchall()]
        if not rows:
            return
        yield rows
        after = rows[-1]["chunk_id"]


def inspect_eligibility(conn: Any, method_id: int, batch_size: int = 256) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    all_chunks = int(conn.execute(sql.CHUNK_COUNT_SQL).fetchone()["count"])
    decision_count = int(conn.execute(sql.DECISION_COUNT_SQL, (method_id,)).fetchone()["count"])
    for page in _decision_pages(conn, method_id, batch_size):
        for row in page:
            result = eligibility.classify(row["chunk_id"], row["classification"], row["representative_chunk_id"],
                                          row["decision_status"])
            counts[result.status] += 1
    expected_decisions = counts[eligibility.ELIGIBLE] + counts[eligibility.SUPPRESSED]
    if decision_count != all_chunks or expected_decisions != all_chunks:
        raise GoldPublisherError(
            f"chunk eligibility is incomplete: chunks={all_chunks}, decisions={decision_count}, "
            f"confirmed_eligible_or_suppressed={expected_decisions}, statuses={dict(counts)}"
        )
    if int(conn.execute(sql.SUPPRESSED_ORPHAN_SQL, (method_id,)).fetchone()["count"]) != 0:
        raise GoldPublisherError("one or more suppressed chunks do not resolve to a confirmed eligible representative")
    if counts[eligibility.NOT_CONFIRMED] or counts[eligibility.UNDECIDED]:
        raise GoldPublisherError(f"unpublishable chunk decisions exist: {dict(counts)}")
    return {"chunks": all_chunks, "decisions": decision_count, "eligible": counts[eligibility.ELIGIBLE],
            "suppressed": counts[eligibility.SUPPRESSED]}


def _validate_bridges(conn: Any) -> dict[str, str]:
    from app.processing.gold_bridge import spec as bridge_spec

    mappings = {row["source_channel"]: row["dataset_id"] for row in conn.execute(sql.BRIDGE_ROWS_SQL).fetchall()}
    if set(mappings) != SOURCE_CHANNELS:
        raise GoldPublisherError(f"source_dataset_map channels differ from locked bridge: {sorted(mappings)}")
    resolved: dict[str, str] = {}
    for channel in sorted(SOURCE_CHANNELS):
        dataset_id = gold_bridge.resolve_dataset_id(conn, channel)
        if dataset_id != bridge_spec.FROZEN_DATASET_IDS[channel] or mappings[channel] != dataset_id:
            raise GoldPublisherError(f"locked bridge resolution for {channel} differs from the frozen dataset id")
        resolved[channel] = str(dataset_id)
    return resolved


def audit_shadow_state(conn: Any, method_id: int, *, allowed_statuses: set[str] | None = None) -> dict[str, int]:
    model_row = read_model(conn)
    validate_model_row(model_row, allowed_statuses=allowed_statuses or {"active_gold"})
    eligibility_counts = inspect_eligibility(conn, method_id)
    bad_eligible = int(conn.execute(sql.ELIGIBLE_EMBEDDING_AUDIT_SQL,
                                    (MODEL_ID, MODEL_ID, method_id)).fetchone()["count"])
    suppressed_embedded = int(conn.execute(sql.SUPPRESSED_EMBEDDED_SQL, (MODEL_ID, method_id)).fetchone()["count"])
    if bad_eligible or suppressed_embedded:
        raise GoldPublisherError(f"shadow state invalid: eligible_missing_or_bad={bad_eligible}, "
                                 f"suppressed_embedded={suppressed_embedded}")
    return {**eligibility_counts, "embedding_results": int(conn.execute(
        "SELECT count(*) AS count FROM kbp.chunk_embeddings WHERE embedding_model_id = %s", (MODEL_ID,)
    ).fetchone()["count"]),
        "vectors": int(conn.execute("SELECT count(*) AS count FROM kbp.chunk_embedding_vectors WHERE embedding_model_id = %s",
                                    (MODEL_ID,)).fetchone()["count"])}


def preflight_public_schema(conn: Any) -> None:
    metadata_expected = (
        ("id", "uuid", True), ("source_id", "uuid", True), ("domain", "character varying(100)", False),
        ("source_url", "text", False), ("source_title", "text", False),
        ("source_type", "character varying(50)", False),
        ("published_at", "timestamp with time zone", False), ("updated_at", "timestamp with time zone", False),
        ("crawled_at", "timestamp with time zone", False),
    )
    metadata = conn.execute(
        "SELECT a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod), a.attnotnull "
        "FROM pg_catalog.pg_attribute a WHERE a.attrelid='public.metadata'::regclass "
        "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
    ).fetchall()
    if tuple((row["attname"], row["format_type"], row["attnotnull"]) for row in metadata) != metadata_expected:
        raise GoldPublisherError("public.metadata schema differs from the Gold V2 contract (eight columns + crawled_at;"
                                 " run scripts/db/admin_gold_v2_patch.sql)")
    kb = conn.execute(
        "SELECT a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod), a.attnotnull "
        "FROM pg_catalog.pg_attribute a WHERE a.attrelid='public.kb'::regclass "
        "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
    ).fetchall()
    if tuple((row["attname"], row["format_type"], row["attnotnull"]) for row in kb) != (
        ("id", "uuid", True), ("chunk", "text", True), ("embedding", "vector(768)", False),
        ("source_id", "uuid", False),
    ):
        raise GoldPublisherError("public.kb schema differs from the Gold V2 contract (id, chunk, embedding + source_id;"
                                 " run scripts/db/admin_gold_v2_patch.sql)")
    constraints = conn.execute(
        "SELECT conname, contype, pg_catalog.pg_get_constraintdef(oid), condeferrable, confupdtype, confdeltype "
        "FROM pg_catalog.pg_constraint WHERE conrelid='public.kb'::regclass ORDER BY conname"
    ).fetchall()
    expected = (
        ("kb_id_fkey", "f", "FOREIGN KEY (id) REFERENCES metadata(id) ON DELETE CASCADE", False, "a", "c"),
        ("kb_pkey", "p", "PRIMARY KEY (id)", False, None, None),
    )
    if tuple((row["conname"], row["contype"], row["pg_get_constraintdef"], row["condeferrable"],
              row["confupdtype"].strip() or None, row["confdeltype"].strip() or None)
             for row in constraints) != expected:
        raise GoldPublisherError("public.kb PK/FK contract differs from the documented non-deferrable metadata FK")


def validate_provenance(row: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Expected ``public.metadata`` and ``public.datasets`` rows for one representative chunk."""
    channel = row["source_channel"]
    if channel not in SOURCE_CHANNELS:
        raise GoldPublisherError(f"unsupported representative provenance channel {channel!r}")
    source_url = row["source_url"]
    if not isinstance(source_url, str) or not source_url:
        raise GoldPublisherError(f"chunk {row['chunk_id']} has no representative source URL")
    try:
        source_id = source_document.source_document_id(row)
        dates = source_document.gold_dates(row)
    except source_document.SourceDocumentError as exc:
        raise GoldPublisherError(str(exc)) from None
    if source_id in bridge_dataset_ids():
        raise GoldPublisherError(f"chunk {row['chunk_id']}: source id collides with a channel bridge dataset")
    metadata = {
        "id": row["chunk_id"],
        "source_id": source_id,
        "source_url": source_url,
        "source_title": row["source_title"],
        "source_type": source_document.document_type(row),  # real document type; the channel is in the dataset row
        "domain": row["subject_domain"],
        "published_at": dates["published_at"],
        "updated_at": dates["updated_at"],
        "crawled_at": dates["crawled_at"],
    }
    return metadata, source_document.dataset_row(row, source_id, dates)


def bridge_dataset_ids() -> set[uuid.UUID]:
    from app.processing.gold_bridge import spec as bridge_spec

    return set(bridge_spec.FROZEN_DATASET_IDS.values())


def validate_vector_row(row: Mapping[str, Any] | None) -> tuple[str, bytes]:
    if row is None:
        raise GoldPublisherError("stored shadow vector row is missing")
    if row["embedding_status"] != "stored_shadow" or row["embedding_published_at"] is not None:
        raise GoldPublisherError("embedding result is not immutable stored_shadow state")
    if row["dimensions"] != embedding_model.DIMENSION:
        raise GoldPublisherError(f"shadow vector dimension is {row['dimensions']!r}, expected 768")
    vector_hash = bytes(row["vector_sha256"])
    recomputed = bytes(row["recomputed_sha256"])
    if len(vector_hash) != 32 or vector_hash != recomputed:
        raise GoldPublisherError("shadow vector hash does not match PostgreSQL's vector_send bytes")
    values = [float(value) for value in row["embedding_text"].strip("[]").split(",")]
    if len(values) != embedding_model.DIMENSION or not all(math.isfinite(value) for value in values):
        raise GoldPublisherError("shadow vector is wrong-sized or contains non-finite values")
    norm = math.sqrt(sum(value * value for value in values))
    if abs(norm - 1.0) > 1e-3 or abs(float(row["norm"]) - 1.0) > 1e-3:
        raise GoldPublisherError("shadow vector is not L2-normalized within the frozen tolerance")
    wire = bytes(row["vector_wire"])
    if hashlib.sha256(wire[4:]).digest() != vector_hash:
        raise GoldPublisherError("shadow vector wire bytes do not match the stored vector hash")
    return row["embedding_text"], wire


METADATA_FIELDS = ("id", "source_id", "source_url", "source_title", "source_type", "domain", "published_at",
                   "updated_at", "crawled_at")


def _same_metadata(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return all(actual[key] == expected[key] for key in METADATA_FIELDS)


def _json_normal(value: Any) -> Any:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _same_dataset(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return (actual["id"] == expected["id"] and actual["source_id"] == expected["source_id"]
            and actual["dataset_name"] == expected["dataset_name"] and actual["status"] == expected["status"]
            and _json_normal(actual["metadata"]) == _json_normal(expected["metadata"]))


def _same_kb(actual: Mapping[str, Any], chunk_id: uuid.UUID, source_id: uuid.UUID, chunk_text: str,
             wire: bytes) -> bool:
    """``kb.id`` is the unique chunk id; ``kb.source_id`` the shared original-document id (= metadata.source_id)."""
    return (actual["id"] == chunk_id and actual["source_id"] == source_id and actual["chunk"] == chunk_text
            and bytes(actual["embedding_wire"]) == wire)


def classify_existing_gold(metadata: Mapping[str, Any] | None, kb: Mapping[str, Any] | None,
                            ledger: Mapping[str, Any] | None, *, expected_metadata: Mapping[str, Any],
                            chunk_id: uuid.UUID, chunk_text: str,
                            vector_wire: bytes, model_id: int = MODEL_ID) -> str:
    """Return absent/reused or refuse any partial or immutable-field conflict."""
    present = (metadata is not None, kb is not None, ledger is not None)
    if not any(present):
        return "absent"
    if not all(present):
        raise GoldConflict(f"partial Gold state for {chunk_id}: metadata/kb/ledger presence={present}")
    if not _same_metadata(metadata, expected_metadata):
        raise GoldConflict(f"public.metadata for {chunk_id} differs from deterministic representative provenance")
    if not _same_kb(kb, chunk_id, expected_metadata["source_id"], chunk_text, vector_wire):
        raise GoldConflict(f"public.kb for {chunk_id} differs from canonical text or exact shadow vector bytes")
    if ledger["chunk_id"] != chunk_id or int(ledger["embedding_model_id"]) != model_id:
        raise GoldConflict(f"gold_publications ledger row for {chunk_id} has an unexpected identity")
    return "reused"


def _injected(failure_hook: Callable[[str, uuid.UUID], None] | None, point: str, chunk_id: uuid.UUID) -> None:
    if failure_hook is not None:
        failure_hook(point, chunk_id)


def publish_one(conn: Any, *, chunk_id: uuid.UUID, method_id: int, run_id: uuid.UUID,
                failure_hook: Callable[[str, uuid.UUID], None] | None = None) -> dict[str, Any]:
    """Publish one eligible representative. Caller must surround this with one transaction."""
    lock_key = int.from_bytes(chunk_id.bytes[:8], "big", signed=True)
    conn.execute(sql.LOCK_CHUNK_SQL, (lock_key,))

    decision = conn.execute(sql.ELIGIBILITY_SQL, (chunk_id, method_id)).fetchone()
    if decision is None:
        raise GoldPublisherError(f"chunk {chunk_id} has no decision for the active dedup method")
    resolved = eligibility.classify(chunk_id, decision["classification"], decision["representative_chunk_id"],
                                    decision["decision_status"])
    if not resolved.eligible:
        raise GoldPublisherError(f"chunk {chunk_id} is not an eligible representative ({resolved.status})")

    source = conn.execute(sql.PROVENANCE_SQL, (chunk_id,)).fetchone()
    if source is None:
        raise GoldPublisherError(f"chunk {chunk_id} has no canonical chunk/document/representative observation")
    source_row = _mapping(source)
    expected_metadata, expected_dataset = validate_provenance(source_row)

    vector_row = conn.execute(sql.SHADOW_VECTOR_SQL, (chunk_id, MODEL_ID)).fetchone()
    embedding_text, expected_wire = validate_vector_row(None if vector_row is None else _mapping(vector_row))

    # The source-document dataset row is shared by every chunk of the document: insert once, verify always.
    dataset_existing = conn.execute(sql.DATASET_GET_SQL, (expected_dataset["id"],)).fetchone()
    if dataset_existing is None:
        conn.execute(sql.DATASET_INSERT_SQL, (
            expected_dataset["id"], expected_dataset["source_id"], expected_dataset["dataset_name"],
            expected_dataset["status"], json.dumps(expected_dataset["metadata"], sort_keys=True, default=str)))
        dataset_existing = conn.execute(sql.DATASET_GET_SQL, (expected_dataset["id"],)).fetchone()
    if dataset_existing is None or not _same_dataset(_mapping(dataset_existing), expected_dataset):
        raise GoldConflict(f"public.datasets source document {expected_dataset['id']} differs from its provenance")

    metadata_existing = conn.execute(sql.METADATA_GET_SQL, (chunk_id,)).fetchone()
    kb_existing = conn.execute(sql.KB_GET_SQL, (chunk_id,)).fetchone()
    ledger_existing = conn.execute(sql.LEDGER_GET_SQL, (chunk_id, MODEL_ID)).fetchone()
    ledger_mapping = None if ledger_existing is None else _mapping(ledger_existing)
    state = classify_existing_gold(None if metadata_existing is None else _mapping(metadata_existing),
                                   None if kb_existing is None else _mapping(kb_existing), ledger_mapping,
                                   expected_metadata=expected_metadata, chunk_id=chunk_id,
                                   chunk_text=source_row["chunk_text"], vector_wire=expected_wire)
    if state == "reused":
        return {"chunk_id": str(chunk_id), "action": "reused", "source_type": expected_metadata["source_type"]}

    conn.execute(sql.METADATA_INSERT_SQL, tuple(expected_metadata[key] for key in METADATA_FIELDS))
    _injected(failure_hook, "after_metadata", chunk_id)
    metadata_after = conn.execute(sql.METADATA_GET_SQL, (chunk_id,)).fetchone()
    if metadata_after is None or not _same_metadata(_mapping(metadata_after), expected_metadata):
        raise GoldConflict(f"public.metadata insert for {chunk_id} did not verify")

    source_id = expected_metadata["source_id"]
    conn.execute(sql.KB_INSERT_SQL, (chunk_id, source_id, source_row["chunk_text"], embedding_text))
    _injected(failure_hook, "after_kb", chunk_id)
    kb_after = conn.execute(sql.KB_GET_SQL, (chunk_id,)).fetchone()
    if kb_after is None or not _same_kb(_mapping(kb_after), chunk_id, source_id, source_row["chunk_text"],
                                        expected_wire):
        raise GoldConflict(f"public.kb insert for {chunk_id} did not verify against the shadow vector bytes")

    _injected(failure_hook, "before_ledger", chunk_id)
    conn.execute(sql.LEDGER_INSERT_SQL, (chunk_id, MODEL_ID, run_id))
    ledger_after = conn.execute(sql.LEDGER_GET_SQL, (chunk_id, MODEL_ID)).fetchone()
    if (ledger_after is None or ledger_after["chunk_id"] != chunk_id
            or int(ledger_after["embedding_model_id"]) != MODEL_ID):
        raise GoldConflict(f"gold_publications insert for {chunk_id} did not verify")
    return {"chunk_id": str(chunk_id), "action": "created", "source_type": expected_metadata["source_type"]}


def _iter_eligible(conn: Any, method_id: int, batch_size: int) -> Iterator[list[uuid.UUID]]:
    for page in _decision_pages(conn, method_id, batch_size):
        eligible_ids = []
        for row in page:
            result = eligibility.classify(row["chunk_id"], row["classification"], row["representative_chunk_id"],
                                          row["decision_status"])
            if result.eligible:
                eligible_ids.append(row["chunk_id"])
        if eligible_ids:
            yield eligible_ids


def _suppressed_report(conn: Any, method_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(sql.SUPPRESSED_SQL, (MODEL_ID, MODEL_ID, method_id)).fetchall()
    return [{key: (str(value) if isinstance(value, uuid.UUID) else value) for key, value in _mapping(row).items()}
            for row in rows]


def _shared_report(conn: Any) -> list[dict[str, Any]]:
    return [{key: (str(value) if isinstance(value, uuid.UUID) else value) for key, value in _mapping(row).items()}
            for row in conn.execute(sql.SHARED_CHUNKS_SQL).fetchall()]


def _audit_gold(conn: Any, method_id: int) -> dict[str, Any]:
    suppressed = _suppressed_report(conn, method_id)
    if any(
        row["suppressed_in_kb"] or row["suppressed_in_metadata"] or row["suppressed_in_ledger"]
        or (row["representative_in_ledger"] and
            (not row["representative_in_kb"] or not row["representative_in_metadata"]))
        or (not row["representative_in_ledger"] and
            (row["representative_in_kb"] or row["representative_in_metadata"]))
        for row in suppressed
    ):
        raise GoldPublisherError("suppressed chunk/representative Gold resolution is inconsistent")
    row = _mapping(conn.execute(sql.GOLD_AUDIT_SQL, (method_id, MODEL_ID)).fetchone())
    problems = {key: int(value) for key, value in row.items() if key != "ledger_rows" and int(value) != 0}
    if problems:
        raise GoldPublisherError(f"Gold ledger/content audit failed: {problems}")
    source_counts = {record["source_type"]: int(record["rows"])
                     for record in conn.execute(sql.SOURCE_COUNTS_SQL, (MODEL_ID,)).fetchall()}
    return {"ledger_rows": int(row["ledger_rows"]), "suppressed": suppressed,
            "source_type_counts": source_counts, "shared_chunks": _shared_report(conn)}


def _create_run(conn: Any, notes: str) -> uuid.UUID:
    run_id = uuid.uuid4()
    conn.execute(sql.CREATE_RUN_SQL, (run_id, socket.gethostname(), notes[:4000]))
    return run_id


def _finish_run(conn: Any, run_id: uuid.UUID, status: str, notes: str) -> None:
    result = conn.execute(sql.FINISH_RUN_SQL, (status, notes[:4000], run_id))
    if result.rowcount != 1:
        raise GoldPublisherError(f"pipeline run {run_id} did not finish from running state")


def _transient(exc: BaseException) -> bool:
    return isinstance(exc, (psycopg.OperationalError, psycopg.errors.SerializationFailure,
                            psycopg.errors.DeadlockDetected, psycopg.errors.LockNotAvailable))


def _publish_with_retry(conn: Any, *, chunk_id: uuid.UUID, method_id: int, run_id: uuid.UUID,
                        failure_hook: Callable[[str, uuid.UUID], None] | None,
                        rng: random.Random) -> dict[str, Any]:
    for attempt in range(1, TRANSIENT_RETRIES.max_attempts + 1):
        try:
            with conn.transaction():
                return publish_one(conn, chunk_id=chunk_id, method_id=method_id, run_id=run_id,
                                   failure_hook=failure_hook)
        except BaseException as exc:
            if not _transient(exc) or attempt == TRANSIENT_RETRIES.max_attempts:
                raise
            time.sleep(max(0.05, jittered_backoff(TRANSIENT_RETRIES, attempt, rng)))
    raise AssertionError("retry loop exhausted without a result")  # pragma: no cover


def run_publisher(conn: Any, *, expect_database: str, limit: int = 100_000, batch_size: int = 4,
                  failure_hook: Callable[[str, uuid.UUID], None] | None = None) -> dict[str, Any]:
    if limit < 1 or batch_size < 1 or batch_size > 100:
        raise ValueError("limit must be >=1 and batch_size must be 1..100")
    identity = verify_target(conn, expect_database, role=RUNTIME_ROLE)
    model_row = read_model(conn)
    validate_model_row(model_row, allowed_statuses={"active_gold"})
    preflight_public_schema(conn)
    method_id = resolve_method_id(conn)
    shadow_audit = audit_shadow_state(conn, method_id)
    bridges = _validate_bridges(conn)
    baseline = {key: value for key, value in conn.execute(sql.STATE_COUNTS_SQL).fetchone().items()}
    gold_before = _audit_gold(conn, method_id)
    run_notes = json.dumps({"device": "not_applicable_copy_shadow", "gold_publisher": "v2",
                            "model_id": MODEL_ID, "embedding_version": embedding_model.EMBEDDING_VERSION,
                            "eligible": shadow_audit["eligible"]}, sort_keys=True)
    with conn.transaction():
        run_id = _create_run(conn, run_notes)
    created = reused = attempted = 0
    by_source: Counter[str] = Counter()
    rng = random.Random()
    try:
        for page in _iter_eligible(conn, method_id, batch_size):
            for chunk_id in page:
                if attempted >= limit:
                    break
                item = _publish_with_retry(conn, chunk_id=chunk_id, method_id=method_id, run_id=run_id,
                                           failure_hook=failure_hook, rng=rng)
                attempted += 1
                by_source[item["source_type"]] += 1
                if item["action"] == "created":
                    created += 1
                else:
                    reused += 1
            if attempted >= limit:
                break
        if attempted < min(shadow_audit["eligible"], limit):
            raise GoldPublisherError(f"publisher processed {attempted} chunks; expected {shadow_audit['eligible']}")
        after = _audit_gold(conn, method_id)
        expected_rows = min(shadow_audit["eligible"], gold_before["ledger_rows"] + created)
        if after["ledger_rows"] != expected_rows:
            raise GoldPublisherError("published Gold row count does not equal eligible representative count")
        final_notes = json.dumps({"gold_publisher": "v2", "attempted": attempted, "created": created,
                                  "reused": reused, "source_type_counts": dict(by_source)}, sort_keys=True)
        with conn.transaction():
            _finish_run(conn, run_id, RunStatus.SUCCEEDED.value, final_notes)
    except BaseException as exc:
        with conn.transaction():
            _finish_run(conn, run_id, RunStatus.FAILED.value,
                        json.dumps({"gold_publisher": "v2", "error": type(exc).__name__, "message": str(exc)},
                                   sort_keys=True))
        raise
    final_counts = {key: value for key, value in conn.execute(sql.STATE_COUNTS_SQL).fetchone().items()}
    return {"database": identity["database"], "revision": identity["revision"], "run_id": str(run_id),
            "model_id": MODEL_ID, "model_status": "active_gold", "eligibility": shadow_audit,
            "processed": attempted, "created": created, "reused": reused,
            "source_type_counts": dict(by_source), "gold_before": gold_before, "audit": after,
            "counts_before": baseline, "counts_after": final_counts}
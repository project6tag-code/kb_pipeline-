"""DCA_v2 V1 test Gold source bridge: two-stage, idempotent, insert-or-verify bootstrap (no UPDATE path).

**Stage A, admin** (``bootstrap_public``). Run by a PostgreSQL superuser or the
owner of the public tables, through KB_ADMIN_DATABASE_URL (the operator's own
credentials). It creates or reuses:

* the two ``public.dataset_sources`` parents (``common_crawl``, ``github``);
* the two ``public.datasets`` bridge rows (the frozen ids, status ``KB_REFERENCE``).

No pipeline role gains a public-table privilege.

**Stage B, migrator** (``bootstrap_map``). Run as ``kb_pipeline_migrator``,
the owner of ``kbp``. It verifies the Stage A rows (SELECT only), then creates
or reuses the two ``kbp.source_dataset_map`` rows.

Both stages refuse to write unless all of the following hold:

* ``current_database()`` equals ``--expect-database`` AND is authorized for
  this V1 test bridge (``kb_pipeline_test`` only);
* ``data_collection_pipeline`` is never accepted;
* the kbp revision is ``kbp_001e``;
* the expected role is connected;
* the public tables match the verified 5-column stub contract exactly
  (columns, types, nullability, no defaults, constraints, no triggers).

**Conflicts.** For every row the stage looks up the deterministic id AND the
natural key:

* missing → INSERT ``ON CONFLICT DO NOTHING``, then re-read and verify;
* an identical row → reuse;
* anything else (same id with different fields, same name with a different
  id, same dataset with a wrong parent, a map row pointing elsewhere) →
  ``BridgeConflict``; the transaction rolls back.

Existing rows are never modified.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from typing import Any

from app.processing.gold_bridge import spec


class BridgeRefused(RuntimeError):
    """A precondition failed (target, role, revision, schema, missing Stage A rows); nothing was written."""


class BridgeConflict(RuntimeError):
    """An existing row disagrees with the frozen bridge contract; nothing was modified."""


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


IDENTITY_SQL = """
SELECT current_database() AS database, current_user AS role,
       (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) AS superuser,
       to_regclass('kbp.alembic_version') IS NOT NULL AS has_kbp,
       (SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE oid = 'public.datasets'::regclass)
           AS datasets_owner,
       (SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class
        WHERE oid = 'public.dataset_sources'::regclass) AS dataset_sources_owner
"""
REVISION_SQL = "SELECT version_num FROM kbp.alembic_version"
COLUMNS_SQL = """
SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull,
       (d.adbin IS NOT NULL) AS has_default, a.attidentity, a.attgenerated
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = CAST(:table AS regclass) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""
CONSTRAINTS_SQL = """
SELECT conname, contype, pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint
WHERE conrelid = CAST(:table AS regclass)
"""
TRIGGERS_SQL = """
SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid = CAST(:table AS regclass) AND NOT tgisinternal
"""
PIN_SEARCH_PATH_SQL = "SET LOCAL search_path = pg_catalog, public"  # deterministic catalog text; transaction-local

SOURCE_BY_NAME_SQL = """
SELECT id, name, source_type, authentication_required, status FROM public.dataset_sources WHERE name = :name
"""
SOURCE_BY_ID_SQL = """
SELECT id, name, source_type, authentication_required, status FROM public.dataset_sources WHERE id = :id
"""
INSERT_SOURCE_SQL = """
INSERT INTO public.dataset_sources (id, name, source_type, authentication_required, status)
VALUES (:id, :name, :source_type, :authentication_required, :status)
ON CONFLICT DO NOTHING
RETURNING id
"""
DATASET_BY_ID_SQL = """
SELECT id, source_id, dataset_name, status, metadata FROM public.datasets WHERE id = :id
"""
DATASETS_BY_NAME_SQL = """
SELECT id FROM public.datasets WHERE dataset_name = :dataset_name
"""
INSERT_DATASET_SQL = """
INSERT INTO public.datasets (id, source_id, dataset_name, status, metadata)
VALUES (:id, :source_id, :dataset_name, :status, CAST(:metadata AS jsonb))
ON CONFLICT (id) DO NOTHING
RETURNING id
"""
MAP_BY_CHANNEL_SQL = """
SELECT source_channel, dataset_source_name, bridge_identity_name, dataset_id
FROM kbp.source_dataset_map WHERE source_channel = :source_channel
"""
MAP_BY_DATASET_SQL = """
SELECT source_channel FROM kbp.source_dataset_map WHERE dataset_id = :dataset_id OR bridge_identity_name = :bridge
"""
INSERT_MAP_SQL = """
INSERT INTO kbp.source_dataset_map (source_channel, dataset_source_name, bridge_identity_name, dataset_id)
VALUES (:source_channel, :dataset_source_name, :bridge_identity_name, :dataset_id)
ON CONFLICT DO NOTHING
RETURNING source_channel
"""
COUNTS_SQL = """
SELECT (SELECT count(*) FROM public.dataset_sources) AS dataset_sources,
       (SELECT count(*) FROM public.datasets) AS datasets,
       (SELECT count(*) FROM kbp.source_dataset_map) AS source_dataset_map
"""


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #

def verify_target(conn: Any, expect_database: str, *, stage: str) -> dict:
    """Database, revision and role guards. ``stage`` is 'admin' or 'migrator'."""
    try:
        allowed = spec.allowed_databases()
    except spec.target.TargetConfigError as exc:
        raise BridgeRefused(str(exc)) from None
    if expect_database in spec.FORBIDDEN_DATABASES or expect_database not in allowed:
        raise BridgeRefused(f"--expect-database {expect_database!r} is not authorized for the Gold bridge "
                            f"(allowed: {sorted(allowed)}; set {spec.target.ENV_NAME})")
    row = dict(conn.execute(_text(IDENTITY_SQL)).mappings().one())
    if row["database"] in spec.FORBIDDEN_DATABASES or row["database"] != expect_database:
        raise BridgeRefused(f"connected to database {row['database']!r}, expected {expect_database!r}")
    if not row["has_kbp"]:
        raise BridgeRefused("kbp.alembic_version is missing")
    revision = conn.execute(_text(REVISION_SQL)).scalar_one()
    if revision != spec.EXPECTED_REVISION:
        raise BridgeRefused(f"kbp revision is {revision!r}, expected {spec.EXPECTED_REVISION!r}")
    if stage == "admin":
        owner = row["role"] == row["datasets_owner"] == row["dataset_sources_owner"]
        if not (row["superuser"] or owner):
            raise BridgeRefused(f"connected as {row['role']!r}; the admin stage needs a superuser or the owner of "
                                "public.datasets / public.dataset_sources")
        if row["role"] in ("kb_pipeline_app", "kb_pipeline_migrator"):
            raise BridgeRefused("the admin stage never runs as a pipeline role")
    elif stage == "migrator":
        if row["role"] != "kb_pipeline_migrator":
            raise BridgeRefused(f"connected as {row['role']!r}; the map stage runs as kb_pipeline_migrator")
    else:  # pragma: no cover - programming error
        raise ValueError(stage)
    return {"database": row["database"], "role": row["role"], "superuser": row["superuser"], "revision": revision}


def verify_public_schema(conn: Any) -> None:
    """Refuse unless public.dataset_sources / public.datasets are exactly the verified 5-column V1 contract."""
    conn.execute(_text(PIN_SEARCH_PATH_SQL))
    problems = []
    for table, expected in spec.EXPECTED_COLUMNS.items():
        rows = [tuple(r) for r in conn.execute(_text(COLUMNS_SQL), {"table": f"public.{table}"})]
        actual = [(name, typ, notnull) for name, typ, notnull, *_rest in rows]
        if actual != expected:
            problems.append(f"{table} columns {actual} != {expected}")
        if any(has_default or identity or generated for *_c, has_default, identity, generated in rows):
            problems.append(f"{table} has a default, identity or generated column")
        constraints = {tuple(r) for r in conn.execute(_text(CONSTRAINTS_SQL), {"table": f"public.{table}"})}
        if constraints != spec.EXPECTED_CONSTRAINTS[table]:
            problems.append(f"{table} constraints {sorted(constraints)} != {sorted(spec.EXPECTED_CONSTRAINTS[table])}")
        if conn.execute(_text(TRIGGERS_SQL), {"table": f"public.{table}"}).scalar_one():
            problems.append(f"{table} has user triggers")
    if problems:
        raise BridgeRefused("public schema differs from the verified V1 bridge contract: " + "; ".join(problems))


# --------------------------------------------------------------------------- #
# Stage A (admin): public.dataset_sources + public.datasets
# --------------------------------------------------------------------------- #

def _source_matches(row: Any, want: spec.SourceRow) -> bool:
    return row is not None and (row[0], row[1], row[2], bool(row[3]), row[4]) == (
        want.id, want.name, want.source_type, want.authentication_required, want.status)


def _dataset_matches(row: Any, want: spec.DatasetRow) -> bool:
    if row is None:
        return False
    metadata = row[4] if isinstance(row[4], dict) else json.loads(row[4])
    return (row[0], row[1], row[2], row[3], metadata) == (want.id, want.source_id, want.dataset_name, want.status,
                                                          want.metadata)


def ensure_source(conn: Any, want: spec.SourceRow) -> str:
    by_name = conn.execute(_text(SOURCE_BY_NAME_SQL), {"name": want.name}).one_or_none()
    by_id = conn.execute(_text(SOURCE_BY_ID_SQL), {"id": want.id}).one_or_none()
    if by_name is not None or by_id is not None:
        if _source_matches(by_name, want) and _source_matches(by_id, want):
            return "reused"
        raise BridgeConflict(f"dataset_sources conflict for {want.name!r}: by name {by_name and tuple(by_name)}, "
                             f"by id {by_id and tuple(by_id)}, expected {asdict(want)}")
    inserted = conn.execute(_text(INSERT_SOURCE_SQL), asdict(want)).one_or_none()
    if not _source_matches(conn.execute(_text(SOURCE_BY_ID_SQL), {"id": want.id}).one_or_none(), want):
        raise BridgeConflict(f"dataset_sources row for {want.name!r} did not verify after insert")
    return "created" if inserted is not None else "reused"


def ensure_dataset(conn: Any, want: spec.DatasetRow) -> str:
    by_id = conn.execute(_text(DATASET_BY_ID_SQL), {"id": want.id}).one_or_none()
    same_name = {r[0] for r in conn.execute(_text(DATASETS_BY_NAME_SQL), {"dataset_name": want.dataset_name})}
    if same_name - {want.id}:
        raise BridgeConflict(f"datasets name {want.dataset_name!r} is already used by {sorted(map(str, same_name))}")
    if by_id is not None:
        if _dataset_matches(by_id, want):
            return "reused"
        raise BridgeConflict(f"datasets id {want.id} exists with different fields {tuple(by_id)}")
    inserted = conn.execute(_text(INSERT_DATASET_SQL), {**asdict(want),
                                                        "metadata": json.dumps(want.metadata, sort_keys=True)}
                            ).one_or_none()
    if not _dataset_matches(conn.execute(_text(DATASET_BY_ID_SQL), {"id": want.id}).one_or_none(), want):
        raise BridgeConflict(f"datasets row {want.id} did not verify after insert")
    return "created" if inserted is not None else "reused"


def bootstrap_public(conn: Any, *, expect_database: str) -> dict:
    """Stage A. The caller owns the transaction (commit on success, rollback on any exception)."""
    identity = verify_target(conn, expect_database, stage="admin")
    verify_public_schema(conn)
    before = dict(conn.execute(_text(COUNTS_SQL)).mappings().one())
    actions = {}
    for name, ch in spec.CHANNELS.items():
        actions[f"dataset_sources.{name}"] = ensure_source(conn, ch.source)
        actions[f"datasets.{name}"] = ensure_dataset(conn, ch.dataset)
    after = dict(conn.execute(_text(COUNTS_SQL)).mappings().one())
    return {"stage": "admin", **identity, "before": before, "after": after, "actions": actions}


# --------------------------------------------------------------------------- #
# Stage B (migrator): kbp.source_dataset_map
# --------------------------------------------------------------------------- #

def ensure_map(conn: Any, want: spec.MapRow) -> str:
    current = conn.execute(_text(MAP_BY_CHANNEL_SQL), {"source_channel": want.source_channel}).one_or_none()
    others = {r[0] for r in conn.execute(_text(MAP_BY_DATASET_SQL), {"dataset_id": want.dataset_id,
                                                                     "bridge": want.bridge_identity_name})}
    if others - {want.source_channel}:
        raise BridgeConflict(f"dataset {want.dataset_id} / {want.bridge_identity_name} is already mapped by "
                             f"{sorted(others)}")
    expected = (want.source_channel, want.dataset_source_name, want.bridge_identity_name, want.dataset_id)
    if current is not None:
        if tuple(current) == expected:
            return "reused"
        raise BridgeConflict(f"source_dataset_map {want.source_channel!r} is {tuple(current)}, expected {expected}")
    inserted = conn.execute(_text(INSERT_MAP_SQL), asdict(want)).one_or_none()
    stored = conn.execute(_text(MAP_BY_CHANNEL_SQL), {"source_channel": want.source_channel}).one_or_none()
    if stored is None or tuple(stored) != expected:
        raise BridgeConflict(f"source_dataset_map {want.source_channel!r} did not verify after insert")
    return "created" if inserted is not None else "reused"


def bootstrap_map(conn: Any, *, expect_database: str) -> dict:
    """Stage B. Requires the exact Stage A rows; writes only kbp.source_dataset_map."""
    identity = verify_target(conn, expect_database, stage="migrator")
    verify_public_schema(conn)
    for name, ch in spec.CHANNELS.items():
        source = conn.execute(_text(SOURCE_BY_NAME_SQL), {"name": ch.source.name}).one_or_none()
        dataset = conn.execute(_text(DATASET_BY_ID_SQL), {"id": ch.dataset.id}).one_or_none()
        if source is None or dataset is None:
            raise BridgeRefused(f"Stage A rows for {name!r} are missing; run the admin bootstrap first")
        if not (_source_matches(source, ch.source) and _dataset_matches(dataset, ch.dataset)):
            raise BridgeConflict(f"Stage A rows for {name!r} differ from the bridge contract")
    before = dict(conn.execute(_text(COUNTS_SQL)).mappings().one())
    actions = {f"source_dataset_map.{name}": ensure_map(conn, ch.mapping) for name, ch in spec.CHANNELS.items()}
    after = dict(conn.execute(_text(COUNTS_SQL)).mappings().one())
    return {"stage": "migrator", **identity, "before": before, "after": after, "actions": actions}


def resolve_dataset_id(conn: Any, source_channel: str) -> uuid.UUID:
    """Gold-side lookup: source channel -> bridge dataset id (refuses unknown channels and missing maps)."""
    spec.channel(source_channel)
    if hasattr(conn, "cursor") and not hasattr(conn, "exec_driver_sql"):
        row = conn.execute(
            "SELECT source_channel, dataset_source_name, bridge_identity_name, dataset_id "
            "FROM kbp.source_dataset_map WHERE source_channel = %s",
            (source_channel,),
        ).fetchone()
        if row is not None and hasattr(row, "keys"):
            return row["dataset_id"]
    else:
        row = conn.execute(_text(MAP_BY_CHANNEL_SQL), {"source_channel": source_channel}).one_or_none()
    if row is None:
        raise BridgeRefused(f"no source_dataset_map row for {source_channel!r}: Gold publication is blocked")
    return row[3]

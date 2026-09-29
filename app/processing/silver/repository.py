"""Silver persistence on the existing Migration 001A tables (no new tables, no DDL).

| Silver datum                              | where it lives (schema section)                                   |
|-------------------------------------------|-------------------------------------------------------------------|
| document_id, content_hash, canonical_text | ``document_registry`` (§5.12; hash-binding CHECKs)                |
| status + machine-readable reasons         | ``document_registry.status`` / ``status_reason``                  |
| language / confidence                     | ``document_registry.language`` / ``language_confidence``          |
| quality / suitability scores              | ``document_registry.quality_score`` / ``factuality_score``        |
| subject domain ("Domain/Subdomain")       | ``document_registry.subject_domain``                              |
| every component version                   | ``document_registry.component_versions`` + ``policy_version``     |
| observation -> document, exact duplicates | ``observation_extractions`` (``document_was_new = FALSE``, §9.5)  |
| early rejections (empty/NUL/lang/prefilter/failed) | ``observation_extractions.outcome`` (D13)                |
| near-duplicate / version / related        | ``document_dedup_decisions`` + ``document_relationships`` (§5.24-25) |
| corrupt evidence, quarantines, internal errors | ``dead_letters`` (§5.21)                                     |

Only privileges granted to ``kb_pipeline_app`` are used (INSERT on the
registries; no UPDATE, no DELETE). Registering a dedup method is a migrator
bootstrap step (``app.processing.silver.bootstrap``), not a runtime write.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from app.db import repositories
from app.queue.states import DeadLetterItemType, Disposition, ErrorClass, Stage

# A transaction-scoped advisory lock serializes the registry/near-duplicate
# section across workers (see processor). A fixed, arbitrary signed 64-bit key.
REGISTRY_LOCK_KEY = 0x5B1A7E0C9D4F2A61 - (1 << 63)


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock(:key)"

EXISTING_EXTRACTIONS_SQL = """
SELECT observation_id, outcome, document_id
FROM kbp.observation_extractions
WHERE extraction_version = :version AND observation_id = ANY(CAST(:ids AS uuid[]))
"""

EXISTING_OBSERVATIONS_SQL = """
SELECT observation_id FROM kbp.source_observations WHERE observation_id = ANY(CAST(:ids AS uuid[]))
"""

EXISTING_DOCUMENTS_SQL = """
SELECT document_id FROM kbp.document_registry WHERE document_id = ANY(CAST(:ids AS uuid[]))
"""

CANDIDATE_TEXTS_SQL = """
SELECT document_id, canonical_text
FROM kbp.document_registry
WHERE document_id = ANY(CAST(:ids AS uuid[])) AND status = 'accepted' AND canonical_text IS NOT NULL
"""

LINEAGE_SQL = """
SELECT e.document_id, o.source_channel, o.source_url, g.repository_id, g.file_path
FROM kbp.observation_extractions AS e
JOIN kbp.source_observations AS o ON o.observation_id = e.observation_id
LEFT JOIN kbp.github_file_observations AS g ON g.observation_id = e.observation_id
WHERE e.document_id = ANY(CAST(:ids AS uuid[]))
LIMIT :limit
"""

DEDUP_METHOD_SQL = """
SELECT dedup_method_id FROM kbp.dedup_methods WHERE algorithm = :algorithm AND algorithm_version = :version
"""

INSERT_DOCUMENT_SQL = """
INSERT INTO kbp.document_registry (document_id, content_hash, canonical_text, char_length, language,
                                   language_confidence, quality_score, factuality_score, subject_domain, status,
                                   status_reason, policy_version, component_versions, first_observation_id,
                                   status_updated_at)
VALUES (:document_id, :content_hash, :canonical_text, :char_length, :language, :language_confidence,
        :quality_score, :factuality_score, :subject_domain, :status, :status_reason, :policy_version,
        CAST(:component_versions AS jsonb), :first_observation_id, now())
ON CONFLICT (document_id) DO NOTHING
RETURNING document_id
"""

INSERT_EXTRACTION_SQL = """
INSERT INTO kbp.observation_extractions (observation_id, extraction_version, outcome, document_id,
                                         document_was_new, extracted_at)
VALUES (:observation_id, :extraction_version, :outcome, :document_id, :document_was_new, now())
ON CONFLICT (observation_id, extraction_version) DO NOTHING
RETURNING observation_id
"""

INSERT_DECISION_SQL = """
INSERT INTO kbp.document_dedup_decisions (document_id, dedup_method_id, classification, canonical_document_id,
                                          similarity_score, decision_status, decided_at, run_id)
VALUES (:document_id, :dedup_method_id, :classification, :canonical_document_id, :similarity_score, 'confirmed',
        now(), :run_id)
ON CONFLICT (document_id, dedup_method_id) DO NOTHING
"""

INSERT_RELATIONSHIP_SQL = """
INSERT INTO kbp.document_relationships (document_id, related_document_id, relationship_type, dedup_method_id,
                                        similarity_score, decision_status)
VALUES (:document_id, :related_document_id, :relationship_type, :dedup_method_id, :similarity_score, 'confirmed')
ON CONFLICT (document_id, related_document_id, relationship_type, dedup_method_id) DO NOTHING
"""

DOCUMENT_STATUS_COUNTS_SQL = """
SELECT status, count(*) AS documents FROM kbp.document_registry GROUP BY status ORDER BY status
"""

EXTRACTION_OUTCOME_COUNTS_SQL = """
SELECT extraction_version, outcome, count(*) AS observations
FROM kbp.observation_extractions GROUP BY extraction_version, outcome ORDER BY extraction_version, outcome
"""


@dataclass(frozen=True)
class DocumentRow:
    document_id: uuid.UUID
    content_hash: str                  # lowercase hex (Identity V1 textual form)
    canonical_text: str | None
    char_length: int
    language: str
    language_confidence: float | None
    quality_score: float | None
    factuality_score: float | None
    subject_domain: str | None
    status: str
    status_reason: str
    policy_version: str
    component_versions: Mapping[str, Any]
    first_observation_id: uuid.UUID

    def __post_init__(self) -> None:
        if self.canonical_text is not None and "\x00" in self.canonical_text:
            raise ValueError("canonical text containing U+0000 must never reach PostgreSQL")
        if self.char_length <= 0:
            raise ValueError("char_length must be positive")
        if self.subject_domain is not None and len(self.subject_domain) > 100:
            raise ValueError("subject_domain exceeds 100 characters")


class SilverStore(Protocol):
    def lock_registry(self, conn: Any) -> None: ...
    def existing_extractions(self, conn: Any, observation_ids: Iterable[uuid.UUID],
                             version: str) -> dict[uuid.UUID, tuple[str, uuid.UUID | None]]: ...
    def existing_observations(self, conn: Any, observation_ids: Iterable[uuid.UUID]) -> set[uuid.UUID]: ...
    def existing_documents(self, conn: Any, document_ids: Iterable[uuid.UUID]) -> set[uuid.UUID]: ...
    def candidate_texts(self, conn: Any, document_ids: Iterable[uuid.UUID]) -> dict[uuid.UUID, str]: ...
    def lineage(self, conn: Any, document_ids: Iterable[uuid.UUID], limit: int) -> dict[uuid.UUID, set[str]]: ...
    def dedup_method_id(self, conn: Any, algorithm: str, version: str) -> int | None: ...
    def insert_document(self, conn: Any, row: DocumentRow) -> bool: ...
    def insert_extraction(self, conn: Any, observation_id: uuid.UUID, version: str, outcome: str,
                          document_id: uuid.UUID | None, was_new: bool | None) -> bool: ...
    def insert_decision(self, conn: Any, document_id: uuid.UUID, method_id: int, classification: str,
                        canonical_document_id: uuid.UUID | None, similarity: float | None,
                        run_id: uuid.UUID | None) -> None: ...
    def insert_relationship(self, conn: Any, document_id: uuid.UUID, related_document_id: uuid.UUID,
                            relationship_type: str, method_id: int, similarity: float | None) -> None: ...
    def dead_letter(self, conn: Any, *, item_type: DeadLetterItemType, item_key: str, stage: Stage,
                    disposition: Disposition, error_class: ErrorClass, error_code: str, error_message: str,
                    attempt_count: int, job_id: int | None, run_id: uuid.UUID | None,
                    payload_ref: str | None) -> None: ...


def lineage_key(channel: str, source_url: str, repository_id: int | None, file_path: str | None) -> str:
    if channel == "github" and repository_id is not None and file_path is not None:
        return f"github:{repository_id}:{file_path}"
    return f"url:{source_url}"


class PostgresSilverStore:
    def lock_registry(self, conn: Any) -> None:
        conn.execute(_text(ADVISORY_LOCK_SQL), {"key": REGISTRY_LOCK_KEY})

    def existing_extractions(self, conn: Any, observation_ids: Iterable[uuid.UUID],
                             version: str) -> dict[uuid.UUID, tuple[str, uuid.UUID | None]]:
        ids = [str(i) for i in observation_ids]
        if not ids:
            return {}
        rows = conn.execute(_text(EXISTING_EXTRACTIONS_SQL), {"ids": ids, "version": version})
        return {r[0]: (r[1], r[2]) for r in rows}

    def existing_observations(self, conn: Any, observation_ids: Iterable[uuid.UUID]) -> set[uuid.UUID]:
        ids = [str(i) for i in observation_ids]
        return {r[0] for r in conn.execute(_text(EXISTING_OBSERVATIONS_SQL), {"ids": ids})} if ids else set()

    def existing_documents(self, conn: Any, document_ids: Iterable[uuid.UUID]) -> set[uuid.UUID]:
        ids = [str(i) for i in document_ids]
        return {r[0] for r in conn.execute(_text(EXISTING_DOCUMENTS_SQL), {"ids": ids})} if ids else set()

    def candidate_texts(self, conn: Any, document_ids: Iterable[uuid.UUID]) -> dict[uuid.UUID, str]:
        ids = [str(i) for i in document_ids]
        return {r[0]: r[1] for r in conn.execute(_text(CANDIDATE_TEXTS_SQL), {"ids": ids})} if ids else {}

    def lineage(self, conn: Any, document_ids: Iterable[uuid.UUID], limit: int) -> dict[uuid.UUID, set[str]]:
        ids = [str(i) for i in document_ids]
        out: dict[uuid.UUID, set[str]] = {}
        if ids:
            for doc, channel, url, repo, path in conn.execute(_text(LINEAGE_SQL), {"ids": ids, "limit": limit}):
                out.setdefault(doc, set()).add(lineage_key(channel, url, repo, path))
        return out

    def dedup_method_id(self, conn: Any, algorithm: str, version: str) -> int | None:
        row = conn.execute(_text(DEDUP_METHOD_SQL), {"algorithm": algorithm, "version": version}).one_or_none()
        return None if row is None else int(row[0])

    def insert_document(self, conn: Any, row: DocumentRow) -> bool:
        params = {
            "document_id": row.document_id, "content_hash": bytes.fromhex(row.content_hash),
            "canonical_text": row.canonical_text, "char_length": row.char_length, "language": row.language,
            "language_confidence": row.language_confidence, "quality_score": row.quality_score,
            "factuality_score": row.factuality_score, "subject_domain": row.subject_domain, "status": row.status,
            "status_reason": row.status_reason, "policy_version": row.policy_version,
            "component_versions": json.dumps(dict(row.component_versions), sort_keys=True, separators=(",", ":")),
            "first_observation_id": row.first_observation_id,
        }
        return conn.execute(_text(INSERT_DOCUMENT_SQL), params).one_or_none() is not None

    def insert_extraction(self, conn: Any, observation_id: uuid.UUID, version: str, outcome: str,
                          document_id: uuid.UUID | None, was_new: bool | None) -> bool:
        return conn.execute(_text(INSERT_EXTRACTION_SQL), {
            "observation_id": observation_id, "extraction_version": version, "outcome": outcome,
            "document_id": document_id, "document_was_new": was_new}).one_or_none() is not None

    def insert_decision(self, conn: Any, document_id: uuid.UUID, method_id: int, classification: str,
                        canonical_document_id: uuid.UUID | None, similarity: float | None,
                        run_id: uuid.UUID | None) -> None:
        conn.execute(_text(INSERT_DECISION_SQL), {
            "document_id": document_id, "dedup_method_id": method_id, "classification": classification,
            "canonical_document_id": canonical_document_id, "similarity_score": similarity, "run_id": run_id})

    def insert_relationship(self, conn: Any, document_id: uuid.UUID, related_document_id: uuid.UUID,
                            relationship_type: str, method_id: int, similarity: float | None) -> None:
        conn.execute(_text(INSERT_RELATIONSHIP_SQL), {
            "document_id": document_id, "related_document_id": related_document_id,
            "relationship_type": relationship_type, "dedup_method_id": method_id, "similarity_score": similarity})

    def dead_letter(self, conn: Any, *, item_type: DeadLetterItemType, item_key: str, stage: Stage,
                    disposition: Disposition, error_class: ErrorClass, error_code: str, error_message: str,
                    attempt_count: int, job_id: int | None, run_id: uuid.UUID | None,
                    payload_ref: str | None) -> None:
        repositories.record_dead_letter(conn, item_type=item_type, item_key=item_key, stage=stage,
                                        disposition=disposition, error_class=error_class, error_code=error_code,
                                        error_message=error_message, attempt_count=attempt_count, job_id=job_id,
                                        run_id=run_id, payload_ref=payload_ref)


def status_counts(conn: Any) -> dict[str, Any]:
    return {"documents": [dict(r) for r in conn.execute(_text(DOCUMENT_STATUS_COUNTS_SQL)).mappings()],
            "extractions": [dict(r) for r in conn.execute(_text(EXTRACTION_OUTCOME_COUNTS_SQL)).mappings()]}

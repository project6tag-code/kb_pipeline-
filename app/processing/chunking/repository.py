"""Chunking persistence on Migration 001A (``chunk_registry``, ``chunk_occurrences``; no DDL).

| datum                                   | column                                                         |
|-----------------------------------------|----------------------------------------------------------------|
| canonical chunk text, hash, id          | ``chunk_registry`` (PK chunk_id + UNIQUE chunk_hash + hash CHECK) |
| token count (first writer's tokenizer)  | ``chunk_registry.token_count``                                  |
| first document / representative source  | ``chunk_registry.first_document_id`` / ``representative_observation_id`` |
| chunk position in a document + version  | ``chunk_occurrences`` PK (document_id, chunking_version_id, ordinal) |
| code-point span in the Silver text      | ``chunk_occurrences.char_start`` / ``char_end``                 |
| heading path / table-header context     | ``chunk_occurrences.section_path`` (JSON)                       |
| document-level chunking failures        | ``dead_letters`` (item ``document``, stage ``SILVER``)          |

Only privileges granted to ``kb_pipeline_app`` are used: INSERT on the two
chunk tables, SELECT on ``chunking_versions``. Registering a chunking version is
a migrator bootstrap step (``app.processing.chunking.bootstrap``).
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from app.db import repositories
from app.queue.states import DeadLetterItemType, Disposition, ErrorClass, Stage


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock(:key)"

CHUNKING_VERSION_SQL = """
SELECT chunking_version_id, tokenizer, parameters FROM kbp.chunking_versions WHERE name = :name
"""

EXISTING_OCCURRENCES_SQL = """
SELECT ordinal, chunk_id, char_start, char_end, section_path
FROM kbp.chunk_occurrences
WHERE document_id = :document_id AND chunking_version_id = :version_id
ORDER BY ordinal
"""

INSERT_CHUNK_SQL = """
INSERT INTO kbp.chunk_registry (chunk_id, chunk_hash, chunk_text, char_length, token_count, chunking_version_id,
                                first_document_id, representative_observation_id)
VALUES (:chunk_id, :chunk_hash, :chunk_text, :char_length, :token_count, :version_id, :document_id,
        :observation_id)
ON CONFLICT (chunk_id) DO NOTHING
RETURNING chunk_id
"""

INSERT_OCCURRENCE_SQL = """
INSERT INTO kbp.chunk_occurrences (document_id, chunking_version_id, ordinal, chunk_id, char_start, char_end,
                                   section_path)
VALUES (:document_id, :version_id, :ordinal, :chunk_id, :char_start, :char_end, :section_path)
ON CONFLICT (document_id, chunking_version_id, ordinal) DO NOTHING
RETURNING ordinal
"""

PENDING_DOCUMENTS_SQL = """
SELECT d.document_id, d.content_hash, d.canonical_text, d.char_length, d.status, d.status_reason,
       d.first_observation_id
FROM kbp.document_registry AS d
WHERE d.status = 'accepted'
  AND (CAST(:after AS uuid) IS NULL OR d.document_id > CAST(:after AS uuid))
  AND NOT EXISTS (SELECT 1 FROM kbp.chunk_occurrences AS o
                  WHERE o.document_id = d.document_id AND o.chunking_version_id = :version_id)
  AND NOT EXISTS (SELECT 1 FROM kbp.dead_letters AS x
                  WHERE x.item_type = 'document' AND x.item_key = d.document_id::text AND x.stage = 'SILVER'
                    AND x.resolved_at IS NULL)
ORDER BY d.document_id
LIMIT :limit
"""

CHUNK_COUNTS_SQL = """
SELECT v.name, v.chunking_version_id,
       (SELECT count(*) FROM kbp.chunk_occurrences o WHERE o.chunking_version_id = v.chunking_version_id)
           AS occurrences,
       (SELECT count(DISTINCT o.document_id) FROM kbp.chunk_occurrences o
        WHERE o.chunking_version_id = v.chunking_version_id) AS documents
FROM kbp.chunking_versions v ORDER BY v.chunking_version_id
"""


@dataclass(frozen=True)
class SilverDocument:
    """An accepted Silver document as stored in ``kbp.document_registry``."""

    document_id: uuid.UUID
    content_hash: bytes
    canonical_text: str | None
    char_length: int
    status: str
    status_reason: str | None
    first_observation_id: uuid.UUID

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "SilverDocument":
        digest = row["content_hash"]
        return cls(row["document_id"], bytes(digest) if not isinstance(digest, bytes) else digest,
                   row["canonical_text"], row["char_length"], row["status"], row["status_reason"],
                   row["first_observation_id"])


@dataclass(frozen=True)
class ChunkRow:
    chunk_id: uuid.UUID
    chunk_hash: bytes
    chunk_text: str
    token_count: int
    version_id: int
    document_id: uuid.UUID
    observation_id: uuid.UUID


@dataclass(frozen=True)
class OccurrenceRow:
    document_id: uuid.UUID
    version_id: int
    ordinal: int
    chunk_id: uuid.UUID
    char_start: int
    char_end: int
    section_path: str | None

    def key(self) -> tuple:
        return (self.ordinal, self.chunk_id, self.char_start, self.char_end, self.section_path)


class ChunkStore(Protocol):
    def chunking_version(self, conn: Any, name: str) -> tuple[int, str | None, dict] | None: ...
    def lock_document(self, conn: Any, document_id: uuid.UUID) -> None: ...
    def existing_occurrences(self, conn: Any, document_id: uuid.UUID, version_id: int) -> list[tuple]: ...
    def insert_chunk(self, conn: Any, row: ChunkRow) -> bool: ...
    def insert_occurrence(self, conn: Any, row: OccurrenceRow) -> bool: ...
    def pending_documents(self, conn: Any, version_id: int, after: uuid.UUID | None,
                          limit: int) -> list[SilverDocument]: ...
    def dead_letter(self, conn: Any, *, document_id: uuid.UUID, error_class: ErrorClass, error_code: str,
                    error_message: str, job_id: int | None, run_id: uuid.UUID | None) -> None: ...


def document_lock_key(document_id: uuid.UUID) -> int:
    """A signed 64-bit advisory-lock key derived from the document id (collisions only serialize)."""
    return int.from_bytes(document_id.bytes[:8], "big", signed=True)


class PostgresChunkStore:
    def chunking_version(self, conn: Any, name: str) -> tuple[int, str | None, dict] | None:
        row = conn.execute(_text(CHUNKING_VERSION_SQL), {"name": name}).one_or_none()
        return None if row is None else (int(row[0]), row[1], dict(row[2]))

    def lock_document(self, conn: Any, document_id: uuid.UUID) -> None:
        conn.execute(_text(ADVISORY_LOCK_SQL), {"key": document_lock_key(document_id)})

    def existing_occurrences(self, conn: Any, document_id: uuid.UUID, version_id: int) -> list[tuple]:
        return [tuple(r) for r in conn.execute(_text(EXISTING_OCCURRENCES_SQL),
                                              {"document_id": document_id, "version_id": version_id})]

    def insert_chunk(self, conn: Any, row: ChunkRow) -> bool:
        return conn.execute(_text(INSERT_CHUNK_SQL), {
            "chunk_id": row.chunk_id, "chunk_hash": row.chunk_hash, "chunk_text": row.chunk_text,
            "char_length": len(row.chunk_text), "token_count": row.token_count, "version_id": row.version_id,
            "document_id": row.document_id, "observation_id": row.observation_id}).one_or_none() is not None

    def insert_occurrence(self, conn: Any, row: OccurrenceRow) -> bool:
        return conn.execute(_text(INSERT_OCCURRENCE_SQL), {
            "document_id": row.document_id, "version_id": row.version_id, "ordinal": row.ordinal,
            "chunk_id": row.chunk_id, "char_start": row.char_start, "char_end": row.char_end,
            "section_path": row.section_path}).one_or_none() is not None

    def pending_documents(self, conn: Any, version_id: int, after: uuid.UUID | None,
                          limit: int) -> list[SilverDocument]:
        rows = conn.execute(_text(PENDING_DOCUMENTS_SQL), {
            "version_id": version_id, "after": str(after) if after else None, "limit": limit}).mappings()
        return [SilverDocument.from_row(r) for r in rows]

    def dead_letter(self, conn: Any, *, document_id: uuid.UUID, error_class: ErrorClass, error_code: str,
                    error_message: str, job_id: int | None, run_id: uuid.UUID | None) -> None:
        repositories.record_dead_letter(
            conn, item_type=DeadLetterItemType.DOCUMENT, item_key=str(document_id), stage=Stage.SILVER,
            disposition=Disposition.DEAD_LETTER, error_class=error_class, error_code=error_code,
            error_message=error_message, attempt_count=1, job_id=job_id, run_id=run_id, payload_ref=None)


def chunk_counts(conn: Any) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(_text(CHUNK_COUNTS_SQL)).mappings()]

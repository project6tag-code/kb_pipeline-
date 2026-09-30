"""Chunk near-dedup persistence on Migration 001B (no DDL, insert-only).

| datum                                    | table / column                                                   |
|------------------------------------------|------------------------------------------------------------------|
| method + frozen parameters               | ``dedup_methods`` (registered by the migrator bootstrap)          |
| one decision per chunk per method        | ``chunk_dedup_decisions`` PK (chunk_id, dedup_method_id)          |
| suppressed -> representative             | ``chunk_dedup_decisions.representative_chunk_id``                 |
| NEAR / RELATED edges                     | ``chunk_relationships`` PK (chunk_id, related_chunk_id, method), chunk_id < related_chunk_id |
| run provenance                           | ``run_id`` -> ``pipeline_runs`` on both tables                    |
| unexpected conflicts                     | ``dead_letters`` (item ``chunk``, stage ``CHUNK_DEDUPLICATED``)   |

Only ``kb_pipeline_app`` privileges are used:

* INSERT on the two 001B tables and on ``pipeline_runs`` / ``dead_letters``
  (through the shared repository helpers);
* SELECT elsewhere.

Nothing is updated or deleted. ``chunk_registry`` and ``chunk_occurrences``
are only read. Writers of one method serialize on a transaction-scoped advisory
lock (``lock_method``), so decisions are made against a consistent,
fully committed set of representatives.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from app.db import repositories
from app.processing.chunk_dedup import method
from app.processing.chunk_dedup.verify import ChunkText, Decision, Relationship
from app.queue.states import DeadLetterItemType, Disposition, ErrorClass, Stage


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


METHOD_LOCK_NAMESPACE = 0x6B62_7063_6464_0000  # "kbpcdd", combined with dedup_method_id

ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock(:key)"

GET_METHOD_SQL = """
SELECT dedup_method_id, parameters FROM kbp.dedup_methods WHERE algorithm = :algorithm AND algorithm_version = :version
"""

PENDING_CHUNKS_SQL = """
SELECT c.chunk_id, c.chunk_hash, c.chunk_text
FROM kbp.chunk_registry AS c
WHERE c.chunk_hash > :after
  AND NOT EXISTS (SELECT 1 FROM kbp.chunk_dedup_decisions AS d
                  WHERE d.chunk_id = c.chunk_id AND d.dedup_method_id = :method_id)
  AND NOT EXISTS (SELECT 1 FROM kbp.dead_letters AS x
                  WHERE x.item_type = 'chunk' AND x.item_key = c.chunk_id::text AND x.stage = 'CHUNK_DEDUPLICATED'
                    AND x.resolved_at IS NULL)
ORDER BY c.chunk_hash
LIMIT :limit
"""

RESOLVE_REPRESENTATIVES_SQL = """
SELECT c.chunk_id, c.chunk_hash, c.chunk_text
FROM unnest(CAST(:lows AS bytea[]), CAST(:highs AS bytea[])) AS k(lo, hi)
JOIN kbp.chunk_registry AS c ON c.chunk_hash BETWEEN k.lo AND k.hi
JOIN kbp.chunk_dedup_decisions AS d ON d.chunk_id = c.chunk_id AND d.dedup_method_id = :method_id
WHERE d.classification <> 'NEAR_DUPLICATE' AND d.decision_status = 'confirmed'
ORDER BY c.chunk_hash
"""

# The WHERE clause enforces the representative invariant in the database: a NEAR_DUPLICATE may only point at a
# chunk that already holds a confirmed, non-NEAR_DUPLICATE decision under the same method (no chains, no cycles).
INSERT_DECISION_SQL = """
INSERT INTO kbp.chunk_dedup_decisions (chunk_id, dedup_method_id, classification, representative_chunk_id,
                                       similarity_score, decision_status, decided_at, run_id)
SELECT :chunk_id, :method_id, :classification, CAST(:representative AS uuid), :similarity, 'confirmed',
       clock_timestamp(), :run_id
WHERE CAST(:representative AS uuid) IS NULL
   OR EXISTS (SELECT 1 FROM kbp.chunk_dedup_decisions AS r
              WHERE r.chunk_id = CAST(:representative AS uuid) AND r.dedup_method_id = :method_id
                AND r.classification <> 'NEAR_DUPLICATE' AND r.decision_status = 'confirmed')
ON CONFLICT (chunk_id, dedup_method_id) DO NOTHING
RETURNING chunk_id
"""

GET_DECISION_SQL = """
SELECT classification, representative_chunk_id, similarity_score, decision_status
FROM kbp.chunk_dedup_decisions WHERE chunk_id = :chunk_id AND dedup_method_id = :method_id
"""

INSERT_RELATIONSHIP_SQL = """
INSERT INTO kbp.chunk_relationships (chunk_id, related_chunk_id, relationship_type, dedup_method_id,
                                     similarity_score, decision_status, run_id)
VALUES (:chunk_id, :related_chunk_id, :relationship_type, :method_id, :similarity, 'confirmed', :run_id)
ON CONFLICT (chunk_id, related_chunk_id, dedup_method_id) DO NOTHING
RETURNING chunk_id
"""

GET_RELATIONSHIP_SQL = """
SELECT relationship_type FROM kbp.chunk_relationships
WHERE chunk_id = :chunk_id AND related_chunk_id = :related_chunk_id AND dedup_method_id = :method_id
"""

REPRESENTATIVE_COUNT_SQL = """
SELECT count(*) FROM kbp.chunk_dedup_decisions
WHERE dedup_method_id = :method_id AND classification <> 'NEAR_DUPLICATE' AND decision_status = 'confirmed'
"""

REPRESENTATIVES_PAGE_SQL = """
SELECT c.chunk_hash, c.chunk_text
FROM kbp.chunk_dedup_decisions AS d
JOIN kbp.chunk_registry AS c ON c.chunk_id = d.chunk_id
WHERE d.dedup_method_id = :method_id AND d.classification <> 'NEAR_DUPLICATE' AND d.decision_status = 'confirmed'
  AND c.chunk_hash > :after
ORDER BY c.chunk_hash
LIMIT :limit
"""

DECISIONS_IN_ORDER_SQL = """
SELECT d.chunk_id, c.chunk_hash, c.chunk_text, d.classification, d.representative_chunk_id, d.similarity_score
FROM kbp.chunk_dedup_decisions AS d
JOIN kbp.chunk_registry AS c ON c.chunk_id = d.chunk_id
WHERE d.dedup_method_id = :method_id
ORDER BY d.decided_at, c.chunk_hash
"""

RELATIONSHIPS_SQL = """
SELECT chunk_id, related_chunk_id, relationship_type, similarity_score
FROM kbp.chunk_relationships WHERE dedup_method_id = :method_id
"""

# Read-only audit. Every *_violations query must return 0.
AUDIT_SQL: dict[str, str] = {
    "near_representative_without_decision_violations": """
        SELECT count(*) FROM kbp.chunk_dedup_decisions d
        WHERE d.dedup_method_id = :method_id AND d.classification = 'NEAR_DUPLICATE'
          AND NOT EXISTS (SELECT 1 FROM kbp.chunk_dedup_decisions r
                          WHERE r.chunk_id = d.representative_chunk_id AND r.dedup_method_id = d.dedup_method_id)""",
    "representative_is_suppressed_violations": """
        SELECT count(*) FROM kbp.chunk_dedup_decisions d
        JOIN kbp.chunk_dedup_decisions r
          ON r.chunk_id = d.representative_chunk_id AND r.dedup_method_id = d.dedup_method_id
        WHERE d.dedup_method_id = :method_id AND d.classification = 'NEAR_DUPLICATE'
          AND (r.classification = 'NEAR_DUPLICATE' OR r.decision_status <> 'confirmed')""",
    "near_without_representative_edge_violations": """
        SELECT count(*) FROM kbp.chunk_dedup_decisions d
        WHERE d.dedup_method_id = :method_id AND d.classification = 'NEAR_DUPLICATE'
          AND NOT EXISTS (SELECT 1 FROM kbp.chunk_relationships e
                          WHERE e.dedup_method_id = d.dedup_method_id AND e.relationship_type = 'NEAR_DUPLICATE'
                            AND e.chunk_id = LEAST(d.chunk_id, d.representative_chunk_id)
                            AND e.related_chunk_id = GREATEST(d.chunk_id, d.representative_chunk_id))""",
    "edge_without_decisions_violations": """
        SELECT count(*) FROM kbp.chunk_relationships e
        WHERE e.dedup_method_id = :method_id
          AND (NOT EXISTS (SELECT 1 FROM kbp.chunk_dedup_decisions a
                           WHERE a.chunk_id = e.chunk_id AND a.dedup_method_id = e.dedup_method_id)
            OR NOT EXISTS (SELECT 1 FROM kbp.chunk_dedup_decisions b
                           WHERE b.chunk_id = e.related_chunk_id AND b.dedup_method_id = e.dedup_method_id))""",
    "near_edge_between_representatives_violations": """
        SELECT count(*) FROM kbp.chunk_relationships e
        JOIN kbp.chunk_dedup_decisions a ON a.chunk_id = e.chunk_id AND a.dedup_method_id = e.dedup_method_id
        JOIN kbp.chunk_dedup_decisions b
          ON b.chunk_id = e.related_chunk_id AND b.dedup_method_id = e.dedup_method_id
        WHERE e.dedup_method_id = :method_id AND e.relationship_type = 'NEAR_DUPLICATE'
          AND a.classification <> 'NEAR_DUPLICATE' AND b.classification <> 'NEAR_DUPLICATE'""",
    "decided_chunk_without_occurrence_violations": """
        SELECT count(*) FROM kbp.chunk_dedup_decisions d
        WHERE d.dedup_method_id = :method_id
          AND NOT EXISTS (SELECT 1 FROM kbp.chunk_occurrences o WHERE o.chunk_id = d.chunk_id)""",
    "decisions": "SELECT count(*) FROM kbp.chunk_dedup_decisions WHERE dedup_method_id = :method_id",
    "near_duplicate": """SELECT count(*) FROM kbp.chunk_dedup_decisions
                         WHERE dedup_method_id = :method_id AND classification = 'NEAR_DUPLICATE'""",
    "related": """SELECT count(*) FROM kbp.chunk_dedup_decisions
                  WHERE dedup_method_id = :method_id AND classification = 'RELATED'""",
    "unique": """SELECT count(*) FROM kbp.chunk_dedup_decisions
                 WHERE dedup_method_id = :method_id AND classification = 'UNIQUE'""",
    "relationships": "SELECT count(*) FROM kbp.chunk_relationships WHERE dedup_method_id = :method_id",
    "near_edges": """SELECT count(*) FROM kbp.chunk_relationships
                     WHERE dedup_method_id = :method_id AND relationship_type = 'NEAR_DUPLICATE'""",
    "related_edges": """SELECT count(*) FROM kbp.chunk_relationships
                        WHERE dedup_method_id = :method_id AND relationship_type = 'RELATED'""",
    "chunks_without_decision": """
        SELECT count(*) FROM kbp.chunk_registry c
        WHERE NOT EXISTS (SELECT 1 FROM kbp.chunk_dedup_decisions d
                          WHERE d.chunk_id = c.chunk_id AND d.dedup_method_id = :method_id)""",
    "runs": """SELECT count(DISTINCT run_id) FROM kbp.chunk_dedup_decisions WHERE dedup_method_id = :method_id""",
    "open_dead_letters": """SELECT count(*) FROM kbp.dead_letters
                            WHERE item_type = 'chunk' AND stage = 'CHUNK_DEDUPLICATED' AND resolved_at IS NULL""",
}


@dataclass(frozen=True)
class StoredDecision:
    classification: str
    representative_chunk_id: uuid.UUID | None
    similarity: float | None
    decision_status: str


class ChunkDedupStore(Protocol):
    def method(self, conn: Any) -> tuple[int, dict] | None: ...
    def lock_method(self, conn: Any, method_id: int) -> None: ...
    def pending_chunks(self, conn: Any, method_id: int, after: bytes, limit: int) -> list[ChunkText]: ...
    def resolve_representatives(self, conn: Any, method_id: int, keys: list[int]) -> list[ChunkText]: ...
    def insert_decision(self, conn: Any, method_id: int, chunk_id: uuid.UUID, decision: Decision,
                        run_id: uuid.UUID) -> bool: ...
    def get_decision(self, conn: Any, method_id: int, chunk_id: uuid.UUID) -> StoredDecision | None: ...
    def insert_relationship(self, conn: Any, method_id: int, chunk_id: uuid.UUID, relationship: Relationship,
                            run_id: uuid.UUID) -> bool: ...
    def get_relationship_type(self, conn: Any, method_id: int, pair: tuple[uuid.UUID, uuid.UUID]) -> str | None: ...
    def representative_count(self, conn: Any, method_id: int) -> int: ...
    def representatives_page(self, conn: Any, method_id: int, after: bytes, limit: int) -> list[tuple[bytes, str]]: ...
    def dead_letter(self, conn: Any, *, chunk_id: uuid.UUID, error_code: str, error_message: str,
                    run_id: uuid.UUID | None) -> None: ...


def method_lock_key(method_id: int) -> int:
    """A positive signed-64-bit advisory-lock key for one dedup method (one writer per method at a time)."""
    return METHOD_LOCK_NAMESPACE | (method_id & 0xFFFF)


def _bytes(value: Any) -> bytes:
    return value if isinstance(value, bytes) else bytes(value)


class PostgresChunkDedupStore:
    def method(self, conn: Any) -> tuple[int, dict] | None:
        row = conn.execute(_text(GET_METHOD_SQL), {"algorithm": method.ALGORITHM,
                                                   "version": method.VERSION}).one_or_none()
        return None if row is None else (int(row[0]), dict(row[1]))

    def lock_method(self, conn: Any, method_id: int) -> None:
        conn.execute(_text(ADVISORY_LOCK_SQL), {"key": method_lock_key(method_id)})

    def pending_chunks(self, conn: Any, method_id: int, after: bytes, limit: int) -> list[ChunkText]:
        rows = conn.execute(_text(PENDING_CHUNKS_SQL), {"method_id": method_id, "after": after, "limit": limit})
        return [ChunkText(r[0], _bytes(r[1]), r[2]) for r in rows]

    def resolve_representatives(self, conn: Any, method_id: int, keys: list[int]) -> list[ChunkText]:
        if not keys:
            return []
        ranges = [method.hash_range(k) for k in keys]
        rows = conn.execute(_text(RESOLVE_REPRESENTATIVES_SQL), {
            "lows": [lo for lo, _hi in ranges], "highs": [hi for _lo, hi in ranges], "method_id": method_id})
        return [ChunkText(r[0], _bytes(r[1]), r[2]) for r in rows]

    def insert_decision(self, conn: Any, method_id: int, chunk_id: uuid.UUID, decision: Decision,
                        run_id: uuid.UUID) -> bool:
        rep = decision.representative_chunk_id
        return conn.execute(_text(INSERT_DECISION_SQL), {
            "chunk_id": chunk_id, "method_id": method_id, "classification": decision.classification,
            "representative": str(rep) if rep else None, "similarity": decision.similarity,
            "run_id": run_id}).one_or_none() is not None

    def get_decision(self, conn: Any, method_id: int, chunk_id: uuid.UUID) -> StoredDecision | None:
        row = conn.execute(_text(GET_DECISION_SQL), {"chunk_id": chunk_id, "method_id": method_id}).one_or_none()
        return None if row is None else StoredDecision(row[0], row[1], None if row[2] is None else float(row[2]),
                                                       row[3])

    def insert_relationship(self, conn: Any, method_id: int, chunk_id: uuid.UUID, relationship: Relationship,
                            run_id: uuid.UUID) -> bool:
        low, high = relationship.ordered(chunk_id)
        return conn.execute(_text(INSERT_RELATIONSHIP_SQL), {
            "chunk_id": low, "related_chunk_id": high, "relationship_type": relationship.relationship_type,
            "method_id": method_id, "similarity": relationship.similarity, "run_id": run_id}).one_or_none() is not None

    def get_relationship_type(self, conn: Any, method_id: int, pair: tuple[uuid.UUID, uuid.UUID]) -> str | None:
        return conn.execute(_text(GET_RELATIONSHIP_SQL), {"chunk_id": pair[0], "related_chunk_id": pair[1],
                                                          "method_id": method_id}).scalar_one_or_none()

    def representative_count(self, conn: Any, method_id: int) -> int:
        return int(conn.execute(_text(REPRESENTATIVE_COUNT_SQL), {"method_id": method_id}).scalar_one())

    def representatives_page(self, conn: Any, method_id: int, after: bytes, limit: int) -> list[tuple[bytes, str]]:
        rows = conn.execute(_text(REPRESENTATIVES_PAGE_SQL), {"method_id": method_id, "after": after, "limit": limit})
        return [(_bytes(r[0]), r[1]) for r in rows]

    def dead_letter(self, conn: Any, *, chunk_id: uuid.UUID, error_code: str, error_message: str,
                    run_id: uuid.UUID | None) -> None:
        repositories.record_dead_letter(
            conn, item_type=DeadLetterItemType.CHUNK, item_key=str(chunk_id), stage=Stage.CHUNK_DEDUPLICATED,
            disposition=Disposition.DEAD_LETTER, error_class=ErrorClass.INTERNAL_BUG, error_code=error_code,
            error_message=error_message, attempt_count=1, job_id=None, run_id=run_id, payload_ref=None)


def decisions_in_order(conn: Any, method_id: int) -> list[tuple]:
    return [(r[0], _bytes(r[1]), r[2], r[3], r[4], None if r[5] is None else float(r[5]))
            for r in conn.execute(_text(DECISIONS_IN_ORDER_SQL), {"method_id": method_id})]


def stored_relationships(conn: Any, method_id: int) -> list[tuple]:
    return [tuple(r) for r in conn.execute(_text(RELATIONSHIPS_SQL), {"method_id": method_id})]


def audit(conn: Any, method_id: int) -> dict[str, int]:
    return {name: int(conn.execute(_text(sql), {"method_id": method_id}).scalar_one())
            for name, sql in AUDIT_SQL.items()}

"""Embeddings V1 persistence on Migrations 001A + 001C (shadow vectors; no Gold).

| datum                                   | table                                                        |
|-----------------------------------------|--------------------------------------------------------------|
| frozen model / version / config         | ``embedding_models`` (registered by the migrator bootstrap)  |
| pending work, leases, attempts, errors  | ``embedding_queue`` (transient; the row is DELETEd on success or dead letter) |
| result state ``stored_shadow``          | ``chunk_embeddings`` PK (chunk_id, embedding_model_id)       |
| the vector + its SHA-256                | ``chunk_embedding_vectors`` PK (chunk_id, embedding_model_id), FK to the result row |
| exhausted / invalid / nondeterministic  | ``dead_letters`` (item ``chunk_embedding``, stage ``EMBEDDED``) |

The privileges used by ``kb_pipeline_app`` are:

* INSERT, UPDATE and DELETE on ``embedding_queue``;
* INSERT on ``chunk_embeddings`` and ``chunk_embedding_vectors``;
* SELECT elsewhere.

Stored vectors are never updated or deleted. Every queue transition after the
claim is fenced by ``lease_token``, the Runtime Engine pattern (§16.4).

The only ``public`` names used are pgvector's type and its binary send
function, both schema-qualified: ``public.vector`` and ``public.vector_send``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from app.db import repositories
from app.processing.embedding import model as spec
from app.queue.states import DeadLetterItemType, Disposition, ErrorClass, Stage


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


GET_MODEL_SQL = """
SELECT embedding_model_id, embedding_dimension, vector_type, distance_metric, is_normalized, max_input_tokens,
       config, status
FROM kbp.embedding_models
WHERE model_name = :model_name AND model_revision = :model_revision AND embedding_version = :embedding_version
"""

# Eligible chunk ids come from app.processing.chunk_dedup.eligibility; this inserts only the missing ones.
ENQUEUE_SQL = """
INSERT INTO kbp.embedding_queue (chunk_id, embedding_model_id, status, priority, attempt_count, max_attempts,
                                 next_attempt_at, enqueued_at)
SELECT i.chunk_id, :model_id, 'pending', :priority, 0, :max_attempts, now(), now()
FROM unnest(CAST(:chunk_ids AS uuid[])) AS i(chunk_id)
WHERE NOT EXISTS (SELECT 1 FROM kbp.chunk_embeddings AS e
                  WHERE e.chunk_id = i.chunk_id AND e.embedding_model_id = :model_id)
  AND NOT EXISTS (SELECT 1 FROM kbp.dead_letters AS x
                  WHERE x.item_type = 'chunk_embedding' AND x.stage = 'EMBEDDED' AND x.resolved_at IS NULL
                    AND x.item_key = i.chunk_id::text || ':' || CAST(:model_id AS text))
ON CONFLICT (chunk_id, embedding_model_id) DO NOTHING
RETURNING chunk_id
"""

CLAIM_SQL = """
WITH candidate AS (
    SELECT chunk_id
    FROM kbp.embedding_queue
    WHERE embedding_model_id = :model_id
      AND status IN ('pending', 'retry_pending')
      AND next_attempt_at <= now()
      AND attempt_count < max_attempts
    ORDER BY priority, next_attempt_at, chunk_id
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
)
UPDATE kbp.embedding_queue AS q
SET status = 'running',
    worker_id = :worker_id,
    lease_token = gen_random_uuid(),
    locked_at = now(),
    lease_expires_at = now() + make_interval(secs => CAST(:lease_seconds AS double precision)),
    attempt_count = q.attempt_count + 1
FROM candidate AS c
WHERE q.chunk_id = c.chunk_id AND q.embedding_model_id = :model_id
RETURNING q.chunk_id, q.attempt_count, q.max_attempts, q.lease_token
"""

CHUNK_TEXTS_SQL = """
SELECT chunk_id, chunk_text FROM kbp.chunk_registry WHERE chunk_id = ANY (CAST(:chunk_ids AS uuid[]))
"""

INSERT_RESULT_SQL = """
INSERT INTO kbp.chunk_embeddings (chunk_id, embedding_model_id, status, attempt_count, embedded_at, published_at,
                                  run_id)
VALUES (:chunk_id, :model_id, 'stored_shadow', :attempt_count, now(), NULL, :run_id)
ON CONFLICT (chunk_id, embedding_model_id) DO NOTHING
RETURNING chunk_id
"""

INSERT_VECTOR_SQL = """
INSERT INTO kbp.chunk_embedding_vectors (chunk_id, embedding_model_id, embedding, vector_sha256)
VALUES (:chunk_id, :model_id, CAST(:embedding AS public.vector), :vector_sha256)
ON CONFLICT (chunk_id, embedding_model_id) DO NOTHING
RETURNING chunk_id
"""

# Step 3 of the 001C contract: the result row and the vector exist together, and the database's own
# recomputation of the stored bytes matches the stored hash.
VERIFY_STORED_SQL = """
SELECT e.status, v.vector_sha256, sha256(substring(public.vector_send(v.embedding) FROM 5)) AS recomputed
FROM kbp.chunk_embeddings AS e
LEFT JOIN kbp.chunk_embedding_vectors AS v
  ON v.chunk_id = e.chunk_id AND v.embedding_model_id = e.embedding_model_id
WHERE e.chunk_id = :chunk_id AND e.embedding_model_id = :model_id
"""

COMPLETE_SQL = """
DELETE FROM kbp.embedding_queue
WHERE chunk_id = :chunk_id AND embedding_model_id = :model_id AND lease_token = :lease_token AND status = 'running'
RETURNING chunk_id
"""

RESCHEDULE_SQL = """
UPDATE kbp.embedding_queue
SET status = 'retry_pending',
    attempt_count = attempt_count - CAST(:refund_attempt AS integer),
    next_attempt_at = GREATEST(now() + make_interval(secs => CAST(:delay_seconds AS double precision)),
                               CAST(:not_before AS timestamptz)),
    last_error_class = :error_class,
    last_error_message = :error_message,
    worker_id = NULL, lease_token = NULL, locked_at = NULL, lease_expires_at = NULL
WHERE chunk_id = :chunk_id AND embedding_model_id = :model_id AND lease_token = :lease_token AND status = 'running'
RETURNING next_attempt_at
"""

# Crash recovery: a lease that expired returns the item to retry_pending (the attempt stays consumed).
RECOVER_EXPIRED_SQL = """
UPDATE kbp.embedding_queue
SET status = 'retry_pending',
    next_attempt_at = now(),
    last_error_class = 'lease_expired',
    last_error_message = 'lease expired before completion',
    worker_id = NULL, lease_token = NULL, locked_at = NULL, lease_expires_at = NULL
WHERE embedding_model_id = :model_id AND status = 'running' AND lease_expires_at < now()
RETURNING chunk_id, attempt_count, max_attempts
"""

# Items that can never be claimed again (attempts exhausted) are dead-lettered and removed.
EXHAUSTED_SQL = """
SELECT chunk_id, attempt_count, last_error_class, last_error_message
FROM kbp.embedding_queue
WHERE embedding_model_id = :model_id AND status IN ('pending', 'retry_pending') AND attempt_count >= max_attempts
ORDER BY chunk_id
LIMIT :limit
FOR UPDATE SKIP LOCKED
"""

DELETE_UNLEASED_SQL = """
DELETE FROM kbp.embedding_queue
WHERE chunk_id = :chunk_id AND embedding_model_id = :model_id AND status IN ('pending', 'retry_pending')
RETURNING chunk_id
"""

STATUS_SQL: dict[str, str] = {
    "queue_by_status": """SELECT status, count(*) FROM kbp.embedding_queue WHERE embedding_model_id = :model_id
                          GROUP BY status ORDER BY status""",
    "chunk_embeddings": """SELECT count(*) FROM kbp.chunk_embeddings WHERE embedding_model_id = :model_id""",
    "vectors": """SELECT count(*) FROM kbp.chunk_embedding_vectors WHERE embedding_model_id = :model_id""",
    "result_without_vector": """
        SELECT count(*) FROM kbp.chunk_embeddings e WHERE e.embedding_model_id = :model_id
          AND NOT EXISTS (SELECT 1 FROM kbp.chunk_embedding_vectors v
                          WHERE v.chunk_id = e.chunk_id AND v.embedding_model_id = e.embedding_model_id)""",
    "vector_hash_mismatch": """
        SELECT count(*) FROM kbp.chunk_embedding_vectors WHERE embedding_model_id = :model_id
          AND vector_sha256 <> sha256(substring(public.vector_send(embedding) FROM 5))""",
    "vector_wrong_dimension": """SELECT count(*) FROM kbp.chunk_embedding_vectors
                                 WHERE embedding_model_id = :model_id AND public.vector_dims(embedding) <> 768""",
    "vector_norm_out_of_tolerance": """
        SELECT count(*) FROM kbp.chunk_embedding_vectors WHERE embedding_model_id = :model_id
          AND abs(public.vector_norm(embedding) - 1.0) > 0.001""",
    "embedded_suppressed_near_duplicates": """
        SELECT count(*) FROM kbp.chunk_embeddings e JOIN kbp.chunk_dedup_decisions d ON d.chunk_id = e.chunk_id
        WHERE e.embedding_model_id = :model_id AND d.dedup_method_id = :dedup_method_id
          AND d.classification = 'NEAR_DUPLICATE'""",
    "eligible_without_embedding": """
        SELECT count(*) FROM kbp.chunk_dedup_decisions d
        WHERE d.dedup_method_id = :dedup_method_id AND d.classification <> 'NEAR_DUPLICATE'
          AND d.decision_status = 'confirmed'
          AND NOT EXISTS (SELECT 1 FROM kbp.chunk_embeddings e
                          WHERE e.chunk_id = d.chunk_id AND e.embedding_model_id = :model_id)""",
    "open_dead_letters": """SELECT count(*) FROM kbp.dead_letters
                            WHERE item_type = 'chunk_embedding' AND stage = 'EMBEDDED' AND resolved_at IS NULL""",
}


@dataclass(frozen=True)
class Claim:
    chunk_id: uuid.UUID
    attempt_count: int
    max_attempts: int
    lease_token: uuid.UUID


@dataclass(frozen=True)
class StoredCheck:
    status: str
    vector_sha256: bytes | None
    recomputed_sha256: bytes | None


def dead_letter_key(chunk_id: uuid.UUID, model_id: int) -> str:
    return f"{chunk_id}:{model_id}"


class PostgresEmbeddingStore:
    def model(self, conn: Any) -> dict | None:
        row = conn.execute(_text(GET_MODEL_SQL), {
            "model_name": spec.MODEL_NAME, "model_revision": spec.MODEL_REVISION,
            "embedding_version": spec.EMBEDDING_VERSION}).mappings().one_or_none()
        return None if row is None else dict(row)

    def enqueue(self, conn: Any, model_id: int, chunk_ids: list[uuid.UUID], *, max_attempts: int,
                priority: int = 100) -> int:
        if not chunk_ids:
            return 0
        return len(conn.execute(_text(ENQUEUE_SQL), {"chunk_ids": chunk_ids, "model_id": model_id,
                                                     "max_attempts": max_attempts, "priority": priority}).all())

    def claim(self, conn: Any, model_id: int, *, worker_id: str, limit: int, lease_seconds: int) -> list[Claim]:
        rows = conn.execute(_text(CLAIM_SQL), {"model_id": model_id, "worker_id": worker_id, "limit": limit,
                                               "lease_seconds": lease_seconds})
        return sorted((Claim(r[0], int(r[1]), int(r[2]), r[3]) for r in rows), key=lambda c: c.chunk_id)

    def chunk_texts(self, conn: Any, chunk_ids: list[uuid.UUID]) -> dict[uuid.UUID, str]:
        return {r[0]: r[1] for r in conn.execute(_text(CHUNK_TEXTS_SQL), {"chunk_ids": chunk_ids})}

    def insert_result(self, conn: Any, model_id: int, chunk_id: uuid.UUID, attempt_count: int,
                      run_id: uuid.UUID | None) -> bool:
        return conn.execute(_text(INSERT_RESULT_SQL), {"chunk_id": chunk_id, "model_id": model_id,
                                                       "attempt_count": attempt_count,
                                                       "run_id": run_id}).one_or_none() is not None

    def insert_vector(self, conn: Any, model_id: int, chunk_id: uuid.UUID, literal: str, sha: bytes) -> bool:
        return conn.execute(_text(INSERT_VECTOR_SQL), {"chunk_id": chunk_id, "model_id": model_id,
                                                       "embedding": literal,
                                                       "vector_sha256": sha}).one_or_none() is not None

    def verify_stored(self, conn: Any, model_id: int, chunk_id: uuid.UUID) -> StoredCheck | None:
        row = conn.execute(_text(VERIFY_STORED_SQL), {"chunk_id": chunk_id, "model_id": model_id}).one_or_none()
        if row is None:
            return None
        return StoredCheck(row[0], None if row[1] is None else bytes(row[1]), None if row[2] is None else bytes(row[2]))

    def complete(self, conn: Any, model_id: int, claim: Claim) -> bool:
        return conn.execute(_text(COMPLETE_SQL), {"chunk_id": claim.chunk_id, "model_id": model_id,
                                                  "lease_token": claim.lease_token}).one_or_none() is not None

    def reschedule(self, conn: Any, model_id: int, claim: Claim, *, delay_seconds: float, not_before,
                   refund_attempt: bool, error_class: str, error_message: str | None) -> bool:
        return conn.execute(_text(RESCHEDULE_SQL), {
            "chunk_id": claim.chunk_id, "model_id": model_id, "lease_token": claim.lease_token,
            "delay_seconds": delay_seconds, "not_before": not_before, "refund_attempt": int(refund_attempt),
            "error_class": error_class, "error_message": error_message}).one_or_none() is not None

    def recover_expired(self, conn: Any, model_id: int) -> int:
        return len(conn.execute(_text(RECOVER_EXPIRED_SQL), {"model_id": model_id}).all())

    def exhausted(self, conn: Any, model_id: int, limit: int = 500) -> list[tuple]:
        return [tuple(r) for r in conn.execute(_text(EXHAUSTED_SQL), {"model_id": model_id, "limit": limit})]

    def delete_unleased(self, conn: Any, model_id: int, chunk_id: uuid.UUID) -> bool:
        return conn.execute(_text(DELETE_UNLEASED_SQL), {"chunk_id": chunk_id,
                                                         "model_id": model_id}).one_or_none() is not None

    def dead_letter(self, conn: Any, model_id: int, chunk_id: uuid.UUID, *, error_class: ErrorClass | str,
                    error_code: str, error_message: str | None, attempt_count: int, run_id: uuid.UUID | None,
                    quarantine: bool = False) -> None:
        repositories.record_dead_letter(
            conn, item_type=DeadLetterItemType.CHUNK_EMBEDDING, item_key=dead_letter_key(chunk_id, model_id),
            stage=Stage.EMBEDDED, disposition=Disposition.QUARANTINED if quarantine else Disposition.DEAD_LETTER,
            error_class=error_class, error_code=error_code, error_message=error_message,
            attempt_count=max(1, attempt_count), job_id=None, run_id=run_id, payload_ref=None)


def status(conn: Any, model_id: int, dedup_method_id: int) -> dict:
    out: dict = {}
    for name, sql in STATUS_SQL.items():
        result = conn.execute(_text(sql), {"model_id": model_id, "dedup_method_id": dedup_method_id})
        out[name] = dict(result.all()) if name == "queue_by_status" else int(result.scalar_one())
    return out

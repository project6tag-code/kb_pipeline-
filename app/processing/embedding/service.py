"""Embeddings V1: eligibility-driven enqueue and the bounded MPNet worker (shadow vectors only; no Gold).

**Enqueue.** Chunk ids come ONLY from ``app.processing.chunk_dedup.eligibility.eligible_chunks``:
confirmed decisions that are not NEAR_DUPLICATE, i.e. representatives.
Suppressed near-duplicates are never enqueued; they resolve to their
representative through ``eligibility.eligibility(...).embed_chunk_id``. Only
chunks with no ``chunk_embeddings`` row for this model, no queue row and no
open dead letter are inserted, ``ON CONFLICT DO NOTHING``. The run is
keyset-paged, bounded and rerunnable.

**Worker.** One loop iteration does the following:

1. Recover expired leases (crash safety) and dead-letter items whose attempts
   are exhausted.
2. Claim up to ``claim_batch`` items (``FOR UPDATE SKIP LOCKED``, a fresh
   ``lease_token``, ``attempt_count + 1``) and commit, so the leases are
   visible.
3. Read ``chunk_registry.chunk_text`` and encode the batch OUTSIDE any
   transaction.
4. Validate every vector: 768 dimensions, finite, unit norm.
5. Per item, run the Migration 001C write contract in ONE transaction:
   1. INSERT ``chunk_embeddings`` (``stored_shadow``) ``ON CONFLICT DO NOTHING``;
   2. INSERT ``chunk_embedding_vectors`` ``ON CONFLICT DO NOTHING``;
   3. verify that the result row and the vector both exist, that the stored
      hash equals the computed hash, and that the database's recomputation of
      the stored bytes equals the computed hash;
   4. lease-fenced DELETE of the queue row; 0 rows means the lease was lost,
      so the transaction is rolled back.

   A different stored hash is nondeterminism. That transaction rolls back,
   then a separate lease-fenced transaction dead-letters the item
   (``internal_bug``) and removes its queue row. Nothing is ever overwritten.

Failures go through the Runtime Engine retry controller (``app.queue.retry.decide``):

* transient, resource_exhausted, internal_bug and lease_expired → retry with
  full-jitter exponential backoff until ``max_attempts``, then dead letter;
* data_invalid → dead letter at once.

SIGINT/SIGTERM set ``stop_event``: the current batch finishes, then the loop
stops.
"""

from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from app.processing.chunk_dedup import eligibility
from app.processing.embedding import model as spec
from app.processing.embedding.embedder import Embedder
from app.processing.embedding.repository import Claim, PostgresEmbeddingStore
from app.processing.embedding.validator import CheckedVector, VectorInvalid, as_float32_matrix, check
from app.queue.retry import DecisionKind, Failure, JobFailure, RetryPolicy, classify_exception, decide
from app.queue.states import ErrorClass

log = logging.getLogger(__name__)

DEFAULT_POLICY = RetryPolicy(max_attempts=5, base_delay_seconds=10.0, max_delay_seconds=600.0)


class LeaseLost(RuntimeError):
    pass


class Nondeterministic(RuntimeError):
    pass


class IncoherentState(RuntimeError):
    pass


def resolve_model(store: PostgresEmbeddingStore, conn: Any) -> int:
    """The registered V1 model row; its frozen fields must equal the code's spec."""
    row = store.model(conn)
    if row is None:
        raise JobFailure(ErrorClass.INTERNAL_BUG, f"embedding model {spec.MODEL_NAME}@{spec.MODEL_REVISION} "
                         f"({spec.EMBEDDING_VERSION}) is not registered (run: python -m "
                         "app.processing.embedding.cli register-model --expect-database <db>)",
                         code="embedding_model_not_registered")
    expected = spec.registration_row()
    stored = {"embedding_dimension": row["embedding_dimension"], "vector_type": row["vector_type"],
              "distance_metric": row["distance_metric"], "is_normalized": row["is_normalized"],
              "max_input_tokens": row["max_input_tokens"], "config": dict(row["config"])}
    if stored != {k: expected[k] for k in stored}:
        raise JobFailure(ErrorClass.INTERNAL_BUG, "embedding model is registered with a different frozen config",
                         code="embedding_model_mismatch")
    if row["status"] == "retired":
        raise JobFailure(ErrorClass.INTERNAL_BUG, "embedding model is retired", code="embedding_model_retired")
    return int(row["embedding_model_id"])


# --------------------------------------------------------------------------- #
# Enqueue / backfill
# --------------------------------------------------------------------------- #

@dataclass
class EnqueueReport:
    model_id: int = 0
    scanned_eligible: int = 0
    enqueued: int = 0
    batches: int = 0
    stopped: bool = False


def enqueue_eligible(transaction: Callable[[], Any], store: PostgresEmbeddingStore, *, model_id: int,
                     dedup_method_id: int, limit: int, batch_size: int = 500,
                     max_attempts: int = DEFAULT_POLICY.max_attempts,
                     stop_event: threading.Event | None = None) -> EnqueueReport:
    if not 1 <= batch_size <= 5000 or limit < 1:
        raise ValueError("batch_size must be 1..5000 and limit >= 1")
    report = EnqueueReport(model_id=model_id)
    after: uuid.UUID | None = None
    while report.scanned_eligible < limit:
        if stop_event is not None and stop_event.is_set():
            report.stopped = True
            break
        size = min(batch_size, limit - report.scanned_eligible)
        with transaction() as conn:
            ids = eligibility.eligible_chunks(conn, dedup_method_id, after=after, limit=size)
            report.enqueued += store.enqueue(conn, model_id, ids, max_attempts=max_attempts)
        if not ids:
            break
        after = ids[-1]
        report.scanned_eligible += len(ids)
        report.batches += 1
    return report


# --------------------------------------------------------------------------- #
# Worker
# --------------------------------------------------------------------------- #

@dataclass
class WorkerReport:
    model_id: int = 0
    device: str = ""
    claimed: int = 0
    batches: int = 0
    encode_seconds: float = 0.0
    stopped: bool = False
    counts: Counter = field(default_factory=Counter)

    def summary(self) -> dict:
        per_chunk = self.encode_seconds / self.counts["embedded_new"] if self.counts["embedded_new"] else None
        return {"model_id": self.model_id, "device": self.device, "claimed": self.claimed, "batches": self.batches,
                "encode_seconds": round(self.encode_seconds, 4),
                "encode_ms_per_new_chunk": round(per_chunk * 1000, 3) if per_chunk else None,
                "stopped": self.stopped, "counts": dict(sorted(self.counts.items()))}


def _classify(exc: BaseException) -> Failure:
    if isinstance(exc, VectorInvalid):
        return Failure(ErrorClass.DATA_INVALID, "embedding_invalid_vector", str(exc))
    name = type(exc).__name__
    if "OutOfMemory" in name:
        return Failure(ErrorClass.RESOURCE_EXHAUSTED, name, str(exc)[:500])
    try:
        from sqlalchemy.exc import DBAPIError, OperationalError

        if isinstance(exc, OperationalError) or (isinstance(exc, DBAPIError) and exc.connection_invalidated):
            return Failure(ErrorClass.TRANSIENT, name, str(exc)[:500])
    except ImportError:  # pragma: no cover
        pass
    return classify_exception(exc)


class EmbeddingWorker:
    def __init__(self, store: PostgresEmbeddingStore, embedder: Embedder, *, model_id: int,
                 run_id: uuid.UUID | None, worker_id: str, policy: RetryPolicy = DEFAULT_POLICY,
                 lease_seconds: int = 300, encode_batch_size: int = 16, rng: random.Random | None = None,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        if not 1 <= encode_batch_size <= 256:
            raise ValueError("encode_batch_size must be 1..256")
        self.store, self.embedder, self.model_id, self.run_id = store, embedder, model_id, run_id
        self.worker_id, self.policy, self.lease_seconds = worker_id, policy, lease_seconds
        self.encode_batch_size = encode_batch_size
        self.rng = rng or random.Random()
        self.sleep = sleep

    def run(self, transaction: Callable[[], Any], *, limit: int, claim_batch: int = 32,
            stop_event: threading.Event | None = None) -> WorkerReport:
        report = WorkerReport(model_id=self.model_id, device=getattr(self.embedder, "device", "?"))
        while report.claimed < limit:
            if stop_event is not None and stop_event.is_set():
                report.stopped = True
                break
            self.housekeeping(transaction, report.counts)
            with transaction() as conn:
                claims = self.store.claim(conn, self.model_id, worker_id=self.worker_id,
                                          limit=min(claim_batch, limit - report.claimed),
                                          lease_seconds=self.lease_seconds)
            if not claims:
                break
            report.claimed += len(claims)
            report.batches += 1
            self.process(transaction, claims, report)
        return report

    def housekeeping(self, transaction: Callable[[], Any], counts: Counter) -> None:
        with transaction() as conn:
            counts["lease_recovered"] += self.store.recover_expired(conn, self.model_id)
            for chunk_id, attempts, error_class, message in self.store.exhausted(conn, self.model_id):
                self.store.dead_letter(conn, self.model_id, chunk_id, error_class=error_class or "internal_bug",
                                       error_code="embedding_attempts_exhausted", error_message=message,
                                       attempt_count=attempts, run_id=self.run_id)
                self.store.delete_unleased(conn, self.model_id, chunk_id)
                counts["dead_lettered"] += 1

    def process(self, transaction: Callable[[], Any], claims: list[Claim], report: WorkerReport) -> None:
        with transaction() as conn:
            texts = self.store.chunk_texts(conn, [c.chunk_id for c in claims])
        missing = [c for c in claims if not texts.get(c.chunk_id)]
        for claim in missing:
            self.fail(transaction, claim, Failure(ErrorClass.DATA_INVALID, "embedding_no_chunk_text",
                                                  "chunk_registry has no text for this chunk"), report.counts)
        claims = [c for c in claims if texts.get(c.chunk_id)]
        if not claims:
            return
        started = time.perf_counter()
        try:
            matrix = as_float32_matrix(self.embedder.encode([texts[c.chunk_id] for c in claims],
                                                            self.encode_batch_size), len(claims))
        except Exception as exc:  # the whole batch failed to encode
            failure = _classify(exc)
            for claim in claims:
                self.fail(transaction, claim, failure, report.counts)
            return
        finally:
            report.encode_seconds += time.perf_counter() - started
        for claim, row in zip(claims, matrix):
            try:
                checked = check(row)
            except VectorInvalid as exc:
                self.fail(transaction, claim, _classify(exc), report.counts)
                continue
            self.persist(transaction, claim, checked, report.counts)

    def persist(self, transaction: Callable[[], Any], claim: Claim, checked: CheckedVector, counts: Counter) -> None:
        try:
            with transaction() as conn:
                counts[self.write(conn, claim, checked)] += 1
        except LeaseLost:
            counts["lease_lost"] += 1  # another worker owns it now; nothing was written
        except Nondeterministic as exc:
            with transaction() as conn:
                self.store.dead_letter(conn, self.model_id, claim.chunk_id, error_class=ErrorClass.INTERNAL_BUG,
                                       error_code="embedding_nondeterministic", error_message=str(exc),
                                       attempt_count=claim.attempt_count, run_id=self.run_id)
                removed = self.store.complete(conn, self.model_id, claim)
            counts["nondeterministic_dead_lettered" if removed else "lease_lost"] += 1
        except Exception as exc:
            self.fail(transaction, claim, _classify(exc), counts)

    def write(self, conn: Any, claim: Claim, checked: CheckedVector) -> str:
        """The Migration 001C write contract for one item (the caller owns the transaction)."""
        new_result = self.store.insert_result(conn, self.model_id, claim.chunk_id, claim.attempt_count, self.run_id)
        new_vector = self.store.insert_vector(conn, self.model_id, claim.chunk_id, checked.literal(), checked.sha256)
        stored = self.store.verify_stored(conn, self.model_id, claim.chunk_id)
        if stored is None or stored.vector_sha256 is None:
            raise IncoherentState(f"chunk {claim.chunk_id}: result row and vector are not both present")
        if stored.status != "stored_shadow":
            raise IncoherentState(f"chunk {claim.chunk_id}: result status {stored.status!r}")
        if stored.vector_sha256 != checked.sha256:
            raise Nondeterministic(f"chunk {claim.chunk_id}: stored vector_sha256 {stored.vector_sha256.hex()[:16]} "
                                   f"!= recomputed {checked.sha256.hex()[:16]}")
        if stored.recomputed_sha256 != checked.sha256:
            raise IncoherentState(f"chunk {claim.chunk_id}: stored vector bytes do not hash to vector_sha256")
        if not self.store.complete(conn, self.model_id, claim):
            raise LeaseLost(f"lease on chunk {claim.chunk_id} was lost")
        if new_vector != new_result:
            raise IncoherentState(f"chunk {claim.chunk_id}: result/vector insert mismatch")
        return "embedded_new" if new_vector else "already_embedded_same_hash"

    def fail(self, transaction: Callable[[], Any], claim: Claim, failure: Failure, counts: Counter) -> None:
        decision = decide(self.policy, failure, claim.attempt_count, claim.max_attempts, self.rng)
        with transaction() as conn:
            if decision.kind in (DecisionKind.RETRY, DecisionKind.DEFER):
                ok = self.store.reschedule(conn, self.model_id, claim, delay_seconds=decision.delay_seconds,
                                           not_before=decision.not_before,
                                           refund_attempt=not decision.consumes_attempt,
                                           error_class=failure.error_class.value, error_message=failure.message)
                counts["retry_scheduled" if ok else "lease_lost"] += 1
            else:
                self.store.dead_letter(conn, self.model_id, claim.chunk_id, error_class=failure.error_class,
                                       error_code=failure.code or "embedding_failed", error_message=failure.message,
                                       attempt_count=claim.attempt_count, run_id=self.run_id,
                                       quarantine=decision.kind is DecisionKind.QUARANTINE)
                removed = self.store.complete(conn, self.model_id, claim)
                counts["dead_lettered" if removed else "lease_lost"] += 1
        log.warning("embedding of chunk %s failed (%s): %s", claim.chunk_id, failure.error_class.value,
                    decision.kind.value)

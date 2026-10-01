"""Offline fixtures for Embeddings V1 tests (no database, no torch).

``InMemoryEmbeddingStore`` mirrors ``PostgresEmbeddingStore`` with the
Migration 001A/001C rules that matter:

* ``embedding_queue``: PK (chunk, model), status pending/running/retry_pending,
  lease-token fencing, ``attempt_count < max_attempts`` to claim;
* ``chunk_embeddings``: PK, insert-once;
* ``chunk_embedding_vectors``: PK, composite FK to the result row, a 32-byte
  hash and exactly 768 dimensions, insert-once (never updated);
* dead letters keyed by ``(item_type, item_key, stage)``.

``transaction()`` is atomic: any exception restores every table. A fake clock
drives ``now()``, and failures can be injected into any store call.
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import threading
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np

from app.processing.embedding.repository import Claim, StoredCheck, dead_letter_key
from app.processing.embedding.validator import DIMENSION, from_literal, vector_sha256


class InjectedFailure(ConnectionError):
    pass


class FakeEmbedder:
    """Deterministic unit vectors derived from the text; faults are injectable."""

    device = "fake"

    def __init__(self) -> None:
        self.calls = 0
        self.fail_next: BaseException | None = None
        self.corrupt: dict[str, str] = {}  # text -> "nan" | "inf" | "short" | "unnormalized" | "shifted"

    def vector(self, text: str) -> np.ndarray:
        seed = int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")
        values = np.random.default_rng(seed).standard_normal(DIMENSION)
        return (values / np.linalg.norm(values)).astype(np.float32)

    def encode(self, texts: list[str], batch_size: int) -> np.ndarray:
        self.calls += 1
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        rows = []
        for text in texts:
            vector = self.vector(text)
            mode = self.corrupt.get(text)
            if mode == "nan":
                vector[3] = np.nan
            elif mode == "inf":
                vector[3] = np.inf
            elif mode == "unnormalized":
                vector = vector * 2
            elif mode == "shifted":
                vector = vector.copy()
                vector[0] = np.nextafter(vector[0], np.float32(1))
            rows.append(vector if mode != "short" else vector[:767])
        if any(len(r) != DIMENSION for r in rows):
            return np.array(rows, dtype=object)
        return np.stack(rows)


class InMemoryEmbeddingStore:
    _TABLES = ("queue", "results", "vectors", "dead_letters")

    def __init__(self, chunks: dict[uuid.UUID, str], *, model_id: int = 2) -> None:
        self.chunks = chunks
        self.model_id = model_id
        self.queue: dict[uuid.UUID, dict] = {}
        self.results: dict[uuid.UUID, dict] = {}
        self.vectors: dict[uuid.UUID, dict] = {}
        self.dead_letters: dict[str, dict] = {}
        self.now = datetime(2026, 10, 1, tzinfo=timezone.utc)
        self.lock = threading.RLock()
        self.fail: dict[str, int] = {}
        self.calls: dict[str, int] = {}

    # helpers
    def _tick(self, name: str) -> None:
        self.calls[name] = self.calls.get(name, 0) + 1
        if self.fail.get(name) == self.calls[name]:
            raise InjectedFailure(f"injected failure in {name}")

    @contextlib.contextmanager
    def transaction(self):
        with self.lock:
            saved = {name: copy.deepcopy(getattr(self, name)) for name in self._TABLES}
            try:
                yield None
            except BaseException:
                for name, value in saved.items():
                    setattr(self, name, value)
                raise

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)

    def state(self) -> tuple:
        return (sorted((str(k), v["status"]) for k, v in self.results.items()),
                sorted((str(k), v["sha"].hex()) for k, v in self.vectors.items()),
                sorted(self.dead_letters), len(self.queue))

    # protocol (mirrors PostgresEmbeddingStore)
    def enqueue(self, conn, model_id, chunk_ids, *, max_attempts, priority=100) -> int:
        count = 0
        for chunk_id in chunk_ids:
            if chunk_id in self.results or chunk_id in self.queue:
                continue
            if dead_letter_key(chunk_id, model_id) in self.dead_letters:
                continue
            self.queue[chunk_id] = {"status": "pending", "attempt_count": 0, "max_attempts": max_attempts,
                                    "next_attempt_at": self.now, "lease_token": None, "lease_expires_at": None,
                                    "priority": priority, "last_error_class": None, "last_error_message": None}
            count += 1
        return count

    def claim(self, conn, model_id, *, worker_id, limit, lease_seconds):
        ready = sorted((k for k, q in self.queue.items() if q["status"] in ("pending", "retry_pending")
                        and q["next_attempt_at"] <= self.now and q["attempt_count"] < q["max_attempts"]),
                       key=lambda k: (self.queue[k]["priority"], self.queue[k]["next_attempt_at"], k))[:limit]
        claims = []
        for chunk_id in ready:
            q = self.queue[chunk_id]
            q.update(status="running", lease_token=uuid.uuid4(), worker_id=worker_id,
                     lease_expires_at=self.now + timedelta(seconds=lease_seconds),
                     attempt_count=q["attempt_count"] + 1)
            claims.append(Claim(chunk_id, q["attempt_count"], q["max_attempts"], q["lease_token"]))
        return claims

    def chunk_texts(self, conn, chunk_ids):
        return {c: self.chunks[c] for c in chunk_ids if c in self.chunks}

    def insert_result(self, conn, model_id, chunk_id, attempt_count, run_id) -> bool:
        self._tick("insert_result")
        assert chunk_id in self.chunks and attempt_count > 0
        if chunk_id in self.results:
            return False
        self.results[chunk_id] = {"status": "stored_shadow", "attempt_count": attempt_count, "run_id": run_id}
        return True

    def insert_vector(self, conn, model_id, chunk_id, literal, sha) -> bool:
        self._tick("insert_vector")
        assert chunk_id in self.results, "FK chunk_embedding_vectors -> chunk_embeddings"
        assert len(sha) == 32, "CHECK octet_length(vector_sha256) = 32"
        values = from_literal(literal)
        assert values.shape == (DIMENSION,), "vector(768)"
        if chunk_id in self.vectors:
            return False
        self.vectors[chunk_id] = {"values": values, "sha": sha}
        return True

    def verify_stored(self, conn, model_id, chunk_id):
        if chunk_id not in self.results:
            return None
        vector = self.vectors.get(chunk_id)
        return StoredCheck(self.results[chunk_id]["status"], vector and vector["sha"],
                           vector and vector_sha256(vector["values"]))

    def complete(self, conn, model_id, claim) -> bool:
        self._tick("complete")
        q = self.queue.get(claim.chunk_id)
        if q is None or q["status"] != "running" or q["lease_token"] != claim.lease_token:
            return False
        del self.queue[claim.chunk_id]
        return True

    def reschedule(self, conn, model_id, claim, *, delay_seconds, not_before, refund_attempt, error_class,
                   error_message) -> bool:
        q = self.queue.get(claim.chunk_id)
        if q is None or q["status"] != "running" or q["lease_token"] != claim.lease_token:
            return False
        q.update(status="retry_pending", attempt_count=q["attempt_count"] - int(refund_attempt),
                 next_attempt_at=self.now + timedelta(seconds=delay_seconds), last_error_class=error_class,
                 last_error_message=error_message, lease_token=None, lease_expires_at=None)
        return True

    def recover_expired(self, conn, model_id) -> int:
        expired = [k for k, q in self.queue.items() if q["status"] == "running" and q["lease_expires_at"] < self.now]
        for k in expired:
            self.queue[k].update(status="retry_pending", next_attempt_at=self.now, lease_token=None,
                                 lease_expires_at=None, last_error_class="lease_expired")
        return len(expired)

    def exhausted(self, conn, model_id, limit=500):
        return [(k, q["attempt_count"], q["last_error_class"], q["last_error_message"])
                for k, q in sorted(self.queue.items())
                if q["status"] in ("pending", "retry_pending") and q["attempt_count"] >= q["max_attempts"]][:limit]

    def delete_unleased(self, conn, model_id, chunk_id) -> bool:
        q = self.queue.get(chunk_id)
        if q is None or q["status"] == "running":
            return False
        del self.queue[chunk_id]
        return True

    def dead_letter(self, conn, model_id, chunk_id, *, error_class, error_code, error_message, attempt_count, run_id,
                    quarantine=False) -> None:
        self.dead_letters[dead_letter_key(chunk_id, model_id)] = {
            "error_class": str(getattr(error_class, "value", error_class)), "error_code": error_code,
            "attempt_count": attempt_count, "quarantine": quarantine}


class FakeEligibility:
    """Stand-in for app.processing.chunk_dedup.eligibility over fixed decisions (keyset by chunk_id)."""

    def __init__(self, representatives: list[uuid.UUID], suppressed: dict[uuid.UUID, uuid.UUID]) -> None:
        self.representatives = sorted(representatives)
        self.suppressed = suppressed
        self.calls = 0

    def eligible_chunks(self, conn, method_id, *, after=None, limit=1000):
        self.calls += 1
        return [c for c in self.representatives if after is None or c > after][:limit]

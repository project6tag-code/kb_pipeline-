"""Global chunk near-dedup: per-chunk flow, bounded backfill, index rebuild and replay audit.

Per chunk C, inside the batch transaction, which holds the method's advisory
lock:

1. comparison features, fact signature and the 10 LSH band keys of C;
2. bounded candidate lookup in the side index: at most 256 rows per bucket and
   at most 64 candidates. Hot-bucket metrics are recorded;
3. the candidates are resolved in PostgreSQL to chunks that hold a confirmed,
   non-NEAR_DUPLICATE decision under this method, i.e. representatives. Stale
   or colliding index keys drop out here;
4. the exact verifier and fact guard decide (``verify.decide``);
5. the decision is INSERTed ``ON CONFLICT DO NOTHING``, with the
   representative invariant enforced in the INSERT itself. A conflict must
   equal the recomputed decision; a difference becomes an ``internal_bug``
   dead letter, never an overwrite;
6. NEAR_DUPLICATE / RELATED edges are INSERTed ``ON CONFLICT DO NOTHING``;
7. if C is not NEAR_DUPLICATE it is now a representative, and its postings are
   added to the side index BEFORE the commit. If the batch rolls back, the
   items it added are removed.

**Processing order.** Pending chunks run in ``chunk_hash`` order within a
run, and a batch sees every representative committed before it. The first
chunk of a near-duplicate cluster in that order becomes the representative.
Later arrivals map to it, and a decision is never rewritten (immutable per
method version).
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from app.processing.chunk_dedup import method
from app.processing.chunk_dedup.index import (CandidateIndex, IndexIdentity, IndexIncomplete, IndexMetrics,
                                              build_index)
from app.processing.chunk_dedup.repository import ChunkDedupStore
from app.processing.chunk_dedup.verify import NEAR_DUPLICATE, ChunkText, decide
from app.queue.retry import JobFailure
from app.queue.states import ErrorClass

log = logging.getLogger(__name__)


class ChunkDedupError(RuntimeError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def resolve_method(store: ChunkDedupStore, conn: Any) -> int:
    found = store.method(conn)
    if found is None:
        raise JobFailure(ErrorClass.INTERNAL_BUG, f"dedup method {method.ALGORITHM}:{method.VERSION} is not "
                         "registered (run: python -m app.processing.chunk_dedup.cli register-method "
                         "--expect-database <db>)", code="chunk_dedup_method_not_registered")
    method_id, parameters = found
    if parameters != method.PARAMETERS:
        raise JobFailure(ErrorClass.INTERNAL_BUG, f"dedup method {method.ALGORITHM}:{method.VERSION} is registered "
                         "with different parameters", code="chunk_dedup_method_mismatch")
    return method_id


class ChunkNearDeduplicator:
    def __init__(self, store: ChunkDedupStore, index: CandidateIndex, *, run_id: uuid.UUID,
                 metrics: IndexMetrics | None = None, min_free_bytes: int = 0) -> None:
        self.store, self.index, self.run_id = store, index, run_id
        self.metrics = metrics or IndexMetrics()
        self.min_free_bytes = min_free_bytes
        self._method_id: int | None = None

    def method_id(self, conn: Any) -> int:
        if self._method_id is None:
            self._method_id = resolve_method(self.store, conn)
            if self.index.identity.dedup_method_id != self._method_id:
                raise ChunkDedupError("candidate index identity does not match the registered method",
                                      code="chunk_dedup_index_incompatible")
        return self._method_id

    def ensure_complete(self, conn: Any) -> dict:
        """Every committed representative must be in the index (else rebuild). Stale extras are harmless."""
        method_id = self.method_id(conn)
        representatives, items = self.store.representative_count(conn, method_id), self.index.item_count()
        if items < representatives:
            raise IndexIncomplete(f"candidate index holds {items} items but PostgreSQL has {representatives} "
                                  "representatives (run: python -m app.processing.chunk_dedup.cli rebuild-index)")
        return {"representatives": representatives, "index_items": items, "stale_items": items - representatives}

    def process_batch(self, conn: Any, *, after: bytes, limit: int,
                      added: list[int]) -> tuple[list[ChunkText], Counter]:
        method_id = self.method_id(conn)
        self.store.lock_method(conn, method_id)  # one writer per method; sees every committed representative
        self.index.ensure_free_space(self.min_free_bytes)
        chunks = self.store.pending_chunks(conn, method_id, after, limit)
        counts: Counter = Counter()
        for chunk in chunks:
            counts.update(self.process_chunk(conn, method_id, chunk, added))
        self.metrics.check()  # degraded: raise before commit, so this batch's decisions are not persisted
        return chunks, counts

    def process_chunk(self, conn: Any, method_id: int, chunk: ChunkText, added: list[int]) -> Counter:
        counts: Counter = Counter()
        current = chunk.comparison()
        keys = method.band_keys(current.features)
        own = method.item_key(chunk.chunk_hash)
        item_keys, stats = self.index.candidates(keys)
        self.metrics.record(stats)
        candidates = [r.comparison() for r in self.store.resolve_representatives(conn, method_id, item_keys)
                      if r.chunk_id != chunk.chunk_id]
        decision = decide(current, candidates)
        counts["fact_guard_blocked_pairs"] += decision.fact_blocked
        if not self.store.insert_decision(conn, method_id, chunk.chunk_id, decision, self.run_id):
            stored = self.store.get_decision(conn, method_id, chunk.chunk_id)
            if stored is None:
                raise ChunkDedupError(f"representative {decision.representative_chunk_id} of chunk {chunk.chunk_id} "
                                      "is not a confirmed representative", code="chunk_dedup_representative_invalid")
            if decision.same_decision(stored.classification, stored.representative_chunk_id, stored.similarity):
                counts["chunk_already_decided"] += 1
                if stored.classification != NEAR_DUPLICATE:
                    self._index(own, keys, added)  # repair: a representative must be indexed
            else:
                self.store.dead_letter(conn, chunk_id=chunk.chunk_id, error_code="chunk_dedup_nondeterministic",
                                       error_message=f"{method.ALGORITHM}:{method.VERSION}: recomputed "
                                       f"{decision.classification}/{decision.representative_chunk_id} differs from "
                                       f"stored {stored.classification}/{stored.representative_chunk_id}",
                                       run_id=self.run_id)
                counts["chunk_decision_conflict"] += 1
            return counts
        for relationship in decision.relationships:
            if self.store.insert_relationship(conn, method_id, chunk.chunk_id, relationship, self.run_id):
                counts["relationship_new"] += 1
                continue
            existing = self.store.get_relationship_type(conn, method_id, relationship.ordered(chunk.chunk_id))
            if existing != relationship.relationship_type:
                raise ChunkDedupError(f"contradictory relationship for {relationship.ordered(chunk.chunk_id)}: "
                                      f"{existing} vs {relationship.relationship_type}",
                                      code="chunk_dedup_relationship_conflict")
            counts["relationship_existing"] += 1
        if decision.classification != NEAR_DUPLICATE:
            self._index(own, keys, added)
        counts[f"decision_{decision.classification.lower()}"] += 1
        return counts

    def _index(self, own: int, keys: tuple[int, ...], added: list[int]) -> None:
        if keys and self.index.add(own, keys):
            added.append(own)


# --------------------------------------------------------------------------- #
# Backfill / incremental processing
# --------------------------------------------------------------------------- #

@dataclass
class BackfillReport:
    method: str = f"{method.ALGORITHM}:{method.VERSION}"
    run_id: str = ""
    chunks: int = 0
    batches: int = 0
    retries: int = 0
    stopped: bool = False
    completeness: dict = field(default_factory=dict)
    counts: Counter = field(default_factory=Counter)

    def summary(self, metrics: IndexMetrics) -> dict:
        return {"method": self.method, "run_id": self.run_id, "chunks": self.chunks, "batches": self.batches,
                "retries": self.retries, "stopped": self.stopped, "completeness": self.completeness,
                "counts": dict(sorted(self.counts.items())), "index_metrics": metrics.summary()}


def _transient(exc: BaseException) -> bool:
    try:
        from sqlalchemy.exc import DBAPIError, OperationalError
    except ImportError:  # pragma: no cover
        return False
    return isinstance(exc, OperationalError) or (isinstance(exc, DBAPIError) and exc.connection_invalidated)


def run_backfill(transaction: Callable[[], Any], dedup: ChunkNearDeduplicator, *, limit: int, batch_size: int = 100,
                 max_attempts: int = 3, stop_event: threading.Event | None = None,
                 sleep: Callable[[float], None] = time.sleep,
                 is_transient: Callable[[BaseException], bool] = _transient,
                 progress: Callable[[BackfillReport], None] | None = None) -> BackfillReport:
    """Decide up to ``limit`` chunks without a decision for this method, ``batch_size`` per transaction.

    The same command is the incremental path: new chunks are simply the next
    pending set. It is resumable (the pending set is derived from PostgreSQL),
    rerunnable, bounded and interruptible between batches.
    """
    if not 1 <= batch_size <= 1000 or limit < 1:
        raise ValueError("batch_size must be 1..1000 and limit >= 1")
    report = BackfillReport(run_id=str(dedup.run_id))
    with transaction() as conn:
        report.completeness = dedup.ensure_complete(conn)
    after = b""
    while report.chunks < limit:
        if stop_event is not None and stop_event.is_set():
            report.stopped = True
            break
        size = min(batch_size, limit - report.chunks)
        for attempt in range(1, max_attempts + 1):
            added: list[int] = []
            try:
                with transaction() as conn:
                    chunks, batch = dedup.process_batch(conn, after=after, limit=size, added=added)
                break
            except Exception as exc:
                dedup.index.remove(added)  # the batch rolled back: its new representatives are not real
                if attempt == max_attempts or not is_transient(exc):
                    raise
                report.retries += 1
                log.warning("chunk near-dedup batch failed transiently (attempt %s): %s", attempt, type(exc).__name__)
                sleep(min(30.0, 2.0 ** attempt))
        if not chunks:
            break
        after = chunks[-1].chunk_hash
        report.chunks += len(chunks)
        report.batches += 1
        report.counts.update(batch)
        if progress is not None:
            progress(report)
    return report


# --------------------------------------------------------------------------- #
# Rebuild and replay audit
# --------------------------------------------------------------------------- #

def rebuild_index(transaction: Callable[[], Any], store: ChunkDedupStore, path: Any, *, database: str,
                  page: int = 5_000) -> dict:
    """Rebuild the side index from PostgreSQL under the method lock (writers wait), then swap it in atomically."""
    with transaction() as conn:
        method_id = resolve_method(store, conn)
        store.lock_method(conn, method_id)

        def representatives():
            after = b""
            while True:
                rows = store.representatives_page(conn, method_id, after, page)
                if not rows:
                    return
                yield from rows
                after = rows[-1][0]

        result = build_index(path, IndexIdentity(database, method_id), representatives())
        result["pg_representatives"] = store.representative_count(conn, method_id)
    return result


def replay(decisions: list[tuple], relationships: list[tuple], *, max_decisions: int = 2_000_000) -> dict:
    """Re-derive every decision from scratch, in the original processing order, and compare.

    ``decisions`` are (chunk_id, chunk_hash, chunk_text, classification, representative, similarity) in decided
    order. ``relationships`` are the stored (chunk_id, related_chunk_id, type, similarity) rows.

    This proves the decisions are a deterministic function of the chunk set,
    the processing order and the method, independent of any side-index file.
    It keeps representatives in memory, so it is an audit tool bounded by
    ``max_decisions``, not a production path.
    """
    if len(decisions) > max_decisions:
        raise ValueError(f"replay is bounded to {max_decisions} decisions")
    index = CandidateIndex(":memory:", IndexIdentity("replay", 0))
    by_key: dict[int, list] = {}
    decision_mismatches, expected_edges = [], set()
    try:
        for chunk_id, chunk_hash, chunk_text, classification, representative, similarity in decisions:
            chunk = ChunkText(chunk_id, bytes(chunk_hash), chunk_text)
            current = chunk.comparison()
            keys = method.band_keys(current.features)
            item_keys, _stats = index.candidates(keys)
            candidates = [c for key in item_keys for c in by_key.get(key, ()) if c.chunk_id != chunk_id]
            decision = decide(current, candidates)
            if not decision.same_decision(classification, representative,
                                          None if similarity is None else float(similarity)):
                decision_mismatches.append({"chunk_id": str(chunk_id), "stored": [classification, str(representative)],
                                            "replayed": [decision.classification,
                                                         str(decision.representative_chunk_id)]})
            for rel in decision.relationships:
                low, high = rel.ordered(chunk_id)
                expected_edges.add((low, high, rel.relationship_type))
            if decision.classification != NEAR_DUPLICATE:
                own = method.item_key(chunk.chunk_hash)
                index.add(own, keys)
                by_key.setdefault(own, []).append(current)
    finally:
        index.close()
    stored_edges = {(a, b, t) for a, b, t, _s in relationships}
    return {"decisions_replayed": len(decisions), "decision_mismatches": decision_mismatches[:20],
            "decision_mismatch_count": len(decision_mismatches),
            "relationships_stored": len(stored_edges), "relationships_replayed": len(expected_edges),
            "relationship_mismatch_count": len(stored_edges ^ expected_edges)}


def run_notes(report: BackfillReport, metrics: IndexMetrics) -> str:
    return json.dumps(report.summary(metrics), sort_keys=True, default=str)[:4000]

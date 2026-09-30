"""Disposable SQLite candidate index for chunk near-dedup (derived data, never authoritative).

The index holds ``postings(band_key, item_key)`` for chunks whose confirmed
decision under this method is not NEAR_DUPLICATE, i.e. the representatives.
``item_key`` is the first 8 bytes of chunk_hash. Every candidate is resolved
and re-verified against PostgreSQL, so the index can always be deleted and
rebuilt from ``kbp.chunk_dedup_decisions`` + ``kbp.chunk_registry`` without
losing any decision.

* **Identity.** The file's ``meta`` records the algorithm, version, parameters
  digest, database and dedup_method_id. Opening a file that belongs to
  anything else is refused, so method versions and databases never mix.
* **Bounded reads.** Each band bucket is read with ``LIMIT max_bucket_rows +
  1``, so a truncated read is detected rather than silent. The result is capped
  at ``max_candidates``, ranked by shared bands, then by item key.
* **Write ordering.** Entries are written BEFORE the PostgreSQL commit. If the
  batch rolls back, its newly added items are removed (compensation). A hard
  crash can only leave stale entries: PostgreSQL filters them out, and a
  rebuild removes them. A committed representative therefore never lacks its
  entries unless the file itself is lost, which ``ensure_complete`` detects.
* **Rebuild.** It builds into a temporary file, then ``os.replace`` swaps it in
  atomically.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import threading
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from app.processing.chunk_dedup import method


class CandidateIndexError(RuntimeError):
    code = "chunk_dedup_index_error"


class IndexIncompatible(CandidateIndexError):
    code = "chunk_dedup_index_incompatible"


class IndexIncomplete(CandidateIndexError):
    code = "chunk_dedup_index_incomplete"


class IndexDegraded(CandidateIndexError):
    code = "chunk_dedup_index_degraded"


@dataclass(frozen=True)
class IndexIdentity:
    database: str
    dedup_method_id: int
    algorithm: str = method.ALGORITHM
    version: str = method.VERSION
    parameters_sha256: str = field(default_factory=method.parameters_sha256)

    def as_json(self) -> str:
        return json.dumps(self.__dict__, sort_keys=True)


@dataclass
class LookupStats:
    probes: int = 0
    rows_read: int = 0
    truncated_probes: int = 0
    candidates: int = 0


@dataclass
class IndexMetrics:
    """Running hot-bucket metrics for one run; ``check`` raises when recall is being lost."""

    lookups: int = 0
    probes: int = 0
    rows_read: int = 0
    truncated_probes: int = 0
    candidates: int = 0
    max_candidates_seen: int = 0
    candidate_histogram: Counter = field(default_factory=Counter)
    truncation_rate_limit: float = method.DEGRADED_TRUNCATION_RATE
    min_probes: int = method.DEGRADED_MIN_PROBES

    def record(self, stats: LookupStats) -> None:
        self.lookups += 1
        self.probes += stats.probes
        self.rows_read += stats.rows_read
        self.truncated_probes += stats.truncated_probes
        self.candidates += stats.candidates
        self.max_candidates_seen = max(self.max_candidates_seen, stats.candidates)
        self.candidate_histogram["at_cap" if stats.candidates >= method.MAX_CANDIDATES else
                                 "0" if stats.candidates == 0 else "1-8" if stats.candidates <= 8 else "9-63"] += 1

    @property
    def truncation_rate(self) -> float:
        return self.truncated_probes / self.probes if self.probes else 0.0

    def degraded(self) -> bool:
        return self.probes >= self.min_probes and self.truncation_rate > self.truncation_rate_limit

    def check(self) -> None:
        if self.degraded():
            raise IndexDegraded(f"{self.truncated_probes}/{self.probes} bucket reads truncated "
                                f"({self.truncation_rate:.4%} > {self.truncation_rate_limit:.2%}): candidate recall "
                                "is being lost; the method/index configuration is degraded")

    def summary(self) -> dict:
        return {"lookups": self.lookups, "probes": self.probes, "bucket_rows_read": self.rows_read,
                "truncated_probes": self.truncated_probes, "truncation_rate": round(self.truncation_rate, 6),
                "candidates_avg": round(self.candidates / self.lookups, 3) if self.lookups else 0.0,
                "candidates_max": self.max_candidates_seen,
                "candidates_histogram": dict(sorted(self.candidate_histogram.items())),
                "degraded": self.degraded()}


def default_index_path(database: str, root: Path | str = "data/chunk_dedup") -> Path:
    return Path(root) / database / f"{method.ALGORITHM}-{method.VERSION}.sqlite3"


class CandidateIndex:
    def __init__(self, path: Path | str, identity: IndexIdentity, *, create: bool = False,
                 max_bucket_rows: int = method.MAX_BUCKET_ROWS, max_candidates: int = method.MAX_CANDIDATES,
                 _building: bool = False) -> None:
        self.path = str(path)
        self.identity = identity
        self.max_bucket_rows, self.max_candidates = max_bucket_rows, max_candidates
        memory = self.path == ":memory:"
        if not memory and not create and not os.path.exists(self.path):
            raise IndexIncomplete(f"candidate index {self.path} does not exist (run rebuild-index)")
        if not memory:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=60)
        if not memory:
            self._db.execute("PRAGMA journal_mode=" + ("DELETE" if _building else "WAL"))
        self._db.execute("PRAGMA synchronous=" + ("OFF" if _building else "FULL"))
        self._db.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        self._db.execute("CREATE TABLE IF NOT EXISTS postings (band_key INTEGER NOT NULL, item_key INTEGER NOT NULL, "
                         "PRIMARY KEY (band_key, item_key)) WITHOUT ROWID")
        self._db.execute("CREATE TABLE IF NOT EXISTS items (item_key INTEGER PRIMARY KEY)")
        stored = dict(self._db.execute("SELECT k, v FROM meta"))
        if not stored:
            if not (create or memory):
                raise IndexIncompatible(f"candidate index {self.path} has no identity")
            self._db.execute("INSERT INTO meta (k, v) VALUES ('identity', ?), ('state', ?)",
                             (identity.as_json(), "building" if _building else "ready"))
        else:
            if stored.get("identity") != identity.as_json():
                raise IndexIncompatible(f"candidate index {self.path} belongs to {stored.get('identity')}, "
                                        f"not {identity.as_json()}")
            if stored.get("state") != "ready" and not _building:
                raise IndexIncomplete(f"candidate index {self.path} is in state {stored.get('state')!r}")

    # ----------------------------------------------------------------- reads
    def candidates(self, keys: Iterable[int], *, exclude: int | None = None) -> tuple[list[int], LookupStats]:
        stats = LookupStats()
        hits: Counter = Counter()
        with self._lock:
            for key in keys:
                stats.probes += 1
                rows = self._db.execute("SELECT item_key FROM postings WHERE band_key = ? ORDER BY item_key LIMIT ?",
                                        (key, self.max_bucket_rows + 1)).fetchall()
                if len(rows) > self.max_bucket_rows:
                    stats.truncated_probes += 1
                    rows = rows[:self.max_bucket_rows]
                stats.rows_read += len(rows)
                hits.update(row[0] for row in rows if row[0] != exclude)
        ranked = [item for item, _n in sorted(hits.items(), key=lambda kv: (-kv[1], kv[0]))[:self.max_candidates]]
        stats.candidates = len(ranked)
        return ranked, stats

    def item_count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT count(*) FROM items").fetchone()[0]

    def contains(self, key: int) -> bool:
        with self._lock:
            return self._db.execute("SELECT 1 FROM items WHERE item_key = ?", (key,)).fetchone() is not None

    def bucket_stats(self) -> dict:
        with self._lock:
            row = self._db.execute("SELECT count(*), coalesce(avg(n), 0), coalesce(max(n), 0), "
                                   "coalesce(sum(n > ?), 0) FROM (SELECT count(*) AS n FROM postings "
                                   "GROUP BY band_key)", (self.max_bucket_rows,)).fetchone()
            postings = self._db.execute("SELECT count(*) FROM postings").fetchone()[0]
        return {"items": self.item_count(), "postings": postings, "buckets": row[0],
                "bucket_size_avg": round(row[1], 3), "bucket_size_max": row[2], "buckets_over_read_cap": row[3]}

    def fingerprint(self) -> str:
        digest = hashlib.sha256()
        with self._lock:
            for band_key, key in self._db.execute("SELECT band_key, item_key FROM postings ORDER BY band_key, item_key"):
                digest.update(f"{band_key}:{key};".encode("ascii"))
        return digest.hexdigest()

    # ---------------------------------------------------------------- writes
    def add(self, key: int, keys: Iterable[int]) -> bool:
        """Index one representative. Returns True when the item is new (so a rollback may remove it)."""
        keys = tuple(keys)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                new = self._db.execute("INSERT OR IGNORE INTO items (item_key) VALUES (?)", (key,)).rowcount == 1
                self._db.executemany("INSERT OR IGNORE INTO postings (band_key, item_key) VALUES (?, ?)",
                                     [(band, key) for band in keys])
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return new

    def remove(self, keys: Iterable[int]) -> int:
        """Compensation for a rolled-back batch: drop items that batch added."""
        keys = list(keys)
        if not keys:
            return 0
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.executemany("DELETE FROM postings WHERE item_key = ?", [(k,) for k in keys])
                removed = self._db.executemany("DELETE FROM items WHERE item_key = ?", [(k,) for k in keys]).rowcount
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return removed

    def ensure_free_space(self, min_free_bytes: int) -> None:
        if self.path == ":memory:" or not min_free_bytes:
            return
        free = shutil.disk_usage(os.path.dirname(os.path.abspath(self.path))).free
        if free < min_free_bytes:
            raise CandidateIndexError(f"candidate index: free disk {free} below the guard {min_free_bytes}")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # --------------------------------------------------------------- rebuild
    def _bulk_add(self, rows: list[tuple[int, tuple[int, ...]]]) -> None:
        with self._lock:
            self._db.execute("BEGIN")
            self._db.executemany("INSERT OR IGNORE INTO items (item_key) VALUES (?)", [(k,) for k, _b in rows])
            self._db.executemany("INSERT OR IGNORE INTO postings (band_key, item_key) VALUES (?, ?)",
                                 [(band, k) for k, bands in rows for band in bands])
            self._db.execute("COMMIT")

    def _mark_ready(self) -> None:
        with self._lock:
            self._db.execute("UPDATE meta SET v = 'ready' WHERE k = 'state'")


def build_index(path: Path | str, identity: IndexIdentity, representatives: Iterable[tuple[bytes, str]], *,
                batch: int = 5_000) -> dict:
    """Build a fresh index from (chunk_hash, chunk_text) of every representative, then swap it in atomically."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f"{target.name}.rebuild-{os.getpid()}.tmp")
    for leftover in (temp, Path(f"{temp}-journal")):
        leftover.unlink(missing_ok=True)
    index = CandidateIndex(temp, identity, create=True, _building=True)
    items = 0
    try:
        pending: list[tuple[int, tuple[int, ...]]] = []
        for chunk_hash, chunk_text in representatives:
            pending.append((method.item_key(chunk_hash), method.band_keys(method.features(chunk_text))))
            if len(pending) >= batch:
                index._bulk_add(pending)
                items += len(pending)
                pending.clear()
        if pending:
            index._bulk_add(pending)
            items += len(pending)
        index._mark_ready()
        stats = index.bucket_stats()
        fingerprint = index.fingerprint()
    finally:
        index.close()
    for stale in (Path(f"{target}-wal"), Path(f"{target}-shm")):  # never apply an old WAL to the new file
        stale.unlink(missing_ok=True)
    os.replace(temp, target)
    return {"path": str(target), "representatives": items, "fingerprint": fingerprint, **stats}

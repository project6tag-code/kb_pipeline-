"""Gate 20: near-duplicate / version analysis (``minhash_lsh`` / ``oph-mh128-b32r4-w5-v1``).

Candidate generation is sub-linear (no O(N^2) corpus comparison):

* shingles: lower-cased word 5-grams, hashed with BLAKE2b-64 (deterministic,
  independent of Python's ``hash``);
* signature: one-permutation MinHash (Li et al. 2012) with 128 bins and
  rotation densification of empty bins (Shrivastava & Li 2014): one hash per
  shingle, so it is fast in pure Python, and LSH-compatible;
* LSH: 32 bands x 4 rows (S-curve midpoint ~0.42), so pairs at Jaccard >= 0.5
  collide with high probability;
* each band bucket is read with ``LIMIT max_bucket_rows``; at most
  ``max_candidates`` candidates (most shared bands first) are verified by the
  EXACT Jaccard similarity of their shingle sets.

Classification (conservative; topic similarity alone never merges):

* J >= near_threshold -> NEAR_DUPLICATE of the best match (plus a VERSION
  relationship when both come from the same URL / repository path);
* same lineage and J >= version_threshold -> VERSION of the best such match;
* J >= related_threshold -> RELATED;
* otherwise UNIQUE.

Physical index (schema §9.5 "signature storage ... is part of the
near-duplicate implementation"): a SQLite side file holding band keys of
representative (accepted) documents only. It is derived data, rebuildable
from ``document_registry``. Entries are written before the PostgreSQL commit
and every candidate is re-checked against ``document_registry`` (accepted,
text present), so a rolled-back writer leaves only harmless stale entries.
"""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import threading
import uuid
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

ALGORITHM = "minhash_lsh"
VERSION = "oph-mh128-b32r4-w5-v1"
PARAMETERS = {"shingle": "word", "shingle_size": 5, "hash": "blake2b-64", "signature": "one-permutation-minhash",
              "bins": 128, "densification": "rotation", "bands": 32, "rows": 4}
BINS, BANDS, ROWS, SHINGLE = 128, 32, 4, 5
_VALUE_BITS = 57
_VALUE_MASK = (1 << _VALUE_BITS) - 1
_TOKEN = re.compile(r"\w+")

EXACT_DUPLICATE, NEAR_DUPLICATE, VERSION_REL, RELATED, UNIQUE = (
    "EXACT_DUPLICATE", "NEAR_DUPLICATE", "VERSION", "RELATED", "UNIQUE")


def _h64(data: str) -> int:
    return int.from_bytes(hashlib.blake2b(data.encode("utf-8"), digest_size=8).digest(), "big")


def shingles(text: str) -> frozenset[int]:
    tokens = [t.lower() for t in _TOKEN.findall(text)]
    if not tokens:
        return frozenset()
    if len(tokens) < SHINGLE:
        return frozenset({_h64(" ".join(tokens))})
    return frozenset(_h64(" ".join(tokens[i:i + SHINGLE])) for i in range(len(tokens) - SHINGLE + 1))


def signature(shingle_set: frozenset[int]) -> tuple[int, ...]:
    if not shingle_set:
        return ()
    bins: list[int | None] = [None] * BINS
    for h in shingle_set:
        b, v = h >> _VALUE_BITS, h & _VALUE_MASK
        if bins[b] is None or v < bins[b]:
            bins[b] = v
    out = []
    for i in range(BINS):
        if bins[i] is not None:
            out.append(bins[i])
            continue
        for step in range(1, BINS):  # rotation densification: borrow the next non-empty bin
            j = (i + step) % BINS
            if bins[j] is not None:
                out.append(bins[j] + step * (_VALUE_MASK + 1))
                break
    return tuple(out)


def band_keys(sig: tuple[int, ...]) -> list[int]:
    keys = []
    for band in range(BANDS):
        values = sig[band * ROWS:(band + 1) * ROWS]
        digest = hashlib.blake2b(f"{band}:{','.join(map(str, values))}".encode("ascii"), digest_size=8).digest()
        keys.append(int.from_bytes(digest, "big", signed=True))
    return keys


def jaccard(a: frozenset[int], b: frozenset[int]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


class NearIndexError(RuntimeError):
    pass


class NearIndex:
    """Bounded LSH band index in a SQLite side file (``:memory:`` for tests)."""

    def __init__(self, path: Path | str, *, min_free_bytes: int = 0) -> None:
        self.path = str(path)
        self.min_free_bytes = min_free_bytes
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        if self.path != ":memory:":
            self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        self._db.execute("CREATE TABLE IF NOT EXISTS bands (band_key INTEGER NOT NULL, document_id BLOB NOT NULL, "
                         "PRIMARY KEY (band_key, document_id)) WITHOUT ROWID")
        row = self._db.execute("SELECT v FROM meta WHERE k = 'version'").fetchone()
        if row is None:
            self._db.execute("INSERT INTO meta (k, v) VALUES ('version', ?)", (f"{ALGORITHM}:{VERSION}",))
        elif row[0] != f"{ALGORITHM}:{VERSION}":
            raise NearIndexError(f"near-duplicate index {self.path} belongs to {row[0]}, not {ALGORITHM}:{VERSION}")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def candidates(self, sig: tuple[int, ...], *, max_bucket_rows: int, max_candidates: int) -> list[uuid.UUID]:
        if not sig:
            return []
        hits: Counter = Counter()
        with self._lock:
            for key in band_keys(sig):
                for (doc,) in self._db.execute("SELECT document_id FROM bands WHERE band_key = ? ORDER BY document_id "
                                               "LIMIT ?", (key, max_bucket_rows)):
                    hits[bytes(doc)] += 1
        ranked = sorted(hits.items(), key=lambda kv: (-kv[1], kv[0]))[:max_candidates]
        return [uuid.UUID(bytes=doc) for doc, _n in ranked]

    def add(self, document_id: uuid.UUID, sig: tuple[int, ...]) -> None:
        if not sig:
            return
        if self.path != ":memory:" and self.min_free_bytes:
            import shutil

            if shutil.disk_usage(os.path.dirname(os.path.abspath(self.path))).free < self.min_free_bytes:
                raise OSError("near-duplicate index: free disk space below the guard")
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._db.executemany("INSERT OR IGNORE INTO bands (band_key, document_id) VALUES (?, ?)",
                                     [(key, document_id.bytes) for key in band_keys(sig)])
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def size(self) -> int:
        with self._lock:
            return self._db.execute("SELECT count(DISTINCT document_id) FROM bands").fetchone()[0]


@dataclass(frozen=True)
class Candidate:
    document_id: uuid.UUID
    shingles: frozenset[int]
    lineage: frozenset[str]


@dataclass(frozen=True)
class NearDecision:
    classification: str
    canonical_document_id: uuid.UUID | None
    similarity: float | None
    relationships: tuple[tuple[uuid.UUID, str, float], ...] = field(default_factory=tuple)
    candidates_checked: int = 0


def decide(current: frozenset[int], lineage: str, candidates: list[Candidate], *, near_threshold: float,
           version_threshold: float, related_threshold: float) -> NearDecision:
    scored = sorted(((jaccard(current, c.shingles), c) for c in candidates),
                    key=lambda sc: (-sc[0], str(sc[1].document_id)))
    relationships: list[tuple[uuid.UUID, str, float]] = []
    for score, cand in scored:
        same = lineage in cand.lineage
        if score >= near_threshold:
            relationships.append((cand.document_id, NEAR_DUPLICATE, score))
            if same:
                relationships.append((cand.document_id, VERSION_REL, score))
        elif same and score >= version_threshold:
            relationships.append((cand.document_id, VERSION_REL, score))
        elif score >= related_threshold:
            relationships.append((cand.document_id, RELATED, score))
    checked = len(candidates)
    for kind in (NEAR_DUPLICATE, VERSION_REL, RELATED):
        match = next(((doc, s) for doc, k, s in relationships if k == kind), None)
        if match is not None:
            return NearDecision(kind, match[0], round(match[1], 4), tuple(relationships), checked)
    return NearDecision(UNIQUE, None, round(scored[0][0], 4) if scored else None, (), checked)

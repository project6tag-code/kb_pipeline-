"""Frozen definition of the chunk near-dedup method ``chunk_minhash_lsh`` / ``oph-w3-b10r4-j70-v1``.

This is the design approved in docs/chunk_near_dedup_candidate_index_v1.md:

* comparison features are case-folded word 3-grams hashed with BLAKE2b-64.
  They are used for comparison only: chunk_text, chunk_hash and chunk_id are
  never changed;
* the signature is a one-permutation MinHash with 40 bins (splitmix64 mixing,
  rotation densification);
* the LSH has 10 bands of 4 rows, and each band key is BLAKE2b-64;
* candidate reads are bounded: at most 256 rows per bucket and at most 64
  candidates, ranked by shared bands, then by item key;
* the exact verifier decides NEAR_DUPLICATE when Jaccard >= 0.70 AND the fact
  signatures are equal (``facts.py``), RELATED when Jaccard >= 0.30,
  otherwise UNIQUE.

Every parameter is registered in ``kbp.dedup_methods.parameters``. A changed
parameter must be a NEW ``VERSION``: the bootstrap refuses to reuse a version
with different stored parameters.
"""

from __future__ import annotations

import hashlib
import json
import re
import struct

from app.processing.chunk_dedup import facts

ALGORITHM = "chunk_minhash_lsh"
VERSION = "oph-w3-b10r4-j70-v1"
SHINGLE = 3
BANDS, ROWS = 10, 4
BINS = BANDS * ROWS
MAX_BUCKET_ROWS = 256
MAX_CANDIDATES = 64
NEAR_THRESHOLD = 0.70
RELATED_THRESHOLD = 0.30
DEGRADED_TRUNCATION_RATE = 0.001  # >0.1% truncated bucket reads: candidate recall is being lost
DEGRADED_MIN_PROBES = 1000
SCORE_DIGITS = 4

PARAMETERS: dict = {
    "features": {"tokens": "\\w+ casefold", "shingle": SHINGLE, "hash": "blake2b-64"},
    "signature": {"kind": "one-permutation-minhash", "bins": BINS, "mix": "splitmix64", "densification": "rotation"},
    "lsh": {"bands": BANDS, "rows": ROWS, "band_key": "blake2b-64"},
    "index": {"item_key": "chunk_hash[0:8] as signed int64", "max_bucket_rows": MAX_BUCKET_ROWS,
              "max_candidates": MAX_CANDIDATES, "rank": "shared bands desc, item key asc",
              "scope": "confirmed non-NEAR_DUPLICATE decisions (representatives) only"},
    "verify": {"similarity": "jaccard", "near_threshold": NEAR_THRESHOLD, "related_threshold": RELATED_THRESHOLD,
               "score_digits": SCORE_DIGITS, "fact_guard": facts.FACT_GUARD_VERSION,
               "fact_lexicon_sha256": facts.lexicon_sha256()},
    "representative": "immutable per method version; processing order is chunk_hash ascending within a run; a "
                      "NEAR_DUPLICATE maps to the verified representative with the highest similarity, ties by "
                      "chunk_hash ascending",
    "degraded": {"truncation_rate": DEGRADED_TRUNCATION_RATE, "min_probes": DEGRADED_MIN_PROBES},
}

_WORD = re.compile(r"\w+", re.UNICODE)
_M64 = (1 << 64) - 1


def parameters_sha256() -> str:
    return hashlib.sha256(json.dumps(PARAMETERS, sort_keys=True).encode("utf-8")).hexdigest()


def _h64(data: bytes) -> int:
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


def _signed(value: int) -> int:
    value &= _M64
    return value if value < (1 << 63) else value - (1 << 64)


def _mix64(x: int) -> int:  # splitmix64 finalizer
    x = (x + 0x9E3779B97F4A7C15) & _M64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & _M64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & _M64
    return x ^ (x >> 31)


def features(text: str) -> frozenset[int]:
    words = [token.casefold() for token in _WORD.findall(text)]
    if not words:
        return frozenset()
    if len(words) < SHINGLE:
        return frozenset({_h64(" ".join(words).encode("utf-8"))})
    return frozenset(_h64(" ".join(words[i:i + SHINGLE]).encode("utf-8")) for i in range(len(words) - SHINGLE + 1))


def jaccard(left: frozenset[int], right: frozenset[int]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def signature(feature_set: frozenset[int]) -> tuple[int, ...]:
    if not feature_set:
        return ()
    bins: list[int | None] = [None] * BINS
    for value in feature_set:
        mixed = _mix64(value & _M64)
        slot, rank = mixed % BINS, mixed // BINS
        current = bins[slot]
        if current is None or rank < current:
            bins[slot] = rank
    out = []
    for i in range(BINS):
        if bins[i] is not None:
            out.append(bins[i])
            continue
        for step in range(1, BINS):
            j = (i + step) % BINS
            if bins[j] is not None:
                out.append(bins[j] + step * (1 << 64))
                break
    return tuple(out)


def band_keys(feature_set: frozenset[int]) -> tuple[int, ...]:
    sig = signature(feature_set)
    if not sig:
        return ()
    return tuple(_signed(_h64(f"oph{BINS}:{band}:{','.join(map(str, sig[band * ROWS:(band + 1) * ROWS]))}"
                              .encode("ascii"))) for band in range(BANDS))


def item_key(chunk_hash: bytes) -> int:
    """Compact index key: the first 8 bytes of chunk_hash. Collisions are resolved by PostgreSQL."""
    if len(chunk_hash) != 32:
        raise ValueError("chunk_hash must be 32 bytes")
    return struct.unpack(">q", chunk_hash[:8])[0]


def hash_range(key: int) -> tuple[bytes, bytes]:
    """The inclusive chunk_hash range that shares the item key's 8-byte prefix."""
    prefix = struct.pack(">q", key)
    return prefix + b"\x00" * 24, prefix + b"\xff" * 24


def score(value: float) -> float:
    return round(value, SCORE_DIGITS)

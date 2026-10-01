"""Vector validation, the frozen ``vector_sha256`` and the pgvector text literal (Embeddings V1).

``vector_sha256`` follows Migration 001C exactly: SHA-256 of the 768
components as IEEE-754 float32, **big-endian**, in order. That is pgvector's
binary send payload after its 4-byte header, so PostgreSQL recomputes it as
``sha256(substring(public.vector_send(embedding) FROM 5))``.

The literal sent to PostgreSQL is the shortest decimal that round-trips each
float32 (numpy's ``float32`` repr). pgvector parses it back to the same
float32, so the stored vector is bit-identical to the computed one. The write
transaction proves this by comparing the database recomputation with the local
hash.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import numpy as np

DIMENSION = 768
NORM_TOLERANCE = 1e-3


class VectorInvalid(ValueError):
    """A model output that must never be stored (wrong shape, non-finite, not unit length)."""


@dataclass(frozen=True)
class CheckedVector:
    values: np.ndarray  # float32, shape (768,), unit L2 norm
    sha256: bytes
    norm: float

    def literal(self) -> str:
        return to_literal(self.values)


def as_float32_matrix(raw: object, rows: int) -> np.ndarray:
    matrix = np.asarray(raw)
    if matrix.ndim != 2 or matrix.shape[0] != rows:
        raise VectorInvalid(f"expected {rows} vectors, got array of shape {matrix.shape}")
    return np.ascontiguousarray(matrix, dtype=np.float32)


def check(vector: np.ndarray) -> CheckedVector:
    values = np.ascontiguousarray(vector, dtype=np.float32)
    if values.shape != (DIMENSION,):
        raise VectorInvalid(f"expected {DIMENSION} dimensions, got shape {values.shape}")
    if np.isnan(values).any():
        raise VectorInvalid("vector contains NaN")
    if not np.isfinite(values).all():
        raise VectorInvalid("vector contains Inf")
    norm = float(np.linalg.norm(values.astype(np.float64)))
    if abs(norm - 1.0) > NORM_TOLERANCE:
        raise VectorInvalid(f"vector is not L2-normalized (norm {norm:.6f})")
    return CheckedVector(values, vector_sha256(values), norm)


def vector_sha256(values: np.ndarray) -> bytes:
    """SHA-256 of the float32 components in big-endian order (Migration 001C definition)."""
    return hashlib.sha256(np.ascontiguousarray(values, dtype=">f4").tobytes()).digest()


def to_literal(values: np.ndarray) -> str:
    """pgvector text form; ``str(np.float32)`` is the shortest decimal that round-trips the float32."""
    return "[" + ",".join(str(x) for x in np.asarray(values, dtype=np.float32)) + "]"


def from_literal(text: str) -> np.ndarray:
    return np.asarray([np.float32(x) for x in text.strip("[]").split(",")], dtype=np.float32)

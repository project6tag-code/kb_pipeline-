"""Deterministic content hashing primitives.

All content hashes in the pipeline are SHA-256 over the UTF-8 encoding of a
Python ``str``, rendered as lowercase hexadecimal. These functions hash exactly
what they are given; canonicalization is the caller's responsibility (see
``app.utils.ids`` for the canonical document/chunk hash functions).
"""

from __future__ import annotations

import hashlib
import re

SHA256_HEX_LENGTH = 64

_SHA256_HEX_RE = re.compile(r"[0-9a-fA-F]{64}")


class InvalidHashError(ValueError):
    """Raised when a value is not a well-formed SHA-256 hex digest."""


def sha256_text(text: str) -> str:
    """Return the lowercase hex SHA-256 digest of ``text`` encoded as UTF-8.

    Raises:
        TypeError: if ``text`` is not a ``str`` (bytes are rejected on purpose,
            so callers cannot bypass text canonicalization).
        UnicodeEncodeError: if ``text`` contains lone surrogates, which have
            no UTF-8 encoding.
    """
    if not isinstance(text, str):
        raise TypeError(f"sha256_text() expects str, got {type(text).__name__}")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def validate_sha256_hex(value: str) -> str:
    """Validate a SHA-256 hex digest and return it in canonical lowercase form.

    Raises:
        TypeError: if ``value`` is not a ``str``.
        InvalidHashError: if ``value`` is not exactly 64 hexadecimal characters.
    """
    if not isinstance(value, str):
        raise TypeError(f"SHA-256 hash must be str, got {type(value).__name__}")
    if _SHA256_HEX_RE.fullmatch(value) is None:
        raise InvalidHashError(
            f"invalid SHA-256 hex digest (expected {SHA256_HEX_LENGTH} hex characters): {value!r}"
        )
    return value.lower()

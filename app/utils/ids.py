"""Deterministic document and chunk identities (Identity Specification V1).

    content_hash = SHA256(utf8(canonicalize_text(document_text)))
    document_id  = UUIDv5(KB_NAMESPACE, "document:v1:" + content_hash)

    chunk_hash   = SHA256(utf8(canonicalize_text(chunk_text)))
    chunk_id     = UUIDv5(KB_NAMESPACE, "chunk:v1:" + chunk_hash)

Identities depend only on canonical content: never on timestamps, randomness,
paths, URLs, database sequences, or Python's ``hash()``.

Identity Extension V1 (docs/identity_extension_v1.md) adds, in the same
namespace, location-based source-observation identities and bootstrap
identities for the Gold bridge datasets:

    bridge_dataset_id = UUIDv5(KB_NAMESPACE, "dataset-bridge:v1:" + source_name)
    cc observation    = UUIDv5(KB_NAMESPACE, "observation:v1:common_crawl:" + canonical_json)
    github observation= UUIDv5(KB_NAMESPACE, "observation:v1:github:" + canonical_json)
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any, Final

from app.processing.normalization.text import canonicalize_text
from app.utils.hashing import sha256_text, validate_sha256_hex

# FROZEN. Generated once for this knowledge base and committed permanently.
# Changing this value changes every document_id and chunk_id (including kb.id),
# which is a breaking migration of the entire knowledge base. Do not edit.
KB_NAMESPACE: Final[uuid.UUID] = uuid.UUID("dd5af6b8-db26-4170-945d-557b16114d80")

DOCUMENT_ID_PREFIX: Final[str] = "document:v1:"
CHUNK_ID_PREFIX: Final[str] = "chunk:v1:"


def _canonical_content_hash(text: str, kind: str) -> str:
    canonical = canonicalize_text(text)
    if not canonical:
        raise ValueError(f"{kind} text is empty after canonicalization")
    return sha256_text(canonical)


def document_content_hash(text: str) -> str:
    """Return the SHA-256 hex digest of the canonical form of a document.

    Raises:
        TypeError: if ``text`` is not a ``str``.
        ValueError: if the canonical text is empty.
    """
    return _canonical_content_hash(text, "document")


def chunk_content_hash(text: str) -> str:
    """Return the SHA-256 hex digest of the canonical form of a chunk.

    Raises:
        TypeError: if ``text`` is not a ``str``.
        ValueError: if the canonical text is empty.
    """
    return _canonical_content_hash(text, "chunk")


def document_id_from_hash(content_hash: str) -> uuid.UUID:
    """Derive the document UUIDv5 from a document content hash.

    Hex case is normalized to lowercase before derivation.

    Raises:
        TypeError: if ``content_hash`` is not a ``str``.
        InvalidHashError: if ``content_hash`` is not 64 hex characters.
    """
    return uuid.uuid5(KB_NAMESPACE, DOCUMENT_ID_PREFIX + validate_sha256_hex(content_hash))


def chunk_id_from_hash(chunk_hash: str) -> uuid.UUID:
    """Derive the chunk UUIDv5 from a chunk hash.

    Hex case is normalized to lowercase before derivation.

    Raises:
        TypeError: if ``chunk_hash`` is not a ``str``.
        InvalidHashError: if ``chunk_hash`` is not 64 hex characters.
    """
    return uuid.uuid5(KB_NAMESPACE, CHUNK_ID_PREFIX + validate_sha256_hex(chunk_hash))


def document_id_from_text(text: str) -> uuid.UUID:
    """Canonicalize ``text``, hash it, and derive the document UUID."""
    return document_id_from_hash(document_content_hash(text))


def chunk_id_from_text(text: str) -> uuid.UUID:
    """Canonicalize ``text``, hash it, and derive the chunk UUID."""
    return chunk_id_from_hash(chunk_content_hash(text))


# ---------------------------------------------------------------------------
# Identity Extension V1 (LOCKED). See docs/identity_extension_v1.md.
# Changing any prefix, field name, field order, validation rule or the
# serialization below changes identities and requires Identity Extension V2.
# ---------------------------------------------------------------------------

DATASET_BRIDGE_PREFIX: Final[str] = "dataset-bridge:v1:"
CC_OBSERVATION_PREFIX: Final[str] = "observation:v1:common_crawl:"
GITHUB_OBSERVATION_PREFIX: Final[str] = "observation:v1:github:"

SUPPORTED_BRIDGE_SOURCES: Final[frozenset[str]] = frozenset({"common_crawl", "github"})

_CC_CRAWL_ID_RE = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")
_GIT_OBJECT_ID_RE = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_WHITESPACE_OR_CONTROL_RE = re.compile(r"[\s\x00-\x1f\x7f]")
_MAX_BIGINT: Final[int] = 2**63 - 1


def _require_str(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be str, got {type(value).__name__}")
    if not value:
        raise ValueError(f"{field} must be non-empty")
    return value


def _require_int(value: object, field: str, minimum: int) -> int:
    # bool is a subclass of int and must not be accepted as an integer.
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be int, got {type(value).__name__}")
    if value < minimum:
        raise ValueError(f"{field} must be >= {minimum}, got {value}")
    if value > _MAX_BIGINT:
        raise ValueError(f"{field} exceeds the 64-bit signed integer range")
    return value


def _require_git_object_id(value: object, field: str) -> str:
    value = _require_str(value, field)
    if _GIT_OBJECT_ID_RE.fullmatch(value) is None:
        raise ValueError(
            f"{field} must be 40 (SHA-1) or 64 (SHA-256) lowercase hex characters: {value!r}"
        )
    return value


def _canonical_json(payload: dict[str, Any]) -> str:
    """Serialize ``payload`` as compact, ASCII-only JSON in insertion order."""
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), allow_nan=False)


def make_dataset_bridge_id(source_name: str) -> uuid.UUID:
    """Return the deterministic id of the Gold bridge ``public.datasets`` row.

    ``source_name`` is the ``public.dataset_sources.name`` of the channel and
    must be exactly ``"common_crawl"`` or ``"github"``.

    Raises:
        TypeError: if ``source_name`` is not a ``str``.
        ValueError: if ``source_name`` is not a supported source.
    """
    source_name = _require_str(source_name, "source_name")
    if source_name not in SUPPORTED_BRIDGE_SOURCES:
        raise ValueError(
            f"unsupported bridge source {source_name!r}; "
            f"expected one of {sorted(SUPPORTED_BRIDGE_SOURCES)}"
        )
    return uuid.uuid5(KB_NAMESPACE, DATASET_BRIDGE_PREFIX + source_name)


def cc_observation_payload(
    crawl_id: str, warc_filename: str, warc_offset: int, warc_length: int
) -> str:
    """Validate a Common Crawl capture location and return its canonical JSON.

    Raises:
        TypeError: if a field has the wrong type (``bool`` is not an ``int``).
        ValueError: if a field is empty or malformed.
    """
    crawl_id = _require_str(crawl_id, "crawl_id")
    if _CC_CRAWL_ID_RE.fullmatch(crawl_id) is None:
        raise ValueError(f"crawl_id must match CC-MAIN-YYYY-WW: {crawl_id!r}")
    warc_filename = _require_str(warc_filename, "warc_filename")
    if _WHITESPACE_OR_CONTROL_RE.search(warc_filename):
        raise ValueError("warc_filename must not contain whitespace or control characters")
    if not warc_filename.startswith(f"crawl-data/{crawl_id}/"):
        raise ValueError(
            f"warc_filename must be the relative key starting with 'crawl-data/{crawl_id}/'"
        )
    warc_offset = _require_int(warc_offset, "warc_offset", minimum=0)
    warc_length = _require_int(warc_length, "warc_length", minimum=1)
    return _canonical_json(
        {
            "crawl_id": crawl_id,
            "warc_filename": warc_filename,
            "warc_offset": warc_offset,
            "warc_length": warc_length,
        }
    )


def cc_observation_id(
    crawl_id: str, warc_filename: str, warc_offset: int, warc_length: int
) -> uuid.UUID:
    """Return the deterministic id of one Common Crawl WARC record observation."""
    payload = cc_observation_payload(crawl_id, warc_filename, warc_offset, warc_length)
    return uuid.uuid5(KB_NAMESPACE, CC_OBSERVATION_PREFIX + payload)


def _validate_repo_path(path: object) -> str:
    path = _require_str(path, "path")
    if "\x00" in path:
        raise ValueError("path must not contain U+0000")
    if any(0xD800 <= ord(ch) <= 0xDFFF for ch in path):
        raise ValueError("path must not contain surrogate code points (not valid UTF-8)")
    if path.startswith("/") or path.endswith("/"):
        raise ValueError(f"path must be repository-relative without leading/trailing '/': {path!r}")
    for segment in path.split("/"):
        if segment in ("", ".", ".."):
            raise ValueError(f"path must not contain empty, '.' or '..' segments: {path!r}")
    return path


def github_observation_payload(
    repository_id: int, commit_sha: str, path: str, blob_sha: str
) -> str:
    """Validate a GitHub file observation location and return its canonical JSON.

    ``repository_id`` is GitHub's numeric repository database id (REST ``id``),
    not the GraphQL node id and not owner/name.

    Raises:
        TypeError: if a field has the wrong type (``bool`` is not an ``int``).
        ValueError: if a field is empty or malformed.
    """
    repository_id = _require_int(repository_id, "repository_id", minimum=1)
    commit_sha = _require_git_object_id(commit_sha, "commit_sha")
    path = _validate_repo_path(path)
    blob_sha = _require_git_object_id(blob_sha, "blob_sha")
    if len(commit_sha) != len(blob_sha):
        raise ValueError("commit_sha and blob_sha must use the same object format (SHA-1 or SHA-256)")
    return _canonical_json(
        {
            "repository_id": repository_id,
            "commit_sha": commit_sha,
            "path": path,
            "blob_sha": blob_sha,
        }
    )


def github_observation_id(
    repository_id: int, commit_sha: str, path: str, blob_sha: str
) -> uuid.UUID:
    """Return the deterministic id of one GitHub file observed at one commit."""
    payload = github_observation_payload(repository_id, commit_sha, path, blob_sha)
    return uuid.uuid5(KB_NAMESPACE, GITHUB_OBSERVATION_PREFIX + payload)

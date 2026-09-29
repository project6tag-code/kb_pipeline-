"""Gates 01-02: Bronze integrity validation and source-specific payload extraction.

Nothing is parsed before its evidence verifies.

Common Crawl (``BronzeStore`` layout of Acquisition V1):
* the observation id re-derives from (crawl, filename, offset, length) and the
  manifest's identity payload equals the LOCKED canonical JSON;
* the manifest (completion marker) exists; the record's size and SHA-256 equal
  the manifest; the size equals ``warc_length``; temp files are never read;
* the manifest's source URL matches the observation;
* the index row is rebuilt from the manifest and the record re-validated with
  the acquisition parser (target URI, status, payload digest, bounded gunzip);
* then the HTTP payload is extracted (bounded), refusing content/transfer codings.

GitHub (``GitHubBronze`` layout):
* the observation id re-derives from (repository, commit, path, blob); the file
  manifest's identity payload, ids, path and sizes match;
* the blob verifies against its git object id (``read_blob``), and its SHA-256
  equals the manifest's ``content_sha256``.

Missing/incomplete/damaged evidence is retryable (a dead letter, no extraction
row: re-acquisition can repair it). Identity mismatches are permanent. A
deterministic over-limit is an ``extraction_failed`` outcome.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from app.processing.silver.config import LimitsConfig
from app.processing.silver.model import SourceRef
from app.sources.commoncrawl.bronze import BronzeCorrupt, BronzeState, BronzeStore, UnsafePath
from app.sources.commoncrawl.compression import CompressionError, decompress_single_member
from app.sources.commoncrawl.index import IndexRowError, parse_cdx_line
from app.sources.commoncrawl.warc import WarcRecordError, parse_warc_record
from app.sources.github.bronze import GitHubBronze
from app.utils.ids import (
    cc_observation_id,
    cc_observation_payload,
    github_observation_id,
    github_observation_payload,
)


class BronzeIntegrityError(Exception):
    """kind: 'retryable' / 'permanent' (dead letter only, no extraction row), 'outcome' (extraction_failed),
    'prefilter' (rejected_prefilter)."""

    def __init__(self, code: str, message: str, kind: str) -> None:
        super().__init__(message)
        self.code, self.kind = code, kind


@dataclass(frozen=True)
class Payload:
    body: bytes
    content_type: str | None
    http_status: int | None
    index_mime: str | None
    language_hint: str | None
    title: str | None


def _retryable(code: str, message: str) -> BronzeIntegrityError:
    return BronzeIntegrityError(code, message, "retryable")


def _permanent(code: str, message: str) -> BronzeIntegrityError:
    return BronzeIntegrityError(code, message, "permanent")


def _split(data: bytes, what: str) -> tuple[bytes, bytes]:
    end = data.find(b"\r\n\r\n", 0, 64 * 1024 + 4)
    if end < 0:
        raise BronzeIntegrityError(f"bad_{what}_header", f"{what} header block missing", "outcome")
    return data[:end], data[end + 4:]


def _headers(head: bytes) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in head.decode("latin-1").split("\r\n")[1:]:
        name, sep, value = line.partition(":")
        if sep and name.strip():
            headers.setdefault(name.strip().lower(), value.strip())
    return headers


def read_common_crawl(ref: SourceRef, bronze: BronzeStore, limits: LimitsConfig) -> Payload:
    try:
        expected = cc_observation_id(ref.crawl_id, ref.warc_filename, ref.warc_offset, ref.warc_length)
    except (TypeError, ValueError) as exc:
        raise _permanent("identity_invalid", f"invalid capture coordinates: {exc}") from None
    if expected != ref.observation_id:
        raise _permanent("identity_mismatch", "observation id does not derive from its capture coordinates")
    try:
        inspection = bronze.inspect(ref.crawl_id, ref.observation_id)
    except (BronzeCorrupt, UnsafePath) as exc:
        raise _retryable("bronze_corrupt", str(exc)) from None
    if inspection.state is not BronzeState.COMPLETE:
        raise _retryable(f"bronze_{inspection.state.value}", f"Bronze evidence is {inspection.state.value}")
    manifest = inspection.manifest
    if manifest["record_bytes"] > limits.max_bronze_bytes:
        raise BronzeIntegrityError("bronze_too_large", "Bronze record exceeds max_bronze_bytes", "outcome")
    try:
        verified = bronze.read_verified(ref.crawl_id, ref.observation_id)
    except (BronzeCorrupt, OSError) as exc:
        raise _retryable("bronze_corrupt", str(exc)) from None
    if verified is None:
        raise _retryable("bronze_changed", "Bronze evidence changed during verification")
    data, manifest = verified
    payload = cc_observation_payload(ref.crawl_id, ref.warc_filename, ref.warc_offset, ref.warc_length)
    if (manifest.get("identity") != payload or manifest.get("crawl_id") != ref.crawl_id
            or manifest.get("warc_filename") != ref.warc_filename or manifest.get("warc_offset") != ref.warc_offset
            or manifest.get("warc_length") != ref.warc_length or len(data) != ref.warc_length):
        raise _permanent("manifest_mismatch", "Bronze manifest does not match the observation coordinates")
    if ref.source_url and manifest.get("source_url") != ref.source_url:
        raise _permanent("manifest_url_mismatch", "Bronze manifest URL differs from the observation URL")
    index = manifest.get("index") if isinstance(manifest.get("index"), dict) else {}
    fields = index.get("fields")
    if not isinstance(fields, dict) or not isinstance(index.get("urlkey"), str) \
            or not isinstance(index.get("timestamp"), str):
        raise _permanent("manifest_index_invalid", "Bronze manifest has no usable index row")
    try:
        line = f"{index['urlkey']} {index['timestamp']} {json.dumps(fields, ensure_ascii=True)}".encode("utf-8")
        record = parse_cdx_line(line, ref.crawl_id)
        parse_warc_record(data, record, max_decompressed=limits.max_decompressed_bytes, verify_payload_digest=True)
    except IndexRowError as exc:
        raise _permanent(f"index_{exc.code}", str(exc)) from None
    except WarcRecordError as exc:
        kind = "outcome" if exc.code == "decompression_limit" else "permanent"
        raise BronzeIntegrityError(f"warc_{exc.code}", str(exc), kind) from None
    try:
        raw = decompress_single_member(data, limits.max_decompressed_bytes)
    except CompressionError as exc:  # pragma: no cover - parse_warc_record already decompressed it
        raise _permanent(f"warc_{exc.code}", str(exc)) from None
    warc_head, rest = _split(raw, "warc")
    block_length = int(_headers(warc_head).get("content-length", "0") or 0)
    http_head, body = _split(rest[:block_length], "http")
    http = _headers(http_head)
    for name in ("content-encoding", "transfer-encoding"):
        if http.get(name, "identity").lower() not in ("", "identity"):
            raise BronzeIntegrityError("encoded_payload", f"payload has {name} {http[name][:20]!r}", "prefilter")
    if len(body) > limits.max_payload_bytes:
        raise BronzeIntegrityError("payload_too_large", "HTTP payload exceeds max_payload_bytes", "outcome")
    status_line = http_head.split(b"\r\n", 1)[0].decode("latin-1")
    try:
        status = int(status_line.split()[1])
    except (IndexError, ValueError):
        status = None
    html_meta = manifest.get("html_metadata") if isinstance(manifest.get("html_metadata"), dict) else {}
    languages = fields.get("languages")
    return Payload(body, http.get("content-type"), status, fields.get("mime-detected") or fields.get("mime"),
                   languages if isinstance(languages, str) else None,
                   html_meta.get("title") if isinstance(html_meta.get("title"), str) else None)


def read_github(ref: SourceRef, bronze: GitHubBronze, limits: LimitsConfig) -> Payload:
    try:
        expected = github_observation_id(ref.repository_id, ref.commit_sha, ref.path, ref.blob_sha)
        identity = github_observation_payload(ref.repository_id, ref.commit_sha, ref.path, ref.blob_sha)
    except (TypeError, ValueError) as exc:
        raise _permanent("identity_invalid", f"invalid file coordinates: {exc}") from None
    if expected != ref.observation_id:
        raise _permanent("identity_mismatch", "observation id does not derive from its file coordinates")
    try:
        manifest = bronze.read_json(bronze.observation_path(ref.repository_id, ref.commit_sha, ref.observation_id))
    except (BronzeCorrupt, UnsafePath) as exc:
        raise _retryable("bronze_corrupt", str(exc)) from None
    if manifest is None:
        raise _retryable("bronze_missing", "GitHub file manifest is missing")
    if (manifest.get("identity") != identity or manifest.get("observation_id") != str(ref.observation_id)
            or manifest.get("repository_id") != ref.repository_id or manifest.get("commit_sha") != ref.commit_sha
            or manifest.get("blob_sha") != ref.blob_sha or manifest.get("path") != ref.path):
        raise _permanent("manifest_mismatch", "GitHub file manifest does not match the observation")
    size = manifest.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise _permanent("manifest_invalid", "GitHub file manifest has no valid size")
    if size > limits.max_bronze_bytes or size > limits.max_payload_bytes:
        raise BronzeIntegrityError("bronze_too_large", "GitHub blob exceeds the size limit", "outcome")
    try:
        content = bronze.read_blob(ref.blob_sha)
    except (UnsafePath, OSError) as exc:
        raise _retryable("bronze_corrupt", str(exc)) from None
    if content is None:
        raise _retryable("bronze_blob_missing", "blob is missing or does not match its git object id")
    if len(content) != size or hashlib.sha256(content).hexdigest() != manifest.get("content_sha256"):
        raise _permanent("content_hash_mismatch", "blob content differs from the manifest hash")
    return Payload(content, None, None, None, None, None)


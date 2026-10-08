"""Parsing and validation of one fetched Common Crawl WARC record.

Input: the exact compressed byte range ``[offset, offset + length)`` of a
WARC file. It must be one complete gzip member holding one WARC ``response``
record whose ``WARC-Target-URI`` equals the index row's URL.

Validation (every failure is ``data_invalid`` for that record):
* one gzip member, no trailing bytes, output bounded (decompression bomb);
* ``WARC/1.0`` or ``WARC/1.1``; bounded header block; ``Content-Length`` present
  and covered by the record;
* ``WARC-Type: response`` with an ``application/http`` block;
* target URI and HTTP status equal to the index row;
* ``WARC-Payload-Digest`` (sha1, base32) recomputed over the payload when it is
  verifiable (no transfer coding), and compared with the index digest.

Metadata taken from the source only (never synthesized):
* ``source_title``: the literal first HTML ``<title>`` text (entity-decoded,
  whitespace-collapsed) or None;
* ``canonical_url``: the literal ``<link rel="canonical" href>`` resolved against
  the target URI, or None;
* page dates (``app.sources.commoncrawl.dates``): published / modified dates the
  publisher declared in the page (JSON-LD, ``<meta>``, ``<time>``) and the HTTP
  ``Last-Modified`` header, each validated against the crawl time; never guessed.

A ``WARC-Record-ID`` is required: it is the original archived document's own
identity and becomes the Gold ``source_id`` (docs/cc_fact_kb_v2.md).
"""

from __future__ import annotations

import base64
import codecs
import hashlib
import html
import re
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urljoin

from app.sources.commoncrawl.compression import CompressionError, decompress_single_member
from app.sources.commoncrawl.dates import PageDates, extract_dates
from app.sources.commoncrawl.index import CdxRecord, InvalidSourceUrl, source_url_host

HTML_METADATA_VERSION = "ccmeta-v2"
MAX_HEADER_BLOCK_BYTES = 64 * 1024
HTML_SCAN_BYTES = 256 * 1024
DATE_SCAN_BYTES = 8 * 1024 * 1024  # dates may sit at the end of a page (Wikipedia's JSON-LD does)
MAX_TITLE_CHARS = 2000
MAX_HEADER_VALUE_CHARS = 1000
HTML_MIMES = frozenset({"text/html", "application/xhtml+xml"})

_STATUS_LINE = re.compile(r"^HTTP/[0-9](?:\.[0-9])? ([1-5][0-9]{2})(?: .*)?$")
_RECORD_ID = re.compile(r"^<urn:uuid:([0-9a-fA-F-]{36})>$")
_CHARSET_PARAM = re.compile(r"charset\s*=\s*[\"']?([A-Za-z0-9._:-]{1,40})", re.I)
_META_CHARSET = re.compile(rb"<meta[^>]{0,512}?charset\s*=\s*[\"']?([A-Za-z0-9._:-]{1,40})", re.I)
_TITLE = re.compile(r"<title\b[^>]*>(.*?)</title\s*>", re.I | re.S)
_LINK_TAG = re.compile(r"<link\b[^>]*>", re.I)
_ATTR = re.compile(r"([A-Za-z_:][A-Za-z0-9_:.-]*)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s\"'>]+))")


class WarcRecordError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class WarcCapture:
    warc_version: str
    warc_type: str
    target_uri: str
    record_id: uuid.UUID | None
    warc_date: datetime | None
    payload_digest: str | None
    payload_digest_verified: bool
    http_status: int
    http_content_type: str | None
    redirect_location: str | None
    source_title: str | None
    canonical_url: str | None
    payload_bytes: int
    page_dates: PageDates = PageDates(None, None, None, None, None)
    warc_headers: dict[str, str] = field(default_factory=dict)
    http_headers: dict[str, str] = field(default_factory=dict)


def _split_head(data: bytes, what: str) -> tuple[bytes, bytes]:
    end = data.find(b"\r\n\r\n", 0, MAX_HEADER_BLOCK_BYTES + 4)
    if end < 0:
        raise WarcRecordError(f"bad_{what}_header", f"{what} header block missing or larger than 64 KiB")
    return data[:end], data[end + 4:]


def _parse_headers(head: bytes, what: str) -> tuple[str, dict[str, str]]:
    lines = head.decode("latin-1").split("\r\n")
    first, headers = lines[0], {}
    last = None
    for line in lines[1:]:
        if line[:1] in (" ", "\t") and last is not None:  # obsolete line folding
            headers[last] = (headers[last] + " " + line.strip()).strip()
            continue
        name, sep, value = line.partition(":")
        if not sep or not name.strip():
            raise WarcRecordError(f"bad_{what}_header", f"malformed {what} header line")
        last = name.strip().lower()
        headers.setdefault(last, value.strip())  # the first occurrence wins
    return first, headers


def _clean(value: str | None, limit: int = MAX_HEADER_VALUE_CHARS) -> str | None:
    """Header value as recorded, or None if unusable (too long / NUL); never altered otherwise."""
    if value is None or not value or len(value) > limit or "\x00" in value:
        return None
    return value


def _sha1_base32(data: bytes) -> str:
    return base64.b32encode(hashlib.sha1(data).digest()).decode("ascii")


def _parse_warc_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _charset(content_type: str | None, body: bytes) -> str | None:
    candidates = []
    if content_type:
        match = _CHARSET_PARAM.search(content_type)
        if match:
            candidates.append(match.group(1))
    match = _META_CHARSET.search(body[:4096])
    if match:
        candidates.append(match.group(1).decode("ascii"))
    candidates.append("utf-8")
    for name in candidates:
        try:
            return codecs.lookup(name).name
        except LookupError:
            return None  # a declared but unknown charset: do not guess
    return None  # pragma: no cover


def _decoded_prefix(body: bytes, content_type: str | None, limit: int = HTML_SCAN_BYTES) -> str | None:
    encoding = _charset(content_type, body)
    if encoding is None:
        return None
    try:
        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
        return decoder.decode(body[:limit], final=False)
    except (UnicodeDecodeError, LookupError, ValueError):
        return None


def extract_title(text: str) -> str | None:
    match = _TITLE.search(text)
    if not match:
        return None
    title = " ".join(html.unescape(match.group(1)).split())
    if not title or len(title) > MAX_TITLE_CHARS:
        return None
    if any(unicodedata.category(ch) in ("Cc", "Cs") for ch in title):
        return None
    return title


def extract_canonical(text: str, base_url: str) -> str | None:
    for tag in _LINK_TAG.finditer(text):
        attrs: dict[str, str] = {}
        for name, v1, v2, v3 in _ATTR.findall(tag.group(0)):
            attrs.setdefault(name.lower(), v1 or v2 or v3)
        if "canonical" not in attrs.get("rel", "").lower().split():
            continue
        href = html.unescape(attrs.get("href", "")).strip()
        if not href:
            return None
        resolved = urljoin(base_url, href)
        try:
            source_url_host(resolved)
        except InvalidSourceUrl:
            return None
        return resolved
    return None


def parse_warc_record(compressed: bytes, expected: CdxRecord, *, max_decompressed: int,
                      verify_payload_digest: bool = True) -> WarcCapture:
    """Validate one fetched record against its index row and extract provenance fields."""
    if len(compressed) != expected.length:
        raise WarcRecordError("length_mismatch", "record bytes differ from the index length")
    try:
        data = decompress_single_member(compressed, max_decompressed)
    except CompressionError as exc:
        raise WarcRecordError(exc.code, str(exc)) from None

    head, rest = _split_head(data, "warc")
    version, warc_headers = _parse_headers(head, "warc")
    if version not in ("WARC/1.0", "WARC/1.1"):
        raise WarcRecordError("unsupported_warc_version", "not a WARC/1.0 or WARC/1.1 record")
    warc_type = warc_headers.get("warc-type", "")
    if warc_type != "response":
        raise WarcRecordError("unsupported_warc_type", f"WARC-Type {warc_type[:40]!r} is not 'response'")
    length_text = warc_headers.get("content-length", "")
    if not length_text.isdigit():
        raise WarcRecordError("bad_warc_header", "WARC Content-Length missing or invalid")
    block_length = int(length_text)
    if block_length > len(rest):
        raise WarcRecordError("truncated_block", "WARC block is shorter than its Content-Length")
    block = rest[:block_length]
    if not warc_headers.get("content-type", "").lower().startswith("application/http"):
        raise WarcRecordError("unsupported_block", "WARC block is not an HTTP message")

    target = warc_headers.get("warc-target-uri", "")
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    if target != expected.url:
        raise WarcRecordError("target_mismatch", "WARC-Target-URI differs from the index URL")

    http_head, body = _split_head(block, "http")
    status_line, http_headers = _parse_headers(http_head, "http")
    match = _STATUS_LINE.match(status_line)
    if not match:
        raise WarcRecordError("bad_http_header", "invalid HTTP status line")
    http_status = int(match.group(1))
    if expected.status is not None and http_status != expected.status:
        raise WarcRecordError("status_mismatch", "HTTP status differs from the index status")

    recorded_digest = _clean(warc_headers.get("warc-payload-digest"), 200)
    verified = False
    if recorded_digest:
        algorithm, _sep, value = recorded_digest.partition(":")
        if expected.digest is not None and algorithm.lower() == "sha1" and value != expected.digest:
            raise WarcRecordError("index_digest_mismatch", "WARC-Payload-Digest differs from the index digest")
        transfer_coded = "transfer-encoding" in http_headers
        if verify_payload_digest and algorithm.lower() == "sha1" and not transfer_coded:
            if _sha1_base32(body) != value:
                raise WarcRecordError("payload_digest_mismatch", "payload does not match WARC-Payload-Digest")
            verified = True
    payload_digest = recorded_digest or (f"sha1:{expected.digest}" if expected.digest else None)

    record_id = None
    id_match = _RECORD_ID.match(warc_headers.get("warc-record-id", ""))
    if id_match:
        try:
            record_id = uuid.UUID(id_match.group(1))
        except ValueError:
            record_id = None
    if record_id is None:
        raise WarcRecordError("missing_record_id", "WARC-Record-ID is missing or not a urn:uuid")
    warc_date = _parse_warc_date(warc_headers.get("warc-date"))
    if warc_date is None:
        raise WarcRecordError("missing_warc_date", "WARC-Date is missing or not an ISO 8601 UTC timestamp")

    content_type = _clean(http_headers.get("content-type"))
    title = canonical = text = None
    encoded = http_headers.get("content-encoding", "identity").lower() not in ("", "identity")
    if expected.effective_mime in HTML_MIMES and not encoded and "transfer-encoding" not in http_headers:
        text = _decoded_prefix(body, content_type)
        if text is not None:
            title = extract_title(text)
            canonical = extract_canonical(text, expected.url)
    date_text = text
    if text is not None and len(body) > HTML_SCAN_BYTES:
        date_text = _decoded_prefix(body, content_type, DATE_SCAN_BYTES) or text
    page_dates = extract_dates(date_text, http_last_modified=http_headers.get("last-modified"), crawled_at=warc_date)

    keep = ("warc-type", "warc-record-id", "warc-date", "warc-target-uri", "warc-payload-digest",
            "warc-block-digest", "warc-identified-payload-type", "warc-ip-address", "warc-truncated",
            "content-type", "content-length")
    return WarcCapture(
        warc_version=version, warc_type=warc_type, target_uri=target, record_id=record_id,
        warc_date=warc_date, payload_digest=payload_digest,
        payload_digest_verified=verified, http_status=http_status, http_content_type=content_type,
        redirect_location=_clean(http_headers.get("location"), 4096), source_title=title,
        canonical_url=canonical, payload_bytes=len(body), page_dates=page_dates,
        warc_headers={k: v for k, v in warc_headers.items() if k in keep and len(v) <= 4096},
        http_headers={k: v for k, v in http_headers.items()
                      if k in ("content-type", "content-length", "content-language", "last-modified",
                               "location", "x-crawler-content-encoding", "x-crawler-transfer-encoding")
                      and len(v) <= 4096},
    )

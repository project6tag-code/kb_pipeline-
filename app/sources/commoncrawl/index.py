"""Common Crawl CDX index: record parsing, validation and index-level filtering.

A CDX line (zipnum shard ``cdx-NNNNN.gz``) is::

    <SURT urlkey> <14-digit timestamp> <JSON object>

The JSON object carries ``url``, ``filename``, ``offset``, ``length``,
``status``, ``mime``, ``mime-detected``, ``digest``, ``languages``,
``charset``, ``truncated``, ... . Every field is treated as untrusted:
``parse_cdx_line`` validates it strictly and raises ``IndexRowError``
(a malformed row) instead of guessing. ``rejection_reason`` then applies the
configured, versioned candidate filter; rejected rows are never fetched and
never stored, only counted (schema §7.1).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

from app.sources.commoncrawl.config import FilterConfig

MAX_URL_CHARS = 4096
MAX_BIGINT = 2 ** 63 - 1
MAX_INTEGER = 2 ** 31 - 1  # cc_captures.warc_record_length is integer

_TIMESTAMP = re.compile(r"^[0-9]{14}$")
_DIGITS = re.compile(r"^[0-9]{1,19}$")
_WARC_FILENAME = re.compile(
    r"^crawl-data/(CC-MAIN-[0-9]{4}-[0-9]{2})/segments/[0-9]+(?:\.[0-9]+)?/"
    r"(warc|crawldiagnostics|robotstxt)/[A-Za-z0-9._-]+\.warc\.gz$")
_DIGEST = re.compile(r"^[A-Z2-7]{32}$")
_CONTROL = re.compile(r"[\x00-\x20\x7f]")

# Index fields preserved verbatim in the Bronze manifest (provenance).
PRESERVED_INDEX_FIELDS = ("url", "mime", "mime-detected", "status", "digest", "length", "offset", "filename",
                          "charset", "languages", "truncated", "redirect")


class IndexRowError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class InvalidSourceUrl(ValueError):
    pass


def source_url_host(url: object) -> str:
    """Validate a captured URL (http/https, host, no whitespace/control) and return its lowercased host."""
    if not isinstance(url, str) or not url or len(url) > MAX_URL_CHARS:
        raise InvalidSourceUrl("url must be a non-empty string of at most 4096 characters")
    if _CONTROL.search(url):
        raise InvalidSourceUrl("url contains whitespace or control characters")
    try:
        parts = urlsplit(url)
        parts.port  # noqa: B018 - raises ValueError on an invalid port
    except ValueError as exc:
        raise InvalidSourceUrl(f"url is not parseable: {exc}") from None
    if parts.scheme.lower() not in ("http", "https"):
        raise InvalidSourceUrl("url scheme must be http or https")
    if not parts.hostname:
        raise InvalidSourceUrl("url has no host")
    return parts.hostname.lower()


@dataclass(frozen=True)
class CdxRecord:
    urlkey: str
    timestamp: str
    url: str
    url_host: str
    crawl_id: str
    filename: str
    record_folder: str  # warc | crawldiagnostics | robotstxt
    offset: int
    length: int
    status: int | None
    mime: str | None
    mime_detected: str | None
    digest: str | None
    languages: tuple[str, ...]
    truncated: str | None
    index_fields: dict[str, Any]

    @property
    def effective_mime(self) -> str | None:
        value = self.mime_detected or self.mime
        if not value:
            return None
        return value.split(";", 1)[0].strip().lower() or None


def _reject_constant(name: str) -> Any:
    raise ValueError(f"JSON constant {name} is not allowed")


def _opt_str(obj: dict[str, Any], key: str, limit: int = 1024) -> str | None:
    value = obj.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > limit or "\x00" in value:
        raise IndexRowError("invalid_field", f"index field {key!r} is not a short string")
    return value


def _int_field(obj: dict[str, Any], key: str, low: int, high: int) -> int:
    value = obj.get(key)
    if isinstance(value, bool):
        raise IndexRowError("invalid_field", f"index field {key!r} is not an integer")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and _DIGITS.match(value):
        number = int(value)
    else:
        raise IndexRowError("invalid_field", f"index field {key!r} is missing or not an integer")
    if not low <= number <= high:
        raise IndexRowError("invalid_field", f"index field {key!r} out of range")
    return number


def parse_cdx_line(line: bytes, crawl_id: str, *, max_line_bytes: int = 64 * 1024) -> CdxRecord:
    """Parse and validate one CDX line of ``crawl_id``; raise IndexRowError on anything malformed."""
    if len(line) > max_line_bytes:
        raise IndexRowError("line_too_long", "index line exceeds the size limit")
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError:
        raise IndexRowError("not_utf8", "index line is not valid UTF-8") from None
    parts = text.rstrip("\r").split(" ", 2)
    if len(parts) != 3:
        raise IndexRowError("bad_layout", "index line is not '<urlkey> <timestamp> <json>'")
    urlkey, timestamp, payload = parts
    if not urlkey or len(urlkey) > MAX_URL_CHARS or _CONTROL.search(urlkey):
        raise IndexRowError("bad_urlkey", "invalid urlkey")
    if not _TIMESTAMP.match(timestamp):
        raise IndexRowError("bad_timestamp", "invalid 14-digit timestamp")
    try:
        obj = json.loads(payload, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        raise IndexRowError("bad_json", "index JSON is not parseable") from None
    if not isinstance(obj, dict):
        raise IndexRowError("bad_json", "index JSON is not an object")

    url = obj.get("url")
    try:
        host = source_url_host(url)
    except InvalidSourceUrl as exc:
        raise IndexRowError("invalid_url", str(exc)) from None
    filename = obj.get("filename")
    match = _WARC_FILENAME.match(filename) if isinstance(filename, str) else None
    if not match:
        raise IndexRowError("invalid_filename", "filename is not a Common Crawl WARC key")
    if match.group(1) != crawl_id:
        raise IndexRowError("crawl_mismatch", "filename belongs to another crawl")
    offset = _int_field(obj, "offset", 0, MAX_BIGINT)
    length = _int_field(obj, "length", 1, MAX_INTEGER)
    status = None
    if obj.get("status") is not None:
        status = _int_field(obj, "status", 100, 599)
    digest = _opt_str(obj, "digest", 64)
    if digest is not None and not _DIGEST.match(digest):
        raise IndexRowError("invalid_field", "digest is not a base32 SHA-1")
    languages_raw = _opt_str(obj, "languages", 256)
    languages = tuple(sorted({code.strip().lower() for code in (languages_raw or "").split(",") if code.strip()}))
    preserved = {k: obj[k] for k in PRESERVED_INDEX_FIELDS if k in obj and isinstance(obj[k], (str, int))}
    return CdxRecord(
        urlkey=urlkey, timestamp=timestamp, url=url, url_host=host, crawl_id=crawl_id, filename=filename,
        record_folder=match.group(2), offset=offset, length=length, status=status,
        mime=_opt_str(obj, "mime", 255), mime_detected=_opt_str(obj, "mime-detected", 255), digest=digest,
        languages=languages, truncated=_opt_str(obj, "truncated", 64), index_fields=preserved,
    )


# --------------------------------------------------------------------------- #
# URL policy (URL_POLICY_VERSION ``url2``): page kinds that are never factual KB material
# --------------------------------------------------------------------------- #

MAX_POLICY_URL_CHARS = 300
MAX_PATH_SEGMENTS = 8
MAX_QUERY_PARAMS = 2

_IPV4 = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
# Whole path segments (case-insensitive, without extension) naming non-content pages.
JUNK_SEGMENTS = frozenset({
    # commerce
    "cart", "basket", "checkout", "shop", "store", "shopping", "product", "products", "catalog", "catalogue",
    "pricing", "price", "order", "orders", "buy", "deals", "coupon", "coupons", "wishlist",
    # accounts / admin
    "login", "logout", "signin", "sign-in", "signup", "sign-up", "register", "account", "my-account", "profile",
    "wp-admin", "wp-login", "admin", "dashboard", "password", "cgi-bin",
    # listings / navigation / feeds
    "search", "tag", "tags", "category", "categories", "archive", "archives", "author", "authors", "feed", "feeds",
    "rss", "atom", "sitemap", "print", "share", "calendar", "comment", "comments", "attachment",
    # legal / corporate boilerplate
    "privacy", "privacy-policy", "terms", "terms-of-use", "terms-of-service", "terms-and-conditions", "tos",
    "cookie", "cookies", "cookie-policy", "disclaimer", "legal", "imprint", "impressum", "contact", "contact-us",
    "careers", "jobs", "advertise", "advertising", "subscribe", "newsletter", "unsubscribe", "donate",
    # test / sample / synthetic areas
    "test", "tests", "testing", "sandbox", "demo", "demos", "sample", "samples", "example", "examples",
    "dummy", "staging", "dev", "lorem-ipsum", "placeholder",
})
_JUNK_QUERY_KEYS = re.compile(r"^(?:utm_\w*|sessionid|sid|phpsessid|jsessionid|replytocom|share|print|printable"
                              r"|action|oldid|diff|curid|redlink|sort|order|orderby|filter|filters|page|paged|p"
                              r"|start|offset|limit|lang|ref|source|fbclid|gclid|add-to-cart|attachment_id)$", re.I)
# MediaWiki non-article namespaces (Talk:, User:, Special:, File:, Category: ...).
_WIKI_NAMESPACE = re.compile(r"^(?:special|talk|user|user_talk|file|file_talk|template|template_talk|help"
                             r"|help_talk|category|category_talk|portal|portal_talk|wikipedia|wikipedia_talk|wp"
                             r"|draft|draft_talk|mediawiki|module|timedtext|book|media|project|image)(?:_talk)?:",
                             re.I)


def host_matches(host: str, domains: tuple[str, ...]) -> bool:
    """``host`` equals a listed domain or is one of its subdomains."""
    host = host.lower().removeprefix("www.")
    return any(host == d or host.endswith("." + d) for d in domains)


def url_rejection(url: str) -> str | None:
    """Why ``url`` is not a candidate under URL policy ``url1``, or None. Pure; never fetches."""
    if len(url) > MAX_POLICY_URL_CHARS:
        return "url_too_long"
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return "url_unparseable"
    host = (parts.hostname or "").lower()
    if _IPV4.match(host) or ":" in host:
        return "ip_host"
    if port is not None and port not in (80, 443):
        return "non_default_port"
    if "xn--" in host:
        return "punycode_host"
    if host.count("-") >= 4 or any(len(label) > 40 for label in host.split(".")):
        return "suspicious_host"
    segments = [unquote(s) for s in parts.path.split("/") if s]
    if not segments or (len(segments) == 1 and segments[0].lower() in ("index.html", "index.htm", "index.php",
                                                                        "default.aspx", "home", "home.html")):
        return "homepage"  # a site's front page is a navigation / news hub, not an article
    if len(segments) > MAX_PATH_SEGMENTS:
        return "deep_path"
    for index, segment in enumerate(segments):
        if segment.lower() == "page" and index + 1 < len(segments) and segments[index + 1].isdigit():
            return "pagination"
        stem = segment.lower().rsplit(".", 1)[0]
        if stem in JUNK_SEGMENTS:
            return f"junk_path:{stem}"
        if _WIKI_NAMESPACE.match(segment):
            return "wiki_namespace"
    if segments and segments[-1].lower().rsplit(".", 1)[-1] in ("xml", "json", "rss", "atom", "txt", "csv"):
        return "data_file"
    if parts.query:
        params = [kv for kv in parts.query.split("&") if kv]
        if len(params) > MAX_QUERY_PARAMS:
            return "query_too_complex"
        for param in params:
            if _JUNK_QUERY_KEYS.match(unquote(param.split("=", 1)[0])):
                return "junk_query"
    return None


def rejection_reason(record: CdxRecord, config: FilterConfig) -> str | None:
    """Return why ``record`` is not an acquisition candidate, or None if it is one."""
    if config.profile == "reference" and not host_matches(record.url_host, config.reference_domains):
        return "not_reference_domain"
    url_reason = url_rejection(record.url)
    if url_reason is not None:
        return url_reason
    if record.record_folder != "warc":
        return "record_type"  # crawldiagnostics / robotstxt are not page responses
    if record.status != config.required_status:
        return "status"
    mime = record.effective_mime
    if mime is None or mime not in config.mime_types:
        return "mime"
    if config.languages and record.languages and not set(record.languages) & set(config.languages):
        return "language"
    if config.reject_truncated and record.truncated:
        return "truncated"
    if record.length > config.max_record_bytes:
        return "too_large"
    return None


def split_block_lines(block: bytes) -> list[bytes]:
    """The lines of a decompressed CDX block (a trailing newline does not add a line)."""
    lines = block.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return lines

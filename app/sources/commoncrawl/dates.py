"""Publication / modification dates of one archived page (DATE_EXTRACTOR_VERSION ``ccdate-v1``).

Only evidence that the publisher put into the page or its HTTP response is
used; nothing is guessed from the URL or the text. The crawl date
(``WARC-Date``) is recorded separately and is never presented as a page date
here: the Gold publisher applies the documented crawl-date fallback.

Evidence, strongest first (the first valid value of each kind wins):

| kind      | sources                                                                                  |
|-----------|------------------------------------------------------------------------------------------|
| published | JSON-LD ``datePublished`` / ``dateCreated``; ``<meta>`` ``article:published_time``,      |
|           | ``og:published_time``, ``citation_publication_date``, ``citation_date``,                 |
|           | ``dc.date.issued``, ``dcterms.issued``, ``dcterms.created``, ``dc.date``, ``date``,      |
|           | ``pubdate``, ``publish-date``, ``itemprop=datePublished``; ``<time itemprop=datePublished>`` |
| modified  | JSON-LD ``dateModified``; ``<meta>`` ``article:modified_time``, ``og:updated_time``,     |
|           | ``dcterms.modified``, ``dc.date.modified``, ``last-modified``, ``revised``,             |
|           | ``itemprop=dateModified``; ``<time itemprop=dateModified>``                               |
| http      | the HTTP ``Last-Modified`` response header (RFC 9110 IMF-fixdate)                         |

Validation (a value failing any rule is discarded, never clamped):
* parseable ISO 8601 date or date-time; a date-only value means 00:00 UTC and a
  date-time without an offset is read as UTC;
* not before 1990-01-01 (before the web) and not later than the crawl time plus
  one day (a page cannot be published after it was archived);
* a modified date earlier than the published date is discarded.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

DATE_EXTRACTOR_VERSION = "ccdate-v1"
EARLIEST = datetime(1990, 1, 1, tzinfo=timezone.utc)
FUTURE_TOLERANCE = timedelta(days=1)
MAX_JSONLD_BLOCKS = 20
MAX_JSONLD_CHARS = 200_000
MAX_JSONLD_NODES = 2_000

_META_TAG = re.compile(r"<meta\b[^>]*>", re.I)
_TIME_TAG = re.compile(r"<time\b[^>]*>", re.I)
_ATTR = re.compile(r"([A-Za-z_:][A-Za-z0-9_:.-]*)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s\"'>]+))")
_JSONLD = re.compile(r"<script\b[^>]*type\s*=\s*[\"']?application/ld\+json[\"']?[^>]*>(.*?)</script\s*>", re.I | re.S)
_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:[.,]\d{1,9})?)?"
                  r"\s*(Z|[+-]\d{2}:?\d{2})?)?$", re.I)
_COMPACT = re.compile(r"^(\d{4})(\d{2})(\d{2})$")

# <meta name|property|itemprop = KEY content=...>, lower-cased keys, strongest first.
PUBLISHED_META = ("article:published_time", "og:published_time", "citation_publication_date", "citation_date",
                  "dc.date.issued", "dcterms.issued", "dcterms.created", "dc.date.created", "dc.date", "date",
                  "pubdate", "publish-date", "publish_date", "datepublished")
MODIFIED_META = ("article:modified_time", "og:updated_time", "dcterms.modified", "dc.date.modified",
                 "last-modified", "revised", "datemodified")
PUBLISHED_JSONLD = ("datePublished", "dateCreated")
MODIFIED_JSONLD = ("dateModified",)


@dataclass(frozen=True)
class PageDates:
    published_at: datetime | None
    published_evidence: str | None
    modified_at: datetime | None
    modified_evidence: str | None
    http_last_modified: datetime | None


def parse_iso(value: object) -> datetime | None:
    """ISO 8601 date or date-time -> aware UTC datetime, or None (never guesses a format)."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if len(text) > 64:
        return None
    compact = _COMPACT.match(text)
    if compact:
        text = f"{compact.group(1)}-{compact.group(2)}-{compact.group(3)}"
    match = _ISO.match(text)
    if not match:
        return None
    year, month, day, hour, minute, second, offset = match.groups()
    try:
        moment = datetime(int(year), int(month), int(day), int(hour or 0), int(minute or 0), int(second or 0))
    except ValueError:
        return None
    tz = timezone.utc
    if offset and offset.upper() != "Z":
        sign = 1 if offset[0] == "+" else -1
        digits = offset[1:].replace(":", "")
        hours, minutes = int(digits[:2]), int(digits[2:])
        if hours > 23 or minutes > 59:
            return None
        tz = timezone(sign * timedelta(hours=hours, minutes=minutes))
    return moment.replace(tzinfo=tz).astimezone(timezone.utc)


def parse_http_date(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip() or len(value) > 64:
        return None
    try:
        parsed = parsedate_to_datetime(value.strip())
    except (TypeError, ValueError, IndexError):
        return None
    if parsed is None or parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _plausible(moment: datetime | None, crawled_at: datetime | None) -> bool:
    if moment is None or moment < EARLIEST:
        return False
    return crawled_at is None or moment <= crawled_at + FUTURE_TOLERANCE


def _attrs(tag: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for name, v1, v2, v3 in _ATTR.findall(tag):
        attrs.setdefault(name.lower(), html.unescape(v1 or v2 or v3))
    return attrs


def jsonld_nodes(text: str) -> list[dict]:
    """Bounded JSON-LD objects of a page (lists, ``@graph`` and ``mainEntity`` flattened); bad blocks skipped."""
    nodes: list[dict] = []
    for index, match in enumerate(_JSONLD.finditer(text)):
        if index >= MAX_JSONLD_BLOCKS:
            break
        raw = match.group(1).strip()
        if not raw or len(raw) > MAX_JSONLD_CHARS:
            continue
        try:
            data = json.loads(raw)
        except (ValueError, RecursionError):
            continue
        stack = [data]
        while stack and len(nodes) < MAX_JSONLD_NODES:
            item = stack.pop(0)
            if isinstance(item, list):
                stack.extend(item)
            elif isinstance(item, dict):
                nodes.append(item)
                graph = item.get("@graph")
                if isinstance(graph, list):
                    stack.extend(graph)
                main = item.get("mainEntity") or item.get("mainEntityOfPage")
                if isinstance(main, (dict, list)):
                    stack.append(main)
    return nodes


def _from_jsonld(nodes: list[dict], keys: tuple[str, ...], crawled_at: datetime | None) -> tuple[datetime, str] | None:
    for key in keys:
        for node in nodes:
            moment = parse_iso(node.get(key))
            if _plausible(moment, crawled_at):
                return moment, f"jsonld:{key}"
    return None


def _from_meta(metas: list[dict[str, str]], times: list[dict[str, str]], keys: tuple[str, ...], itemprop: str,
               crawled_at: datetime | None) -> tuple[datetime, str] | None:
    for key in keys:
        for attrs in metas:
            name = (attrs.get("property") or attrs.get("name") or attrs.get("itemprop") or "").strip().lower()
            if name == key:
                moment = parse_iso(attrs.get("content"))
                if _plausible(moment, crawled_at):
                    return moment, f"meta:{key}"
    for attrs in times:
        if attrs.get("itemprop", "").strip().lower() == itemprop.lower():
            moment = parse_iso(attrs.get("datetime"))
            if _plausible(moment, crawled_at):
                return moment, f"time:{itemprop}"
    return None


def extract_dates(text: str | None, *, http_last_modified: str | None, crawled_at: datetime | None) -> PageDates:
    """Page dates from decoded HTML (may be None for non-HTML) and the HTTP Last-Modified header."""
    published = modified = None
    if text:
        nodes = jsonld_nodes(text)
        metas = [_attrs(m.group(0)) for m in _META_TAG.finditer(text)]
        times = [_attrs(t.group(0)) for t in _TIME_TAG.finditer(text)]
        published = (_from_jsonld(nodes, PUBLISHED_JSONLD, crawled_at)
                     or _from_meta(metas, times, PUBLISHED_META, "datePublished", crawled_at))
        modified = (_from_jsonld(nodes, MODIFIED_JSONLD, crawled_at)
                    or _from_meta(metas, times, MODIFIED_META, "dateModified", crawled_at))
    if published and modified and modified[0] < published[0]:
        modified = None
    last_modified = parse_http_date(http_last_modified)
    if not _plausible(last_modified, crawled_at):
        last_modified = None
    return PageDates(
        published_at=published[0] if published else None, published_evidence=published[1] if published else None,
        modified_at=modified[0] if modified else None, modified_evidence=modified[1] if modified else None,
        http_last_modified=last_modified,
    )

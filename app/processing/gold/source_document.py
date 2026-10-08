"""Gold V2 source-document identity, dates and the per-document ``public.datasets`` row (pure).

``public.metadata.source_id`` identifies the ORIGINAL source document of a
chunk, i.e. of its representative observation (docs/cc_fact_kb_v2.md):

| channel       | source_id                                                         | why                                   |
|---------------|-------------------------------------------------------------------|---------------------------------------|
| common_crawl  | the record's own ``WARC-Record-ID`` (a UUID assigned by the archive) | the archived document's native id |
| github        | UUIDv5(KB_NAMESPACE, ``github-blob:v1:<blob_sha>``)               | the git blob SHA is the file's native id; ``source_id`` is a uuid column, so it is derived deterministically and the raw SHA, repository and path are kept in the dataset row |

Every chunk of one original document shares that id; different documents
never do. ``metadata.source_id`` must reference ``public.datasets(id)``, so
one ``KB_SOURCE_DOCUMENT`` dataset row per source document is written first,
under the channel's ``dataset_sources`` parent.

Dates (``public.metadata``; never NULL for new rows):

| column       | value                                                                                       |
|--------------|---------------------------------------------------------------------------------------------|
| crawled_at   | CC: ``WARC-Date`` (``source_dates.crawled_at``); GitHub: acquisition time                    |
| published_at | the page's declared publication date; else the crawl date (fallback)                        |
| updated_at   | first of: declared modification date, HTTP Last-Modified, declared publication date,      |
|              | crawl date, that is not earlier than ``published_at``; else ``published_at``                |

GitHub: published_at = updated_at = the snapshot commit's ``committed_at`` (else acquisition time).
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from app.processing.gold_bridge import spec as bridge_spec
from app.utils.ids import KB_NAMESPACE

SOURCE_DOCUMENT_VERSION = "srcdoc-v2"  # v2: dataset row records document_type; metadata.source_type is it
STATUS_SOURCE_DOCUMENT = "KB_SOURCE_DOCUMENT"
GITHUB_BLOB_PREFIX = "github-blob:v1:"
MAX_DATASET_NAME = 255
_BLOB_SHA = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class SourceDocumentError(ValueError):
    """The representative observation lacks the provenance needed for a Gold row."""


_CC_MIME_TYPES = {
    "text/html": "web_page", "application/xhtml+xml": "web_page", "application/pdf": "pdf_document",
    "text/plain": "plain_text", "text/xml": "xml_document", "application/xml": "xml_document",
    "application/json": "data_file",
}
_GITHUB_EXTENSIONS = {
    **dict.fromkeys((".md", ".markdown", ".mdx"), "markdown_document"),
    **dict.fromkeys((".rst", ".adoc", ".asciidoc", ".org", ".tex", ".txt"), "documentation_text"),
    **dict.fromkeys((".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".xml"), "configuration_file"),
}
SOURCE_TYPES = frozenset({"web_page", "pdf_document", "plain_text", "xml_document", "data_file",
                          "markdown_document", "documentation_text", "configuration_file", "source_code"})


def document_type(row: Mapping[str, Any]) -> str:
    """The real document type of the representative source (``public.metadata.source_type``), not its channel.

    Common Crawl: from the capture's detected MIME type (else its HTTP Content-Type, else the URL extension);
    GitHub: from the file extension. Always one of ``SOURCE_TYPES``.
    """
    channel = row["source_channel"]
    if channel == "github":
        path = str(row.get("file_path") or "").lower()
        extension = path[path.rfind("."):] if "." in path.rsplit("/", 1)[-1] else ""
        return _GITHUB_EXTENSIONS.get(extension, "source_code")
    for candidate in (row.get("detected_mime"), row.get("http_content_type")):
        mime = str(candidate or "").split(";", 1)[0].strip().lower()
        if mime in _CC_MIME_TYPES:
            return _CC_MIME_TYPES[mime]
    if str(row.get("source_url") or "").lower().split("?", 1)[0].endswith(".pdf"):
        return "pdf_document"
    return "web_page"


def github_source_id(blob_sha: str) -> uuid.UUID:
    if not isinstance(blob_sha, str) or not _BLOB_SHA.match(blob_sha):
        raise SourceDocumentError("GitHub source document needs a hex blob SHA")
    return uuid.uuid5(KB_NAMESPACE, GITHUB_BLOB_PREFIX + blob_sha)


def source_document_id(row: Mapping[str, Any]) -> uuid.UUID:
    channel = row["source_channel"]
    if channel == "common_crawl":
        record_id = row.get("warc_record_id")
        if not isinstance(record_id, uuid.UUID):
            raise SourceDocumentError(f"chunk {row['chunk_id']}: representative capture has no WARC-Record-ID")
        return record_id
    if channel == "github":
        return github_source_id(row.get("blob_sha"))
    raise SourceDocumentError(f"unsupported source channel {channel!r}")


def _first_not_before(floor: datetime, *candidates: datetime | None) -> datetime:
    for candidate in candidates:
        if candidate is not None and candidate >= floor:
            return candidate
    return floor


def gold_dates(row: Mapping[str, Any]) -> dict[str, Any]:
    """published_at / updated_at / crawled_at plus where each came from (for the dataset row)."""
    channel = row["source_channel"]
    if channel == "common_crawl":
        crawled = row.get("crawled_at")
        if crawled is None:
            raise SourceDocumentError(f"chunk {row['chunk_id']}: no kbp.source_dates row (crawl date unknown)")
        page_published, page_modified = row.get("page_published_at"), row.get("page_modified_at")
        published = page_published or crawled
        updated = _first_not_before(published, page_modified, row.get("http_last_modified"), page_published, crawled)
        published_from = row.get("published_evidence") if page_published else "crawl_date_fallback"
        if updated == page_modified:
            updated_from = row.get("modified_evidence")
        elif updated == row.get("http_last_modified"):
            updated_from = "http:last-modified"
        elif page_published is not None and updated == page_published:
            updated_from = "published_date"
        else:
            updated_from = "crawl_date_fallback"
    elif channel == "github":
        crawled = row.get("acquired_at")
        if crawled is None:
            raise SourceDocumentError(f"chunk {row['chunk_id']}: observation has no acquisition time")
        committed = row.get("committed_at")
        published = updated = committed or crawled
        published_from = updated_from = "git:committed_at" if committed else "crawl_date_fallback"
    else:
        raise SourceDocumentError(f"unsupported source channel {channel!r}")
    return {"published_at": published, "updated_at": updated, "crawled_at": crawled,
            "published_from": published_from, "updated_from": updated_from}


def dataset_row(row: Mapping[str, Any], source_id: uuid.UUID, dates: Mapping[str, Any]) -> dict[str, Any]:
    """The deterministic ``public.datasets`` row registering one original source document."""
    channel = row["source_channel"]
    name = (row.get("source_title") or row["source_url"]).strip()[:MAX_DATASET_NAME]
    metadata: dict[str, Any] = {
        "purpose": "dca_v2_source_document", "version": SOURCE_DOCUMENT_VERSION, "source_channel": channel,
        "document_type": document_type(row), "source_document_id": str(source_id), "source_url": row["source_url"],
        "crawled_at": dates["crawled_at"].isoformat(), "published_from": dates["published_from"],
        "updated_from": dates["updated_from"],
    }
    if channel == "common_crawl":
        metadata.update(warc_record_id=str(row["warc_record_id"]), crawl_id=row.get("crawl_id"),
                        warc_filename=row.get("warc_filename"), warc_record_offset=row.get("warc_record_offset"),
                        warc_record_length=row.get("warc_record_length"), payload_digest=row.get("payload_digest"))
    else:
        metadata.update(blob_sha=row.get("blob_sha"), repository_id=row.get("repository_id"),
                        commit_sha=row.get("commit_sha"), file_path=row.get("file_path"))
    return {"id": source_id, "source_id": bridge_spec.dataset_source_id(channel), "dataset_name": name,
            "status": STATUS_SOURCE_DOCUMENT, "metadata": metadata}

"""PostgreSQL persistence for Common Crawl Acquisition V1 (role kb_pipeline_app).

Uses exactly the Schema V1 tables and the grants of
docs/database_privileges_v1.md §5.1: INSERT (never UPDATE or DELETE) on
``cc_crawls``, ``cc_warc_files``, ``source_observations`` and ``cc_captures``,
plus ``source_dates`` (Migration 001E); dead letters and rate limits through
``app.db.repositories``.

``source_observations.source_published_at`` holds only a publication date the
page itself declared (``app.sources.commoncrawl.dates``), or NULL.

Conflict targets follow schema §20:
* ``cc_crawls (crawl_id)``: DO NOTHING
* ``cc_warc_files (warc_filename)``: DO NOTHING, then SELECT the id
* ``source_observations (observation_id)``: DO NOTHING
* ``source_dates (observation_id)``: DO NOTHING
* ``cc_captures (observation_id)``: DO NOTHING; the natural key
  UNIQUE (warc_file_id, warc_record_offset) is deliberately left to raise
  (a derivation bug must fail loudly, schema §19).

Every statement is a module constant ending in ``_SQL`` and is checked by
tests/unit/test_cc_sql.py against the migration's columns and grants.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from app.db import repositories
from app.queue.states import DeadLetterItemType, Disposition, ErrorClass, SourceChannel, Stage
from app.sources.commoncrawl.dates import DATE_EXTRACTOR_VERSION, PageDates
from app.utils.time import ensure_utc

INDEX_KIND = "cdx"


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


INSERT_CRAWL_SQL = """
INSERT INTO kbp.cc_crawls (crawl_id, index_kind, registered_at)
VALUES (:crawl_id, :index_kind, now())
ON CONFLICT (crawl_id) DO NOTHING
RETURNING crawl_id
"""

INSERT_WARC_FILE_SQL = """
INSERT INTO kbp.cc_warc_files (crawl_id, warc_filename, registered_at)
VALUES (:crawl_id, :warc_filename, now())
ON CONFLICT (warc_filename) DO NOTHING
RETURNING warc_file_id
"""

GET_WARC_FILE_SQL = """
SELECT warc_file_id, crawl_id FROM kbp.cc_warc_files WHERE warc_filename = :warc_filename
"""

EXISTING_CAPTURES_SQL = """
SELECT observation_id FROM kbp.cc_captures WHERE observation_id = ANY (CAST(:observation_ids AS uuid[]))
"""

INSERT_OBSERVATION_SQL = """
INSERT INTO kbp.source_observations (observation_id, source_channel, source_url, url_host, source_title,
                                     source_published_at, acquired_at, run_id)
VALUES (:observation_id, 'common_crawl', :source_url, :url_host, :source_title, :source_published_at, :acquired_at,
        :run_id)
ON CONFLICT (observation_id) DO NOTHING
RETURNING observation_id
"""

INSERT_CAPTURE_SQL = """
INSERT INTO kbp.cc_captures (observation_id, warc_file_id, warc_record_offset, warc_record_length, warc_record_id,
                             warc_date, http_status, http_content_type, detected_mime, redirect_location,
                             canonical_url, payload_digest)
VALUES (:observation_id, :warc_file_id, :warc_record_offset, :warc_record_length, :warc_record_id, :warc_date,
        :http_status, :http_content_type, :detected_mime, :redirect_location, :canonical_url, :payload_digest)
ON CONFLICT (observation_id) DO NOTHING
RETURNING observation_id
"""

INSERT_SOURCE_DATES_SQL = """
INSERT INTO kbp.source_dates (observation_id, extractor_version, published_at, published_evidence, modified_at,
                              modified_evidence, http_last_modified, crawled_at, recorded_at)
VALUES (:observation_id, :extractor_version, :published_at, :published_evidence, :modified_at, :modified_evidence,
        :http_last_modified, :crawled_at, now())
ON CONFLICT (observation_id) DO NOTHING
"""

PARTITION_SUMMARY_SQL = """
SELECT status, count(*) AS jobs, sum(attempt_count) AS attempts, sum(deferral_count) AS deferrals
FROM kbp.pipeline_jobs
WHERE job_type = 'cc_index_partition'
GROUP BY status
ORDER BY status
"""

CAPTURE_COUNT_SQL = """
SELECT w.crawl_id, count(*) AS captures
FROM kbp.cc_captures AS c
JOIN kbp.cc_warc_files AS w ON w.warc_file_id = c.warc_file_id
GROUP BY w.crawl_id
ORDER BY w.crawl_id
"""


class WarcFileMismatch(RuntimeError):
    """A WARC file is registered under a different crawl (index inconsistency)."""


@dataclass(frozen=True)
class CaptureRow:
    """One acquired WARC record: a source_observations row plus its cc_captures row."""

    observation_id: uuid.UUID
    crawl_id: str
    warc_filename: str
    warc_record_offset: int
    warc_record_length: int
    source_url: str
    url_host: str | None
    source_title: str | None
    acquired_at: datetime
    run_id: uuid.UUID | None
    warc_record_id: uuid.UUID | None
    warc_date: datetime | None
    http_status: int | None
    http_content_type: str | None
    detected_mime: str | None
    redirect_location: str | None
    canonical_url: str | None
    payload_digest: str | None
    page_dates: PageDates = PageDates(None, None, None, None, None)

    def __post_init__(self) -> None:
        object.__setattr__(self, "acquired_at", ensure_utc(self.acquired_at))
        if self.warc_record_id is None or self.warc_date is None:
            raise ValueError("a capture needs its WARC-Record-ID and WARC-Date (source identity and crawl date)")
        for name in ("source_url", "url_host", "source_title", "http_content_type", "detected_mime",
                     "redirect_location", "canonical_url", "payload_digest"):
            value = getattr(self, name)
            if isinstance(value, str) and "\x00" in value:
                raise ValueError(f"{name} contains U+0000")


class CaptureStore(Protocol):
    """Connection-scoped persistence used by the acquisition handler and planner."""

    def register_crawl(self, conn: Any, crawl_id: str) -> bool: ...
    def existing_capture_ids(self, conn: Any, observation_ids: Iterable[uuid.UUID]) -> set[uuid.UUID]: ...
    def persist_capture(self, conn: Any, row: CaptureRow) -> bool: ...
    def record_item_dead_letter(self, conn: Any, *, item_key: str, stage: Stage, error_class: ErrorClass,
                                error_code: str, error_message: str, attempt_count: int, job_id: int | None,
                                run_id: uuid.UUID | None, payload_ref: str | None) -> None: ...
    def blocked_until(self, conn: Any, scope: str) -> datetime | None: ...
    def record_rate_limit(self, conn: Any, scope: str, *, http_status: int | None,
                          retry_after_seconds: int | None) -> None: ...


class PostgresCaptureStore:
    """``CaptureStore`` on kbp Schema V1.

    WARC file ids are deliberately not cached across transactions: an id
    inserted by a transaction that later rolls back must never be reused.
    """

    def register_crawl(self, conn: Any, crawl_id: str) -> bool:
        row = conn.execute(_text(INSERT_CRAWL_SQL), {"crawl_id": crawl_id, "index_kind": INDEX_KIND}).one_or_none()
        return row is not None

    def warc_file_id(self, conn: Any, crawl_id: str, warc_filename: str) -> int:
        row = conn.execute(_text(INSERT_WARC_FILE_SQL), {"crawl_id": crawl_id,
                                                         "warc_filename": warc_filename}).one_or_none()
        if row is not None:
            return int(row[0])
        # Inserted earlier (by this or a committed transaction). A new statement
        # snapshot sees a concurrent committed insert (READ COMMITTED).
        existing = conn.execute(_text(GET_WARC_FILE_SQL), {"warc_filename": warc_filename}).one()
        if existing[1] != crawl_id:
            raise WarcFileMismatch(f"{warc_filename} is registered under crawl {existing[1]}")
        return int(existing[0])

    def existing_capture_ids(self, conn: Any, observation_ids: Iterable[uuid.UUID]) -> set[uuid.UUID]:
        ids = [str(i) for i in observation_ids]
        if not ids:
            return set()
        rows = conn.execute(_text(EXISTING_CAPTURES_SQL), {"observation_ids": ids}).all()
        return {uuid.UUID(str(r[0])) for r in rows}

    def persist_capture(self, conn: Any, row: CaptureRow) -> bool:
        """Insert the observation and its capture; False if it already existed (idempotent)."""
        file_id = self.warc_file_id(conn, row.crawl_id, row.warc_filename)
        conn.execute(_text(INSERT_OBSERVATION_SQL), {
            "observation_id": row.observation_id, "source_url": row.source_url, "url_host": row.url_host,
            "source_title": row.source_title, "source_published_at": row.page_dates.published_at,
            "acquired_at": row.acquired_at, "run_id": row.run_id})
        inserted = conn.execute(_text(INSERT_CAPTURE_SQL), {
            "observation_id": row.observation_id, "warc_file_id": file_id,
            "warc_record_offset": row.warc_record_offset, "warc_record_length": row.warc_record_length,
            "warc_record_id": row.warc_record_id, "warc_date": row.warc_date, "http_status": row.http_status,
            "http_content_type": row.http_content_type, "detected_mime": row.detected_mime,
            "redirect_location": row.redirect_location, "canonical_url": row.canonical_url,
            "payload_digest": row.payload_digest}).one_or_none()
        dates = row.page_dates
        conn.execute(_text(INSERT_SOURCE_DATES_SQL), {
            "observation_id": row.observation_id, "extractor_version": DATE_EXTRACTOR_VERSION,
            "published_at": dates.published_at, "published_evidence": dates.published_evidence,
            "modified_at": dates.modified_at, "modified_evidence": dates.modified_evidence,
            "http_last_modified": dates.http_last_modified, "crawled_at": row.warc_date})
        return inserted is not None

    def record_item_dead_letter(self, conn: Any, *, item_key: str, stage: Stage, error_class: ErrorClass,
                                error_code: str, error_message: str, attempt_count: int, job_id: int | None,
                                run_id: uuid.UUID | None, payload_ref: str | None) -> None:
        repositories.record_dead_letter(
            conn, item_type=DeadLetterItemType.OBSERVATION, item_key=item_key, stage=stage,
            disposition=Disposition.DEAD_LETTER, error_class=error_class, error_code=error_code,
            error_message=error_message, attempt_count=attempt_count, job_id=job_id, run_id=run_id,
            payload_ref=payload_ref)

    def blocked_until(self, conn: Any, scope: str) -> datetime | None:
        return repositories.rate_limit_blocked_until(conn, SourceChannel.COMMON_CRAWL, scope)

    def record_rate_limit(self, conn: Any, scope: str, *, http_status: int | None,
                          retry_after_seconds: int | None) -> None:
        repositories.record_rate_limit(conn, repositories.RateLimitObservation(
            SourceChannel.COMMON_CRAWL, scope, retry_after_seconds=retry_after_seconds,
            last_http_status=http_status))


def partition_summary(conn: Any) -> list[Mapping[str, Any]]:
    return [dict(r) for r in conn.execute(_text(PARTITION_SUMMARY_SQL)).mappings()]


def capture_counts(conn: Any) -> list[Mapping[str, Any]]:
    return [dict(r) for r in conn.execute(_text(CAPTURE_COUNT_SQL)).mappings()]

"""Common Crawl Acquisition V1: the ``cc_index_partition`` job handler.

One job = one index partition (a contiguous range of CDX blocks of one shard,
see ``discovery``). For each block, in order:

1. fetch the block by byte range, decompress it (bounded), split lines;
2. per line: parse and validate (malformed -> item dead letter), apply the
   versioned candidate filter (rejected -> counted only), derive the LOCKED
   observation id ``cc_observation_id(crawl_id, filename, offset, length)``;
3. skip observations already captured (DB) whose Bronze evidence is complete;
   reuse complete local Bronze evidence without network (crash recovery);
4. fetch the remaining records by byte range with a bounded thread pool and a
   bounded in-flight window, consumed strictly in index order;
5. validate each WARC record, store Bronze evidence (atomic, no clobber), then
   persist ``source_observations`` + ``cc_captures`` for a batch of lines in ONE
   transaction together with the job checkpoint ``{block, line}``.

Failures
--------
* Item level (the unit continues): malformed index rows, permanent HTTP
  statuses (404/410/416/...), invalid or mismatching WARC records, Bronze
  conflicts -> ``kbp.dead_letters`` (item_type ``observation``).
* Unit level (Runtime Engine retry policy): transient network/5xx/timeout/short
  reads -> ``transient``; 429/503 -> ``rate_limited`` (deferred, attempt not
  consumed, ``Retry-After`` honoured, ``source_rate_limits`` updated); disk
  guard -> ``resource_exhausted``. The processed prefix is committed with the
  checkpoint first, so the retry resumes exactly at the failing record.
* A record that causes ``item_transient_limit`` unit retries is dead-lettered
  (``item_retry_limit``) so one poisoned record cannot block a partition.
* Shutdown: in-flight fetches finish, the processed prefix is committed and the
  job is handed back (``Yielded``, attempt not consumed).

Silver (optional, ``silver=``): with a Bronze -> Silver processor attached the
unit continues to the SILVER stage (schema §9.3, §16.3): each batch's new and
already-present observations are evaluated from verified Bronze before the
batch transaction, and their extraction/document/dedup rows are written in the
SAME transaction as the observations and the checkpoint. Without it,
acquisition is unchanged and the unit completes at ACQUIRED.
"""

from __future__ import annotations

import re
import time
import uuid
from collections import Counter, deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.processing.silver.model import SourceRef
from app.queue.job_queue import ClaimedJob
from app.queue.retry import JobFailure, RetryPolicy
from app.queue.states import ErrorClass, Stage
from app.sources.commoncrawl.bronze import BronzeConflict, BronzeCorrupt, BronzeStore, DiskSpaceError, UnsafePath
from app.sources.commoncrawl.compression import CompressionError, decompress_single_member
from app.sources.commoncrawl.config import URL_POLICY_VERSION, AcquisitionConfig, FilterConfig
from app.sources.commoncrawl.dates import DATE_EXTRACTOR_VERSION
from app.sources.commoncrawl.discovery import CHECKPOINT_VERSION
from app.sources.commoncrawl.http import FetchError, HttpClient
from app.sources.commoncrawl.index import CdxRecord, IndexRowError, parse_cdx_line, rejection_reason, split_block_lines
from app.sources.commoncrawl.repository import CaptureRow, CaptureStore
from app.sources.commoncrawl.warc import HTML_METADATA_VERSION, WarcCapture, WarcRecordError, parse_warc_record
from app.utils.ids import cc_observation_id, cc_observation_payload
from app.utils.time import utc_now
from app.workers.registry import Completed, JobContext, Yielded

# Per-job-type retry configuration (schema §17.1; O-20: benchmark and tune).
CC_PARTITION_POLICY = RetryPolicy(max_attempts=8, base_delay_seconds=60.0, max_delay_seconds=3600.0,
                                  rate_limit_jitter_seconds=30.0)
DATA_SCOPE = "data.commoncrawl.org"  # source_rate_limits.limit_scope for WARC and index range fetches
BRONZE_MANIFEST_SOURCE = "common_crawl"

_PARTITION = re.compile(
    r"^(CC-MAIN-[0-9]{4}-[0-9]{2})/(cc-index/collections/(CC-MAIN-[0-9]{4}-[0-9]{2})/indexes/cdx-[0-9]{5}\.gz)"
    r"#filter=([A-Za-z0-9._:/+-]+)#range=([0-9]+)-([0-9]+)$")
_ITEM_ERRORS = (ErrorClass.PERMANENT_SOURCE, ErrorClass.DATA_INVALID)


# --------------------------------------------------------------------------- #
# Partition plan and checkpoint
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class PartitionPlan:
    crawl_id: str
    cdx_key: str
    filter_config: FilterConfig
    first_block: int
    blocks: tuple[tuple[int, int], ...]


def _invalid(message: str) -> JobFailure:
    return JobFailure(ErrorClass.DATA_INVALID, message, code="invalid_partition")


def _dates_manifest(dates: Any) -> dict[str, Any]:
    def iso(value: datetime | None) -> str | None:
        return value.isoformat() if value else None

    return {"version": DATE_EXTRACTOR_VERSION, "published_at": iso(dates.published_at),
            "published_evidence": dates.published_evidence, "modified_at": iso(dates.modified_at),
            "modified_evidence": dates.modified_evidence, "http_last_modified": iso(dates.http_last_modified)}


def load_plan(job: ClaimedJob, config: AcquisitionConfig) -> PartitionPlan:
    """Rebuild and validate the partition from the job identity and its checkpoint."""
    match = _PARTITION.match(job.source_identifier)
    if not match or match.group(1) != match.group(3):
        raise _invalid("source_identifier is not a cc_index_partition identity")
    crawl_id, key, filter_version = match.group(1), match.group(2), match.group(4)
    first, last = int(match.group(5)), int(match.group(6))
    cp = job.checkpoint
    if cp.get("v") != CHECKPOINT_VERSION or cp.get("cdx_key") != key:
        raise _invalid("checkpoint does not belong to this partition")
    try:
        params = cp["filter"]
        filter_config = FilterConfig(mime_types=tuple(params["mime_types"]), languages=tuple(params["languages"]),
                                     reject_truncated=bool(params["reject_truncated"]),
                                     max_record_bytes=int(params["max_record_bytes"]),
                                     required_status=int(params["status"]), profile=str(params["profile"]),
                                     reference_domains=tuple(params["reference_domains"]))
        if params.get("url_policy") != URL_POLICY_VERSION:
            raise ValueError(f"url policy {params.get('url_policy')!r} is not {URL_POLICY_VERSION!r}")
    except (KeyError, TypeError, ValueError) as exc:
        raise _invalid(f"checkpoint filter is invalid: {exc}") from None
    if filter_config.version != filter_version:
        raise _invalid("checkpoint filter does not match the filter version in the job identity")
    blocks_raw = cp.get("blocks")
    if not isinstance(blocks_raw, list) or len(blocks_raw) != last - first + 1:
        raise _invalid("checkpoint block list does not match the partition range")
    blocks: list[tuple[int, int]] = []
    for item in blocks_raw:
        if (not isinstance(item, list) or len(item) != 2 or any(isinstance(v, bool) or not isinstance(v, int)
                                                                 for v in item)):
            raise _invalid("checkpoint block entries must be [offset, length]")
        offset, length = item
        if offset < 0 or not 1 <= length <= config.max_index_block_bytes:
            raise _invalid("checkpoint block offset/length out of range")
        if blocks and blocks[-1][0] + blocks[-1][1] != offset:
            raise _invalid("checkpoint blocks are not contiguous")
        blocks.append((offset, length))
    return PartitionPlan(crawl_id, key, filter_config, first, tuple(blocks))


@dataclass
class Progress:
    """The mutable part of the checkpoint."""

    block: int = 0
    line: int = 0
    item_failures: dict[str, int] = field(default_factory=dict)
    totals: Counter = field(default_factory=Counter)

    @classmethod
    def from_checkpoint(cls, checkpoint: dict[str, Any], plan: PartitionPlan) -> "Progress":
        block, line = checkpoint.get("block", 0), checkpoint.get("line", 0)
        if (isinstance(block, bool) or isinstance(line, bool) or not isinstance(block, int)
                or not isinstance(line, int) or not 0 <= block <= len(plan.blocks) or line < 0):
            raise _invalid("checkpoint position is invalid")
        failures = checkpoint.get("item_failures") or {}
        totals = checkpoint.get("totals") or {}
        if not isinstance(failures, dict) or not isinstance(totals, dict):
            raise _invalid("checkpoint counters are invalid")
        return cls(block, line, {str(k): int(v) for k, v in failures.items()},
                   Counter({str(k): int(v) for k, v in totals.items()}))

    def to_checkpoint(self, base: dict[str, Any]) -> dict[str, Any]:
        state = dict(base)
        state.update(block=self.block, line=self.line, totals=dict(sorted(self.totals.items())))
        if self.item_failures:
            state["item_failures"] = dict(self.item_failures)
        else:
            state.pop("item_failures", None)
        return state


# --------------------------------------------------------------------------- #
# Per-line work items and outcomes
# --------------------------------------------------------------------------- #

@dataclass
class LineItem:
    block: int
    line: int
    block_offset: int
    raw: bytes
    record: CdxRecord | None = None
    observation_id: uuid.UUID | None = None
    # Outcome (exactly one is set once processed).
    skip: str | None = None                      # counted only
    capture: CaptureRow | None = None            # to persist
    item_error: tuple[ErrorClass, str, str] | None = None  # to dead-letter
    unit_error: FetchError | JobFailure | None = None      # stops the unit
    counters: Counter = field(default_factory=Counter)

    @property
    def position(self) -> str:
        return f"{self.block}:{self.line}"


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

class CCPartitionHandler:
    """Handler for ``cc_index_partition`` jobs (register with CC_PARTITION_POLICY)."""

    def __init__(self, *, client: HttpClient, store: CaptureStore, bronze: BronzeStore,
                 config: AcquisitionConfig, run_id: uuid.UUID | None = None,
                 clock: Callable[[], datetime] = utc_now, monotonic: Callable[[], float] = time.monotonic,
                 silver: Any = None) -> None:
        self.client, self.store, self.bronze, self.config = client, store, bronze, config
        self.run_id = run_id
        self.silver = silver  # app.processing.silver.processor.SilverProcessor (prepare/persist) or None
        self.clock, self.monotonic = clock, monotonic

    # -- entry point ----------------------------------------------------------- #

    def __call__(self, ctx: JobContext) -> Completed | Yielded:
        plan = load_plan(ctx.job, self.config)
        progress = Progress.from_checkpoint(ctx.checkpoint, plan)
        base = dict(ctx.checkpoint)
        metrics: Counter = Counter()

        with ctx.transaction() as conn:
            blocked = self.store.blocked_until(conn, DATA_SCOPE)
        if blocked is not None:
            raise JobFailure(ErrorClass.RATE_LIMITED, "Common Crawl data endpoint is rate limited",
                             code="source_rate_limit", retry_at=blocked)
        try:
            self.bronze.check_space(plan.filter_config.max_record_bytes)
        except DiskSpaceError as exc:
            raise JobFailure(ErrorClass.RESOURCE_EXHAUSTED, str(exc), code="disk_space") from None

        self._renewed = self.monotonic()
        with ThreadPoolExecutor(max_workers=self.config.fetch_concurrency, thread_name_prefix="cc-fetch") as pool:
            while progress.block < len(plan.blocks):
                if ctx.stop_requested:
                    return self._yield(ctx, base, progress)
                lines = self._load_block(ctx, plan, progress, metrics, base)
                if lines is not None:
                    stopped = self._process_block(ctx, plan, progress, lines, pool, metrics, base)
                    if stopped:
                        return self._yield(ctx, base, progress)
                progress.block += 1
                progress.line = 0
                metrics["blocks_done"] += 1
                progress.totals["blocks_done"] += 1
                with ctx.transaction() as conn:
                    ctx.checkpoint_progress(progress.to_checkpoint(base), stage=Stage.ACQUISITION_PENDING, conn=conn)
                self._renewed = self.monotonic()
        return Completed(metrics=dict(sorted(metrics.items())), checkpoint=progress.to_checkpoint(base),
                         stage=Stage.SILVER if self.silver is not None else Stage.ACQUIRED)

    def _yield(self, ctx: JobContext, base: dict[str, Any], progress: Progress) -> Yielded:
        return Yielded(checkpoint=progress.to_checkpoint(base))

    # -- index block ------------------------------------------------------------ #

    def _load_block(self, ctx: JobContext, plan: PartitionPlan, progress: Progress, metrics: Counter,
                    base: dict[str, Any]) -> list[bytes] | None:
        offset, length = plan.blocks[progress.block]
        try:
            data = self.client.get_range(self.client.endpoints.data_url(plan.cdx_key), offset, length)
            lines = split_block_lines(decompress_single_member(data, self.config.max_index_block_decompressed_bytes))
        except FetchError as exc:
            if exc.error_class is ErrorClass.RATE_LIMITED:
                self._record_rate_limit(ctx, exc)
            raise JobFailure(exc.error_class, f"index block {offset}+{length}: {exc}", code=exc.code,
                             retry_after_seconds=exc.retry_after_seconds) from None
        except CompressionError as exc:
            # A corrupt block is one bad item: dead-letter it and continue with the next block.
            with ctx.transaction() as conn:
                self.store.record_item_dead_letter(
                    conn, item_key=f"cdx:{plan.cdx_key}@{offset}", stage=Stage.DISCOVERED,
                    error_class=ErrorClass.DATA_INVALID, error_code=f"index_block_{exc.code}",
                    error_message=str(exc), attempt_count=ctx.job.attempt_count, job_id=ctx.job.job_id,
                    run_id=self._run_id(ctx), payload_ref=f"{plan.cdx_key}@{offset}+{length}")
            metrics["blocks_dead_lettered"] += 1
            progress.totals["blocks_dead_lettered"] += 1
            return None
        metrics["bytes_index"] += length
        if progress.line > len(lines):
            raise _invalid("checkpoint line is beyond the end of the index block")
        return lines

    # -- lines ------------------------------------------------------------------- #

    def _process_block(self, ctx: JobContext, plan: PartitionPlan, progress: Progress, lines: list[bytes],
                       pool: ThreadPoolExecutor, metrics: Counter, base: dict[str, Any]) -> bool:
        """Process the rest of one block in committed batches. Returns True if stopped for shutdown."""
        offset = plan.blocks[progress.block][0]
        while progress.line < len(lines):
            if ctx.stop_requested:
                return True
            end = min(progress.line + self.config.batch_size, len(lines))
            items = [LineItem(progress.block, n, offset, lines[n]) for n in range(progress.line, end)]
            self._classify(ctx, plan, items, progress)
            done, stop_reason = self._run_items(ctx, plan, items, pool)
            failed = items[len(done)] if stop_reason == "unit_error" else None
            self._commit(ctx, plan, progress, done, metrics, base, failed)
            if stop_reason == "shutdown":
                return True
            if failed is not None:
                error = failed.unit_error
                if isinstance(error, FetchError):
                    raise JobFailure(error.error_class, f"record {failed.position}: {error}", code=error.code,
                                     retry_after_seconds=error.retry_after_seconds)
                raise error
        return False

    def _classify(self, ctx: JobContext, plan: PartitionPlan, items: list[LineItem], progress: Progress) -> None:
        """Parse, filter and derive identities; mark items that need no fetch."""
        seen: set[uuid.UUID] = set()
        candidates: list[LineItem] = []
        for item in items:
            item.counters["lines"] += 1
            try:
                record = parse_cdx_line(item.raw, plan.crawl_id, max_line_bytes=self.config.max_index_line_bytes)
            except IndexRowError as exc:
                item.item_error = (ErrorClass.DATA_INVALID, f"index_{exc.code}", str(exc))
                item.counters["malformed"] += 1
                continue
            item.record = record
            reason = rejection_reason(record, plan.filter_config)
            if reason is not None:
                item.skip = f"rejected_{reason}"
                continue
            item.observation_id = cc_observation_id(plan.crawl_id, record.filename, record.offset, record.length)
            if item.observation_id in seen:
                item.skip = "duplicate_in_batch"
                continue
            seen.add(item.observation_id)
            item.counters["candidates"] += 1
            if progress.item_failures.get(item.position, 0) >= self.config.item_transient_limit:
                item.item_error = (ErrorClass.TRANSIENT, "item_retry_limit",
                                   f"record caused {progress.item_failures[item.position]} unit retries")
                continue
            candidates.append(item)
        if not candidates:
            return
        with ctx.transaction() as conn:
            existing = self.store.existing_capture_ids(conn, [i.observation_id for i in candidates])
        for item in candidates:
            if item.observation_id in existing:
                try:
                    complete = self.bronze.read_verified(plan.crawl_id, item.observation_id) is not None
                except BronzeCorrupt:
                    complete = False
                if complete:
                    item.skip = "already_present"

    def _needs_fetch(self, item: LineItem) -> bool:
        return item.observation_id is not None and item.skip is None and item.item_error is None

    def _run_items(self, ctx: JobContext, plan: PartitionPlan, items: list[LineItem],
                   pool: ThreadPoolExecutor) -> tuple[list[LineItem], str | None]:
        """Fetch/validate/store in a bounded window, consumed in order.

        Returns the processed prefix and why it stopped early (None = whole batch).
        """
        done: list[LineItem] = []
        window: deque[tuple[LineItem, Future | None]] = deque()
        source = iter(items)
        exhausted = False
        try:
            while True:
                while not exhausted and len(window) < self.config.window:
                    item = next(source, None)
                    if item is None:
                        exhausted = True
                        break
                    future = pool.submit(self._acquire, plan, item) if self._needs_fetch(item) else None
                    window.append((item, future))
                if not window:
                    return done, None
                item, future = window.popleft()
                if future is not None:
                    future.result()  # _acquire never raises: outcomes are stored on the item
                self._heartbeat(ctx)
                if item.unit_error is not None:
                    return done, "unit_error"
                done.append(item)
                if ctx.stop_requested and (window or not exhausted):
                    return done, "shutdown"
        finally:
            pending = [f for _item, f in window if f is not None]
            for future in pending:
                future.cancel()
            wait(pending)  # no fetch task outlives the batch

    def _heartbeat(self, ctx: JobContext) -> None:
        if self.monotonic() - self._renewed >= ctx.lease_seconds / 3:
            ctx.heartbeat()
            self._renewed = self.monotonic()

    def _acquire(self, plan: PartitionPlan, item: LineItem) -> None:
        """Runs in a pool thread. Never raises; the outcome is recorded on ``item``."""
        record = item.record
        try:
            local = self.bronze.read_verified(plan.crawl_id, item.observation_id)
            if local is not None:
                data, manifest = local
                try:
                    retrieved_at = datetime.fromisoformat(manifest["retrieved_at"])
                    if retrieved_at.tzinfo is None:
                        raise ValueError("naive timestamp")
                except (KeyError, TypeError, ValueError):
                    raise BronzeCorrupt("Bronze manifest has no valid retrieved_at") from None
                item.counters["bronze_reused"] += 1
            else:
                data = self.client.get_range(self.client.endpoints.data_url(record.filename), record.offset,
                                             record.length)
                retrieved_at = self.clock()
                item.counters["fetched"] += 1
                item.counters["bytes_fetched"] += len(data)
            capture = parse_warc_record(data, record, max_decompressed=self.config.max_decompressed_record_bytes,
                                        verify_payload_digest=self.config.verify_payload_digest)
            if local is None:
                stored = self.bronze.store(plan.crawl_id, item.observation_id, data,
                                           self._manifest(plan, item, capture, retrieved_at))
                item.counters["bronze_created" if stored.created else
                              "bronze_repaired" if stored.repaired else "bronze_existing"] += 1
            item.capture = self._row(plan, item, capture, retrieved_at)
        except FetchError as exc:
            if exc.error_class in _ITEM_ERRORS:
                item.item_error = (exc.error_class, exc.code, str(exc))
            else:
                item.unit_error = exc
        except WarcRecordError as exc:
            item.item_error = (ErrorClass.DATA_INVALID, f"warc_{exc.code}", str(exc))
        except (BronzeConflict, BronzeCorrupt, UnsafePath) as exc:
            item.item_error = (ErrorClass.DATA_INVALID, exc.code, str(exc))
        except DiskSpaceError as exc:
            item.unit_error = JobFailure(ErrorClass.RESOURCE_EXHAUSTED, str(exc), code="disk_space")
        except OSError as exc:
            item.unit_error = JobFailure(ErrorClass.RESOURCE_EXHAUSTED, f"Bronze I/O error: {exc}",
                                         code="bronze_io")
        except Exception as exc:  # a bug: stop the unit, keep the committed prefix
            item.unit_error = JobFailure(ErrorClass.INTERNAL_BUG, f"{type(exc).__name__}: {exc}",
                                         code=type(exc).__name__)

    # -- persistence ------------------------------------------------------------ #

    def _run_id(self, ctx: JobContext) -> uuid.UUID | None:
        return self.run_id or ctx.job.run_id

    def _manifest(self, plan: PartitionPlan, item: LineItem, capture: WarcCapture,
                  retrieved_at: datetime) -> dict[str, Any]:
        record = item.record
        return {
            "source_type": BRONZE_MANIFEST_SOURCE,
            "crawl_id": plan.crawl_id,
            "warc_filename": record.filename,
            "warc_offset": record.offset,
            "warc_length": record.length,
            "identity": cc_observation_payload(plan.crawl_id, record.filename, record.offset, record.length),
            "source_url": record.url,
            "retrieved_at": retrieved_at.isoformat(),
            "retrieval": {"url": self.client.endpoints.data_url(record.filename),
                          "range": f"bytes={record.offset}-{record.offset + record.length - 1}"},
            "index": {"cdx_key": plan.cdx_key, "block_offset": item.block_offset, "line": item.line,
                      "urlkey": record.urlkey, "timestamp": record.timestamp,
                      "filter_version": plan.filter_config.version, "fields": record.index_fields},
            "warc": {"version": capture.warc_version, "record_id": str(capture.record_id) if capture.record_id else None,
                     "date": capture.warc_date.isoformat() if capture.warc_date else None,
                     "payload_digest": capture.payload_digest,
                     "payload_digest_verified": capture.payload_digest_verified,
                     "payload_bytes": capture.payload_bytes, "headers": capture.warc_headers},
            "http": {"status": capture.http_status, "headers": capture.http_headers},
            "html_metadata": {"version": HTML_METADATA_VERSION, "title": capture.source_title,
                              "canonical_url": capture.canonical_url,
                              "dates": _dates_manifest(capture.page_dates)},
        }

    def _row(self, plan: PartitionPlan, item: LineItem, capture: WarcCapture, retrieved_at: datetime) -> CaptureRow:
        record = item.record
        return CaptureRow(
            observation_id=item.observation_id, crawl_id=plan.crawl_id, warc_filename=record.filename,
            warc_record_offset=record.offset, warc_record_length=record.length, source_url=capture.target_uri,
            url_host=record.url_host, source_title=capture.source_title, acquired_at=retrieved_at,
            run_id=self.run_id, warc_record_id=capture.record_id, warc_date=capture.warc_date,
            http_status=capture.http_status, http_content_type=capture.http_content_type,
            detected_mime=record.mime_detected, redirect_location=capture.redirect_location,
            canonical_url=capture.canonical_url, payload_digest=capture.payload_digest,
            page_dates=capture.page_dates)

    def _commit(self, ctx: JobContext, plan: PartitionPlan, progress: Progress, done: list[LineItem],
                metrics: Counter, base: dict[str, Any], failed: LineItem | None) -> None:
        """Persist the processed prefix and advance the checkpoint in ONE transaction.

        When the unit stops on ``failed``, the checkpoint points at that record
        and, for transient/internal failures, counts one more unit retry caused
        by it (``item_failures``), so a poisoned record is eventually
        dead-lettered instead of blocking the partition.
        """
        batch: Counter = Counter()
        next_line = done[-1].line + 1 if done else progress.line
        failures = dict(progress.item_failures)
        for item in done:
            failures.pop(item.position, None)
        error_class = None
        if failed is not None:
            error = failed.unit_error
            error_class = error.error_class if isinstance(error, FetchError) else error.failure.error_class
            if error_class in (ErrorClass.TRANSIENT, ErrorClass.INTERNAL_BUG):
                failures[failed.position] = failures.get(failed.position, 0) + 1
        prepared = self.silver.prepare(ctx, self._silver_refs(plan, done)) if self.silver is not None else None
        with ctx.transaction() as conn:
            if error_class is ErrorClass.RATE_LIMITED and isinstance(failed.unit_error, FetchError):
                self.store.record_rate_limit(conn, DATA_SCOPE, http_status=failed.unit_error.status,
                                             retry_after_seconds=failed.unit_error.retry_after_seconds)
            dead_letters = 0
            for item in done:
                batch.update(item.counters)
                if item.skip is not None:
                    batch[item.skip] += 1
                elif item.capture is not None:
                    batch["inserted" if self.store.persist_capture(conn, item.capture) else "already_present"] += 1
                elif item.item_error is not None:
                    error_class, code, message = item.item_error
                    batch["dead_lettered"] += 1
                    if metrics["dead_letters_written"] + dead_letters >= self.config.max_dead_letters_per_attempt:
                        batch["dead_letters_suppressed"] += 1
                        continue
                    dead_letters += 1
                    self._dead_letter(conn, ctx, plan, item, error_class, code, message)
            batch["dead_letters_written"] += dead_letters
            if prepared is not None:
                batch.update(self.silver.persist(conn, prepared, job_id=ctx.job.job_id,
                                                 attempt_count=ctx.job.attempt_count))
            progress_next = Progress(progress.block, next_line, failures, progress.totals + batch)
            ctx.checkpoint_progress(progress_next.to_checkpoint(base), stage=Stage.ACQUISITION_PENDING, conn=conn)
        progress.line, progress.item_failures, progress.totals = next_line, failures, progress_next.totals
        metrics.update(batch)
        self._renewed = self.monotonic()

    @staticmethod
    def _silver_refs(plan: PartitionPlan, done: list[LineItem]) -> list[SourceRef]:
        """Observations of this batch that have (or already had) verified Bronze evidence."""
        refs = []
        for item in done:
            if item.observation_id is not None and (item.capture is not None or item.skip == "already_present"):
                record = item.record
                refs.append(SourceRef(item.observation_id, "common_crawl", record.url, crawl_id=plan.crawl_id,
                                      warc_filename=record.filename, warc_offset=record.offset,
                                      warc_length=record.length))
        return refs

    def _dead_letter(self, conn: Any, ctx: JobContext, plan: PartitionPlan, item: LineItem,
                     error_class: ErrorClass, code: str, message: str) -> None:
        if item.observation_id is not None:
            record = item.record
            key = str(item.observation_id)
            stage = Stage.ACQUISITION_PENDING
            payload = cc_observation_payload(plan.crawl_id, record.filename, record.offset, record.length)
        else:
            key = f"cdx:{plan.cdx_key}@{item.block_offset}#line={item.line}"
            stage = Stage.DISCOVERED
            payload = key
        self.store.record_item_dead_letter(
            conn, item_key=key, stage=stage, error_class=error_class, error_code=code, error_message=message,
            attempt_count=ctx.job.attempt_count, job_id=ctx.job.job_id, run_id=self._run_id(ctx),
            payload_ref=payload)

    def _record_rate_limit(self, ctx: JobContext, exc: FetchError) -> None:
        with ctx.transaction() as conn:
            self.store.record_rate_limit(conn, DATA_SCOPE, http_status=exc.status,
                                         retry_after_seconds=exc.retry_after_seconds)

"""Offline fixtures for Common Crawl Acquisition V1 tests (no network, no database).

* ``make_warc_record`` / ``make_cdx_line``: synthetic, spec-shaped WARC records
  (one gzip member each) and CDX index lines.
* ``FakeCommonCrawl``: an in-memory object store laid out like
  data.commoncrawl.org (WARC files, zipnum CDX shards, cluster.idx) plus
  index.commoncrawl.org/collinfo.json.
* ``FakeTransport``: a Range-aware ``Transport`` over ``FakeCommonCrawl`` with
  per-path fault injection (status codes, timeouts, short/wrong ranges,
  ignored Range headers, delays) and concurrency accounting.
* ``InMemoryCaptureStore``: a ``CaptureStore`` with the schema's uniqueness rules.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import socket
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from app.sources.commoncrawl.config import FilterConfig, PlanningConfig
from app.sources.commoncrawl.discovery import cdx_key, cluster_idx_key, plan_partitions

CRAWL = "CC-MAIN-2024-33"
SEGMENT = "1722640353668.0"


def sha1_b32(data: bytes) -> str:
    return base64.b32encode(hashlib.sha1(data).digest()).decode("ascii")


def html_page(title: str | None = "Example title", extra_head: str = "", body: str = "Hello world") -> bytes:
    title_tag = f"<title>{title}</title>" if title is not None else ""
    return f"<!doctype html><html><head>{title_tag}{extra_head}</head><body><p>{body}</p></body></html>".encode()


def make_warc_record(url: str, body: bytes | None = None, *, status: int = 200,
                     content_type: str = "text/html; charset=utf-8", warc_type: str = "response",
                     record_id: uuid.UUID | None = None, date: str = "2024-08-05T23:12:34Z",
                     payload_digest: str | None = "auto", target: str | None = None,
                     http_headers: dict[str, str] | None = None, version: str = "WARC/1.0",
                     compress: bool = True) -> bytes:
    body = html_page() if body is None else body
    headers = {"Content-Type": content_type, "Content-Length": str(len(body))}
    headers.update(http_headers or {})
    http_block = f"HTTP/1.1 {status} OK\r\n".encode() + b"".join(
        f"{k}: {v}\r\n".encode() for k, v in headers.items()) + b"\r\n" + body
    warc_headers = {
        "WARC-Type": warc_type,
        "WARC-Date": date,
        "WARC-Record-ID": f"<urn:uuid:{record_id or uuid.uuid5(uuid.NAMESPACE_URL, url)}>",
        "Content-Length": str(len(http_block)),
        "Content-Type": "application/http; msgtype=response",
        "WARC-Target-URI": target if target is not None else url,
    }
    if payload_digest == "auto":
        warc_headers["WARC-Payload-Digest"] = f"sha1:{sha1_b32(body)}"
    elif payload_digest is not None:
        warc_headers["WARC-Payload-Digest"] = payload_digest
    raw = f"{version}\r\n".encode() + b"".join(
        f"{k}: {v}\r\n".encode() for k, v in warc_headers.items()) + b"\r\n" + http_block + b"\r\n\r\n"
    return gzip.compress(raw, mtime=0) if compress else raw


def make_cdx_line(url: str, filename: str, offset: int, length: int, *, status: str | None = "200",
                  mime: str = "text/html", mime_detected: str | None = "text/html", digest: str | None = None,
                  languages: str | None = "eng", truncated: str | None = None, timestamp: str = "20240805231234",
                  urlkey: str | None = None, **extra: Any) -> bytes:
    obj: dict[str, Any] = {"url": url, "mime": mime, "length": str(length), "offset": str(offset),
                           "filename": filename}
    if status is not None:
        obj["status"] = status
    if mime_detected is not None:
        obj["mime-detected"] = mime_detected
    if digest is not None:
        obj["digest"] = digest
    if languages is not None:
        obj["languages"] = languages
    if truncated is not None:
        obj["truncated"] = truncated
    obj.update(extra)
    key = urlkey or ("com,example)/" + url.rsplit("/", 1)[-1])
    return f"{key} {timestamp} {json.dumps(obj)}".encode()


def warc_filename(leaf: str = "CC-MAIN-20240805230130-20240806020130-00042.warc.gz", crawl: str = CRAWL,
                  folder: str = "warc") -> str:
    return f"crawl-data/{crawl}/segments/{SEGMENT}/{folder}/{leaf}"


class FakeCommonCrawl:
    """In-memory data.commoncrawl.org + index.commoncrawl.org."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self._cluster: dict[str, list[str]] = defaultdict(list)
        self.crawls: list[str] = []

    def put(self, key: str, data: bytes) -> None:
        self.objects["/" + key] = data

    def add_warc(self, records: list[bytes], *, leaf: str = "CC-MAIN-20240805230130-20240806020130-00042.warc.gz",
                 crawl: str = CRAWL, prefix: bytes = b"") -> tuple[str, list[tuple[int, int]]]:
        """Store a WARC file made of ``prefix`` + records; return its key and each record's (offset, length)."""
        name = warc_filename(leaf, crawl)
        data, spans = bytearray(prefix), []
        for record in records:
            spans.append((len(data), len(record)))
            data += record
        self.put(name, bytes(data))
        return name, spans

    def add_index(self, blocks: list[list[bytes]], *, cdx_file: str = "cdx-00000.gz", crawl: str = CRAWL,
                  raw_blocks: dict[int, bytes] | None = None) -> list[tuple[int, int]]:
        """Store a zipnum CDX shard (one gzip member per block) and extend cluster.idx."""
        data, spans = bytearray(), []
        for n, lines in enumerate(blocks):
            member = (raw_blocks or {}).get(n) or gzip.compress(b"\n".join(lines) + b"\n", mtime=0)
            spans.append((len(data), len(member)))
            self._cluster[crawl].append(f"com,example)/{n:05d} 20240805000000\t{cdx_file}\t{len(data)}\t"
                                        f"{len(member)}\t{len(self._cluster[crawl]) + 1}")
            data += member
        self.put(cdx_key(crawl, cdx_file), bytes(data))
        self.put(cluster_idx_key(crawl), ("\n".join(self._cluster[crawl]) + "\n").encode())
        if crawl not in self.crawls:
            self.crawls.append(crawl)
        self.put_collinfo()
        return spans

    def put_collinfo(self, extra: list[dict] | None = None) -> None:
        entries = [{"id": c, "name": c, "cdx-api": f"https://index.commoncrawl.org/{c}-index"} for c in self.crawls]
        entries += extra or []
        self.objects["/collinfo.json"] = json.dumps(entries).encode()

    def cluster_lines(self, crawl: str = CRAWL) -> list[bytes]:
        return [line.encode() for line in self._cluster[crawl]]


# --------------------------------------------------------------------------- #
# Transport
# --------------------------------------------------------------------------- #

class FakeResponse:
    def __init__(self, status: int, headers: dict[str, str], body: bytes, *,
                 read_fault: Callable[[int], None] | None = None) -> None:
        self.status = status
        self._headers = {k.lower(): v for k, v in headers.items()}
        self._body = io.BytesIO(body)
        self._read_fault = read_fault
        self.closed_reusable: bool | None = None

    def header(self, name: str) -> str | None:
        return self._headers.get(name.lower())

    def read(self, amount: int) -> bytes:
        if self._read_fault is not None:
            self._read_fault(self._body.tell())
        return self._body.read(amount)

    def close(self, *, reusable: bool) -> None:
        if self.closed_reusable is None:
            self.closed_reusable = reusable


class FakeTransport:
    """Range-aware transport with fault injection.

    ``faults[path]`` is a list consumed one per request to that path. A fault is
    a dict, e.g. ``{"status": 429, "headers": {"Retry-After": "120"}}``,
    ``{"timeout": True}``, ``{"read_timeout": True}``, ``{"short": 10}`` (drop the
    last 10 bytes), ``{"extra": 5}``, ``{"ignore_range": True}``,
    ``{"wrong_range": True}``, ``{"delay": 0.1}``, ``{"raise": exc}``,
    ``{"body": b"..."}`` (replace the content). ``"*"`` applies to every path.
    """

    def __init__(self, site: FakeCommonCrawl) -> None:
        self.site = site
        self.faults: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.persistent: dict[str, dict[str, Any]] = {}
        self.requests: list[tuple[str, str | None]] = []
        self.started: list[float] = []
        self.finished: list[float] = []
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.on_request: Callable[[str, str | None], None] | None = None

    def count(self, path_part: str) -> int:
        return sum(1 for path, _range in self.requests if path_part in path)

    def open(self, url, headers, *, connect_timeout, read_timeout):
        path, rng = url.path, headers.get("Range")
        with self._lock:
            self.requests.append((path, rng))
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.started.append(time.monotonic())
            fault = self.faults[path].pop(0) if self.faults.get(path) else self.persistent.get(path, {})
        try:
            if self.on_request is not None:
                self.on_request(path, rng)
            if fault.get("delay"):
                time.sleep(fault["delay"])
            if fault.get("raise") is not None:
                raise fault["raise"]
            if fault.get("timeout"):
                raise socket.timeout("timed out")
            if "status" in fault:
                return FakeResponse(fault["status"], fault.get("headers", {}), b"error")
            data = fault.get("body", self.site.objects.get(path))
            if data is None:
                return FakeResponse(404, {}, b"not found")
            read_fault = None
            if fault.get("read_timeout"):
                def read_fault(_pos: int) -> None:
                    raise socket.timeout("read timed out")
            if rng and not fault.get("ignore_range"):
                start, end = (int(x) for x in rng[len("bytes="):].split("-"))
                if start >= len(data):
                    return FakeResponse(416, {"Content-Range": f"bytes */{len(data)}"}, b"")
                piece = data[start:end + 1]
                shown_start, shown_end = (start + 1, end + 1) if fault.get("wrong_range") else (start, end)
                if fault.get("short"):
                    piece = piece[:-fault["short"]]
                if fault.get("extra"):
                    piece = piece + b"x" * fault["extra"]
                headers_out = {"Content-Range": f"bytes {shown_start}-{min(shown_end, len(data) - 1)}/{len(data)}"}
                return FakeResponse(206, headers_out, piece, read_fault=read_fault)
            return FakeResponse(200, {"Content-Length": str(len(data))}, data, read_fault=read_fault)
        finally:
            with self._lock:
                self.active -= 1
                self.finished.append(time.monotonic())


# --------------------------------------------------------------------------- #
# Store
# --------------------------------------------------------------------------- #

class InMemoryCaptureStore:
    """``CaptureStore`` with the schema's keys: PK observation_id, UNIQUE (warc_file_id, offset)."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.crawls: set[str] = set()
        self.warc_files: dict[str, tuple[int, str]] = {}
        self.observations: dict[uuid.UUID, Any] = {}
        self.natural_keys: dict[tuple[int, int], uuid.UUID] = {}
        self.dead_letters: dict[tuple[str, str], dict[str, Any]] = {}
        self.rate_limits: dict[str, datetime] = {}
        self.rate_limit_events: list[tuple[str, int | None, int | None]] = []
        self.fail_persist: Callable[[Any], None] | None = None

    def register_crawl(self, conn, crawl_id: str) -> bool:
        if crawl_id in self.crawls:
            return False
        self.crawls.add(crawl_id)
        return True

    def existing_capture_ids(self, conn, observation_ids) -> set[uuid.UUID]:
        return {i for i in observation_ids if i in self.observations}

    def persist_capture(self, conn, row) -> bool:
        if self.fail_persist is not None:
            self.fail_persist(row)
        file_id, crawl = self.warc_files.setdefault(row.warc_filename, (len(self.warc_files) + 1, row.crawl_id))
        assert crawl == row.crawl_id
        if row.observation_id in self.observations:
            return False
        key = (file_id, row.warc_record_offset)
        if key in self.natural_keys:
            raise AssertionError("natural key collision under a different observation id")
        self.natural_keys[key] = row.observation_id
        self.observations[row.observation_id] = row
        return True

    def record_item_dead_letter(self, conn, *, item_key, stage, error_class, error_code, error_message,
                                attempt_count, job_id, run_id, payload_ref) -> None:
        entry = self.dead_letters.get((item_key, str(stage)))
        if entry is None:
            self.dead_letters[(item_key, str(stage))] = dict(
                item_key=item_key, stage=str(stage), error_class=str(error_class), error_code=error_code,
                error_message=error_message, attempt_count=attempt_count, job_id=job_id, payload_ref=payload_ref,
                occurrence_count=1)
        else:
            entry.update(error_class=str(error_class), error_code=error_code, occurrence_count=entry["occurrence_count"] + 1)

    def blocked_until(self, conn, scope: str):
        until = self.rate_limits.get(scope)
        return until if until is not None and until > self.clock() else None

    def record_rate_limit(self, conn, scope: str, *, http_status, retry_after_seconds) -> None:
        self.rate_limit_events.append((scope, http_status, retry_after_seconds))
        if retry_after_seconds is not None:
            self.rate_limits[scope] = self.clock() + timedelta(seconds=retry_after_seconds)

    def codes(self) -> dict[str, str]:
        return {key: entry["error_code"] for (key, _stage), entry in self.dead_letters.items()}


def partitions(site: FakeCommonCrawl, filter_config: FilterConfig | None = None, *, crawl: str = CRAWL,
               blocks_per_partition: int = 10):
    """The partition specs of ``crawl`` as the planner would create them."""
    return list(plan_partitions(site.cluster_lines(crawl), crawl, PlanningConfig(blocks_per_partition=blocks_per_partition),
                                max_block_bytes=16 * 1024 * 1024))

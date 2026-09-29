"""Bounded, allow-listed HTTP access to Common Crawl (stdlib only).

Security (no SSRF)
------------------
* Every request URL is checked against ``Endpoints``: the exact origin must be
  ``https://data.commoncrawl.org`` or ``https://index.commoncrawl.org`` (port
  443), with no user-info, query or fragment, and a path made only of
  ``[A-Za-z0-9._~/-]`` without ``.``/``..``/empty segments. The origins are
  constants; no environment variable can widen them. URLs are always built
  here from validated relative keys, never taken from index or WARC content.
* Redirects are never followed (``http.client`` does not follow them); a 3xx
  is a permanent failure.
* Certificates are verified (``ssl.create_default_context()``).

Resource bounds
---------------
* One pool per origin, at most ``max_connections_per_host`` connections
  (checked-out + idle), guarded by a semaphore: callers block instead of
  opening more sockets.
* Connect and per-read socket timeouts, plus a total per-request deadline.
* Every body read is bounded (``max_bytes``); ranges must be exactly the
  requested length (``206`` + matching ``Content-Range`` + no extra byte).
* ``Accept-Encoding: identity``: stored gzip members are fetched byte-exact.

No request is retried here: failures are classified (``FetchError``) and the
Runtime Engine retry policy decides (schema §17).
"""

from __future__ import annotations

import http.client
import re
import socket
import ssl
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Protocol
from urllib.parse import SplitResult, urlsplit

from app.queue.states import ErrorClass
from app.sources.commoncrawl.config import HttpConfig

DATA_ORIGIN = "https://data.commoncrawl.org"
INDEX_ORIGIN = "https://index.commoncrawl.org"
PRODUCTION_HOSTS = frozenset({"data.commoncrawl.org", "index.commoncrawl.org"})

MAX_RETRY_AFTER_SECONDS = 86_400
_READ_CHUNK = 64 * 1024
_SAFE_PATH = re.compile(r"^/[A-Za-z0-9._~/-]*$")
_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")


class DisallowedEndpoint(ValueError):
    """The URL is not an approved Common Crawl endpoint."""


class FetchError(Exception):
    """A classified HTTP failure (schema §17.2 error classes)."""

    def __init__(self, error_class: ErrorClass, code: str, message: str, *, status: int | None = None,
                 retry_after_seconds: int | None = None) -> None:
        super().__init__(message)
        self.error_class = ErrorClass(error_class)
        self.code = code
        self.status = status
        self.retry_after_seconds = retry_after_seconds


# --------------------------------------------------------------------------- #
# Endpoints (allow-list)
# --------------------------------------------------------------------------- #

def _origin(url: str) -> tuple[str, str, int]:
    parts = urlsplit(url)
    if parts.scheme not in ("https", "http") or not parts.hostname or parts.username or parts.password:
        raise DisallowedEndpoint("origin must be scheme://host[:port]")
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise DisallowedEndpoint("origin must not have a path, query or fragment")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return parts.scheme, parts.hostname.lower(), port


@dataclass(frozen=True)
class Endpoints:
    """The two approved origins. Only ``COMMON_CRAWL`` is used in production."""

    data_origin: str = DATA_ORIGIN
    index_origin: str = INDEX_ORIGIN
    _allow_loopback: bool = False

    def __post_init__(self) -> None:
        for origin in (self.data_origin, self.index_origin):
            scheme, host, port = _origin(origin)
            if self._allow_loopback:
                if host not in ("127.0.0.1", "localhost", "::1"):
                    raise DisallowedEndpoint("test endpoints must be loopback")
            elif (scheme, port) != ("https", 443) or host not in PRODUCTION_HOSTS:
                raise DisallowedEndpoint(f"{origin!r} is not an approved Common Crawl origin")

    @classmethod
    def loopback_for_tests(cls, origin: str) -> "Endpoints":
        """Offline tests only: a loopback HTTP server stands in for both origins."""
        return cls(origin.rstrip("/"), origin.rstrip("/"), _allow_loopback=True)

    def _origins(self) -> set[tuple[str, str, int]]:
        return {_origin(self.data_origin), _origin(self.index_origin)}

    def check(self, url: str) -> SplitResult:
        """Return the parsed URL if it is an approved endpoint; raise DisallowedEndpoint otherwise."""
        if not isinstance(url, str) or len(url) > 4096 or any(ord(c) <= 32 or ord(c) == 127 for c in url):
            raise DisallowedEndpoint("URL must be a printable string without whitespace")
        parts = urlsplit(url)
        if parts.username is not None or parts.password is not None or "@" in parts.netloc:
            raise DisallowedEndpoint("user-info is not allowed")
        if parts.query or parts.fragment or "?" in url or "#" in url:
            raise DisallowedEndpoint("query and fragment are not allowed")
        try:
            port = parts.port or (443 if parts.scheme == "https" else 80)
        except ValueError:
            raise DisallowedEndpoint("invalid port") from None
        if not parts.hostname or (parts.scheme, parts.hostname.lower(), port) not in self._origins():
            raise DisallowedEndpoint("host is not an approved Common Crawl endpoint")
        if not _SAFE_PATH.match(parts.path):
            raise DisallowedEndpoint("path contains disallowed characters")
        if any(segment in ("", ".", "..") for segment in parts.path[1:].split("/")):
            raise DisallowedEndpoint("path must not contain empty, '.' or '..' segments")
        return parts

    def data_url(self, key: str) -> str:
        """URL of a relative ``data.commoncrawl.org`` key (e.g. a WARC or CDX file)."""
        if not isinstance(key, str) or key.startswith("/") or not key:
            raise DisallowedEndpoint("key must be a non-empty relative path")
        url = f"{self.data_origin}/{key}"
        self.check(url)
        return url

    def collinfo_url(self) -> str:
        url = f"{self.index_origin}/collinfo.json"
        self.check(url)
        return url


COMMON_CRAWL = Endpoints()


# --------------------------------------------------------------------------- #
# Status classification
# --------------------------------------------------------------------------- #

def parse_retry_after(value: str | None, now: datetime | None = None) -> int | None:
    """``Retry-After`` as whole seconds (delta-seconds or HTTP-date), clamped to [0, 86400]."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if value.isdigit():
        return min(int(value), MAX_RETRY_AFTER_SECONDS)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return max(0, min(int((when - now).total_seconds() + 0.999), MAX_RETRY_AFTER_SECONDS))


def status_failure(status: int, retry_after: str | None = None, *, what: str = "request") -> FetchError:
    """Classify an unexpected HTTP status (schema §17.2)."""
    seconds = parse_retry_after(retry_after)
    if status in (429, 503):  # 503 = Common Crawl / S3 "slow down"
        return FetchError(ErrorClass.RATE_LIMITED, f"http_{status}", f"{what}: HTTP {status} (slow down)",
                          status=status, retry_after_seconds=seconds)
    if status >= 500 or status == 408:
        return FetchError(ErrorClass.TRANSIENT, f"http_{status}", f"{what}: HTTP {status}", status=status,
                          retry_after_seconds=seconds)
    if 300 <= status < 400:
        return FetchError(ErrorClass.PERMANENT_SOURCE, f"http_{status}",
                          f"{what}: HTTP {status}; redirects are never followed", status=status)
    if 400 <= status < 500:
        return FetchError(ErrorClass.PERMANENT_SOURCE, f"http_{status}", f"{what}: HTTP {status}", status=status)
    return FetchError(ErrorClass.TRANSIENT, "unexpected_status", f"{what}: unexpected HTTP {status}", status=status)


# --------------------------------------------------------------------------- #
# Transport
# --------------------------------------------------------------------------- #

class RawResponse(Protocol):
    status: int

    def header(self, name: str) -> str | None: ...
    def read(self, amount: int) -> bytes: ...
    def close(self, *, reusable: bool) -> None: ...


class Transport(Protocol):
    def open(self, url: SplitResult, headers: Mapping[str, str], *, connect_timeout: float,
             read_timeout: float) -> RawResponse: ...


class _StdlibResponse:
    def __init__(self, transport: "StdlibTransport", key: tuple[str, str, int],
                 conn: http.client.HTTPConnection, resp: http.client.HTTPResponse) -> None:
        self._transport, self._key, self._conn, self._resp = transport, key, conn, resp
        self.status = resp.status
        self._closed = False

    def header(self, name: str) -> str | None:
        return self._resp.getheader(name)

    def read(self, amount: int) -> bytes:
        return self._resp.read(amount)

    def close(self, *, reusable: bool) -> None:
        if self._closed:
            return
        self._closed = True
        keep = reusable and self._resp.isclosed() and not self._resp.will_close
        if not keep:
            self._resp.close()
            # HTTPResponse.close() does not close the connection's socket; a dropped
            # connection must be closed explicitly (else: unclosed SSL socket warning).
            self._conn.close()
        self._transport._checkin(self._key, self._conn if keep else None)


class StdlibTransport:
    """``http.client`` connection pool: at most ``max_connections`` per origin."""

    def __init__(self, max_connections: int = 4, ssl_context: ssl.SSLContext | None = None) -> None:
        if not 1 <= max_connections <= 64:
            raise ValueError("max_connections must be 1..64")
        self.max_connections = max_connections
        self._ssl = ssl_context or ssl.create_default_context()
        self._lock = threading.Lock()
        self._idle: dict[tuple[str, str, int], list[http.client.HTTPConnection]] = {}
        self._slots: dict[tuple[str, str, int], threading.BoundedSemaphore] = {}

    def _slot(self, key: tuple[str, str, int]) -> threading.BoundedSemaphore:
        with self._lock:
            return self._slots.setdefault(key, threading.BoundedSemaphore(self.max_connections))

    def _checkin(self, key: tuple[str, str, int], conn: http.client.HTTPConnection | None) -> None:
        if conn is not None:
            with self._lock:
                self._idle.setdefault(key, []).append(conn)
        self._slot(key).release()

    def _new(self, key: tuple[str, str, int], connect_timeout: float) -> http.client.HTTPConnection:
        scheme, host, port = key
        if scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=connect_timeout, context=self._ssl)
        return http.client.HTTPConnection(host, port, timeout=connect_timeout)

    def open(self, url: SplitResult, headers: Mapping[str, str], *, connect_timeout: float,
             read_timeout: float) -> RawResponse:
        key = (url.scheme, (url.hostname or "").lower(), url.port or (443 if url.scheme == "https" else 80))
        slot = self._slot(key)
        slot.acquire()
        try:
            for attempt in range(2):
                with self._lock:
                    idle = self._idle.get(key) or []
                    conn = idle.pop() if idle else None
                reused = conn is not None
                if conn is None:
                    conn = self._new(key, connect_timeout)
                try:
                    if conn.sock is None:
                        conn.connect()
                    conn.sock.settimeout(read_timeout)
                    target = url.path + (f"?{url.query}" if url.query else "")  # CC URLs never carry a query
                    conn.request("GET", target, headers=dict(headers))
                    resp = conn.getresponse()
                    return _StdlibResponse(self, key, conn, resp)
                except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
                    conn.close()
                    if reused and attempt == 0:  # a stale keep-alive connection: one fresh retry
                        continue
                    raise
                except BaseException:
                    conn.close()
                    raise
            raise RuntimeError("unreachable")  # pragma: no cover
        except BaseException:
            slot.release()
            raise

    def close(self) -> None:
        with self._lock:
            for conns in self._idle.values():
                for conn in conns:
                    conn.close()
            self._idle.clear()


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #

class HttpClient:
    def __init__(self, config: HttpConfig | None = None, *, endpoints: Endpoints = COMMON_CRAWL,
                 transport: Transport | None = None, clock: Callable[[], float] = time.monotonic) -> None:
        self.config = config or HttpConfig()
        self.endpoints = endpoints
        self.transport = transport or StdlibTransport(self.config.max_connections_per_host)
        self._clock = clock

    def _open(self, url: str, extra: Mapping[str, str] | None = None) -> RawResponse:
        parts = self.endpoints.check(url)
        headers = {"User-Agent": self.config.user_agent, "Accept-Encoding": "identity", **(extra or {})}
        try:
            return self.transport.open(parts, headers, connect_timeout=self.config.connect_timeout_seconds,
                                       read_timeout=self.config.read_timeout_seconds)
        except (socket.timeout, TimeoutError) as exc:
            raise FetchError(ErrorClass.TRANSIENT, "timeout", f"connect/request timed out: {type(exc).__name__}") from exc
        except (OSError, http.client.HTTPException) as exc:
            raise FetchError(ErrorClass.TRANSIENT, "network", f"network error: {type(exc).__name__}") from exc

    def _read(self, resp: RawResponse, limit: int, deadline: float | None, what: str) -> bytes:
        """Read until EOF, at most ``limit`` bytes (raises if the body is longer)."""
        chunks: list[bytes] = []
        total = 0
        while True:
            if deadline is not None and self._clock() > deadline:
                raise FetchError(ErrorClass.TRANSIENT, "deadline_exceeded", f"{what}: request deadline exceeded")
            try:
                chunk = resp.read(min(_READ_CHUNK, limit - total + 1))
            except (socket.timeout, TimeoutError) as exc:
                raise FetchError(ErrorClass.TRANSIENT, "timeout", f"{what}: read timed out") from exc
            except (OSError, http.client.HTTPException) as exc:
                raise FetchError(ErrorClass.TRANSIENT, "short_read",
                                 f"{what}: connection failed while reading ({type(exc).__name__})") from exc
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > limit:
                raise FetchError(ErrorClass.DATA_INVALID, "response_too_large",
                                 f"{what}: body exceeds the {limit}-byte limit")
            chunks.append(chunk)

    def _check_status(self, resp: RawResponse, expected: int, what: str) -> None:
        if resp.status != expected:
            status = resp.status
            retry_after = resp.header("Retry-After")
            resp.close(reusable=False)
            if expected == 206 and status == 200:
                raise FetchError(ErrorClass.TRANSIENT, "range_not_honoured",
                                 f"{what}: server ignored the Range header (HTTP 200); body not read", status=200)
            raise status_failure(status, retry_after, what=what)

    def get_bytes(self, url: str, *, max_bytes: int) -> bytes:
        """GET a small document (HTTP 200 only), bounded to ``max_bytes``."""
        resp = self._open(url)
        ok = False
        try:
            self._check_status(resp, 200, "GET")
            body = self._read(resp, max_bytes, self._clock() + self.config.request_deadline_seconds, "GET")
            declared = resp.header("Content-Length")
            if declared is not None and declared.isdigit() and int(declared) != len(body):
                raise FetchError(ErrorClass.TRANSIENT, "short_read",
                                 f"GET: received {len(body)} of {declared} bytes")
            ok = True
            return body
        finally:
            resp.close(reusable=ok)

    def get_range(self, url: str, offset: int, length: int) -> bytes:
        """Fetch exactly ``[offset, offset + length)``; anything else raises FetchError."""
        if isinstance(offset, bool) or isinstance(length, bool) or offset < 0 or length < 1:
            raise ValueError("offset must be >= 0 and length >= 1")
        last = offset + length - 1
        resp = self._open(url, {"Range": f"bytes={offset}-{last}"})
        ok = False
        try:
            self._check_status(resp, 206, "range GET")
            content_range = (resp.header("Content-Range") or "").strip()
            match = _CONTENT_RANGE.match(content_range)
            if not match:
                raise FetchError(ErrorClass.TRANSIENT, "range_mismatch", "range GET: missing or invalid Content-Range")
            start, end, total = int(match.group(1)), int(match.group(2)), match.group(3)
            if total != "*" and int(total) < offset + length:
                raise FetchError(ErrorClass.PERMANENT_SOURCE, "range_beyond_eof",
                                 f"range GET: requested bytes end past the object size {total}")
            if (start, end) != (offset, last):
                raise FetchError(ErrorClass.TRANSIENT, "range_mismatch",
                                 f"range GET: got bytes {start}-{end}, requested {offset}-{last}")
            declared = resp.header("Content-Length")
            if declared is not None and (not declared.isdigit() or int(declared) != length):
                raise FetchError(ErrorClass.TRANSIENT, "range_mismatch",
                                 f"range GET: Content-Length {declared!r} != requested length {length}")
            try:
                body = self._read(resp, length, self._clock() + self.config.request_deadline_seconds, "range GET")
            except FetchError as exc:
                if exc.code == "response_too_large":
                    raise FetchError(ErrorClass.TRANSIENT, "range_mismatch",
                                     "range GET: more bytes than requested") from exc
                raise
            if len(body) != length:
                raise FetchError(ErrorClass.TRANSIENT, "short_read",
                                 f"range GET: received {len(body)} of {length} bytes")
            ok = True
            return body
        finally:
            resp.close(reusable=ok)

    def iter_lines(self, url: str, *, max_line_bytes: int, max_total_bytes: int) -> Iterator[bytes]:
        """Stream a text document line by line (bounded memory: one chunk + one line)."""
        resp = self._open(url)
        ok = False
        try:
            self._check_status(resp, 200, "GET")
            pending = b""
            total = 0
            while True:
                try:
                    chunk = resp.read(_READ_CHUNK)
                except (socket.timeout, TimeoutError) as exc:
                    raise FetchError(ErrorClass.TRANSIENT, "timeout", "stream: read timed out") from exc
                except (OSError, http.client.HTTPException) as exc:
                    raise FetchError(ErrorClass.TRANSIENT, "short_read",
                                     f"stream: connection failed ({type(exc).__name__})") from exc
                if not chunk:
                    break
                total += len(chunk)
                if total > max_total_bytes:
                    raise FetchError(ErrorClass.DATA_INVALID, "response_too_large",
                                     f"stream exceeds the {max_total_bytes}-byte limit")
                pending += chunk
                *lines, pending = pending.split(b"\n")
                for line in lines:
                    if len(line) > max_line_bytes:
                        raise FetchError(ErrorClass.DATA_INVALID, "line_too_long", "stream: line exceeds limit")
                    yield line
                if len(pending) > max_line_bytes:
                    raise FetchError(ErrorClass.DATA_INVALID, "line_too_long", "stream: line exceeds limit")
            if pending:
                yield pending
            ok = True
        finally:
            resp.close(reusable=ok)

    def close(self) -> None:
        close = getattr(self.transport, "close", None)
        if callable(close):
            close()

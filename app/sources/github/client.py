"""GitHub REST client for Acquisition V1: allow-listed, bounded, rate-aware, token-safe.

Security
* Only ``https://api.github.com`` (port 443). Every URL is built here from
  validated identifiers and must match one of the allow-listed path shapes
  (search, repository by numeric id, branch HEAD, git commit/tree/blob) with
  only the expected query keys. Repository content never produces a URL.
* Redirects are never followed; certificates are verified; responses are read
  with a byte limit and a deadline; ``Accept-Encoding: identity`` (no
  compressed responses, hence no decompression bombs).
* The token is sent only in the ``Authorization`` header and never appears in
  an exception message, ``repr`` or log line.

Rate limits: every response's ``x-ratelimit-*`` headers update a tracker; a
request is refused (``rate_limited``, retry at reset) once the remaining quota
of its resource reaches the configured reserve. No request is retried here.
"""

from __future__ import annotations

import http.client
import json
import re
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

from app.queue.states import ErrorClass
from app.sources.commoncrawl.http import DisallowedEndpoint, StdlibTransport, Transport
from app.sources.github.config import GitHubCredentials, GitHubHttpConfig
from app.sources.github.rate_limit import GitHubFetchError, RateTracker, classify, parse_rate_headers

API_ORIGIN = "https://api.github.com"
API_VERSION = "2022-11-28"
ACCEPT_JSON = "application/vnd.github+json"
ACCEPT_SHA = "application/vnd.github.sha"
ACCEPT_RAW = "application/vnd.github.raw+json"

OWNER = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})"
REPO = r"(?!\.\.?/)[A-Za-z0-9._-]{1,100}"
SHA = r"[0-9a-f]{40}(?:[0-9a-f]{24})?"
REF_RE = re.compile(r"^(?!/)(?!.*\.\.)(?!.*//)(?!.*/$)[A-Za-z0-9._/+-]{1,255}$")
OWNER_RE = re.compile(f"^{OWNER}$")
REPO_RE = re.compile(r"^[A-Za-z0-9._-]{1,100}$")

_ROUTES: tuple[tuple[re.Pattern[str], frozenset[str]], ...] = (
    (re.compile(r"^/search/repositories$"), frozenset({"q", "per_page", "page", "sort", "order"})),
    (re.compile(r"^/repositories/[1-9][0-9]{0,18}$"), frozenset()),
    (re.compile(f"^/repos/{OWNER}/{REPO}/commits/[A-Za-z0-9._/+-]{{1,255}}$"), frozenset()),
    (re.compile(f"^/repos/{OWNER}/{REPO}/git/commits/{SHA}$"), frozenset()),
    (re.compile(f"^/repos/{OWNER}/{REPO}/git/trees/{SHA}$"), frozenset({"recursive"})),
    (re.compile(f"^/repos/{OWNER}/{REPO}/git/blobs/{SHA}$"), frozenset()),
)


def valid_owner(value: object) -> bool:
    return isinstance(value, str) and bool(OWNER_RE.match(value))


def valid_repo_name(value: object) -> bool:
    return isinstance(value, str) and bool(REPO_RE.match(value)) and value not in (".", "..")


def valid_ref(value: object) -> bool:
    return isinstance(value, str) and bool(REF_RE.match(value)) and not value.endswith(".lock")


def redact(text: str, token: str | None) -> str:
    """Remove a token (and bearer/token prefixes) from text that might be logged."""
    if token:
        text = text.replace(token, "<redacted>")
    return re.sub(r"(?i)(authorization|bearer|token)\s*[:=]?\s*[A-Za-z0-9_\-\.]{20,}", r"\1 <redacted>", text)


@dataclass(frozen=True)
class GitHubEndpoints:
    origin: str = API_ORIGIN
    _allow_loopback: bool = False

    def __post_init__(self) -> None:
        parts = urlsplit(self.origin)
        port = parts.port or (443 if parts.scheme == "https" else 80)
        if self._allow_loopback:
            if parts.hostname not in ("127.0.0.1", "localhost", "::1"):
                raise DisallowedEndpoint("test endpoints must be loopback")
        elif (parts.scheme, parts.hostname, port) != ("https", "api.github.com", 443) or parts.path not in ("", "/"):
            raise DisallowedEndpoint(f"{self.origin!r} is not the GitHub API origin")

    @classmethod
    def loopback_for_tests(cls, origin: str) -> "GitHubEndpoints":
        return cls(origin.rstrip("/"), _allow_loopback=True)

    def check(self, url: str) -> Any:
        if not isinstance(url, str) or len(url) > 8192 or any(ord(c) <= 32 or ord(c) == 127 for c in url):
            raise DisallowedEndpoint("URL must be printable without whitespace")
        parts = urlsplit(url)
        origin = urlsplit(self.origin)
        if (parts.scheme, parts.hostname, parts.port) != (origin.scheme, origin.hostname, origin.port):
            raise DisallowedEndpoint("host is not the GitHub API")
        if parts.username or parts.password or "@" in parts.netloc or parts.fragment:
            raise DisallowedEndpoint("user-info and fragments are not allowed")
        if ".." in parts.path.split("/") or "//" in parts.path:
            raise DisallowedEndpoint("path traversal is not allowed")
        for pattern, keys in _ROUTES:
            if pattern.match(parts.path):
                query = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=bool(parts.query))
                if any(k not in keys for k, _v in query) or len({k for k, _v in query}) != len(query):
                    raise DisallowedEndpoint("unexpected query parameter")
                return parts
        raise DisallowedEndpoint("path is not an allow-listed GitHub API route")

    def _url(self, path: str, query: dict[str, str] | None = None) -> str:
        url = self.origin + path + (("?" + urlencode(query)) if query else "")
        self.check(url)
        return url

    def search(self, q: str, page: int, per_page: int = 100) -> str:
        return self._url("/search/repositories",
                         {"q": q, "sort": "stars", "order": "desc", "per_page": str(per_page), "page": str(page)})

    def repository(self, repository_id: int) -> str:
        return self._url(f"/repositories/{int(repository_id)}")

    def head(self, owner: str, repo: str, ref: str) -> str:
        if not (valid_owner(owner) and valid_repo_name(repo) and valid_ref(ref)):
            raise DisallowedEndpoint("invalid owner, repository or ref")
        return self._url(f"/repos/{owner}/{repo}/commits/{ref}")

    def git(self, owner: str, repo: str, kind: str, sha: str, *, recursive: bool = False) -> str:
        if kind not in ("commits", "trees", "blobs") or not (valid_owner(owner) and valid_repo_name(repo)):
            raise DisallowedEndpoint("invalid git object route")
        return self._url(f"/repos/{owner}/{repo}/git/{kind}/{sha}", {"recursive": "1"} if recursive else None)


GITHUB_API = GitHubEndpoints()


@dataclass(frozen=True)
class ApiResponse:
    status: int
    body: bytes
    etag: str | None

    @property
    def not_modified(self) -> bool:
        return self.status == 304


class GitHubClient:
    def __init__(self, config: GitHubHttpConfig | None = None, credentials: GitHubCredentials | None = None, *,
                 endpoints: GitHubEndpoints = GITHUB_API, transport: Transport | None = None,
                 clock: Callable[[], float] = time.monotonic, wall_clock: Callable[[], datetime] | None = None) -> None:
        self.config = config or GitHubHttpConfig()
        self._credentials = credentials or GitHubCredentials()
        self.endpoints = endpoints
        self.transport = transport or StdlibTransport(self.config.max_connections)
        self.rates = RateTracker(self.config.quota_reserve, now=wall_clock)
        self._clock = clock

    def __repr__(self) -> str:
        return f"GitHubClient(authenticated={self.authenticated}, endpoints={self.endpoints.origin!r})"

    @property
    def authenticated(self) -> bool:
        return self._credentials.token is not None

    def _read(self, resp: Any, limit: int, what: str) -> bytes:
        deadline = self._clock() + self.config.request_deadline_seconds
        chunks, total = [], 0
        while True:
            if self._clock() > deadline:
                raise GitHubFetchError(ErrorClass.TRANSIENT, "deadline_exceeded", f"{what}: deadline exceeded")
            try:
                chunk = resp.read(min(64 * 1024, limit - total + 1))
            except (socket.timeout, TimeoutError) as exc:
                raise GitHubFetchError(ErrorClass.TRANSIENT, "timeout", f"{what}: read timed out") from exc
            except (OSError, http.client.HTTPException) as exc:
                raise GitHubFetchError(ErrorClass.TRANSIENT, "short_read",
                                       f"{what}: connection failed ({type(exc).__name__})") from exc
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > limit:
                raise GitHubFetchError(ErrorClass.DATA_INVALID, "response_too_large",
                                       f"{what}: response exceeds {limit} bytes")
            chunks.append(chunk)

    def get(self, url: str, *, accept: str, max_bytes: int, resource: str = "core",
            etag: str | None = None) -> ApiResponse:
        parts = self.endpoints.check(url)
        what = f"GET {parts.path}"
        self.rates.check(resource)
        headers = {"User-Agent": self.config.user_agent, "Accept": accept, "Accept-Encoding": "identity",
                   "X-GitHub-Api-Version": API_VERSION}
        if etag:
            headers["If-None-Match"] = etag
        if self._credentials.token:
            headers["Authorization"] = f"Bearer {self._credentials.token}"
        try:
            resp = self.transport.open(parts, headers, connect_timeout=self.config.connect_timeout_seconds,
                                       read_timeout=self.config.read_timeout_seconds)
        except (socket.timeout, TimeoutError) as exc:
            raise GitHubFetchError(ErrorClass.TRANSIENT, "timeout", f"{what}: timed out") from None
        except (OSError, http.client.HTTPException) as exc:
            raise GitHubFetchError(ErrorClass.TRANSIENT, "network",
                                   f"{what}: network error ({type(exc).__name__})") from None
        ok = False
        try:
            state = parse_rate_headers(resp.header, resp.status, resource)
            self.rates.update(state)
            if resp.status == 304:
                self._read(resp, 64 * 1024, what)
                ok = True
                return ApiResponse(304, b"", etag)
            if resp.status != 200:
                try:
                    snippet = self._read(resp, 4096, what)
                except GitHubFetchError:
                    snippet = b""
                raise classify(resp.status, resp.header, snippet, state=state, what=what)
            body = self._read(resp, max_bytes, what)
            declared = resp.header("Content-Length")
            if declared is not None and declared.isdigit() and int(declared) != len(body):
                raise GitHubFetchError(ErrorClass.TRANSIENT, "short_read",
                                       f"{what}: received {len(body)} of {declared} bytes")
            ok = True
            return ApiResponse(200, body, resp.header("ETag"))
        finally:
            resp.close(reusable=ok)

    def get_json(self, url: str, *, max_bytes: int, resource: str = "core",
                 etag: str | None = None) -> tuple[Any, str | None] | None:
        """Parsed JSON and ETag, or None on 304 Not Modified."""
        response = self.get(url, accept=ACCEPT_JSON, max_bytes=max_bytes, resource=resource, etag=etag)
        if response.not_modified:
            return None
        try:
            return json.loads(response.body.decode("utf-8"), parse_constant=_reject), response.etag
        except (UnicodeDecodeError, ValueError, RecursionError):
            raise GitHubFetchError(ErrorClass.DATA_INVALID, "bad_json", "GitHub returned invalid JSON") from None

    def close(self) -> None:
        close = getattr(self.transport, "close", None)
        if callable(close):
            close()


def _reject(name: str) -> Any:
    raise ValueError(f"JSON constant {name} not allowed")

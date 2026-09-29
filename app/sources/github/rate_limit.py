"""GitHub rate-limit headers and failure classification (primary and secondary limits).

Primary limits: ``x-ratelimit-limit`` / ``-remaining`` / ``-reset`` (epoch
seconds) / ``-resource`` (``core``, ``search``, ...). Exhausted quota answers
403 or 429 with ``remaining = 0``: defer until ``reset``.

Secondary ("abuse") limits: 403/429 with ``Retry-After`` or a secondary-limit
message and quota left: defer for ``Retry-After`` (default 60 s).

Both are ``rate_limited``: the Runtime Engine defers without consuming an
attempt, and the state is shared through ``kbp.source_rate_limits``.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.queue.states import ErrorClass
from app.sources.commoncrawl.http import FetchError, parse_retry_after

SECONDARY_DEFAULT_SECONDS = 60
MAX_RESET_HORIZON = timedelta(hours=2)


class GitHubFetchError(FetchError):
    """FetchError with an optional absolute ``retry_at`` (primary-limit reset)."""

    def __init__(self, error_class: ErrorClass, code: str, message: str, *, status: int | None = None,
                 retry_after_seconds: int | None = None, retry_at: datetime | None = None,
                 resource: str | None = None) -> None:
        super().__init__(error_class, code, message, status=status, retry_after_seconds=retry_after_seconds)
        self.retry_at = retry_at
        self.resource = resource


@dataclass(frozen=True)
class RateState:
    resource: str
    limit: int | None
    remaining: int | None
    reset_at: datetime | None
    last_status: int | None


def _int(value: str | None) -> int | None:
    return int(value) if value is not None and value.strip().isdigit() else None


def parse_rate_headers(header: Callable[[str], str | None], status: int, default_resource: str) -> RateState | None:
    remaining = _int(header("x-ratelimit-remaining"))
    reset = _int(header("x-ratelimit-reset"))
    if remaining is None and reset is None:
        return None
    reset_at = None
    if reset is not None:
        reset_at = datetime.fromtimestamp(reset, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        reset_at = min(reset_at, now + MAX_RESET_HORIZON)  # never trust an absurd horizon
    resource = (header("x-ratelimit-resource") or default_resource).strip().lower()[:40] or default_resource
    return RateState(resource, _int(header("x-ratelimit-limit")), remaining, reset_at, status)


def classify(status: int, header: Callable[[str], str | None], body_snippet: bytes, *,
             state: RateState | None, what: str) -> GitHubFetchError:
    """Classify an unexpected GitHub status (schema §17.2)."""
    retry_after = parse_retry_after(header("retry-after"))
    text = body_snippet[:2000].decode("utf-8", errors="replace").lower()
    resource = state.resource if state else None
    if status in (403, 429):
        if state is not None and state.remaining == 0:
            return GitHubFetchError(ErrorClass.RATE_LIMITED, "primary_rate_limit",
                                    f"{what}: GitHub {state.resource} quota exhausted", status=status,
                                    retry_after_seconds=retry_after, retry_at=state.reset_at, resource=resource)
        if status == 429 or retry_after is not None or "secondary rate limit" in text or "abuse" in text:
            return GitHubFetchError(ErrorClass.RATE_LIMITED, "secondary_rate_limit",
                                    f"{what}: GitHub secondary rate limit", status=status,
                                    retry_after_seconds=retry_after if retry_after is not None
                                    else SECONDARY_DEFAULT_SECONDS, resource=resource)
        return GitHubFetchError(ErrorClass.PERMANENT_SOURCE, "http_403", f"{what}: forbidden", status=status)
    if status >= 500 or status == 408:
        return GitHubFetchError(ErrorClass.TRANSIENT, f"http_{status}", f"{what}: HTTP {status}", status=status,
                                retry_after_seconds=retry_after)
    if status == 401:
        return GitHubFetchError(ErrorClass.PERMANENT_SOURCE, "unauthorized",
                                f"{what}: HTTP 401 (check KB_GH_TOKEN; value not shown)", status=status)
    if 300 <= status < 400:
        return GitHubFetchError(ErrorClass.PERMANENT_SOURCE, f"http_{status}",
                                f"{what}: HTTP {status}; redirects are never followed", status=status)
    if 400 <= status < 500:
        return GitHubFetchError(ErrorClass.PERMANENT_SOURCE, f"http_{status}", f"{what}: HTTP {status}",
                                status=status)
    return GitHubFetchError(ErrorClass.TRANSIENT, "unexpected_status", f"{what}: unexpected HTTP {status}",
                            status=status)


class RateTracker:
    """Latest observed quota per resource (thread-safe), plus a reserve guard."""

    def __init__(self, reserve: int = 50, now: Callable[[], datetime] | None = None) -> None:
        self.reserve = reserve
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._lock = threading.Lock()
        self._states: dict[str, RateState] = {}

    def update(self, state: RateState | None) -> None:
        if state is not None:
            with self._lock:
                self._states[state.resource] = state

    def states(self) -> list[RateState]:
        with self._lock:
            return list(self._states.values())

    def check(self, resource: str, now: datetime | None = None) -> None:
        """Raise ``rate_limited`` (retry at reset) when the remaining quota is at the reserve."""
        with self._lock:
            state = self._states.get(resource)
        now = now or self._now()
        if (state is not None and state.remaining is not None and state.remaining <= self.reserve
                and state.reset_at is not None and state.reset_at > now):
            raise GitHubFetchError(ErrorClass.RATE_LIMITED, "quota_reserve",
                                   f"GitHub {resource} quota at reserve ({state.remaining} left)",
                                   retry_at=state.reset_at, resource=resource)

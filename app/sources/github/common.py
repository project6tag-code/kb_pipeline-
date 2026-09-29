"""Helpers shared by the GitHub discovery and snapshot handlers."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, TypeVar

from app.db.repositories import credential_fingerprint
from app.queue.retry import JobFailure, RetryPolicy
from app.queue.states import ErrorClass
from app.sources.commoncrawl.http import FetchError
from app.sources.github.client import GitHubClient
from app.sources.github.config import GitHubCredentials
from app.sources.github.rate_limit import GitHubFetchError
from app.sources.github.repository import GitHubStore
from app.workers.registry import JobContext

# Per-job-type retry configuration (schema §17.1; O-20: benchmark and tune).
GH_POLICY = RetryPolicy(max_attempts=8, base_delay_seconds=60.0, max_delay_seconds=3600.0,
                        rate_limit_jitter_seconds=30.0)
DEFAULT_RATE_LIMIT_DEFER_SECONDS = 60

T = TypeVar("T")


def fingerprint(credentials: GitHubCredentials) -> str:
    """``source_rate_limits.credential_fingerprint``: '' unauthenticated, else a non-reversible hash."""
    return credential_fingerprint(credentials.fingerprint_identity)


def gate(ctx: JobContext, store: GitHubStore, fp: str, resources: Iterable[str]) -> None:
    """Defer (without consuming an attempt) while the shared state says a resource is blocked."""
    with ctx.transaction() as conn:
        blocked = [until for r in resources if (until := store.blocked_until(conn, r, fp)) is not None]
    if blocked:
        raise JobFailure(ErrorClass.RATE_LIMITED, "GitHub quota exhausted (shared state)", code="shared_rate_limit",
                         retry_at=max(blocked))


def record_rates(conn: Any, store: GitHubStore, client: GitHubClient, fp: str,
                 failure: FetchError | None = None) -> None:
    """Share the latest observed quota (and a rate-limit failure's Retry-After) via source_rate_limits."""
    retry_after_scope = getattr(failure, "resource", None) if failure is not None else None
    for state in client.rates.states():
        retry_after = None
        if failure is not None and failure.error_class is ErrorClass.RATE_LIMITED and state.resource == retry_after_scope:
            retry_after = failure.retry_after_seconds
        store.record_rate_limit(conn, state, fp, retry_after_seconds=retry_after)


def to_job_failure(exc: FetchError, context: str) -> JobFailure:
    retry_at = getattr(exc, "retry_at", None)
    retry_after = exc.retry_after_seconds
    if exc.error_class is ErrorClass.RATE_LIMITED and retry_at is None and retry_after is None:
        retry_after = DEFAULT_RATE_LIMIT_DEFER_SECONDS
    return JobFailure(exc.error_class, f"{context}: {exc}", code=exc.code, retry_after_seconds=retry_after,
                      retry_at=retry_at)


def is_item_error(exc: FetchError) -> bool:
    return exc.error_class in (ErrorClass.PERMANENT_SOURCE, ErrorClass.DATA_INVALID)


def ordered_bounded(pool: ThreadPoolExecutor, items: Iterable[T], fn: Callable[[T], Any], *, window: int,
                    needs: Callable[[T], bool]) -> Iterator[tuple[T, Any]]:
    """Run ``fn`` for items that ``needs`` it, at most ``window`` in flight, yielding in input order.

    Closing the generator early cancels queued work and waits for running tasks.
    """
    pending: deque[tuple[T, Future | None]] = deque()
    source = iter(items)
    try:
        while True:
            while len(pending) < window:
                item = next(source, _END)
                if item is _END:
                    break
                pending.append((item, pool.submit(fn, item) if needs(item) else None))
            if not pending:
                return
            item, future = pending.popleft()
            yield item, None if future is None else future.result()
    finally:
        futures = [f for _i, f in pending if f is not None]
        for future in futures:
            future.cancel()
        wait(futures)


_END: Any = object()

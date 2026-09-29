"""Retry controller (schema §17.1-§17.4). Pure logic: no database access.

Formula (schema §17.1):

* ceiling  = min(cap, base * 2 ** (attempt_count - 1))
* delay    = uniform(0, ceiling)                      ("full jitter")
* Retry-After (seconds, e.g. HTTP 429/503):  delay = max(delay, retry_after)
* absolute reset (e.g. GitHub ``x-ratelimit-reset``): not_before = reset + uniform(0, rate_limit_jitter)
* next_attempt_at = max(now + delay, not_before)      (computed by the database clock)

Classification (schema §17.2, unit level):

* transient, resource_exhausted, internal_bug, lease_expired -> RETRY while
  attempts remain, else DEAD_LETTER (the attempt is consumed);
* rate_limited -> DEFER: the attempt is NOT consumed (deferral_count + 1);
* permanent_source, data_invalid -> DEAD_LETTER immediately;
* policy_quarantine -> QUARANTINE.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from app.queue.states import (
    DEFERRABLE_ERROR_CLASSES,
    PERMANENT_ERROR_CLASSES,
    QUARANTINE_ERROR_CLASSES,
    RETRYABLE_ERROR_CLASSES,
    SMALLINT_MAX,
    AttemptOutcome,
    ErrorClass,
    truncate_message,
)
from app.utils.time import ensure_utc

_MAX_EXPONENT = 62  # keeps 2 ** n finite; the cap applies long before


@dataclass(frozen=True)
class RetryPolicy:
    """Per-job-type retry configuration (schema §17.1: "configuration")."""

    max_attempts: int = 5
    base_delay_seconds: float = 30.0
    max_delay_seconds: float = 3600.0
    rate_limit_jitter_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts <= SMALLINT_MAX:
            raise ValueError(f"max_attempts must be 1..{SMALLINT_MAX}")
        if self.base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be > 0")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")
        if self.rate_limit_jitter_seconds < 0:
            raise ValueError("rate_limit_jitter_seconds must be >= 0")


def backoff_ceiling(policy: RetryPolicy, attempt_count: int) -> float:
    """min(cap, base * 2^(attempt_count - 1)); attempt_count is the attempts so far (>= 1)."""
    exponent = min(max(attempt_count, 1) - 1, _MAX_EXPONENT)
    return min(policy.max_delay_seconds, policy.base_delay_seconds * (2 ** exponent))


def jittered_backoff(policy: RetryPolicy, attempt_count: int, rng: random.Random) -> float:
    """Full jitter: uniform(0, ceiling)."""
    return rng.uniform(0.0, backoff_ceiling(policy, attempt_count))


@dataclass(frozen=True)
class Failure:
    """A classified failure of one job attempt.

    ``retry_after_seconds`` is a server-provided relative delay (HTTP
    ``Retry-After``); ``retry_at`` an absolute earliest time (for example the
    GitHub ``x-ratelimit-reset`` epoch, as an aware datetime).
    """

    error_class: ErrorClass
    code: str | None = None
    message: str | None = None
    retry_after_seconds: int | None = None
    retry_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "error_class", ErrorClass(self.error_class))
        object.__setattr__(self, "message", truncate_message(self.message))
        if self.retry_after_seconds is not None and self.retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must be >= 0")
        if self.retry_at is not None:
            object.__setattr__(self, "retry_at", ensure_utc(self.retry_at))


class JobFailure(Exception):
    """Raised by a handler to report a classified failure."""

    def __init__(self, error_class: ErrorClass | str, message: str | None = None, *, code: str | None = None,
                 retry_after_seconds: int | None = None, retry_at: datetime | None = None) -> None:
        super().__init__(message or str(error_class))
        self.failure = Failure(ErrorClass(error_class), code, message, retry_after_seconds, retry_at)


def classify_exception(exc: BaseException) -> Failure:
    """Map any exception to a Failure. Unclassified exceptions are internal bugs."""
    if isinstance(exc, JobFailure):
        return exc.failure
    if isinstance(exc, MemoryError):
        return Failure(ErrorClass.RESOURCE_EXHAUSTED, type(exc).__name__, str(exc) or "out of memory")
    return Failure(ErrorClass.INTERNAL_BUG, type(exc).__name__, str(exc))


class DecisionKind(StrEnum):
    RETRY = "retry"                # running -> retry_pending, attempt consumed
    DEFER = "defer"                # running -> retry_pending, attempt NOT consumed
    DEAD_LETTER = "dead_letter"    # running -> dead_letter (+ dead_letters row)
    QUARANTINE = "quarantine"      # running -> quarantined (+ dead_letters row)


@dataclass(frozen=True)
class RetryDecision:
    kind: DecisionKind
    outcome: AttemptOutcome
    delay_seconds: float = 0.0
    not_before: datetime | None = None
    consumes_attempt: bool = True


def decide(policy: RetryPolicy, failure: Failure, attempt_count: int, max_attempts: int,
           rng: random.Random) -> RetryDecision:
    """Decide what happens to a job whose current attempt failed.

    ``attempt_count`` already includes the failed attempt (it is incremented
    at claim time, schema §16.4).
    """
    error_class = failure.error_class
    if error_class in QUARANTINE_ERROR_CLASSES:
        return RetryDecision(DecisionKind.QUARANTINE, AttemptOutcome.FAILED)
    if error_class in PERMANENT_ERROR_CLASSES:
        return RetryDecision(DecisionKind.DEAD_LETTER, AttemptOutcome.FAILED)
    if error_class in DEFERRABLE_ERROR_CLASSES:
        not_before = None
        if failure.retry_at is not None:
            delay = float(failure.retry_after_seconds or 0)
            not_before = failure.retry_at
            not_before = not_before + timedelta(seconds=rng.uniform(0.0, policy.rate_limit_jitter_seconds))
        elif failure.retry_after_seconds is not None:
            delay = float(failure.retry_after_seconds) + rng.uniform(0.0, policy.rate_limit_jitter_seconds)
        else:
            delay = jittered_backoff(policy, max(attempt_count, 1), rng)
        return RetryDecision(DecisionKind.DEFER, AttemptOutcome.DEFERRED_RATE_LIMIT, delay, not_before,
                             consumes_attempt=False)
    if error_class in RETRYABLE_ERROR_CLASSES:
        outcome = AttemptOutcome.LEASE_EXPIRED if error_class is ErrorClass.LEASE_EXPIRED else AttemptOutcome.FAILED
        if attempt_count >= max_attempts:
            return RetryDecision(DecisionKind.DEAD_LETTER, outcome)
        delay = jittered_backoff(policy, attempt_count, rng)
        if failure.retry_after_seconds is not None:
            delay = max(delay, float(failure.retry_after_seconds))
        return RetryDecision(DecisionKind.RETRY, outcome, delay, failure.retry_at)
    raise ValueError(f"unclassified error class {error_class!r}")  # pragma: no cover - enum is exhaustive

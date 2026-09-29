"""Job handler registry: the plug-in point for work-unit implementations.

A handler is registered per ``job_type`` (the schema's job_type list) with
its retry policy. The worker claims only registered job types, so a future
Common Crawl or GitHub handler plugs in by registration alone, without any
change to the queue internals.

Handler contract
----------------
``handler(ctx: JobContext) -> Completed | Yielded``

* Resume from ``ctx.checkpoint`` (the last committed resume position).
* Make progress durable with ``ctx.checkpoint_progress(...)``, ideally in the
  same transaction as the item writes (``with ctx.transaction() as conn:
  ...; ctx.checkpoint_progress(state, conn=conn)``). Checkpoints and
  ``ctx.heartbeat()`` renew the lease; do one of them well within
  ``ctx.lease_seconds``.
* Return ``Completed(...)`` when the unit is done, or ``Yielded(...)`` to hand
  the unit back (e.g. when ``ctx.stop_requested`` is set during shutdown);
  a yield does not consume an attempt.
* Raise ``JobFailure(error_class, ...)`` for classified failures (e.g.
  ``rate_limited`` with ``retry_after_seconds`` / ``retry_at``). Any other
  exception is treated as ``internal_bug``.
* Work must be idempotent (schema §16.1): items after the last checkpoint may
  be replayed after a crash.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from app.queue.job_queue import ClaimedJob
from app.queue.retry import RetryPolicy
from app.queue.states import JobType, Stage


@dataclass(frozen=True)
class Completed:
    metrics: Mapping[str, Any] | None = None
    checkpoint: Mapping[str, Any] | None = None
    stage: Stage | str | None = None


@dataclass(frozen=True)
class Yielded:
    checkpoint: Mapping[str, Any] | None = None
    delay_seconds: float = 0.0


HandlerResult = Completed | Yielded


class QueueOperations(Protocol):
    """The queue operations a handler context needs (implemented by JobQueue)."""

    def transaction(self) -> Any: ...
    def checkpoint(self, job: ClaimedJob, checkpoint: Mapping[str, Any], *, stage: Stage | str | None = None,
                   lease_seconds: float, conn: Any = None) -> Any: ...
    def heartbeat(self, job: ClaimedJob, *, lease_seconds: float) -> Any: ...


@dataclass
class JobContext:
    job: ClaimedJob
    queue: QueueOperations
    lease_seconds: float
    stop_event: threading.Event = field(default_factory=threading.Event)
    checkpoint: dict[str, Any] = field(init=False)
    stage: Stage = field(init=False)

    def __post_init__(self) -> None:
        self.checkpoint = dict(self.job.checkpoint)
        self.stage = self.job.stage

    @property
    def stop_requested(self) -> bool:
        return self.stop_event.is_set()

    def transaction(self) -> Any:
        return self.queue.transaction()

    def checkpoint_progress(self, checkpoint: Mapping[str, Any], *, stage: Stage | str | None = None,
                            conn: Any = None) -> None:
        """Persist the resume position (fenced by the lease; renews it)."""
        self.queue.checkpoint(self.job, checkpoint, stage=stage, lease_seconds=self.lease_seconds, conn=conn)
        self.checkpoint = dict(checkpoint)
        if stage is not None:
            self.stage = Stage(stage)

    def heartbeat(self) -> None:
        self.queue.heartbeat(self.job, lease_seconds=self.lease_seconds)


Handler = Callable[[JobContext], HandlerResult]


@dataclass(frozen=True)
class Registration:
    job_type: JobType
    handler: Handler
    policy: RetryPolicy


class HandlerRegistry:
    def __init__(self, default_policy: RetryPolicy | None = None) -> None:
        self._registrations: dict[JobType, Registration] = {}
        self.default_policy = default_policy or RetryPolicy()

    def register(self, job_type: JobType | str, handler: Handler, *, policy: RetryPolicy | None = None) -> None:
        job_type = JobType(job_type)  # only schema job types exist
        if job_type in self._registrations:
            raise ValueError(f"a handler for {job_type} is already registered")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._registrations[job_type] = Registration(job_type, handler, policy or self.default_policy)

    def handler(self, job_type: JobType | str) -> Handler:
        return self._registrations[JobType(job_type)].handler

    def policy(self, job_type: JobType | str) -> RetryPolicy:
        registration = self._registrations.get(JobType(job_type))
        return registration.policy if registration else self.default_policy

    def job_types(self) -> tuple[JobType, ...]:
        return tuple(self._registrations)

    def __contains__(self, job_type: object) -> bool:
        try:
            return JobType(job_type) in self._registrations  # type: ignore[arg-type]
        except ValueError:
            return False

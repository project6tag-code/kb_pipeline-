"""Worker loop and lease-recovery sweeper (Runtime Engine V1).

Worker loop::

    claim (short transaction, SKIP LOCKED)
      -> run the registered handler (checkpoints renew the lease)
      -> Completed : complete    (running -> succeeded)
         Yielded   : release     (running -> retry_pending, attempt not consumed)
         failure   : retry controller decides
                     RETRY       (running -> retry_pending, backoff)
                     DEFER       (running -> retry_pending, attempt not consumed)
                     DEAD_LETTER (running -> dead_letter + dead_letters row)
                     QUARANTINE  (running -> quarantined + dead_letters row)
      -> LeaseLostError at any point: the job was recovered by a sweeper; this
         worker drops its result (fencing prevents double processing).

Graceful shutdown: ``request_stop()`` (or SIGINT/SIGTERM via
``install_signal_handlers``) stops claiming. The in-flight handler sees
``ctx.stop_requested`` and may checkpoint and return ``Yielded``; claimed jobs
not yet started are released without consuming an attempt. A worker killed
outright is covered by lease expiry and the sweeper.
"""

from __future__ import annotations

import logging
import random
import signal
import socket
import os
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from app.config.settings import WorkerSettings
from app.queue.job_queue import ClaimedJob, FailureResult, LeaseLostError, RecoveryResult
from app.queue.retry import Failure, RetryDecision, RetryPolicy, classify_exception, decide
from app.queue.states import JobType
from app.workers.registry import Completed, HandlerRegistry, JobContext, Yielded

log = logging.getLogger(__name__)


class WorkerQueue(Protocol):
    """Queue operations used by the worker (implemented by app.queue.job_queue.JobQueue)."""

    def transaction(self) -> Any: ...
    def claim(self, *, worker_id: str, job_types: Any, limit: int, lease_seconds: float,
              source_identifier_prefix: str | None = None) -> list[ClaimedJob]: ...
    def checkpoint(self, job: ClaimedJob, checkpoint: Any, *, stage: Any = None, lease_seconds: float,
                   conn: Any = None) -> Any: ...
    def heartbeat(self, job: ClaimedJob, *, lease_seconds: float) -> Any: ...
    def complete(self, job: ClaimedJob, *, metrics: Any = None, checkpoint: Any = None, stage: Any = None) -> None: ...
    def fail(self, job: ClaimedJob, failure: Failure, decision: RetryDecision) -> FailureResult: ...
    def release(self, job: ClaimedJob, *, checkpoint: Any = None, delay_seconds: float = 0.0) -> Any: ...
    def recover_expired_leases(self, *, policy_for: Callable[[JobType], RetryPolicy], rng: random.Random,
                               limit: int = 100, source_identifier_prefix: str | None = None) -> list[RecoveryResult]: ...


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


@dataclass
class WorkerStats:
    claimed: int = 0
    succeeded: int = 0
    retried: int = 0
    deferred: int = 0
    dead_lettered: int = 0
    quarantined: int = 0
    released: int = 0
    lease_lost: int = 0
    recovered: int = 0


class Worker:
    def __init__(self, queue: WorkerQueue, registry: HandlerRegistry, *, settings: WorkerSettings | None = None,
                 worker_id: str | None = None, rng: random.Random | None = None,
                 stop_event: threading.Event | None = None, recover_every: int = 0) -> None:
        if not registry.job_types():
            raise ValueError("register at least one handler before starting a worker")
        self.queue = queue
        self.registry = registry
        self.settings = settings or WorkerSettings()
        self.worker_id = worker_id or default_worker_id()
        self.rng = rng or random.Random()
        self.stop_event = stop_event or threading.Event()
        self.recover_every = recover_every  # run the sweeper every N loop iterations (0 = never)
        self.stats = WorkerStats()

    # -- lifecycle ---------------------------------------------------------- #

    def request_stop(self) -> None:
        self.stop_event.set()

    @property
    def stopping(self) -> bool:
        return self.stop_event.is_set()

    def install_signal_handlers(self) -> None:
        """SIGINT/SIGTERM request a graceful stop (main thread only)."""
        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
            if sig is not None:
                signal.signal(sig, lambda *_: self.request_stop())

    def run(self, *, max_iterations: int | None = None) -> WorkerStats:
        iterations = 0
        while not self.stopping and (max_iterations is None or iterations < max_iterations):
            iterations += 1
            if self.recover_every and iterations % self.recover_every == 0:
                self.recover_expired_leases()
            if self.run_once() == 0 and not self.stopping:
                self.stop_event.wait(self.settings.poll_interval_seconds)
        return self.stats

    def run_once(self) -> int:
        """Claim one bounded batch and process it. Returns the number of jobs claimed."""
        if self.stopping:
            return 0
        jobs = self.queue.claim(worker_id=self.worker_id, job_types=self.registry.job_types(),
                                limit=self.settings.claim_batch_size, lease_seconds=self.settings.lease_seconds,
                                source_identifier_prefix=self.settings.source_identifier_prefix)
        self.stats.claimed += len(jobs)
        for job in jobs:
            if self.stopping:
                self._release(job, None)  # claimed but not started: hand back unconsumed
            else:
                self.process(job)
        return len(jobs)

    def recover_expired_leases(self, limit: int = 100) -> list[RecoveryResult]:
        results = self.queue.recover_expired_leases(
            policy_for=self.registry.policy, rng=self.rng, limit=limit,
            source_identifier_prefix=self.settings.source_identifier_prefix)
        self.stats.recovered += len(results)
        return results

    # -- one job ------------------------------------------------------------ #

    def process(self, job: ClaimedJob) -> None:
        ctx = JobContext(job=job, queue=self.queue, lease_seconds=self.settings.lease_seconds,
                         stop_event=self.stop_event)
        try:
            result = self.registry.handler(job.job_type)(ctx)
        except LeaseLostError:
            self._lease_lost(job)
            return
        except (KeyboardInterrupt, SystemExit):
            self.request_stop()
            self._release(job, None)  # interrupted, not failed: the attempt is not consumed
            raise
        except Exception as exc:
            self._fail(job, classify_exception(exc))
            return
        try:
            if isinstance(result, Completed):
                self.queue.complete(job, metrics=result.metrics, checkpoint=result.checkpoint, stage=result.stage)
                self.stats.succeeded += 1
            elif isinstance(result, Yielded):
                self._release(job, result.checkpoint, result.delay_seconds)
            else:
                self._fail(job, classify_exception(TypeError(
                    f"handler for {job.job_type} returned {type(result).__name__}, expected Completed or Yielded")))
        except LeaseLostError:
            self._lease_lost(job)

    def _fail(self, job: ClaimedJob, failure: Failure) -> None:
        decision = decide(self.registry.policy(job.job_type), failure, job.attempt_count, job.max_attempts, self.rng)
        try:
            result = self.queue.fail(job, failure, decision)
        except LeaseLostError:
            self._lease_lost(job)
            return
        kind = result.decision.kind.value
        counter = {"retry": "retried", "defer": "deferred", "dead_letter": "dead_lettered",
                   "quarantine": "quarantined"}[kind]
        setattr(self.stats, counter, getattr(self.stats, counter) + 1)
        log.info("job %s %s: %s -> %s", job.job_id, failure.error_class, kind, result.status)

    def _release(self, job: ClaimedJob, checkpoint: Any, delay_seconds: float = 0.0) -> None:
        try:
            self.queue.release(job, checkpoint=checkpoint, delay_seconds=delay_seconds)
            self.stats.released += 1
        except LeaseLostError:
            self._lease_lost(job)

    def _lease_lost(self, job: ClaimedJob) -> None:
        self.stats.lease_lost += 1
        log.warning("job %s: lease lost (recovered by another process); result discarded", job.job_id)

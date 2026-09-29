"""Worker loop, handler registry, checkpointing, shutdown and recovery (in-memory queue).

The in-memory ``FakeQueue`` mirrors the semantics of app.queue.job_queue (claim
order, lease fencing, attempt refund on deferral/release, dead letters,
lease recovery) so the orchestration can be tested without PostgreSQL. The
SQL itself is covered by tests/unit/test_runtime_sql.py and, against a real
database, by tests/integration/test_runtime_postgres.py.
"""

import contextlib
import random
import threading
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from app.config.settings import WorkerSettings
from app.queue.job_queue import ClaimedJob, FailureResult, LeaseLostError, RecoveryResult
from app.queue.retry import DecisionKind, Failure, JobFailure, RetryPolicy, decide
from app.queue.states import ErrorClass, JobSource, JobStatus, JobType, Stage
from app.workers.registry import Completed, HandlerRegistry, JobContext, Yielded
from app.workers.runtime import Worker

JT = JobType.POLICY_REEVALUATION
FAST = RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=4.0, rate_limit_jitter_seconds=0.0)


class FakeQueue:
    def __init__(self) -> None:
        self.now = datetime(2026, 9, 28, tzinfo=timezone.utc)
        self.jobs: dict[int, dict] = {}
        self.attempts: list[dict] = []
        self.dead_letters: list[dict] = []
        self._ids = 0

    # helpers
    def add(self, sid: str, *, priority: int = 100, max_attempts: int = 3, checkpoint=None,
            job_type: JobType = JT) -> int:
        self._ids += 1
        self.jobs[self._ids] = dict(job_id=self._ids, job_type=job_type, source=JobSource.INTERNAL,
                                    source_identifier=sid, status=JobStatus.PENDING, stage=Stage.DISCOVERED,
                                    checkpoint=dict(checkpoint or {}), priority=priority, attempt_count=0,
                                    max_attempts=max_attempts, deferral_count=0, next_attempt_at=self.now,
                                    lease_token=None, lease_expires_at=None, worker_id=None, last_error_class=None)
        return self._ids

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)

    def outcomes(self, job_id: int) -> list[str]:
        return [a["outcome"] for a in self.attempts if a["job_id"] == job_id]

    def _row(self, job: ClaimedJob) -> dict:
        row = self.jobs[job.job_id]
        if row["status"] != JobStatus.RUNNING or row["lease_token"] != job.lease_token:
            raise LeaseLostError(f"job {job.job_id}")
        return row

    def _attempt(self, job_id: int, token) -> dict:
        return next(a for a in self.attempts if a["job_id"] == job_id and a["lease_token"] == token
                    and a["finished_at"] is None)

    def _clear(self, row: dict) -> None:
        row.update(worker_id=None, lease_token=None, lease_expires_at=None)

    # queue protocol
    def transaction(self):
        return contextlib.nullcontext(self)

    def claim(self, *, worker_id, job_types, limit, lease_seconds, source_identifier_prefix=None):
        eligible = sorted(
            (r for r in self.jobs.values()
             if r["status"] in (JobStatus.PENDING, JobStatus.RETRY_PENDING) and r["next_attempt_at"] <= self.now
             and r["attempt_count"] < r["max_attempts"] and r["job_type"] in job_types
             and (source_identifier_prefix is None or r["source_identifier"].startswith(source_identifier_prefix))),
            key=lambda r: (r["priority"], r["next_attempt_at"], r["job_id"]))[:limit]
        claimed = []
        for r in eligible:
            r.update(status=JobStatus.RUNNING, worker_id=worker_id, lease_token=uuid.uuid4(),
                     lease_expires_at=self.now + timedelta(seconds=lease_seconds),
                     attempt_count=r["attempt_count"] + 1)
            attempt_no = 1 + max((a["attempt_no"] for a in self.attempts if a["job_id"] == r["job_id"]), default=0)
            self.attempts.append(dict(job_id=r["job_id"], attempt_no=attempt_no, lease_token=r["lease_token"],
                                      worker_id=worker_id, finished_at=None, outcome=None, error_class=None))
            claimed.append(ClaimedJob(r["job_id"], r["job_type"], r["source"], r["source_identifier"], r["stage"],
                                      dict(r["checkpoint"]), r["priority"], r["attempt_count"], r["max_attempts"],
                                      r["deferral_count"], r["lease_token"], r["lease_expires_at"], None, worker_id,
                                      len(self.attempts), attempt_no))
        return claimed

    def checkpoint(self, job, checkpoint, *, stage=None, lease_seconds, conn=None):
        row = self._row(job)
        row["checkpoint"] = dict(checkpoint)
        if stage is not None:
            row["stage"] = Stage(stage)
        row["lease_expires_at"] = self.now + timedelta(seconds=lease_seconds)
        return row["lease_expires_at"]

    def heartbeat(self, job, *, lease_seconds):
        row = self._row(job)
        row["lease_expires_at"] = self.now + timedelta(seconds=lease_seconds)
        return row["lease_expires_at"]

    def complete(self, job, *, metrics=None, checkpoint=None, stage=None):
        row = self._row(job)
        row["status"] = JobStatus.SUCCEEDED
        if checkpoint is not None:
            row["checkpoint"] = dict(checkpoint)
        if stage is not None:  # COMPLETE_JOB_SQL: stage = COALESCE(:stage, stage)
            row["stage"] = Stage(stage)
        self._clear(row)
        self._attempt(job.job_id, job.lease_token).update(finished_at=self.now, outcome="succeeded", metrics=metrics)

    def _apply(self, row, token, failure: Failure, decision) -> FailureResult:
        attempt = self._attempt(row["job_id"], token)
        if decision.kind in (DecisionKind.RETRY, DecisionKind.DEFER):
            defer = decision.kind is DecisionKind.DEFER
            next_at = self.now + timedelta(seconds=decision.delay_seconds)
            if decision.not_before is not None:
                next_at = max(next_at, decision.not_before)
            row.update(status=JobStatus.RETRY_PENDING, next_attempt_at=next_at, last_error_class=failure.error_class,
                       attempt_count=row["attempt_count"] - int(defer), deferral_count=row["deferral_count"] + int(defer))
            self._clear(row)
            attempt.update(finished_at=self.now, outcome=decision.outcome.value, error_class=failure.error_class.value)
            return FailureResult(decision, JobStatus.RETRY_PENDING, next_attempt_at=next_at)
        status = JobStatus.QUARANTINED if decision.kind is DecisionKind.QUARANTINE else JobStatus.DEAD_LETTER
        row.update(status=status, last_error_class=failure.error_class)
        self._clear(row)
        attempt.update(finished_at=self.now, outcome=decision.outcome.value, error_class=failure.error_class.value)
        self.dead_letters.append(dict(item_type="job", item_key=str(row["job_id"]), stage=row["stage"],
                                      disposition="quarantined" if status is JobStatus.QUARANTINED else "dead_letter",
                                      error_class=failure.error_class.value, attempt_count=row["attempt_count"]))
        return FailureResult(decision, status, dead_letter_id=len(self.dead_letters))

    def fail(self, job, failure, decision):
        return self._apply(self._row(job), job.lease_token, failure, decision)

    def release(self, job, *, checkpoint=None, delay_seconds=0.0):
        row = self._row(job)
        if checkpoint is not None:
            row["checkpoint"] = dict(checkpoint)
        row.update(status=JobStatus.RETRY_PENDING, attempt_count=row["attempt_count"] - 1,
                   next_attempt_at=self.now + timedelta(seconds=delay_seconds))
        self._attempt(job.job_id, job.lease_token).update(finished_at=self.now, outcome="cancelled")
        self._clear(row)
        return row["next_attempt_at"]

    def recover_expired_leases(self, *, policy_for, rng, limit=100, source_identifier_prefix=None):
        results = []
        for row in list(self.jobs.values()):
            if row["status"] == JobStatus.RUNNING and row["lease_expires_at"] < self.now:
                failure = Failure(ErrorClass.LEASE_EXPIRED, "lease_expired", "lease expired")
                decision = decide(policy_for(row["job_type"]), failure, row["attempt_count"], row["max_attempts"], rng)
                result = self._apply(row, row["lease_token"], failure, decision)
                results.append(RecoveryResult(row["job_id"], result.status, result.next_attempt_at,
                                              result.dead_letter_id))
        return results


def make_worker(queue: FakeQueue, handler, *, policy: RetryPolicy = FAST, batch: int = 1, **kwargs) -> Worker:
    registry = HandlerRegistry()
    registry.register(JT, handler, policy=policy)
    return Worker(queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0,
                                                           claim_batch_size=batch),
                  worker_id="w1", rng=random.Random(0), **kwargs)


class LifecycleTests(unittest.TestCase):
    def test_success(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        worker = make_worker(q, lambda ctx: Completed(metrics={"items": 3}))
        self.assertEqual(worker.run_once(), 1)
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.SUCCEEDED)
        self.assertEqual(q.outcomes(job_id), ["succeeded"])
        self.assertEqual(worker.stats.succeeded, 1)

    def test_retry_then_success_records_every_attempt(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        calls = []

        def handler(ctx):
            calls.append(ctx.job.attempt_no)
            if len(calls) == 1:
                raise JobFailure(ErrorClass.TRANSIENT, "timeout")
            return Completed()

        worker = make_worker(q, handler)
        worker.run_once()
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.RETRY_PENDING)
        self.assertLessEqual(q.jobs[job_id]["next_attempt_at"], q.now + timedelta(seconds=1))  # backoff ceiling 1s
        q.advance(5)
        worker.run_once()
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.SUCCEEDED)
        self.assertEqual(q.outcomes(job_id), ["failed", "succeeded"])
        self.assertEqual(calls, [1, 2])

    def test_max_retries_goes_to_dead_letter(self) -> None:
        q = FakeQueue()
        job_id = q.add("a", max_attempts=3)
        worker = make_worker(q, lambda ctx: (_ for _ in ()).throw(JobFailure("transient", "down")))
        for _ in range(3):
            worker.run_once()
            q.advance(10)
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.DEAD_LETTER)
        self.assertEqual(q.outcomes(job_id), ["failed", "failed", "failed"])
        self.assertEqual(q.dead_letters, [dict(item_type="job", item_key=str(job_id), stage=Stage.DISCOVERED,
                                               disposition="dead_letter", error_class="transient", attempt_count=3)])
        self.assertEqual(worker.run_once(), 0)  # never claimed again

    def test_permanent_failure_dead_letters_immediately(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        worker = make_worker(q, lambda ctx: (_ for _ in ()).throw(JobFailure("permanent_source", "404", code="404")))
        worker.run_once()
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.DEAD_LETTER)
        self.assertEqual(worker.stats.dead_lettered, 1)

    def test_policy_quarantine(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        worker = make_worker(q, lambda ctx: (_ for _ in ()).throw(JobFailure("policy_quarantine", "secret found")))
        worker.run_once()
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.QUARANTINED)
        self.assertEqual(q.dead_letters[0]["disposition"], "quarantined")

    def test_unclassified_exception_is_an_internal_bug(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        worker = make_worker(q, lambda ctx: 1 / 0)
        worker.run_once()
        self.assertEqual(q.jobs[job_id]["last_error_class"], ErrorClass.INTERNAL_BUG)
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.RETRY_PENDING)

    def test_wrong_handler_result_is_an_internal_bug(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        make_worker(q, lambda ctx: "done").run_once()
        self.assertEqual(q.jobs[job_id]["last_error_class"], ErrorClass.INTERNAL_BUG)


class RateLimitTests(unittest.TestCase):
    def test_retry_after_defers_without_consuming_an_attempt(self) -> None:
        q = FakeQueue()
        job_id = q.add("a", max_attempts=1)
        worker = make_worker(q, lambda ctx: (_ for _ in ()).throw(
            JobFailure(ErrorClass.RATE_LIMITED, "429", retry_after_seconds=30)))
        worker.run_once()
        row = q.jobs[job_id]
        self.assertEqual((row["status"], row["attempt_count"], row["deferral_count"]),
                         (JobStatus.RETRY_PENDING, 0, 1))
        self.assertEqual(row["next_attempt_at"], q.now + timedelta(seconds=30))
        self.assertEqual(q.outcomes(job_id), ["deferred_rate_limit"])
        q.advance(29)
        self.assertEqual(worker.run_once(), 0)  # Retry-After honoured
        q.advance(1)
        self.assertEqual(worker.run_once(), 1)  # max_attempts=1 still allows the real attempt

    def test_reset_timestamp(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        reset = q.now + timedelta(minutes=10)
        make_worker(q, lambda ctx: (_ for _ in ()).throw(
            JobFailure(ErrorClass.RATE_LIMITED, "403", retry_at=reset))).run_once()
        self.assertEqual(q.jobs[job_id]["next_attempt_at"], reset)


class CheckpointTests(unittest.TestCase):
    def test_checkpoint_persists_and_resumes_after_yield(self) -> None:
        q = FakeQueue()
        job_id = q.add("a", checkpoint={"row": 0})
        seen = []

        def handler(ctx: JobContext):
            start = ctx.checkpoint["row"]
            seen.append(start)
            for row in range(start, min(start + 2, 5)):
                ctx.checkpoint_progress({"row": row + 1}, stage=Stage.ACQUIRED)
            if ctx.checkpoint["row"] < 5:
                return Yielded()
            return Completed(metrics={"rows": 5})

        worker = make_worker(q, handler)
        for _ in range(3):
            worker.run_once()
        self.assertEqual(seen, [0, 2, 4])
        row = q.jobs[job_id]
        self.assertEqual((row["status"], row["checkpoint"], row["stage"]), (JobStatus.SUCCEEDED, {"row": 5}, Stage.ACQUIRED))
        self.assertEqual(row["attempt_count"], 1)  # yields are not consumed
        self.assertEqual(q.outcomes(job_id), ["cancelled", "cancelled", "succeeded"])

    def test_checkpoint_survives_a_failure(self) -> None:
        q = FakeQueue()
        job_id = q.add("a", checkpoint={"row": 0})

        def handler(ctx):
            ctx.checkpoint_progress({"row": 7})
            raise JobFailure("transient", "later")

        make_worker(q, handler).run_once()
        self.assertEqual(q.jobs[job_id]["checkpoint"], {"row": 7})


class CrashRecoveryTests(unittest.TestCase):
    def test_crashed_worker_job_is_recovered_and_finished_once(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")
        crashed = q.claim(worker_id="crashed", job_types=(JT,), limit=1, lease_seconds=60)[0]  # never finished
        q.advance(61)
        worker = make_worker(q, lambda ctx: Completed())
        recovered = worker.recover_expired_leases()
        self.assertEqual([(r.job_id, r.status) for r in recovered], [(job_id, JobStatus.RETRY_PENDING)])
        q.advance(10)
        worker.run_once()
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.SUCCEEDED)
        self.assertEqual(q.outcomes(job_id), ["lease_expired", "succeeded"])
        with self.assertRaises(LeaseLostError):  # the crashed worker's late result is fenced out
            q.complete(crashed)

    def test_recovery_after_the_last_attempt_dead_letters(self) -> None:
        q = FakeQueue()
        job_id = q.add("a", max_attempts=1)
        q.claim(worker_id="crashed", job_types=(JT,), limit=1, lease_seconds=60)
        q.advance(61)
        make_worker(q, lambda ctx: Completed()).recover_expired_leases()
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.DEAD_LETTER)
        self.assertEqual(q.dead_letters[0]["error_class"], "lease_expired")

    def test_lease_lost_mid_run_discards_the_result(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")

        def slow_handler(ctx):
            q.advance(120)  # the handler outlives its lease without a heartbeat...
            q.recover_expired_leases(policy_for=lambda _t: FAST, rng=random.Random(0))  # ...a sweeper recovers it
            return Completed()

        worker = make_worker(q, slow_handler)
        worker.run_once()
        self.assertEqual(worker.stats.lease_lost, 1)
        self.assertEqual(worker.stats.succeeded, 0)
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.RETRY_PENDING)
        self.assertEqual(q.outcomes(job_id), ["lease_expired"])

    def test_heartbeat_keeps_the_lease(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")

        def handler(ctx):
            for _ in range(3):
                q.advance(50)
                ctx.heartbeat()
                q.recover_expired_leases(policy_for=lambda _t: FAST, rng=random.Random(0))
            return Completed()

        make_worker(q, handler).run_once()
        self.assertEqual(q.jobs[job_id]["status"], JobStatus.SUCCEEDED)


class ClaimOrderAndShutdownTests(unittest.TestCase):
    def test_priority_order(self) -> None:
        q = FakeQueue()
        low, high, mid = q.add("low", priority=200), q.add("high", priority=1), q.add("mid", priority=100)
        order = []
        worker = make_worker(q, lambda ctx: order.append(ctx.job.job_id) or Completed(), batch=3)
        worker.run_once()
        self.assertEqual(order, [high, mid, low])

    def test_graceful_shutdown_yields_in_flight_and_releases_unstarted(self) -> None:
        q = FakeQueue()
        first, second = q.add("a"), q.add("b")
        stop = threading.Event()

        def handler(ctx):
            stop.set()  # shutdown requested while the first job runs
            self.assertTrue(ctx.stop_requested)
            ctx.checkpoint_progress({"row": 1})
            return Yielded()

        worker = make_worker(q, handler, batch=2, stop_event=stop)
        worker.run(max_iterations=5)
        for job_id in (first, second):
            self.assertEqual(q.jobs[job_id]["status"], JobStatus.RETRY_PENDING)
            self.assertEqual(q.jobs[job_id]["attempt_count"], 0)
        self.assertEqual(q.jobs[first]["checkpoint"], {"row": 1})
        self.assertEqual(worker.stats.released, 2)
        self.assertEqual(worker.run_once(), 0)  # no claiming after stop

    def test_keyboard_interrupt_releases_and_reraises(self) -> None:
        q = FakeQueue()
        job_id = q.add("a")

        def handler(ctx):
            raise KeyboardInterrupt

        worker = make_worker(q, handler)
        with self.assertRaises(KeyboardInterrupt):
            worker.run_once()
        self.assertEqual((q.jobs[job_id]["status"], q.jobs[job_id]["attempt_count"]), (JobStatus.RETRY_PENDING, 0))
        self.assertTrue(worker.stopping)


class RegistryTests(unittest.TestCase):
    def test_registration_rules(self) -> None:
        registry = HandlerRegistry()
        registry.register("policy_reevaluation", lambda ctx: Completed())
        with self.assertRaises(ValueError):
            registry.register(JT, lambda ctx: Completed())
        with self.assertRaises(ValueError):
            registry.register("not_a_schema_job_type", lambda ctx: Completed())
        with self.assertRaises(TypeError):
            registry.register(JobType.LEGACY_RECONCILIATION, "not callable")  # type: ignore[arg-type]
        self.assertEqual(registry.job_types(), (JT,))
        self.assertIn("policy_reevaluation", registry)
        self.assertNotIn("x", registry)

    def test_worker_claims_only_registered_types(self) -> None:
        q = FakeQueue()
        other = q.add("x", job_type=JobType.LEGACY_RECONCILIATION)
        make_worker(q, lambda ctx: Completed()).run_once()
        self.assertEqual(q.jobs[other]["status"], JobStatus.PENDING)

    def test_worker_requires_a_handler(self) -> None:
        with self.assertRaises(ValueError):
            Worker(FakeQueue(), HandlerRegistry())


if __name__ == "__main__":
    unittest.main()

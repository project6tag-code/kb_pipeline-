"""Opt-in PostgreSQL integration tests for Runtime Engine V1 (role kb_pipeline_app).

Run only when BOTH are set (never by default, never against an unintended DB):

    KB_RUN_PG_INTEGRATION=1
    KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/<database>

    python -m unittest discover -s tests/integration -v

Prefer the restored copy used for the Phase 1A dry run.

Data rules:
* the tests connect as kb_pipeline_app (verified first) and touch only kbp
  runtime tables; no public table is read or written;
* every job uses a random ``it-<hex>/`` source_identifier prefix, and every
  claim/recovery is restricted to that prefix, so real work is never claimed;
* tests that need no concurrency run in a transaction that is rolled back;
* committed test jobs end in a terminal status, and their dead letters are
  resolved as ``ignored``. The runtime role has no DELETE on these tables (by
  design), so those rows remain as inert, clearly prefixed records.
"""

import os
import random
import threading
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_INTEGRATION_DATABASE_URL"))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_INTEGRATION_DATABASE_URL to run")
class RuntimePostgresTests(unittest.TestCase):
    db = None
    queue = None
    prefix = ""

    @classmethod
    def setUpClass(cls) -> None:
        from app.config.settings import DatabaseSettings
        from app.db.connection import Database
        from app.queue.job_queue import JobQueue

        settings = DatabaseSettings(url=os.environ["KB_INTEGRATION_DATABASE_URL"], pool_size=4,
                                    application_name="kb_pipeline:integration_test")
        cls.db = Database.from_settings(settings)
        cls.db.verify_runtime_identity()  # kb_pipeline_app, not superuser, kbp_001c, UTC
        cls.queue = JobQueue(cls.db)
        cls.prefix = f"it-{uuid.uuid4().hex}/"

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.db is None:
            return
        from sqlalchemy import text

        from app.db import repositories

        with cls.db.transaction() as conn:
            rows = conn.execute(text(
                "SELECT job_id, status FROM kbp.pipeline_jobs WHERE starts_with(source_identifier, :p)"),
                {"p": cls.prefix}).all()
            for job_id, status in rows:
                if status in ("pending", "retry_pending"):
                    conn.execute(text("UPDATE kbp.pipeline_jobs SET status = 'cancelled', completed_at = now(), "
                                      "updated_at = now() WHERE job_id = :j AND status IN ('pending', 'retry_pending')"),
                                 {"j": job_id})
                open_letters = conn.execute(text(
                    "SELECT dead_letter_id FROM kbp.dead_letters WHERE item_type = 'job' AND item_key = :k "
                    "AND resolved_at IS NULL"), {"k": str(job_id)}).scalars().all()
                for dead_letter_id in open_letters:
                    repositories.resolve_dead_letter(conn, dead_letter_id, "ignored")
        cls.db.dispose()

    # -- helpers ------------------------------------------------------------ #

    def spec(self, name: str, **kwargs):
        from app.queue.job_queue import JobSpec

        return JobSpec(job_type="policy_reevaluation", source="internal",
                       source_identifier=f"{self.prefix}{self._testMethodName}/{name}", **kwargs)

    def claim(self, limit: int = 1, lease_seconds: float = 60, worker_id: str = "it-worker"):
        return self.queue.claim(worker_id=worker_id, job_types=("policy_reevaluation",), limit=limit,
                                lease_seconds=lease_seconds, source_identifier_prefix=self.prefix)

    def rolled_back(self):
        """A connection whose transaction is always rolled back."""
        test = self

        class _Ctx:
            def __enter__(self):
                self.conn = test.db.engine.connect()
                self.conn.begin()
                return self.conn

            def __exit__(self, *exc):
                self.conn.rollback()
                self.conn.close()
                return False

        return _Ctx()

    def db_now(self) -> datetime:
        return self.db.health_check().server_time

    # -- enqueue ------------------------------------------------------------ #

    def test_enqueue_is_idempotent(self) -> None:
        from app.queue.job_queue import enqueue_job, get_job

        with self.rolled_back() as conn:
            first = enqueue_job(conn, self.spec("a"))
            second = enqueue_job(conn, self.spec("a", priority=1))
            self.assertTrue(first.created)
            self.assertFalse(second.created)
            self.assertEqual(first.job_id, second.job_id)
            row = get_job(conn, first.job_id)
            self.assertEqual((row["status"], row["priority"], row["attempt_count"]), ("pending", 100, 0))

    def test_duplicate_logical_job_is_prevented_by_the_unique_key(self) -> None:
        from sqlalchemy import text
        from sqlalchemy.exc import IntegrityError

        from app.queue.job_queue import enqueue_job

        with self.rolled_back() as conn:
            job = enqueue_job(conn, self.spec("dup"))
            savepoint = conn.begin_nested()
            with self.assertRaises(IntegrityError):
                conn.execute(text(
                    "INSERT INTO kbp.pipeline_jobs (job_type, source, source_identifier, status, stage, max_attempts, "
                    "next_attempt_at, updated_at) VALUES ('policy_reevaluation', 'internal', :sid, 'pending', "
                    "'DISCOVERED', 3, now(), now())"), {"sid": f"{self.prefix}{self._testMethodName}/dup"})
            savepoint.rollback()
            self.assertEqual(conn.execute(text("SELECT count(*) FROM kbp.pipeline_jobs WHERE source_identifier = :s"),
                                          {"s": f"{self.prefix}{self._testMethodName}/dup"}).scalar_one(), 1)
            self.assertIsNotNone(job.job_id)

    # -- claiming ----------------------------------------------------------- #

    def test_two_workers_never_claim_the_same_job(self) -> None:
        self.queue.enqueue_many([self.spec(f"j{i}") for i in range(20)])
        barrier = threading.Barrier(2)
        claimed: dict[str, list] = {"w1": [], "w2": []}

        def work(worker_id: str) -> None:
            barrier.wait()
            while True:
                jobs = self.claim(limit=3, worker_id=worker_id)
                if not jobs:
                    return
                claimed[worker_id].extend(jobs)

        threads = [threading.Thread(target=work, args=(w,)) for w in claimed]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        ids1 = {j.job_id for j in claimed["w1"]}
        ids2 = {j.job_id for j in claimed["w2"]}
        self.assertEqual(ids1 & ids2, set())
        self.assertEqual(len(ids1 | ids2), 20)
        self.assertEqual(len(claimed["w1"]) + len(claimed["w2"]), 20)
        for job in claimed["w1"] + claimed["w2"]:
            attempts = self.queue.attempts(job.job_id)
            self.assertEqual([a["attempt_no"] for a in attempts], [1])
            self.queue.complete(job)

    def test_priority_ordering(self) -> None:
        self.queue.enqueue_many([self.spec("low", priority=300), self.spec("high", priority=-5),
                                 self.spec("mid", priority=100)])
        jobs = self.claim(limit=3)
        self.assertEqual([j.source_identifier.rsplit("/", 1)[1] for j in jobs], ["high", "mid", "low"])
        for job in jobs:
            self.queue.complete(job)

    def test_available_at_delays_claiming(self) -> None:
        later = datetime.now(timezone.utc) + timedelta(hours=1)
        self.queue.enqueue(self.spec("later", available_at=later))
        self.assertEqual(self.claim(), [])

    # -- retries, attempts, dead letters ------------------------------------ #

    def test_retry_backoff_and_attempt_recording(self) -> None:
        from app.queue.retry import Failure, RetryPolicy, decide
        from app.queue.states import ErrorClass

        policy = RetryPolicy(max_attempts=3, base_delay_seconds=30, max_delay_seconds=60)
        self.queue.enqueue(self.spec("r", max_attempts=3))
        job = self.claim()[0]
        before = self.db_now()
        failure = Failure(ErrorClass.TRANSIENT, "E1", "timeout")
        result = self.queue.fail(job, failure, decide(policy, failure, job.attempt_count, job.max_attempts,
                                                      random.Random(1)))
        after = self.db_now()
        self.assertEqual(result.status, "retry_pending")
        self.assertTrue(before <= result.next_attempt_at <= after + timedelta(seconds=30))
        row = self.queue.get(job.job_id)
        self.assertEqual((row["status"], row["attempt_count"], row["last_error_class"], row["lease_token"]),
                         ("retry_pending", 1, "transient", None))
        (attempt,) = self.queue.attempts(job.job_id)
        self.assertEqual((attempt["attempt_no"], attempt["outcome"], attempt["error_class"], attempt["error_code"]),
                         (1, "failed", "transient", "E1"))
        self.assertIsNotNone(attempt["finished_at"])
        self.assertEqual(attempt["scheduled_next_attempt_at"], row["next_attempt_at"])

    def test_retry_after_defers_without_consuming(self) -> None:
        from app.queue.retry import Failure, RetryPolicy, decide
        from app.queue.states import ErrorClass

        self.queue.enqueue(self.spec("ra", max_attempts=1))
        job = self.claim()[0]
        failure = Failure(ErrorClass.RATE_LIMITED, "429", retry_after_seconds=120)
        before = self.db_now()
        self.queue.fail(job, failure, decide(RetryPolicy(rate_limit_jitter_seconds=0), failure, 1, 1, random.Random()))
        row = self.queue.get(job.job_id)
        self.assertEqual((row["status"], row["attempt_count"], row["deferral_count"]), ("retry_pending", 0, 1))
        self.assertGreaterEqual(row["next_attempt_at"], before + timedelta(seconds=119))
        self.assertEqual(self.queue.attempts(job.job_id)[0]["retry_after_seconds"], 120)

    def test_max_retries_go_to_dead_letter(self) -> None:
        from app.db import repositories
        from app.queue.retry import Failure, RetryPolicy, decide
        from app.queue.states import ErrorClass

        policy = RetryPolicy(max_attempts=2, base_delay_seconds=0.001, max_delay_seconds=0.001)
        self.queue.enqueue(self.spec("dl", max_attempts=2))
        failure = Failure(ErrorClass.TRANSIENT, "E", "always")
        results = []
        for _ in range(2):
            time.sleep(0.05)
            job = self.claim()[0]
            results.append(self.queue.fail(job, failure, decide(policy, failure, job.attempt_count,
                                                                 job.max_attempts, random.Random())))
        self.assertEqual([r.status for r in results], ["retry_pending", "dead_letter"])
        row = self.queue.get(job.job_id)
        self.assertEqual((row["status"], row["attempt_count"]), ("dead_letter", 2))
        self.assertIsNotNone(row["completed_at"])
        with self.db.transaction() as conn:
            letter = repositories.get_dead_letter(conn, "job", str(job.job_id), "DISCOVERED")
        self.assertEqual((letter["disposition"], letter["error_class"], letter["attempt_count"], letter["job_id"]),
                         ("dead_letter", "transient", 2, job.job_id))
        self.assertEqual([a["outcome"] for a in self.queue.attempts(job.job_id)], ["failed", "failed"])
        self.assertEqual(self.claim(), [])

    # -- checkpoints --------------------------------------------------------- #

    def test_checkpoint_persistence_and_resume(self) -> None:
        from app.queue.states import Stage

        self.queue.enqueue(self.spec("cp", checkpoint={"row": 0}))
        job = self.claim(lease_seconds=30)[0]
        new_expiry = self.queue.checkpoint(job, {"row": 500}, stage=Stage.ACQUIRED, lease_seconds=600)
        self.assertGreater(new_expiry, job.lease_expires_at)
        row = self.queue.get(job.job_id)
        self.assertEqual((row["checkpoint"], row["stage"]), ({"row": 500}, "ACQUIRED"))
        self.queue.release(job)
        resumed = self.claim()[0]
        self.assertEqual((resumed.checkpoint, resumed.stage, resumed.attempt_count, resumed.attempt_no),
                         ({"row": 500}, Stage.ACQUIRED, 1, 2))
        self.queue.complete(resumed, metrics={"rows": 500})
        self.assertEqual([a["outcome"] for a in self.queue.attempts(job.job_id)], ["cancelled", "succeeded"])

    def test_checkpoint_is_transactional_with_item_writes(self) -> None:
        self.queue.enqueue(self.spec("tx", checkpoint={"row": 0}))
        job = self.claim()[0]
        with self.assertRaises(RuntimeError):
            with self.db.transaction() as conn:
                self.queue.checkpoint(job, {"row": 9}, lease_seconds=60, conn=conn)
                raise RuntimeError("item write failed")
        self.assertEqual(self.queue.get(job.job_id)["checkpoint"], {"row": 0})
        self.queue.complete(job)

    # -- crash / lease recovery --------------------------------------------- #

    def test_expired_lease_recovery_and_fencing(self) -> None:
        from app.queue.job_queue import LeaseLostError
        from app.queue.retry import RetryPolicy

        self.queue.enqueue(self.spec("crash", max_attempts=3))
        crashed = self.claim(lease_seconds=1)[0]  # the worker "crashes" here
        time.sleep(1.5)
        recovered = self.queue.recover_expired_leases(policy_for=lambda _t: RetryPolicy(), rng=random.Random(),
                                                      source_identifier_prefix=self.prefix)
        self.assertEqual([(r.job_id, r.status) for r in recovered], [(crashed.job_id, "retry_pending")])
        row = self.queue.get(crashed.job_id)
        self.assertEqual((row["status"], row["attempt_count"], row["last_error_class"]),
                         ("retry_pending", 1, "lease_expired"))
        self.assertEqual([a["outcome"] for a in self.queue.attempts(crashed.job_id)], ["lease_expired"])
        with self.assertRaises(LeaseLostError):  # the crashed worker's late completion is rejected
            self.queue.complete(crashed)
        with self.assertRaises(LeaseLostError):
            self.queue.checkpoint(crashed, {"row": 1}, lease_seconds=60)
        self.assertEqual(self.queue.get(crashed.job_id)["status"], "retry_pending")

    def test_crash_on_the_last_attempt_dead_letters(self) -> None:
        from app.queue.retry import RetryPolicy

        self.queue.enqueue(self.spec("crash-last", max_attempts=1))
        crashed = self.claim(lease_seconds=1)[0]
        time.sleep(1.5)
        recovered = self.queue.recover_expired_leases(policy_for=lambda _t: RetryPolicy(), rng=random.Random(),
                                                      source_identifier_prefix=self.prefix)
        self.assertEqual([r.status for r in recovered], ["dead_letter"])
        self.assertEqual(self.queue.get(crashed.job_id)["last_error_class"], "lease_expired")

    # -- worker end to end, rerun idempotency --------------------------------- #

    def test_worker_end_to_end_and_rerun_idempotency(self) -> None:
        from app.config.settings import WorkerSettings
        from app.workers.registry import Completed, HandlerRegistry
        from app.workers.runtime import Worker

        processed = []

        def handler(ctx):
            with ctx.transaction() as conn:
                ctx.checkpoint_progress({"done": True}, conn=conn)
            processed.append(ctx.job.source_identifier)
            return Completed(metrics={"ok": 1})

        registry = HandlerRegistry()
        registry.register("policy_reevaluation", handler)
        worker = Worker(self.queue, registry, settings=WorkerSettings(
            lease_seconds=60, poll_interval_seconds=0, claim_batch_size=5, source_identifier_prefix=self.prefix))
        specs = [self.spec(f"e{i}") for i in range(3)]
        self.assertTrue(all(r.created for r in self.queue.enqueue_many(specs)))
        worker.run(max_iterations=3)
        self.assertEqual(sorted(processed), sorted(s.source_identifier for s in specs))
        rerun = self.queue.enqueue_many(specs)
        self.assertEqual({(r.created, r.status) for r in rerun}, {(False, "succeeded")})
        self.assertEqual(worker.run_once(), 0)
        self.assertEqual(len(processed), 3)

    # -- runs and rate limits (rolled back) ------------------------------------ #

    def test_pipeline_run_lifecycle(self) -> None:
        from app.db import repositories

        with self.rolled_back() as conn:
            run = repositories.create_run(conn, "maintenance", code_version="test", notes="integration test")
            self.assertEqual((run.status, run.identity_spec_version), ("running", "v1"))
            self.assertEqual(repositories.get_run(conn, run.run_id).run_id, run.run_id)
            repositories.update_run_notes(conn, run.run_id, "progress 50%")
            done = repositories.finish_run(conn, run.run_id, "failed", notes="error: boom")
            self.assertEqual((done.status, done.notes), ("failed", "error: boom"))
            self.assertIsNotNone(done.finished_at)
            with self.assertRaises(repositories.InvalidStateTransition):
                repositories.finish_run(conn, run.run_id, "succeeded")

    def test_source_rate_limit_state(self) -> None:
        from app.db import repositories

        scope = f"{self.prefix}scope"
        reset = datetime.now(timezone.utc) + timedelta(minutes=30)
        with self.rolled_back() as conn:
            state = repositories.record_rate_limit(conn, repositories.RateLimitObservation(
                "github", scope, limit_total=5000, remaining=0, reset_at=reset, last_http_status=403))
            self.assertEqual((state.remaining, state.limit_total), (0, 5000))
            self.assertEqual(repositories.rate_limit_blocked_until(conn, "github", scope), state.reset_at)
            state = repositories.record_rate_limit(conn, repositories.RateLimitObservation(
                "github", scope, retry_after_seconds=3600, last_http_status=429))
            self.assertEqual((state.remaining, state.limit_total), (0, 5000))  # unknown values kept
            self.assertIsNotNone(state.retry_after_until)
            self.assertGreater(repositories.rate_limit_blocked_until(conn, "github", scope), state.reset_at)
            self.assertIsNone(repositories.rate_limit_blocked_until(conn, "common_crawl", scope))


if __name__ == "__main__":
    unittest.main()

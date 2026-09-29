"""Deterministic job identities (schema §16.2), JobSpec validation and runtime settings."""

import unittest
import uuid
from datetime import datetime, timezone

from app.config.settings import DatabaseSettings, SettingsError, WorkerSettings
from app.db.connection import EXPECTED_KBP_REVISION, HealthReport, RuntimeDatabaseError, create_runtime_engine
from app.queue import identity
from app.queue.job_queue import JobSpec
from app.queue.retry import RetryPolicy
from app.queue.states import JobSource, JobType, Stage

SHA = "a" * 40


class IdentityBuilderTests(unittest.TestCase):
    def test_formats(self) -> None:
        self.assertEqual(identity.cc_index_partition("CC-MAIN-2024-10", "cc-index/collections/x/cdx-00001.gz", "f1"),
                         "CC-MAIN-2024-10/cc-index/collections/x/cdx-00001.gz#filter=f1")
        self.assertEqual(identity.cc_index_partition("CC-MAIN-2024-10", "p", "f1", (0, 99)),
                         "CC-MAIN-2024-10/p#filter=f1#range=0-99")
        self.assertEqual(identity.gh_repository_discovery("q1", 3), "q1#page=3")
        self.assertEqual(identity.gh_repository_snapshot(123, SHA), f"123@{SHA}")
        a, b = uuid.UUID(int=1), uuid.UUID(int=2)
        self.assertEqual(identity.embedding_enqueue(4, a, b), f"4#chunk_id={a}..{b}")
        self.assertEqual(identity.policy_reevaluation("p2", a, b), f"p2#document_id={a}..{b}")
        self.assertEqual(identity.legacy_reconciliation("r1"), "r1")

    def test_same_unit_same_identity(self) -> None:
        self.assertEqual(identity.gh_repository_snapshot(9, SHA), identity.gh_repository_snapshot(9, SHA))

    def test_invalid_components_rejected(self) -> None:
        for call in (
            lambda: identity.cc_index_partition("CC-MAIN-24", "p", "f"),
            lambda: identity.cc_index_partition("CC-MAIN-2024-10", "p#x", "f"),
            lambda: identity.cc_index_partition("CC-MAIN-2024-10", "p", "f", (5, 1)),
            lambda: identity.gh_repository_discovery("q", -1),
            lambda: identity.gh_repository_discovery("q", True),
            lambda: identity.gh_repository_snapshot(1, "ABC"),
            lambda: identity.gh_repository_snapshot(-1, SHA),
            lambda: identity.embedding_enqueue(1, "not-a-uuid", uuid.UUID(int=1)),
            lambda: identity.legacy_reconciliation(""),
        ):
            with self.assertRaises(ValueError):
                call()


class JobSpecTests(unittest.TestCase):
    def test_defaults_and_policy(self) -> None:
        spec = JobSpec.for_policy(RetryPolicy(max_attempts=7), job_type="policy_reevaluation", source="internal",
                                  source_identifier="p1#document_id=a..b")
        self.assertEqual((spec.max_attempts, spec.priority, spec.stage), (7, 100, Stage.DISCOVERED))
        self.assertIs(spec.job_type, JobType.POLICY_REEVALUATION)

    def test_source_must_match_job_type(self) -> None:
        with self.assertRaises(ValueError):
            JobSpec(job_type=JobType.GH_REPOSITORY_SNAPSHOT, source=JobSource.COMMON_CRAWL,
                    source_identifier=f"1@{SHA}")

    def test_invalid_values(self) -> None:
        base = {"job_type": "legacy_reconciliation", "source": "internal", "source_identifier": "r1"}
        for override in ({"max_attempts": 0}, {"priority": 40000}, {"source_identifier": " r1"},
                         {"source_identifier": ""}, {"source_identifier": "a\x00"}, {"stage": "FAILED"},
                         {"job_type": "unknown"}, {"available_at": datetime(2026, 1, 1)},
                         {"checkpoint": {"x": float("nan")}}):
            with self.subTest(override=override), self.assertRaises((ValueError, TypeError)):
                JobSpec(**{**base, **override})

    def test_available_at_is_utc(self) -> None:
        spec = JobSpec(job_type="legacy_reconciliation", source="internal", source_identifier="r1",
                       available_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(spec.available_at.tzinfo, timezone.utc)


class SettingsTests(unittest.TestCase):
    APP = "postgresql+psycopg://kb_pipeline_app@localhost:5432/db"

    def test_runtime_url_required_and_not_the_migration_url(self) -> None:
        with self.assertRaises(SettingsError):
            DatabaseSettings.from_env({})
        with self.assertRaises(SettingsError) as ctx:
            DatabaseSettings.from_env({"KB_DATABASE_URL": "postgresql+psycopg://x:s3cret@h/db",
                                       "KB_MIGRATION_DATABASE_URL": "postgresql+psycopg://x:s3cret@h/db"})
        self.assertNotIn("s3cret", str(ctx.exception))

    def test_url_is_not_in_repr(self) -> None:
        settings = DatabaseSettings.from_env({"KB_DATABASE_URL": "postgresql+psycopg://kb_pipeline_app:s3cret@h/db"})
        self.assertNotIn("s3cret", repr(settings))

    def test_bounds(self) -> None:
        self.assertEqual(DatabaseSettings.from_env({"KB_DATABASE_URL": self.APP, "KB_DB_POOL_SIZE": "8"}).pool_size, 8)
        for bad in ("0", "51", "x"):
            with self.subTest(bad=bad), self.assertRaises(SettingsError):
                DatabaseSettings.from_env({"KB_DATABASE_URL": self.APP, "KB_DB_POOL_SIZE": bad})
        with self.assertRaises(SettingsError):
            WorkerSettings(claim_batch_size=101)
        self.assertEqual(WorkerSettings.from_env({"KB_WORKER_CLAIM_BATCH": "10"}).claim_batch_size, 10)

    def test_engine_is_bounded_and_runtime_only(self) -> None:
        engine = create_runtime_engine(DatabaseSettings(url=self.APP, pool_size=3))  # no connection is made
        try:
            self.assertEqual(engine.pool.size(), 3)
            self.assertEqual(engine.pool._max_overflow, 0)
        finally:
            engine.dispose()
        with self.assertRaises(RuntimeDatabaseError):
            create_runtime_engine(DatabaseSettings(url="postgresql+psycopg://kb_pipeline_migrator@localhost/db"))
        with self.assertRaises(RuntimeDatabaseError):
            create_runtime_engine(DatabaseSettings(url="postgresql+psycopg://postgres@localhost/db"))

    def test_health_report(self) -> None:
        now = datetime.now(timezone.utc)
        self.assertTrue(HealthReport("kb_pipeline_app", False, "UTC", EXPECTED_KBP_REVISION, now).ok)
        bad = HealthReport("postgres", True, "Europe/Paris", None, now)
        self.assertEqual(len(bad.problems), 4)


if __name__ == "__main__":
    unittest.main()

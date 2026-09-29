"""The runtime vocabulary equals the CHECK lists of Migration 001A (no invented statuses)."""

import re
import unittest

from app.queue import states as s

import tests.unit.test_migration_001a as m001a

LISTS = m001a.parse_migration_in_lists()


def values(enum) -> list[str]:
    return [member.value for member in enum]


class VocabularyMatchesSchemaTests(unittest.TestCase):
    CASES = {
        ("pipeline_jobs", "status"): s.JobStatus,
        ("pipeline_jobs", "job_type"): s.JobType,
        ("pipeline_jobs", "source"): s.JobSource,
        ("pipeline_jobs", "stage"): s.Stage,
        ("pipeline_jobs", "last_error_class"): s.ErrorClass,
        ("embedding_queue", "last_error_class"): s.ErrorClass,
        ("pipeline_runs", "status"): s.RunStatus,
        ("pipeline_runs", "run_kind"): s.RunKind,
        ("processing_attempts", "outcome"): s.AttemptOutcome,
        ("processing_attempts", "error_class"): s.ErrorClass,
        ("dead_letters", "item_type"): s.DeadLetterItemType,
        ("dead_letters", "disposition"): s.Disposition,
        ("dead_letters", "resolution"): s.Resolution,
        ("dead_letters", "stage"): s.Stage,
        ("dead_letters", "error_class"): s.ErrorClass,
        ("source_rate_limits", "source_channel"): s.SourceChannel,
    }

    def test_every_enum_equals_its_check_list(self) -> None:
        for key, enum in self.CASES.items():
            with self.subTest(column=key):
                self.assertEqual(values(enum), list(LISTS[key]))

    def test_no_failed_job_status(self) -> None:
        self.assertNotIn("failed", values(s.JobStatus))
        self.assertIn("failed", values(s.AttemptOutcome))

    def test_terminal_statuses_equal_the_completed_at_check(self) -> None:
        jobs_sql = next(sql for sql in m001a.UPGRADE if "CREATE TABLE kbp.pipeline_jobs" in sql)
        listed = re.search(r"CHECK \(\(status IN \(([^)]*)\)\) = \(completed_at IS NOT NULL\)\)", jobs_sql).group(1)
        self.assertEqual({v.strip(" '") for v in listed.split(",")}, {x.value for x in s.TERMINAL_STATUSES})

    def test_error_classes_are_partitioned(self) -> None:
        groups = (s.RETRYABLE_ERROR_CLASSES, s.DEFERRABLE_ERROR_CLASSES, s.PERMANENT_ERROR_CLASSES,
                  s.QUARANTINE_ERROR_CLASSES)
        union = set().union(*groups)
        self.assertEqual(union, set(s.ErrorClass))
        self.assertEqual(sum(len(g) for g in groups), len(s.ErrorClass))

    def test_job_type_sources(self) -> None:
        self.assertEqual(set(s.JOB_TYPE_SOURCE), set(s.JobType))
        self.assertEqual(s.JOB_TYPE_SOURCE[s.JobType.CC_INDEX_PARTITION], s.JobSource.COMMON_CRAWL)
        self.assertEqual(s.JOB_TYPE_SOURCE[s.JobType.GH_REPOSITORY_SNAPSHOT], s.JobSource.GITHUB)


class TransitionTests(unittest.TestCase):
    def test_schema_17_4_transitions(self) -> None:
        J = s.JobStatus
        for current in (J.PENDING, J.RETRY_PENDING):
            self.assertTrue(s.is_allowed_transition(current, J.RUNNING))
        for target in (J.SUCCEEDED, J.RETRY_PENDING, J.DEAD_LETTER, J.QUARANTINED):
            self.assertTrue(s.is_allowed_transition(J.RUNNING, target))
        for current in (J.DEAD_LETTER, J.QUARANTINED):
            self.assertTrue(s.is_allowed_transition(current, J.PENDING))

    def test_forbidden_transitions(self) -> None:
        J = s.JobStatus
        self.assertFalse(s.is_allowed_transition(J.PENDING, J.SUCCEEDED))
        self.assertFalse(s.is_allowed_transition(J.RUNNING, J.PENDING))
        self.assertFalse(s.is_allowed_transition(J.RUNNING, J.CANCELLED))
        self.assertFalse(s.is_allowed_transition(J.SUCCEEDED, J.RUNNING))
        for terminal in s.TERMINAL_STATUSES:
            self.assertEqual(s.ALLOWED_TRANSITIONS[terminal], frozenset({J.PENDING}))

    def test_message_truncation(self) -> None:
        self.assertIsNone(s.truncate_message(None))
        self.assertEqual(len(s.truncate_message("x" * 10000)), s.MAX_ERROR_MESSAGE_CHARS)
        self.assertEqual(s.truncate_message("short"), "short")


if __name__ == "__main__":
    unittest.main()

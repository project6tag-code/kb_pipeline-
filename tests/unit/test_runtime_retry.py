"""Retry controller: backoff bounds, jitter, Retry-After, rate-limit reset, classification."""

import random
import unittest
from datetime import datetime, timedelta, timezone

from app.queue.retry import (
    DecisionKind,
    Failure,
    JobFailure,
    RetryPolicy,
    backoff_ceiling,
    classify_exception,
    decide,
    jittered_backoff,
)
from app.queue.states import AttemptOutcome, ErrorClass

POLICY = RetryPolicy(max_attempts=5, base_delay_seconds=10.0, max_delay_seconds=300.0, rate_limit_jitter_seconds=2.0)


class BackoffTests(unittest.TestCase):
    def test_ceiling_doubles_and_is_capped(self) -> None:
        self.assertEqual([backoff_ceiling(POLICY, n) for n in range(1, 8)], [10, 20, 40, 80, 160, 300, 300])

    def test_ceiling_never_overflows(self) -> None:
        self.assertEqual(backoff_ceiling(POLICY, 10_000), 300.0)
        self.assertEqual(backoff_ceiling(POLICY, 0), 10.0)

    def test_full_jitter_stays_within_bounds(self) -> None:
        rng = random.Random(7)
        for attempt in range(1, 10):
            ceiling = backoff_ceiling(POLICY, attempt)
            samples = [jittered_backoff(POLICY, attempt, rng) for _ in range(500)]
            with self.subTest(attempt=attempt):
                self.assertTrue(all(0.0 <= d <= ceiling for d in samples))
                self.assertGreater(max(samples), ceiling * 0.8)  # jitter spreads over the range
                self.assertLess(min(samples), ceiling * 0.2)

    def test_jitter_is_deterministic_for_a_seed(self) -> None:
        self.assertEqual(jittered_backoff(POLICY, 3, random.Random(1)), jittered_backoff(POLICY, 3, random.Random(1)))

    def test_policy_validation(self) -> None:
        for kwargs in ({"max_attempts": 0}, {"max_attempts": 40000}, {"base_delay_seconds": 0},
                       {"base_delay_seconds": 10, "max_delay_seconds": 5}, {"rate_limit_jitter_seconds": -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                RetryPolicy(**kwargs)


class DecisionTests(unittest.TestCase):
    def rng(self) -> random.Random:
        return random.Random(42)

    def test_retryable_failure_retries_with_bounded_delay(self) -> None:
        for error_class in (ErrorClass.TRANSIENT, ErrorClass.RESOURCE_EXHAUSTED, ErrorClass.INTERNAL_BUG):
            decision = decide(POLICY, Failure(error_class), attempt_count=2, max_attempts=5, rng=self.rng())
            with self.subTest(error_class=error_class):
                self.assertIs(decision.kind, DecisionKind.RETRY)
                self.assertIs(decision.outcome, AttemptOutcome.FAILED)
                self.assertTrue(decision.consumes_attempt)
                self.assertTrue(0.0 <= decision.delay_seconds <= 20.0)

    def test_max_attempts_exhausted_goes_to_dead_letter(self) -> None:
        decision = decide(POLICY, Failure(ErrorClass.TRANSIENT), attempt_count=5, max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.DEAD_LETTER)
        decision = decide(POLICY, Failure(ErrorClass.TRANSIENT), attempt_count=4, max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.RETRY)

    def test_permanent_failures_dead_letter_immediately(self) -> None:
        for error_class in (ErrorClass.PERMANENT_SOURCE, ErrorClass.DATA_INVALID):
            decision = decide(POLICY, Failure(error_class), attempt_count=1, max_attempts=5, rng=self.rng())
            with self.subTest(error_class=error_class):
                self.assertIs(decision.kind, DecisionKind.DEAD_LETTER)

    def test_policy_quarantine(self) -> None:
        decision = decide(POLICY, Failure(ErrorClass.POLICY_QUARANTINE), attempt_count=1, max_attempts=5,
                          rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.QUARANTINE)

    def test_retry_after_overrides_a_shorter_backoff(self) -> None:
        decision = decide(POLICY, Failure(ErrorClass.TRANSIENT, retry_after_seconds=120), attempt_count=1,
                          max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.RETRY)
        self.assertEqual(decision.delay_seconds, 120.0)  # backoff ceiling for attempt 1 is 10s

    def test_retry_after_does_not_shorten_a_longer_backoff(self) -> None:
        rng = random.Random(3)
        expected = jittered_backoff(POLICY, 6, random.Random(3))
        decision = decide(POLICY, Failure(ErrorClass.TRANSIENT, retry_after_seconds=1), attempt_count=6,
                          max_attempts=9, rng=rng)
        self.assertEqual(decision.delay_seconds, max(expected, 1.0))

    def test_rate_limited_with_retry_after_defers_without_consuming(self) -> None:
        decision = decide(POLICY, Failure(ErrorClass.RATE_LIMITED, retry_after_seconds=60), attempt_count=5,
                          max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.DEFER)
        self.assertIs(decision.outcome, AttemptOutcome.DEFERRED_RATE_LIMIT)
        self.assertFalse(decision.consumes_attempt)
        self.assertTrue(60.0 <= decision.delay_seconds <= 62.0)  # + rate-limit jitter

    def test_rate_limit_reset_timestamp(self) -> None:
        reset = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
        decision = decide(POLICY, Failure(ErrorClass.RATE_LIMITED, retry_at=reset), attempt_count=1,
                          max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.DEFER)
        self.assertTrue(reset <= decision.not_before <= reset + timedelta(seconds=2))
        self.assertEqual(decision.delay_seconds, 0.0)

    def test_rate_limited_without_headers_uses_backoff(self) -> None:
        decision = decide(POLICY, Failure(ErrorClass.RATE_LIMITED), attempt_count=3, max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.DEFER)
        self.assertTrue(0.0 <= decision.delay_seconds <= 40.0)

    def test_lease_expired_is_retryable_and_recorded_as_such(self) -> None:
        decision = decide(POLICY, Failure(ErrorClass.LEASE_EXPIRED), attempt_count=1, max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.RETRY)
        self.assertIs(decision.outcome, AttemptOutcome.LEASE_EXPIRED)
        decision = decide(POLICY, Failure(ErrorClass.LEASE_EXPIRED), attempt_count=5, max_attempts=5, rng=self.rng())
        self.assertIs(decision.kind, DecisionKind.DEAD_LETTER)
        self.assertIs(decision.outcome, AttemptOutcome.LEASE_EXPIRED)


class FailureTests(unittest.TestCase):
    def test_classification_of_exceptions(self) -> None:
        self.assertIs(classify_exception(ValueError("x")).error_class, ErrorClass.INTERNAL_BUG)
        self.assertEqual(classify_exception(ValueError("x")).code, "ValueError")
        self.assertIs(classify_exception(MemoryError()).error_class, ErrorClass.RESOURCE_EXHAUSTED)
        failure = classify_exception(JobFailure("permanent_source", "gone", code="404"))
        self.assertEqual((failure.error_class, failure.code, failure.message), (ErrorClass.PERMANENT_SOURCE, "404", "gone"))

    def test_failure_validation(self) -> None:
        with self.assertRaises(ValueError):
            Failure("not-a-class")
        with self.assertRaises(ValueError):
            Failure(ErrorClass.TRANSIENT, retry_after_seconds=-1)
        with self.assertRaises(ValueError):
            Failure(ErrorClass.RATE_LIMITED, retry_at=datetime(2026, 1, 1))  # naive
        self.assertEqual(len(Failure(ErrorClass.TRANSIENT, message="m" * 9000).message), 4000)


if __name__ == "__main__":
    unittest.main()

"""Persistent job queue on ``kbp.pipeline_jobs`` (schema §16, §17).

Guarantees
----------
* **Idempotent enqueue.** ``INSERT ... ON CONFLICT (job_type, source_identifier)
  DO NOTHING``: the UNIQUE key is the logical job identity (schema §16.2), so
  re-enqueueing returns the existing job and never creates a duplicate.
* **Atomic claiming.** One ``UPDATE ... FROM (SELECT ... FOR UPDATE SKIP
  LOCKED LIMIT n)`` statement selects eligible rows (``pending`` /
  ``retry_pending``, ``next_attempt_at <= now()``, attempts left), ordered by
  ``(priority, next_attempt_at, job_id)``, and moves them to ``running`` with a
  fresh ``lease_token`` (schema §16.4). Rows locked by a concurrent claimer are
  skipped, so two workers can never claim the same job. The claim transaction
  also opens the ``processing_attempts`` row and nothing else (short).
* **Fencing.** Every later write for a claimed job (checkpoint, heartbeat,
  completion, failure, release) is ``WHERE job_id = ? AND lease_token = ? AND
  status = 'running'``. A worker whose lease was recovered gets
  ``LeaseLostError`` and its late result is discarded: no double processing.
* **Transactions.** Each state change (job row + attempt row + dead letter)
  is one transaction. Timestamps come from the database clock (UTC).

Only privileges granted to ``kb_pipeline_app`` are used (no DELETE).
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.db import repositories
from app.queue.identity import validate_job_identity
from app.queue.retry import DecisionKind, Failure, RetryDecision, RetryPolicy, decide
from app.queue.states import (
    DEFAULT_PRIORITY,
    SMALLINT_MAX,
    AttemptOutcome,
    DeadLetterItemType,
    Disposition,
    ErrorClass,
    JobSource,
    JobStatus,
    JobType,
    Resolution,
    Stage,
    truncate_message,
)
from app.utils.time import ensure_utc


class LeaseLostError(RuntimeError):
    """The job is no longer leased by this worker (recovered, or already finished)."""


class JobIdentityConflict(RuntimeError):
    """An existing job has the same (job_type, source_identifier) but another source."""


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


def _json(value: Mapping[str, Any] | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("checkpoints and metrics must be JSON objects (mappings)")
    return json.dumps(dict(value), allow_nan=False, sort_keys=True, separators=(",", ":"))


# --------------------------------------------------------------------------- #
# Data types
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class JobSpec:
    """A logical job to enqueue. Build ``source_identifier`` with app.queue.identity."""

    job_type: JobType | str
    source: JobSource | str
    source_identifier: str
    max_attempts: int = 5
    priority: int = DEFAULT_PRIORITY
    stage: Stage | str = Stage.DISCOVERED
    checkpoint: Mapping[str, Any] = field(default_factory=dict)
    run_id: uuid.UUID | None = None
    available_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "job_type", JobType(self.job_type))
        object.__setattr__(self, "source", JobSource(self.source))
        object.__setattr__(self, "stage", Stage(self.stage))
        validate_job_identity(self.job_type, self.source, self.source_identifier)
        if not 1 <= self.max_attempts <= SMALLINT_MAX:
            raise ValueError(f"max_attempts must be 1..{SMALLINT_MAX}")
        if not -SMALLINT_MAX - 1 <= self.priority <= SMALLINT_MAX:
            raise ValueError("priority must fit smallint")
        if self.available_at is not None:
            object.__setattr__(self, "available_at", ensure_utc(self.available_at))
        _json(self.checkpoint)  # validates serializability

    @classmethod
    def for_policy(cls, policy: RetryPolicy, **kwargs: Any) -> "JobSpec":
        return cls(max_attempts=policy.max_attempts, **kwargs)


@dataclass(frozen=True)
class EnqueueResult:
    job_id: int
    created: bool
    status: str


@dataclass(frozen=True)
class ClaimedJob:
    job_id: int
    job_type: JobType
    source: JobSource
    source_identifier: str
    stage: Stage
    checkpoint: dict[str, Any]
    priority: int
    attempt_count: int
    max_attempts: int
    deferral_count: int
    lease_token: uuid.UUID
    lease_expires_at: datetime
    run_id: uuid.UUID | None
    worker_id: str
    attempt_id: int
    attempt_no: int


@dataclass(frozen=True)
class FailureResult:
    decision: RetryDecision
    status: JobStatus
    next_attempt_at: datetime | None = None
    dead_letter_id: int | None = None


@dataclass(frozen=True)
class RecoveryResult:
    job_id: int
    status: JobStatus
    next_attempt_at: datetime | None
    dead_letter_id: int | None


# --------------------------------------------------------------------------- #
# SQL (checked by tests/unit/test_runtime_sql.py)
# --------------------------------------------------------------------------- #

ENQUEUE_JOB_SQL = """
INSERT INTO kbp.pipeline_jobs (job_type, source, source_identifier, status, stage, checkpoint, priority,
                               max_attempts, next_attempt_at, run_id, updated_at)
VALUES (:job_type, :source, :source_identifier, 'pending', :stage, CAST(:checkpoint AS jsonb), :priority,
        :max_attempts, COALESCE(CAST(:available_at AS timestamptz), now()), :run_id, now())
ON CONFLICT (job_type, source_identifier) DO NOTHING
RETURNING job_id, status
"""

GET_JOB_BY_IDENTITY_SQL = """
SELECT job_id, source, status FROM kbp.pipeline_jobs
WHERE job_type = :job_type AND source_identifier = :source_identifier
"""

JOB_COLUMNS = ("job_id, job_type, source, source_identifier, status, stage, checkpoint, priority, attempt_count, "
               "max_attempts, deferral_count, next_attempt_at, locked_at, lease_expires_at, worker_id, lease_token, "
               "last_error_class, last_error_code, last_error_message, run_id, created_at, started_at, "
               "completed_at, updated_at")

GET_JOB_SQL = f"SELECT {JOB_COLUMNS} FROM kbp.pipeline_jobs WHERE job_id = :job_id"

CLAIM_JOBS_SQL = """
WITH candidate AS (
    SELECT job_id
    FROM kbp.pipeline_jobs
    WHERE status IN ('pending', 'retry_pending')
      AND next_attempt_at <= now()
      AND attempt_count < max_attempts
      AND job_type = ANY (CAST(:job_types AS text[]))
      AND (CAST(:prefix AS text) IS NULL OR starts_with(source_identifier, CAST(:prefix AS text)))
    ORDER BY priority, next_attempt_at, job_id
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
)
UPDATE kbp.pipeline_jobs AS j
SET status = 'running',
    worker_id = :worker_id,
    lease_token = gen_random_uuid(),
    locked_at = now(),
    lease_expires_at = now() + make_interval(secs => CAST(:lease_seconds AS double precision)),
    attempt_count = j.attempt_count + 1,
    started_at = COALESCE(j.started_at, now()),
    updated_at = now()
FROM candidate AS c
WHERE j.job_id = c.job_id
RETURNING j.job_id, j.job_type, j.source, j.source_identifier, j.stage, j.checkpoint, j.priority,
          j.attempt_count, j.max_attempts, j.deferral_count, j.lease_token, j.lease_expires_at, j.run_id
"""

# attempt_no counts every claim, including deferrals (schema §5.20). The job
# row is locked by the claim, so MAX + 1 cannot race.
START_ATTEMPT_SQL = """
INSERT INTO kbp.processing_attempts (job_id, attempt_no, worker_id, lease_token, started_at)
SELECT :job_id, COALESCE(MAX(a.attempt_no), 0) + 1, :worker_id, :lease_token, now()
FROM kbp.processing_attempts AS a
WHERE a.job_id = :job_id
RETURNING attempt_id, attempt_no
"""

CHECKPOINT_JOB_SQL = """
UPDATE kbp.pipeline_jobs
SET checkpoint = CAST(:checkpoint AS jsonb),
    stage = COALESCE(CAST(:stage AS text), stage),
    lease_expires_at = now() + make_interval(secs => CAST(:lease_seconds AS double precision)),
    updated_at = now()
WHERE job_id = :job_id AND lease_token = :lease_token AND status = 'running'
RETURNING lease_expires_at
"""

HEARTBEAT_JOB_SQL = """
UPDATE kbp.pipeline_jobs
SET lease_expires_at = now() + make_interval(secs => CAST(:lease_seconds AS double precision)),
    updated_at = now()
WHERE job_id = :job_id AND lease_token = :lease_token AND status = 'running'
RETURNING lease_expires_at
"""

COMPLETE_JOB_SQL = """
UPDATE kbp.pipeline_jobs
SET status = 'succeeded',
    completed_at = now(),
    checkpoint = COALESCE(CAST(:checkpoint AS jsonb), checkpoint),
    stage = COALESCE(CAST(:stage AS text), stage),
    worker_id = NULL, lease_token = NULL, locked_at = NULL, lease_expires_at = NULL,
    updated_at = now()
WHERE job_id = :job_id AND lease_token = :lease_token AND status = 'running'
RETURNING job_id
"""

# running -> retry_pending (retry, rate-limit deferral, or cooperative release).
RESCHEDULE_JOB_SQL = """
UPDATE kbp.pipeline_jobs
SET status = 'retry_pending',
    attempt_count = attempt_count - CAST(:refund_attempt AS integer),
    deferral_count = deferral_count + CAST(:add_deferral AS integer),
    next_attempt_at = GREATEST(now() + make_interval(secs => CAST(:delay_seconds AS double precision)),
                               CAST(:not_before AS timestamptz)),
    checkpoint = COALESCE(CAST(:checkpoint AS jsonb), checkpoint),
    last_error_class = CASE WHEN CAST(:record_error AS boolean) THEN CAST(:error_class AS text) ELSE last_error_class END,
    last_error_code = CASE WHEN CAST(:record_error AS boolean) THEN CAST(:error_code AS text) ELSE last_error_code END,
    last_error_message = CASE WHEN CAST(:record_error AS boolean) THEN CAST(:error_message AS text) ELSE last_error_message END,
    worker_id = NULL, lease_token = NULL, locked_at = NULL, lease_expires_at = NULL,
    updated_at = now()
WHERE job_id = :job_id AND lease_token = :lease_token AND status = 'running'
RETURNING next_attempt_at
"""

# running -> dead_letter | quarantined (terminal; completed_at set, schema §5.19 CHECK).
TERMINATE_JOB_SQL = """
UPDATE kbp.pipeline_jobs
SET status = :status,
    completed_at = now(),
    last_error_class = :error_class,
    last_error_code = :error_code,
    last_error_message = :error_message,
    worker_id = NULL, lease_token = NULL, locked_at = NULL, lease_expires_at = NULL,
    updated_at = now()
WHERE job_id = :job_id AND lease_token = :lease_token AND status = 'running'
RETURNING job_id, stage, attempt_count, run_id
"""

FINISH_ATTEMPT_SQL = """
UPDATE kbp.processing_attempts
SET finished_at = now(),
    outcome = :outcome,
    error_class = :error_class,
    error_code = :error_code,
    error_message = :error_message,
    retry_after_seconds = :retry_after_seconds,
    scheduled_next_attempt_at = CAST(:scheduled_next_attempt_at AS timestamptz),
    metrics = CAST(:metrics AS jsonb)
WHERE job_id = :job_id AND lease_token = :lease_token AND finished_at IS NULL
RETURNING attempt_id
"""

SELECT_EXPIRED_LEASES_SQL = """
SELECT job_id, job_type, stage, attempt_count, max_attempts, lease_token, run_id
FROM kbp.pipeline_jobs
WHERE status = 'running'
  AND lease_expires_at < now()
  AND (CAST(:prefix AS text) IS NULL OR starts_with(source_identifier, CAST(:prefix AS text)))
ORDER BY lease_expires_at, job_id
LIMIT :limit
FOR UPDATE SKIP LOCKED
"""

CANCEL_JOB_SQL = """
UPDATE kbp.pipeline_jobs
SET status = 'cancelled', completed_at = now(), updated_at = now()
WHERE job_id = :job_id AND status IN ('pending', 'retry_pending')
RETURNING job_id
"""

REQUEUE_JOB_SQL = """
UPDATE kbp.pipeline_jobs
SET status = 'pending',
    completed_at = NULL,
    next_attempt_at = now(),
    max_attempts = attempt_count + CAST(:additional_attempts AS integer),
    updated_at = now()
WHERE job_id = :job_id
  AND status IN ('succeeded', 'dead_letter', 'quarantined', 'cancelled')
  AND attempt_count + CAST(:additional_attempts AS integer) <= 32767
RETURNING job_id
"""

RESOLVE_JOB_DEAD_LETTERS_SQL = """
UPDATE kbp.dead_letters
SET resolution = :resolution, resolved_at = now()
WHERE item_type = 'job' AND item_key = :item_key AND resolved_at IS NULL
RETURNING dead_letter_id
"""

LIST_ATTEMPTS_SQL = """
SELECT attempt_id, job_id, attempt_no, worker_id, lease_token, started_at, finished_at, outcome, error_class,
       error_code, error_message, retry_after_seconds, scheduled_next_attempt_at, metrics
FROM kbp.processing_attempts
WHERE job_id = :job_id
ORDER BY attempt_no
"""


# --------------------------------------------------------------------------- #
# Connection-scoped operations
# --------------------------------------------------------------------------- #

def enqueue_job(conn: Any, spec: JobSpec) -> EnqueueResult:
    params = {
        "job_type": spec.job_type.value, "source": spec.source.value, "source_identifier": spec.source_identifier,
        "stage": spec.stage.value, "checkpoint": _json(spec.checkpoint), "priority": spec.priority,
        "max_attempts": spec.max_attempts, "available_at": spec.available_at, "run_id": spec.run_id,
    }
    row = conn.execute(_text(ENQUEUE_JOB_SQL), params).one_or_none()
    if row is not None:
        return EnqueueResult(int(row[0]), True, row[1])
    existing = conn.execute(_text(GET_JOB_BY_IDENTITY_SQL), {
        "job_type": spec.job_type.value, "source_identifier": spec.source_identifier}).one()
    if existing[1] != spec.source.value:
        raise JobIdentityConflict(
            f"job ({spec.job_type}, {spec.source_identifier!r}) exists with source {existing[1]}")
    return EnqueueResult(int(existing[0]), False, existing[2])


def get_job(conn: Any, job_id: int) -> dict[str, Any]:
    row = conn.execute(_text(GET_JOB_SQL), {"job_id": job_id}).mappings().one_or_none()
    if row is None:
        raise repositories.RecordNotFound(f"job {job_id} not found")
    return dict(row)


def list_attempts(conn: Any, job_id: int) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(_text(LIST_ATTEMPTS_SQL), {"job_id": job_id}).mappings()]


def claim_jobs(conn: Any, *, worker_id: str, job_types: Sequence[JobType | str], limit: int,
               lease_seconds: float, source_identifier_prefix: str | None = None) -> list[ClaimedJob]:
    if not worker_id or len(worker_id) > 200:
        raise ValueError("worker_id must be 1..200 characters")
    if not 1 <= limit <= 100:
        raise ValueError("claim limit must be 1..100")
    if not job_types:
        return []
    rows = conn.execute(_text(CLAIM_JOBS_SQL), {
        "job_types": [JobType(t).value for t in job_types], "prefix": source_identifier_prefix,
        "limit": limit, "worker_id": worker_id, "lease_seconds": float(lease_seconds),
    }).mappings().all()
    claimed = []
    for row in rows:
        attempt = conn.execute(_text(START_ATTEMPT_SQL), {
            "job_id": row["job_id"], "worker_id": worker_id, "lease_token": row["lease_token"]}).one()
        claimed.append(ClaimedJob(
            job_id=int(row["job_id"]), job_type=JobType(row["job_type"]), source=JobSource(row["source"]),
            source_identifier=row["source_identifier"], stage=Stage(row["stage"]),
            checkpoint=dict(row["checkpoint"] or {}), priority=int(row["priority"]),
            attempt_count=int(row["attempt_count"]), max_attempts=int(row["max_attempts"]),
            deferral_count=int(row["deferral_count"]), lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"], run_id=row["run_id"], worker_id=worker_id,
            attempt_id=int(attempt[0]), attempt_no=int(attempt[1]),
        ))
    claimed.sort(key=lambda j: (j.priority, j.job_id))
    return claimed


def _fenced(job: ClaimedJob) -> dict[str, Any]:
    return {"job_id": job.job_id, "lease_token": job.lease_token}


def checkpoint_job(conn: Any, job: ClaimedJob, checkpoint: Mapping[str, Any], *, stage: Stage | str | None = None,
                   lease_seconds: float) -> datetime:
    """Persist the resume position (and optionally the last completed stage); renews the lease."""
    row = conn.execute(_text(CHECKPOINT_JOB_SQL), {
        **_fenced(job), "checkpoint": _json(checkpoint), "stage": None if stage is None else Stage(stage).value,
        "lease_seconds": float(lease_seconds)}).one_or_none()
    if row is None:
        raise LeaseLostError(f"job {job.job_id}: lease lost; checkpoint rejected")
    return row[0]


def heartbeat_job(conn: Any, job: ClaimedJob, *, lease_seconds: float) -> datetime:
    row = conn.execute(_text(HEARTBEAT_JOB_SQL), {**_fenced(job), "lease_seconds": float(lease_seconds)}).one_or_none()
    if row is None:
        raise LeaseLostError(f"job {job.job_id}: lease lost; heartbeat rejected")
    return row[0]


def _finish_attempt(conn: Any, job_id: int, lease_token: uuid.UUID, outcome: AttemptOutcome, *,
                    failure: Failure | None = None, scheduled: datetime | None = None,
                    metrics: Mapping[str, Any] | None = None) -> None:
    conn.execute(_text(FINISH_ATTEMPT_SQL), {
        "job_id": job_id, "lease_token": lease_token, "outcome": AttemptOutcome(outcome).value,
        "error_class": None if failure is None else failure.error_class.value,
        "error_code": None if failure is None else failure.code,
        "error_message": None if failure is None else truncate_message(failure.message),
        "retry_after_seconds": None if failure is None else failure.retry_after_seconds,
        "scheduled_next_attempt_at": scheduled, "metrics": _json(metrics),
    })


def complete_job(conn: Any, job: ClaimedJob, *, metrics: Mapping[str, Any] | None = None,
                 checkpoint: Mapping[str, Any] | None = None, stage: Stage | str | None = None) -> None:
    row = conn.execute(_text(COMPLETE_JOB_SQL), {
        **_fenced(job), "checkpoint": _json(checkpoint),
        "stage": None if stage is None else Stage(stage).value}).one_or_none()
    if row is None:
        raise LeaseLostError(f"job {job.job_id}: lease lost; completion rejected")
    _finish_attempt(conn, job.job_id, job.lease_token, AttemptOutcome.SUCCEEDED, metrics=metrics)


def _reschedule(conn: Any, job_id: int, lease_token: uuid.UUID, *, delay_seconds: float,
                not_before: datetime | None, refund_attempt: bool, add_deferral: bool,
                failure: Failure | None, checkpoint: Mapping[str, Any] | None = None) -> datetime | None:
    row = conn.execute(_text(RESCHEDULE_JOB_SQL), {
        "job_id": job_id, "lease_token": lease_token, "refund_attempt": int(refund_attempt),
        "add_deferral": int(add_deferral), "delay_seconds": max(0.0, float(delay_seconds)),
        "not_before": not_before, "checkpoint": _json(checkpoint), "record_error": failure is not None,
        "error_class": None if failure is None else failure.error_class.value,
        "error_code": None if failure is None else failure.code,
        "error_message": None if failure is None else truncate_message(failure.message),
    }).one_or_none()
    return None if row is None else row[0]


def _terminate(conn: Any, job_id: int, lease_token: uuid.UUID, status: JobStatus,
               failure: Failure) -> tuple[Any, ...] | None:
    if status not in (JobStatus.DEAD_LETTER, JobStatus.QUARANTINED):
        raise ValueError("terminate only moves a job to dead_letter or quarantined")
    return conn.execute(_text(TERMINATE_JOB_SQL), {
        "job_id": job_id, "lease_token": lease_token, "status": status.value,
        "error_class": failure.error_class.value, "error_code": failure.code,
        "error_message": truncate_message(failure.message)}).one_or_none()


def _apply_failure(conn: Any, job_id: int, lease_token: uuid.UUID, failure: Failure,
                   decision: RetryDecision) -> FailureResult | None:
    """Apply a decision to a running job fenced by lease_token; None if the lease is gone."""
    if decision.kind in (DecisionKind.RETRY, DecisionKind.DEFER):
        deferral = decision.kind is DecisionKind.DEFER
        next_at = _reschedule(conn, job_id, lease_token, delay_seconds=decision.delay_seconds,
                              not_before=decision.not_before, refund_attempt=deferral, add_deferral=deferral,
                              failure=failure)
        if next_at is None:
            return None
        _finish_attempt(conn, job_id, lease_token, decision.outcome, failure=failure, scheduled=next_at)
        return FailureResult(decision, JobStatus.RETRY_PENDING, next_attempt_at=next_at)

    status = JobStatus.QUARANTINED if decision.kind is DecisionKind.QUARANTINE else JobStatus.DEAD_LETTER
    row = _terminate(conn, job_id, lease_token, status, failure)
    if row is None:
        return None
    _finish_attempt(conn, job_id, lease_token, decision.outcome, failure=failure)
    dead_letter_id, _count = repositories.record_dead_letter(
        conn, item_type=DeadLetterItemType.JOB, item_key=str(job_id), stage=row[1],
        disposition=Disposition.QUARANTINED if status is JobStatus.QUARANTINED else Disposition.DEAD_LETTER,
        error_class=failure.error_class, error_code=failure.code, error_message=failure.message,
        attempt_count=int(row[2]), job_id=job_id, run_id=row[3])
    return FailureResult(decision, status, dead_letter_id=dead_letter_id)


def fail_job(conn: Any, job: ClaimedJob, failure: Failure, decision: RetryDecision) -> FailureResult:
    result = _apply_failure(conn, job.job_id, job.lease_token, failure, decision)
    if result is None:
        raise LeaseLostError(f"job {job.job_id}: lease lost; failure not recorded by this worker")
    return result


def release_job(conn: Any, job: ClaimedJob, *, checkpoint: Mapping[str, Any] | None = None,
                delay_seconds: float = 0.0) -> datetime:
    """Cooperative release (handler yield, graceful shutdown): running -> retry_pending.

    The attempt is closed as ``cancelled`` and is not consumed.
    """
    next_at = _reschedule(conn, job.job_id, job.lease_token, delay_seconds=delay_seconds, not_before=None,
                          refund_attempt=True, add_deferral=False, failure=None, checkpoint=checkpoint)
    if next_at is None:
        raise LeaseLostError(f"job {job.job_id}: lease lost; release rejected")
    _finish_attempt(conn, job.job_id, job.lease_token, AttemptOutcome.CANCELLED, scheduled=next_at)
    return next_at


def recover_expired_leases(conn: Any, *, policy_for: Callable[[JobType], RetryPolicy], rng: random.Random,
                           limit: int = 100, source_identifier_prefix: str | None = None) -> list[RecoveryResult]:
    """Crash recovery (schema §17.3) for running jobs whose lease expired.

    The open attempt is closed as ``lease_expired`` (the attempt IS consumed);
    the job goes to ``retry_pending`` with backoff, or to ``dead_letter`` (plus
    a dead_letters row) when its attempts are exhausted. Rows are locked with
    SKIP LOCKED, so concurrent sweepers never recover the same job twice, and
    the lease token is rotated away, so the crashed worker's late writes fail.
    """
    rows = conn.execute(_text(SELECT_EXPIRED_LEASES_SQL), {
        "prefix": source_identifier_prefix, "limit": limit}).mappings().all()
    results = []
    for row in rows:
        job_type = JobType(row["job_type"])
        failure = Failure(ErrorClass.LEASE_EXPIRED, "lease_expired",
                          f"lease expired; attempt {row['attempt_count']} of {row['max_attempts']}")
        decision = decide(policy_for(job_type), failure, int(row["attempt_count"]), int(row["max_attempts"]), rng)
        result = _apply_failure(conn, int(row["job_id"]), row["lease_token"], failure, decision)
        if result is not None:  # the row is locked by this transaction, so this always applies
            results.append(RecoveryResult(int(row["job_id"]), result.status, result.next_attempt_at,
                                          result.dead_letter_id))
    return results


def cancel_job(conn: Any, job_id: int) -> None:
    """Operator: pending | retry_pending -> cancelled."""
    if conn.execute(_text(CANCEL_JOB_SQL), {"job_id": job_id}).one_or_none() is None:
        raise repositories.InvalidStateTransition(f"job {job_id} is not pending or retry_pending")


def requeue_job(conn: Any, job_id: int, *, additional_attempts: int = 1) -> None:
    """Operator: terminal -> pending with ``additional_attempts`` more attempts (schema §16.1, §17.4).

    Open dead letters of the job are resolved as ``requeued``.
    """
    if not 1 <= additional_attempts <= 100:
        raise ValueError("additional_attempts must be 1..100")
    if conn.execute(_text(REQUEUE_JOB_SQL), {"job_id": job_id,
                                             "additional_attempts": additional_attempts}).one_or_none() is None:
        raise repositories.InvalidStateTransition(f"job {job_id} is not in a terminal status")
    conn.execute(_text(RESOLVE_JOB_DEAD_LETTERS_SQL), {"item_key": str(job_id),
                                                       "resolution": Resolution.REQUEUED.value})


# --------------------------------------------------------------------------- #
# Service: one transaction per operation
# --------------------------------------------------------------------------- #

class JobQueue:
    """Transaction-per-operation facade used by workers (see app.workers.runtime)."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def transaction(self) -> Any:
        return self.db.transaction()

    def enqueue(self, spec: JobSpec) -> EnqueueResult:
        with self.db.transaction() as conn:
            return enqueue_job(conn, spec)

    def enqueue_many(self, specs: Iterable[JobSpec]) -> list[EnqueueResult]:
        with self.db.transaction() as conn:
            return [enqueue_job(conn, spec) for spec in specs]

    def claim(self, *, worker_id: str, job_types: Sequence[JobType | str], limit: int, lease_seconds: float,
              source_identifier_prefix: str | None = None) -> list[ClaimedJob]:
        with self.db.transaction() as conn:
            return claim_jobs(conn, worker_id=worker_id, job_types=job_types, limit=limit,
                              lease_seconds=lease_seconds, source_identifier_prefix=source_identifier_prefix)

    def checkpoint(self, job: ClaimedJob, checkpoint: Mapping[str, Any], *, stage: Stage | str | None = None,
                   lease_seconds: float, conn: Any = None) -> datetime:
        if conn is not None:  # the caller's transaction (item writes + checkpoint atomically)
            return checkpoint_job(conn, job, checkpoint, stage=stage, lease_seconds=lease_seconds)
        with self.db.transaction() as own:
            return checkpoint_job(own, job, checkpoint, stage=stage, lease_seconds=lease_seconds)

    def heartbeat(self, job: ClaimedJob, *, lease_seconds: float) -> datetime:
        with self.db.transaction() as conn:
            return heartbeat_job(conn, job, lease_seconds=lease_seconds)

    def complete(self, job: ClaimedJob, *, metrics: Mapping[str, Any] | None = None,
                 checkpoint: Mapping[str, Any] | None = None, stage: Stage | str | None = None) -> None:
        with self.db.transaction() as conn:
            complete_job(conn, job, metrics=metrics, checkpoint=checkpoint, stage=stage)

    def fail(self, job: ClaimedJob, failure: Failure, decision: RetryDecision) -> FailureResult:
        with self.db.transaction() as conn:
            return fail_job(conn, job, failure, decision)

    def release(self, job: ClaimedJob, *, checkpoint: Mapping[str, Any] | None = None,
                delay_seconds: float = 0.0) -> datetime:
        with self.db.transaction() as conn:
            return release_job(conn, job, checkpoint=checkpoint, delay_seconds=delay_seconds)

    def recover_expired_leases(self, *, policy_for: Callable[[JobType], RetryPolicy], rng: random.Random,
                               limit: int = 100, source_identifier_prefix: str | None = None) -> list[RecoveryResult]:
        with self.db.transaction() as conn:
            return recover_expired_leases(conn, policy_for=policy_for, rng=rng, limit=limit,
                                          source_identifier_prefix=source_identifier_prefix)

    def cancel(self, job_id: int) -> None:
        with self.db.transaction() as conn:
            cancel_job(conn, job_id)

    def requeue(self, job_id: int, *, additional_attempts: int = 1) -> None:
        with self.db.transaction() as conn:
            requeue_job(conn, job_id, additional_attempts=additional_attempts)

    def get(self, job_id: int) -> dict[str, Any]:
        with self.db.transaction() as conn:
            return get_job(conn, job_id)

    def attempts(self, job_id: int) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            return list_attempts(conn, job_id)

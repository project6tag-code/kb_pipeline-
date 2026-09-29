"""Runtime vocabulary of kbp Schema V1 (docs/database_schema_v1.md).

Every value here is one of the CHECK-list values of Migration 001A; a unit
test (tests/unit/test_runtime_states.py) parses the migration and fails if any
value drifts. Nothing here invents a status: "failed" exists only as a
processing-attempt outcome, never as a job status (schema §16.3).
"""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    """``kbp.pipeline_jobs.status`` (schema §5.19)."""

    PENDING = "pending"
    RUNNING = "running"
    RETRY_PENDING = "retry_pending"
    SUCCEEDED = "succeeded"
    DEAD_LETTER = "dead_letter"
    QUARANTINED = "quarantined"
    CANCELLED = "cancelled"


CLAIMABLE_STATUSES: frozenset[JobStatus] = frozenset({JobStatus.PENDING, JobStatus.RETRY_PENDING})
TERMINAL_STATUSES: frozenset[JobStatus] = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.DEAD_LETTER, JobStatus.QUARANTINED, JobStatus.CANCELLED}
)

# Schema §17.4. Operator actions: cancel (pending/retry_pending -> cancelled)
# and requeue/reprocess (terminal -> pending; §16.1 "a deliberate reprocess
# resets status"). "cancelled" is terminal for workers.
ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RETRY_PENDING: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCEEDED, JobStatus.RETRY_PENDING, JobStatus.DEAD_LETTER, JobStatus.QUARANTINED}
    ),
    JobStatus.SUCCEEDED: frozenset({JobStatus.PENDING}),
    JobStatus.DEAD_LETTER: frozenset({JobStatus.PENDING}),
    JobStatus.QUARANTINED: frozenset({JobStatus.PENDING}),
    JobStatus.CANCELLED: frozenset({JobStatus.PENDING}),
}


def is_allowed_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[JobStatus(current)]


class RunStatus(StrEnum):
    """``kbp.pipeline_runs.status`` (schema §5.1)."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABORTED = "aborted"


FINAL_RUN_STATUSES: frozenset[RunStatus] = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.ABORTED})


class RunKind(StrEnum):
    """``kbp.pipeline_runs.run_kind`` (schema §5.1)."""

    INGEST = "ingest"
    REPROCESS = "reprocess"
    RECONCILE_LEGACY = "reconcile_legacy"
    REEMBED = "reembed"
    MAINTENANCE = "maintenance"


class JobType(StrEnum):
    """``kbp.pipeline_jobs.job_type`` (schema §5.19, §16.2)."""

    CC_INDEX_PARTITION = "cc_index_partition"
    GH_REPOSITORY_DISCOVERY = "gh_repository_discovery"
    GH_REPOSITORY_SNAPSHOT = "gh_repository_snapshot"
    EMBEDDING_ENQUEUE = "embedding_enqueue"
    POLICY_REEVALUATION = "policy_reevaluation"
    LEGACY_RECONCILIATION = "legacy_reconciliation"


class JobSource(StrEnum):
    """``kbp.pipeline_jobs.source`` (schema §5.19)."""

    COMMON_CRAWL = "common_crawl"
    GITHUB = "github"
    INTERNAL = "internal"


# The source each job type belongs to (the two acquisition channels, L8).
JOB_TYPE_SOURCE: dict[JobType, JobSource] = {
    JobType.CC_INDEX_PARTITION: JobSource.COMMON_CRAWL,
    JobType.GH_REPOSITORY_DISCOVERY: JobSource.GITHUB,
    JobType.GH_REPOSITORY_SNAPSHOT: JobSource.GITHUB,
    JobType.EMBEDDING_ENQUEUE: JobSource.INTERNAL,
    JobType.POLICY_REEVALUATION: JobSource.INTERNAL,
    JobType.LEGACY_RECONCILIATION: JobSource.INTERNAL,
}


class Stage(StrEnum):
    """``pipeline_jobs.stage`` / ``dead_letters.stage`` (schema §16.3), in order."""

    DISCOVERED = "DISCOVERED"
    ACQUISITION_PENDING = "ACQUISITION_PENDING"
    ACQUIRED = "ACQUIRED"
    PARSED = "PARSED"
    NORMALIZED = "NORMALIZED"
    LANGUAGE_VALIDATED = "LANGUAGE_VALIDATED"
    QUALITY_VALIDATED = "QUALITY_VALIDATED"
    EXACT_DEDUP_VALIDATED = "EXACT_DEDUP_VALIDATED"
    NEAR_DEDUP_VALIDATED = "NEAR_DEDUP_VALIDATED"
    FACTUALITY_VALIDATED = "FACTUALITY_VALIDATED"
    SOURCE_CLASSIFIED = "SOURCE_CLASSIFIED"
    DOMAIN_CLASSIFIED = "DOMAIN_CLASSIFIED"
    POLICY_VALIDATED = "POLICY_VALIDATED"
    SILVER = "SILVER"
    CHUNKED = "CHUNKED"
    CHUNK_DEDUPLICATED = "CHUNK_DEDUPLICATED"
    EMBEDDED = "EMBEDDED"
    STORED = "STORED"


class ErrorClass(StrEnum):
    """``last_error_class`` / ``error_class`` (schema §17.2)."""

    TRANSIENT = "transient"
    RATE_LIMITED = "rate_limited"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    PERMANENT_SOURCE = "permanent_source"
    DATA_INVALID = "data_invalid"
    POLICY_QUARANTINE = "policy_quarantine"
    INTERNAL_BUG = "internal_bug"
    LEASE_EXPIRED = "lease_expired"


# Schema §17.2 "Action" column, at unit (job) level.
RETRYABLE_ERROR_CLASSES: frozenset[ErrorClass] = frozenset(
    {ErrorClass.TRANSIENT, ErrorClass.RESOURCE_EXHAUSTED, ErrorClass.INTERNAL_BUG, ErrorClass.LEASE_EXPIRED}
)
DEFERRABLE_ERROR_CLASSES: frozenset[ErrorClass] = frozenset({ErrorClass.RATE_LIMITED})
PERMANENT_ERROR_CLASSES: frozenset[ErrorClass] = frozenset({ErrorClass.PERMANENT_SOURCE, ErrorClass.DATA_INVALID})
QUARANTINE_ERROR_CLASSES: frozenset[ErrorClass] = frozenset({ErrorClass.POLICY_QUARANTINE})


class AttemptOutcome(StrEnum):
    """``kbp.processing_attempts.outcome`` (schema §5.20)."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEFERRED_RATE_LIMIT = "deferred_rate_limit"
    LEASE_EXPIRED = "lease_expired"
    CANCELLED = "cancelled"


class DeadLetterItemType(StrEnum):
    """``kbp.dead_letters.item_type`` (schema §5.21)."""

    JOB = "job"
    OBSERVATION = "observation"
    DOCUMENT = "document"
    CHUNK = "chunk"
    CHUNK_EMBEDDING = "chunk_embedding"
    LEGACY_KB_ROW = "legacy_kb_row"


class Disposition(StrEnum):
    """``kbp.dead_letters.disposition`` (schema §5.21)."""

    DEAD_LETTER = "dead_letter"
    QUARANTINED = "quarantined"


class Resolution(StrEnum):
    """``kbp.dead_letters.resolution`` (schema §5.21)."""

    REQUEUED = "requeued"
    IGNORED = "ignored"
    FIXED = "fixed"


class SourceChannel(StrEnum):
    """``kbp.source_rate_limits.source_channel`` (schema §5.3)."""

    COMMON_CRAWL = "common_crawl"
    GITHUB = "github"


# Bounds of the smallint columns priority / attempt_count / max_attempts.
SMALLINT_MAX = 32767
DEFAULT_PRIORITY = 100  # schema §5.19 default; lower value = claimed first (§16.4)
MAX_ERROR_MESSAGE_CHARS = 4000  # "truncated by the application" (§5.18)


def truncate_message(message: str | None) -> str | None:
    if message is None:
        return None
    return message if len(message) <= MAX_ERROR_MESSAGE_CHARS else message[: MAX_ERROR_MESSAGE_CHARS - 3] + "..."

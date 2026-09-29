"""Connection-scoped repositories for kbp runtime tables.

Each function takes an open SQLAlchemy connection, so the caller decides the
transaction boundary (for example, a dead-letter row is written in the same
transaction as the job's transition to ``dead_letter``).

Only operations granted to ``kb_pipeline_app`` are used
(docs/database_privileges_v1.md §5.1): no DELETE, and UPDATE only on granted
columns. Every SQL statement is a module constant ending in ``_SQL`` and is
checked by tests/unit/test_runtime_sql.py against the grants and the schema.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.queue.states import (
    DeadLetterItemType,
    Disposition,
    ErrorClass,
    FINAL_RUN_STATUSES,
    Resolution,
    RunKind,
    RunStatus,
    SourceChannel,
    Stage,
    truncate_message,
)
from app.utils.time import ensure_utc


class RecordNotFound(LookupError):
    pass


class InvalidStateTransition(RuntimeError):
    pass


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


# --------------------------------------------------------------------------- #
# kbp.pipeline_runs  (app: SELECT, INSERT, UPDATE (status, finished_at, notes))
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class PipelineRun:
    run_id: uuid.UUID
    run_kind: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    code_version: str | None
    identity_spec_version: str
    identity_extension_version: str | None
    host: str | None
    notes: str | None


RUN_COLUMNS = ("run_id, run_kind, status, started_at, finished_at, code_version, "
               "identity_spec_version, identity_extension_version, host, notes")

CREATE_RUN_SQL = f"""
INSERT INTO kbp.pipeline_runs (run_id, run_kind, status, started_at, code_version, config_sha256,
                               identity_spec_version, identity_extension_version, host, notes)
VALUES (:run_id, :run_kind, 'running', now(), :code_version, :config_sha256,
        'v1', :identity_extension_version, :host, :notes)
RETURNING {RUN_COLUMNS}
"""

GET_RUN_SQL = f"SELECT {RUN_COLUMNS} FROM kbp.pipeline_runs WHERE run_id = :run_id"

UPDATE_RUN_NOTES_SQL = f"""
UPDATE kbp.pipeline_runs SET notes = :notes
WHERE run_id = :run_id AND status = 'running'
RETURNING {RUN_COLUMNS}
"""

FINISH_RUN_SQL = f"""
UPDATE kbp.pipeline_runs
SET status = :status, finished_at = now(), notes = COALESCE(:notes, notes)
WHERE run_id = :run_id AND status = 'running'
RETURNING {RUN_COLUMNS}
"""


def _run(row: Mapping[str, Any]) -> PipelineRun:
    return PipelineRun(**dict(row))


def create_run(conn: Any, run_kind: RunKind | str, *, code_version: str | None = None,
               config_sha256: bytes | None = None, identity_extension_version: str | None = "v1",
               host: str | None = None, notes: str | None = None) -> PipelineRun:
    """Open a run (status ``running``). run_id is a random UUID (L15)."""
    if config_sha256 is not None and len(config_sha256) != 32:
        raise ValueError("config_sha256 must be 32 bytes")
    row = conn.execute(_text(CREATE_RUN_SQL), {
        "run_id": uuid.uuid4(), "run_kind": RunKind(run_kind).value, "code_version": code_version,
        "config_sha256": config_sha256, "identity_extension_version": identity_extension_version,
        "host": host, "notes": truncate_message(notes),
    }).mappings().one()
    return _run(row)


def get_run(conn: Any, run_id: uuid.UUID) -> PipelineRun:
    row = conn.execute(_text(GET_RUN_SQL), {"run_id": run_id}).mappings().one_or_none()
    if row is None:
        raise RecordNotFound(f"pipeline run {run_id} not found")
    return _run(row)


def update_run_notes(conn: Any, run_id: uuid.UUID, notes: str) -> PipelineRun:
    """Progress/state text of a running run (``notes`` is its only mutable free-text column)."""
    row = conn.execute(_text(UPDATE_RUN_NOTES_SQL),
                       {"run_id": run_id, "notes": truncate_message(notes)}).mappings().one_or_none()
    if row is None:
        raise InvalidStateTransition(f"pipeline run {run_id} is not running")
    return _run(row)


def finish_run(conn: Any, run_id: uuid.UUID, status: RunStatus | str, notes: str | None = None) -> PipelineRun:
    """running -> succeeded | failed | aborted, with finished_at = now().

    Error details of a failed run are kept in ``notes`` (the schema has no
    other error column on runs).
    """
    status = RunStatus(status)
    if status not in FINAL_RUN_STATUSES:
        raise ValueError("a run can only be finished as succeeded, failed or aborted")
    row = conn.execute(_text(FINISH_RUN_SQL), {"run_id": run_id, "status": status.value,
                                               "notes": truncate_message(notes)}).mappings().one_or_none()
    if row is None:
        raise InvalidStateTransition(f"pipeline run {run_id} is not running")
    return _run(row)


# --------------------------------------------------------------------------- #
# kbp.dead_letters  (app: SELECT, INSERT, UPDATE (occurrence_count, last_seen_at,
# attempt_count, error_class, error_code, error_message, payload_ref,
# disposition, resolved_at, resolution))
# --------------------------------------------------------------------------- #

RECORD_DEAD_LETTER_SQL = """
INSERT INTO kbp.dead_letters (item_type, item_key, stage, disposition, job_id, run_id, error_class,
                              error_code, error_message, attempt_count, payload_ref, first_seen_at,
                              last_seen_at)
VALUES (:item_type, :item_key, :stage, :disposition, :job_id, :run_id, :error_class,
        :error_code, :error_message, :attempt_count, :payload_ref, now(), now())
ON CONFLICT (item_type, item_key, stage) DO UPDATE SET
    occurrence_count = kbp.dead_letters.occurrence_count + 1,
    last_seen_at = EXCLUDED.last_seen_at,
    attempt_count = EXCLUDED.attempt_count,
    error_class = EXCLUDED.error_class,
    error_code = EXCLUDED.error_code,
    error_message = EXCLUDED.error_message,
    payload_ref = COALESCE(EXCLUDED.payload_ref, kbp.dead_letters.payload_ref),
    disposition = EXCLUDED.disposition,
    resolved_at = NULL,
    resolution = NULL
RETURNING dead_letter_id, occurrence_count
"""

RESOLVE_DEAD_LETTER_SQL = """
UPDATE kbp.dead_letters SET resolution = :resolution, resolved_at = now()
WHERE dead_letter_id = :dead_letter_id AND resolved_at IS NULL
RETURNING dead_letter_id
"""

LIST_OPEN_DEAD_LETTERS_SQL = """
SELECT dead_letter_id, item_type, item_key, stage, disposition, job_id, run_id, error_class, error_code,
       error_message, attempt_count, occurrence_count, payload_ref, first_seen_at, last_seen_at
FROM kbp.dead_letters
WHERE resolved_at IS NULL
ORDER BY last_seen_at DESC
LIMIT :limit
"""

GET_DEAD_LETTER_SQL = """
SELECT dead_letter_id, item_type, item_key, stage, disposition, job_id, run_id, error_class, error_code,
       error_message, attempt_count, occurrence_count, payload_ref, first_seen_at, last_seen_at,
       resolved_at, resolution
FROM kbp.dead_letters
WHERE item_type = :item_type AND item_key = :item_key AND stage = :stage
"""


def record_dead_letter(conn: Any, *, item_type: DeadLetterItemType | str, item_key: str, stage: Stage | str,
                       disposition: Disposition | str, error_class: ErrorClass | str, attempt_count: int,
                       error_code: str | None = None, error_message: str | None = None,
                       job_id: int | None = None, run_id: uuid.UUID | None = None,
                       payload_ref: str | None = None) -> tuple[int, int]:
    """Insert, or count a repeat of, a dead letter. Returns (dead_letter_id, occurrence_count).

    A repeat of a resolved item re-opens it (resolution cleared): a failure is
    never silently discarded.
    """
    if not item_key:
        raise ValueError("item_key must not be empty")
    row = conn.execute(_text(RECORD_DEAD_LETTER_SQL), {
        "item_type": DeadLetterItemType(item_type).value, "item_key": item_key, "stage": Stage(stage).value,
        "disposition": Disposition(disposition).value, "job_id": job_id, "run_id": run_id,
        "error_class": ErrorClass(error_class).value, "error_code": error_code,
        "error_message": truncate_message(error_message), "attempt_count": attempt_count,
        "payload_ref": payload_ref,
    }).one()
    return int(row[0]), int(row[1])


def resolve_dead_letter(conn: Any, dead_letter_id: int, resolution: Resolution | str) -> None:
    row = conn.execute(_text(RESOLVE_DEAD_LETTER_SQL), {"dead_letter_id": dead_letter_id,
                                                         "resolution": Resolution(resolution).value}).one_or_none()
    if row is None:
        raise InvalidStateTransition(f"dead letter {dead_letter_id} is missing or already resolved")


def list_open_dead_letters(conn: Any, limit: int = 100) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(_text(LIST_OPEN_DEAD_LETTERS_SQL), {"limit": limit}).mappings()]


def get_dead_letter(conn: Any, item_type: DeadLetterItemType | str, item_key: str,
                    stage: Stage | str) -> dict[str, Any] | None:
    row = conn.execute(_text(GET_DEAD_LETTER_SQL), {"item_type": DeadLetterItemType(item_type).value,
                                                     "item_key": item_key, "stage": Stage(stage).value}
                       ).mappings().one_or_none()
    return None if row is None else dict(row)


# --------------------------------------------------------------------------- #
# kbp.source_rate_limits  (app: SELECT, INSERT, UPDATE table-level)
# --------------------------------------------------------------------------- #

_TOKEN_LIKE = re.compile(r"^(ghp_|gho_|ghu_|ghs_|ghr_|github_pat_|bearer\s|token\s)", re.IGNORECASE)
_FINGERPRINT = re.compile(r"^(|sha256:[0-9a-f]{16})$")


def credential_fingerprint(credential_identity: str | None) -> str:
    """Non-reversible fingerprint of a credential identity; '' when unauthenticated.

    Schema §5.3: never a token. Pass an identity (e.g. the token's owner/app
    id), not the secret; secrets are refused as far as they are recognizable.
    """
    if not credential_identity:
        return ""
    if _TOKEN_LIKE.match(credential_identity):
        raise ValueError("refusing to fingerprint a raw token; pass a credential identity instead")
    return "sha256:" + hashlib.sha256(credential_identity.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class RateLimitObservation:
    source_channel: SourceChannel | str
    limit_scope: str
    credential_fingerprint: str = ""
    limit_total: int | None = None
    remaining: int | None = None
    reset_at: datetime | None = None
    retry_after_seconds: int | None = None
    last_http_status: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_channel", SourceChannel(self.source_channel))
        if not self.limit_scope or len(self.limit_scope) > 200:
            raise ValueError("limit_scope must be 1..200 characters")
        if not _FINGERPRINT.match(self.credential_fingerprint):
            raise ValueError("credential_fingerprint must be '' or produced by credential_fingerprint()")
        if self.remaining is not None and self.remaining < 0:
            raise ValueError("remaining must be >= 0")
        if self.retry_after_seconds is not None and self.retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must be >= 0")
        if self.last_http_status is not None and not 100 <= self.last_http_status <= 599:
            raise ValueError("last_http_status must be an HTTP status code")
        if self.reset_at is not None:
            object.__setattr__(self, "reset_at", ensure_utc(self.reset_at))


@dataclass(frozen=True)
class RateLimitState:
    source_channel: str
    limit_scope: str
    credential_fingerprint: str
    limit_total: int | None
    remaining: int | None
    reset_at: datetime | None
    retry_after_until: datetime | None
    last_http_status: int | None
    updated_at: datetime

    def blocked_until(self, now: datetime) -> datetime | None:
        """Schema §17.1: blocked while remaining = 0 AND reset_at > now, or retry_after_until > now."""
        now = ensure_utc(now)
        candidates = []
        if self.remaining == 0 and self.reset_at is not None and self.reset_at > now:
            candidates.append(self.reset_at)
        if self.retry_after_until is not None and self.retry_after_until > now:
            candidates.append(self.retry_after_until)
        return max(candidates) if candidates else None


RATE_LIMIT_COLUMNS = ("source_channel, limit_scope, credential_fingerprint, limit_total, remaining, reset_at, "
                      "retry_after_until, last_http_status, updated_at")

RECORD_RATE_LIMIT_SQL = f"""
INSERT INTO kbp.source_rate_limits (source_channel, limit_scope, credential_fingerprint, limit_total, remaining,
                                    reset_at, retry_after_until, last_http_status, updated_at)
VALUES (:source_channel, :limit_scope, :credential_fingerprint, :limit_total, :remaining, :reset_at,
        CASE WHEN CAST(:retry_after_seconds AS integer) IS NULL THEN NULL
             ELSE now() + make_interval(secs => CAST(:retry_after_seconds AS integer)) END,
        :last_http_status, now())
ON CONFLICT (source_channel, limit_scope, credential_fingerprint) DO UPDATE SET
    limit_total = COALESCE(EXCLUDED.limit_total, kbp.source_rate_limits.limit_total),
    remaining = COALESCE(EXCLUDED.remaining, kbp.source_rate_limits.remaining),
    reset_at = COALESCE(EXCLUDED.reset_at, kbp.source_rate_limits.reset_at),
    retry_after_until = COALESCE(EXCLUDED.retry_after_until, kbp.source_rate_limits.retry_after_until),
    last_http_status = COALESCE(EXCLUDED.last_http_status, kbp.source_rate_limits.last_http_status),
    updated_at = EXCLUDED.updated_at
RETURNING {RATE_LIMIT_COLUMNS}
"""

GET_RATE_LIMIT_SQL = f"""
SELECT {RATE_LIMIT_COLUMNS} FROM kbp.source_rate_limits
WHERE source_channel = :source_channel AND limit_scope = :limit_scope
  AND credential_fingerprint = :credential_fingerprint
"""

RATE_LIMIT_BLOCKED_UNTIL_SQL = """
SELECT GREATEST(
           CASE WHEN remaining = 0 AND reset_at > now() THEN reset_at END,
           CASE WHEN retry_after_until > now() THEN retry_after_until END) AS blocked_until
FROM kbp.source_rate_limits
WHERE source_channel = :source_channel AND limit_scope = :limit_scope
  AND credential_fingerprint = :credential_fingerprint
"""


def record_rate_limit(conn: Any, observation: RateLimitObservation) -> RateLimitState:
    """UPSERT the latest observed limit state (schema §20). Unknown values keep the previous ones."""
    row = conn.execute(_text(RECORD_RATE_LIMIT_SQL), {
        "source_channel": observation.source_channel.value, "limit_scope": observation.limit_scope,
        "credential_fingerprint": observation.credential_fingerprint, "limit_total": observation.limit_total,
        "remaining": observation.remaining, "reset_at": observation.reset_at,
        "retry_after_seconds": observation.retry_after_seconds, "last_http_status": observation.last_http_status,
    }).mappings().one()
    return RateLimitState(**dict(row))


def get_rate_limit(conn: Any, source_channel: SourceChannel | str, limit_scope: str,
                   credential_fingerprint: str = "") -> RateLimitState | None:
    row = conn.execute(_text(GET_RATE_LIMIT_SQL), {
        "source_channel": SourceChannel(source_channel).value, "limit_scope": limit_scope,
        "credential_fingerprint": credential_fingerprint}).mappings().one_or_none()
    return None if row is None else RateLimitState(**dict(row))


def rate_limit_blocked_until(conn: Any, source_channel: SourceChannel | str, limit_scope: str,
                             credential_fingerprint: str = "") -> datetime | None:
    """When work for this scope may resume (database clock); None if not blocked."""
    row = conn.execute(_text(RATE_LIMIT_BLOCKED_UNTIL_SQL), {
        "source_channel": SourceChannel(source_channel).value, "limit_scope": limit_scope,
        "credential_fingerprint": credential_fingerprint}).one_or_none()
    return None if row is None else row[0]

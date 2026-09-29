"""PostgreSQL persistence for GitHub Acquisition V1 (role kb_pipeline_app).

Grants used (docs/database_privileges_v1.md §5.1): INSERT on
``github_repositories`` plus UPDATE of its granted mutable columns;
INSERT only on ``github_commits``, ``source_observations`` and
``github_file_observations``. Conflict targets follow schema §20:

* ``github_repositories (repository_id)``: DO UPDATE the mutable metadata
  WHERE any of it IS DISTINCT FROM the stored value (rename/transfer keeps the id);
* ``github_commits (repository_id, commit_sha)``: DO NOTHING;
* ``source_observations (observation_id)``: DO NOTHING;
* ``github_file_observations (observation_id)``: DO NOTHING; the natural key
  UNIQUE (repository_id, commit_sha, file_path) is left to raise.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from app.db import repositories
from app.queue import job_queue
from app.queue.job_queue import JobSpec
from app.queue.states import DeadLetterItemType, Disposition, ErrorClass, SourceChannel, Stage
from app.sources.github.rate_limit import RateState
from app.utils.time import ensure_utc


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


UPSERT_REPOSITORY_SQL = """
INSERT INTO kbp.github_repositories (repository_id, node_id, owner_login, repo_name, html_url, default_branch,
                                     visibility, license_spdx_id, is_fork, is_archived, first_seen_at,
                                     metadata_refreshed_at)
VALUES (:repository_id, :node_id, :owner_login, :repo_name, :html_url, :default_branch, :visibility,
        :license_spdx_id, :is_fork, :is_archived, now(), now())
ON CONFLICT (repository_id) DO UPDATE SET
    node_id = EXCLUDED.node_id,
    owner_login = EXCLUDED.owner_login,
    repo_name = EXCLUDED.repo_name,
    html_url = EXCLUDED.html_url,
    default_branch = EXCLUDED.default_branch,
    visibility = EXCLUDED.visibility,
    license_spdx_id = EXCLUDED.license_spdx_id,
    is_fork = EXCLUDED.is_fork,
    is_archived = EXCLUDED.is_archived,
    metadata_refreshed_at = EXCLUDED.metadata_refreshed_at
WHERE (kbp.github_repositories.node_id, kbp.github_repositories.owner_login, kbp.github_repositories.repo_name,
       kbp.github_repositories.html_url, kbp.github_repositories.default_branch, kbp.github_repositories.visibility,
       kbp.github_repositories.license_spdx_id, kbp.github_repositories.is_fork, kbp.github_repositories.is_archived)
      IS DISTINCT FROM
      (EXCLUDED.node_id, EXCLUDED.owner_login, EXCLUDED.repo_name, EXCLUDED.html_url, EXCLUDED.default_branch,
       EXCLUDED.visibility, EXCLUDED.license_spdx_id, EXCLUDED.is_fork, EXCLUDED.is_archived)
RETURNING repository_id
"""

REPOSITORY_STATE_SQL = """
SELECT api_etag, last_snapshot_commit_sha FROM kbp.github_repositories WHERE repository_id = :repository_id
"""

SET_ETAG_SQL = """
UPDATE kbp.github_repositories SET api_etag = :api_etag WHERE repository_id = :repository_id
"""

# Compare-and-set: never move last_snapshot_commit_sha backwards past a newer snapshot.
ADVANCE_SNAPSHOT_SQL = """
UPDATE kbp.github_repositories SET last_snapshot_commit_sha = :commit_sha
WHERE repository_id = :repository_id
  AND last_snapshot_commit_sha IS NOT DISTINCT FROM CAST(:previous_sha AS text)
RETURNING repository_id
"""

INSERT_COMMIT_SQL = """
INSERT INTO kbp.github_commits (repository_id, commit_sha, tree_sha, ref_name, committed_at, visibility_at_snapshot,
                                previous_snapshot_sha, snapshot_mode, resolved_at)
VALUES (:repository_id, :commit_sha, :tree_sha, :ref_name, :committed_at, 'public', :previous_snapshot_sha,
        :snapshot_mode, now())
ON CONFLICT (repository_id, commit_sha) DO NOTHING
RETURNING commit_sha
"""

SEEN_FILES_SQL = """
SELECT file_path, blob_sha FROM kbp.github_file_observations
WHERE repository_id = :repository_id AND file_path = ANY (CAST(:paths AS text[]))
"""

INSERT_OBSERVATION_SQL = """
INSERT INTO kbp.source_observations (observation_id, source_channel, source_url, url_host, source_title,
                                     source_published_at, acquired_at, run_id)
VALUES (:observation_id, 'github', :source_url, 'github.com', NULL, NULL, :acquired_at, :run_id)
ON CONFLICT (observation_id) DO NOTHING
RETURNING observation_id
"""

INSERT_FILE_OBSERVATION_SQL = """
INSERT INTO kbp.github_file_observations (observation_id, repository_id, commit_sha, file_path, blob_sha,
                                          file_size_bytes)
VALUES (:observation_id, :repository_id, :commit_sha, :file_path, :blob_sha, :file_size_bytes)
ON CONFLICT (observation_id) DO NOTHING
RETURNING observation_id
"""

GH_JOB_SUMMARY_SQL = """
SELECT job_type, status, count(*) AS jobs, sum(deferral_count) AS deferrals
FROM kbp.pipeline_jobs
WHERE job_type IN ('gh_repository_discovery', 'gh_repository_snapshot')
GROUP BY job_type, status
ORDER BY job_type, status
"""

GH_COUNTS_SQL = """
SELECT (SELECT count(*) FROM kbp.github_repositories) AS repositories,
       (SELECT count(*) FROM kbp.github_commits) AS commits,
       (SELECT count(*) FROM kbp.github_file_observations) AS file_observations
"""


@dataclass(frozen=True)
class RepositoryRow:
    repository_id: int
    node_id: str | None
    owner_login: str
    repo_name: str
    html_url: str
    default_branch: str | None
    visibility: str
    license_spdx_id: str | None
    is_fork: bool | None
    is_archived: bool | None


@dataclass(frozen=True)
class CommitRow:
    repository_id: int
    commit_sha: str
    tree_sha: str | None
    ref_name: str | None
    committed_at: datetime | None
    previous_snapshot_sha: str | None
    snapshot_mode: str


@dataclass(frozen=True)
class FileRow:
    observation_id: uuid.UUID
    repository_id: int
    commit_sha: str
    file_path: str
    blob_sha: str
    file_size_bytes: int
    source_url: str
    acquired_at: datetime
    run_id: uuid.UUID | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "acquired_at", ensure_utc(self.acquired_at))
        if "\x00" in self.file_path or "\x00" in self.source_url:
            raise ValueError("U+0000 is not allowed")


class GitHubStore(Protocol):
    def upsert_repository(self, conn: Any, row: RepositoryRow) -> None: ...
    def repository_state(self, conn: Any, repository_id: int) -> tuple[str | None, str | None] | None: ...
    def set_etag(self, conn: Any, repository_id: int, etag: str | None) -> None: ...
    def advance_snapshot(self, conn: Any, repository_id: int, commit_sha: str, previous_sha: str | None) -> bool: ...
    def insert_commit(self, conn: Any, row: CommitRow) -> bool: ...
    def seen_files(self, conn: Any, repository_id: int, paths: Iterable[str]) -> set[tuple[str, str]]: ...
    def persist_file(self, conn: Any, row: FileRow) -> bool: ...
    def enqueue(self, conn: Any, spec: JobSpec) -> bool: ...
    def record_item_dead_letter(self, conn: Any, *, item_key: str, stage: Stage, error_class: ErrorClass,
                                error_code: str, error_message: str, attempt_count: int, job_id: int | None,
                                run_id: uuid.UUID | None, payload_ref: str | None) -> None: ...
    def blocked_until(self, conn: Any, scope: str, fingerprint: str) -> datetime | None: ...
    def record_rate_limit(self, conn: Any, state: RateState, fingerprint: str, *,
                          retry_after_seconds: int | None = None) -> None: ...


class PostgresGitHubStore:
    def upsert_repository(self, conn: Any, row: RepositoryRow) -> None:
        conn.execute(_text(UPSERT_REPOSITORY_SQL), vars(row))

    def repository_state(self, conn: Any, repository_id: int) -> tuple[str | None, str | None] | None:
        row = conn.execute(_text(REPOSITORY_STATE_SQL), {"repository_id": repository_id}).one_or_none()
        return None if row is None else (row[0], row[1])

    def set_etag(self, conn: Any, repository_id: int, etag: str | None) -> None:
        conn.execute(_text(SET_ETAG_SQL), {"repository_id": repository_id, "api_etag": etag})

    def advance_snapshot(self, conn: Any, repository_id: int, commit_sha: str, previous_sha: str | None) -> bool:
        return conn.execute(_text(ADVANCE_SNAPSHOT_SQL), {
            "repository_id": repository_id, "commit_sha": commit_sha, "previous_sha": previous_sha}).one_or_none() is not None

    def insert_commit(self, conn: Any, row: CommitRow) -> bool:
        return conn.execute(_text(INSERT_COMMIT_SQL), vars(row)).one_or_none() is not None

    def seen_files(self, conn: Any, repository_id: int, paths: Iterable[str]) -> set[tuple[str, str]]:
        paths = list(paths)
        if not paths:
            return set()
        rows = conn.execute(_text(SEEN_FILES_SQL), {"repository_id": repository_id, "paths": paths}).all()
        return {(r[0], r[1]) for r in rows}

    def persist_file(self, conn: Any, row: FileRow) -> bool:
        conn.execute(_text(INSERT_OBSERVATION_SQL), {"observation_id": row.observation_id, "source_url": row.source_url,
                                                     "acquired_at": row.acquired_at, "run_id": row.run_id})
        return conn.execute(_text(INSERT_FILE_OBSERVATION_SQL), {
            "observation_id": row.observation_id, "repository_id": row.repository_id, "commit_sha": row.commit_sha,
            "file_path": row.file_path, "blob_sha": row.blob_sha, "file_size_bytes": row.file_size_bytes,
        }).one_or_none() is not None

    def enqueue(self, conn: Any, spec: JobSpec) -> bool:
        return job_queue.enqueue_job(conn, spec).created

    def record_item_dead_letter(self, conn: Any, *, item_key: str, stage: Stage, error_class: ErrorClass,
                                error_code: str, error_message: str, attempt_count: int, job_id: int | None,
                                run_id: uuid.UUID | None, payload_ref: str | None) -> None:
        repositories.record_dead_letter(
            conn, item_type=DeadLetterItemType.OBSERVATION, item_key=item_key, stage=stage,
            disposition=Disposition.DEAD_LETTER, error_class=error_class, error_code=error_code,
            error_message=error_message, attempt_count=attempt_count, job_id=job_id, run_id=run_id,
            payload_ref=payload_ref)

    def blocked_until(self, conn: Any, scope: str, fingerprint: str) -> datetime | None:
        return repositories.rate_limit_blocked_until(conn, SourceChannel.GITHUB, scope, fingerprint)

    def record_rate_limit(self, conn: Any, state: RateState, fingerprint: str, *,
                          retry_after_seconds: int | None = None) -> None:
        repositories.record_rate_limit(conn, repositories.RateLimitObservation(
            SourceChannel.GITHUB, state.resource, fingerprint, limit_total=state.limit, remaining=state.remaining,
            reset_at=state.reset_at, retry_after_seconds=retry_after_seconds, last_http_status=state.last_status))


def job_summary(conn: Any) -> list[Mapping[str, Any]]:
    return [dict(r) for r in conn.execute(_text(GH_JOB_SUMMARY_SQL)).mappings()]


def counts(conn: Any) -> Mapping[str, Any]:
    return dict(conn.execute(_text(GH_COUNTS_SQL)).mappings().one())

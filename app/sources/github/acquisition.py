"""GitHub Acquisition V1: the ``gh_repository_snapshot`` job handler.

One job = one repository at one immutable commit (``<repository_id>@<commit_sha>``):

1. ``GET /repositories/{id}``: re-verify the repository is public (by numeric
   id, so renames/transfers are followed without redirects) and refresh its
   metadata; a repository that is gone or no longer public dead-letters the job.
2. ``GET git/commits/{sha}``: the commit must be exactly that sha; insert
   ``github_commits`` (``visibility_at_snapshot = 'public'``, ``snapshot_mode``
   ``incremental`` when a previous snapshot exists, else ``full``).
3. ``GET git/trees/{tree}?recursive=1``: bounded; a truncated/oversized tree is
   not guessed at (the job dead-letters with ``tree_truncated``/``tree_too_large``).
   The full blob listing is kept in the Bronze snapshot manifest, including the
   paths deleted since the previous snapshot (history is never destroyed).
4. Qualify files on metadata only (``files.rejection_reason``), order
   documentation first, cap at ``max_files_per_snapshot``.
5. Per batch: skip files whose ``(path, blob_sha)`` was already observed for
   this repository (unchanged content is not re-observed, schema §8.1); reuse
   verified Bronze blobs; fetch the rest (``git/blobs``, raw, size-bounded) with
   a bounded pool; verify the git blob hash; reject binary/non-UTF-8 content;
   store Bronze; insert ``source_observations`` + ``github_file_observations``
   with the checkpoint in ONE transaction. The last batch also advances
   ``github_repositories.last_snapshot_commit_sha`` (compare-and-set).

Silver (optional, ``silver=``): the batch's new observations (and, on a
reprocess, those already observed at this commit) are evaluated from verified
Bronze before the batch transaction and persisted in it (schema §9.3); the
unit then completes at SILVER instead of ACQUIRED.

Failures: 404/403/422 on a file and integrity/binary problems are item
dead letters or skips; 429/403-rate-limit defers (attempt not consumed,
shared ``source_rate_limits``); 5xx/timeouts retry from the checkpoint; a file
causing ``ITEM_TRANSIENT_LIMIT`` unit retries is dead-lettered.
"""

from __future__ import annotations

import hashlib
import re
import time
import uuid
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import quote

from app.processing.silver.model import SourceRef
from app.queue.retry import JobFailure
from app.queue.states import ErrorClass, Stage
from app.sources.commoncrawl.bronze import BronzeCorrupt, DiskSpaceError, UnsafePath
from app.sources.commoncrawl.http import FetchError
from app.sources.github.bronze import OBSERVATION_FORMAT, SNAPSHOT_FORMAT, GitHubBronze
from app.sources.github.client import ACCEPT_RAW, GitHubClient, valid_owner, valid_ref, valid_repo_name
from app.sources.github.common import fingerprint, gate, is_item_error, ordered_bounded, record_rates, to_job_failure
from app.sources.github.config import GitHubAcquisitionConfig, GitHubCredentials
from app.sources.github.files import (
    BlobIntegrityError,
    TreeEntry,
    TreeError,
    parse_tree,
    priority,
    rejection_reason,
    text_rejection,
    verify_blob,
)
from app.sources.github.repository import CommitRow, FileRow, GitHubStore, RepositoryRow
from app.utils.ids import github_observation_id, github_observation_payload
from app.utils.time import utc_now
from app.workers.registry import Completed, JobContext, Yielded

ITEM_TRANSIENT_LIMIT = 3
MAX_METADATA_BYTES = 4 * 1024 * 1024
MAX_DELETED_LISTED = 10_000
_SNAPSHOT_ID = re.compile(r"^([1-9][0-9]{0,18})@([0-9a-f]{40}(?:[0-9a-f]{24})?)$")
_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")


def _bad(message: str, code: str = "invalid_snapshot_job") -> JobFailure:
    return JobFailure(ErrorClass.DATA_INVALID, message, code=code)


@dataclass
class FileItem:
    entry: TreeEntry
    index: int
    observation_id: uuid.UUID
    skip: str | None = None
    row: FileRow | None = None
    item_error: tuple[ErrorClass, str, str] | None = None
    unit_error: FetchError | JobFailure | None = None
    counters: Counter = field(default_factory=Counter)


def repository_row(item: Any, repository_id: int) -> RepositoryRow:
    """Validate a ``/repositories/{id}`` response: same id, public, sane names. Raises JobFailure."""
    if not isinstance(item, dict) or item.get("id") != repository_id:
        raise JobFailure(ErrorClass.DATA_INVALID, "repository response does not match the job", code="repo_mismatch")
    if item.get("private") is not False or item.get("visibility", "public") != "public":
        raise JobFailure(ErrorClass.PERMANENT_SOURCE, "repository is not public", code="not_public")
    owner = item.get("owner") if isinstance(item.get("owner"), dict) else {}
    login, name, html_url = owner.get("login"), item.get("name"), item.get("html_url")
    branch = item.get("default_branch")
    if not valid_owner(login) or not valid_repo_name(name) or not isinstance(html_url, str) \
            or html_url.lower() != f"https://github.com/{login}/{name}".lower():
        raise JobFailure(ErrorClass.DATA_INVALID, "repository metadata is invalid", code="repo_metadata_invalid")
    license_obj = item.get("license")
    spdx = license_obj.get("spdx_id") if isinstance(license_obj, dict) else None
    return RepositoryRow(repository_id=repository_id,
                         node_id=item.get("node_id") if isinstance(item.get("node_id"), str) else None,
                         owner_login=login, repo_name=name, html_url=html_url,
                         default_branch=branch if valid_ref(branch) else None, visibility="public",
                         license_spdx_id=spdx if isinstance(spdx, str) and 0 < len(spdx) <= 100 else None,
                         is_fork=item.get("fork") if isinstance(item.get("fork"), bool) else None,
                         is_archived=item.get("archived") if isinstance(item.get("archived"), bool) else None)


REPOSITORY_MANIFEST_FIELDS = ("id", "node_id", "name", "full_name", "html_url", "description", "fork", "archived",
                              "disabled", "visibility", "default_branch", "size", "language", "stargazers_count",
                              "forks_count", "topics", "created_at", "updated_at", "pushed_at", "homepage")


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


class RepositorySnapshotHandler:
    """Handler for ``gh_repository_snapshot`` (register with GH_POLICY)."""

    def __init__(self, *, client: GitHubClient, store: GitHubStore, bronze: GitHubBronze,
                 config: GitHubAcquisitionConfig, credentials: GitHubCredentials, run_id: uuid.UUID | None = None,
                 clock: Callable[[], datetime] = utc_now, monotonic: Callable[[], float] = time.monotonic,
                 silver: Any = None) -> None:
        self.client, self.store, self.bronze, self.config = client, store, bronze, config
        self.run_id, self.clock, self.monotonic = run_id, clock, monotonic
        self.silver = silver  # app.processing.silver.processor.SilverProcessor (prepare/persist) or None
        self.fp = fingerprint(credentials)

    # -- entry point ------------------------------------------------------------ #

    def __call__(self, ctx: JobContext) -> Completed | Yielded:
        match = _SNAPSHOT_ID.match(ctx.job.source_identifier)
        cp = ctx.checkpoint
        if not match or cp.get("v") != 1:
            raise _bad("source_identifier/checkpoint is not a snapshot job")
        repository_id, commit_sha = int(match.group(1)), match.group(2)
        previous = cp.get("previous")
        position = cp.get("next", 0)
        if (previous is not None and (not isinstance(previous, str) or not _SHA.match(previous))) \
                or isinstance(position, bool) or not isinstance(position, int) or position < 0:
            raise _bad("snapshot checkpoint is invalid")
        gate(ctx, self.store, self.fp, ("core",))
        try:
            self.bronze.check_space(self.config.files.max_file_bytes)
        except DiskSpaceError as exc:
            raise JobFailure(ErrorClass.RESOURCE_EXHAUSTED, str(exc), code="disk_space") from None

        repo_json = self._get(ctx, self.client.endpoints.repository(repository_id), "repository")
        repo = repository_row(repo_json, repository_id)
        commit_json = self._get(ctx, self.client.endpoints.git(repo.owner_login, repo.repo_name, "commits",
                                                               commit_sha), "commit")
        tree_sha = commit_json.get("tree", {}).get("sha") if isinstance(commit_json, dict) else None
        if not isinstance(commit_json, dict) or commit_json.get("sha") != commit_sha \
                or not isinstance(tree_sha, str) or not _SHA.match(tree_sha) or len(tree_sha) != len(commit_sha):
            raise _bad("commit response does not match the requested sha", "commit_mismatch")
        committer = commit_json.get("committer") if isinstance(commit_json.get("committer"), dict) else {}
        mode = "incremental" if previous else "full"
        with ctx.transaction() as conn:
            self.store.upsert_repository(conn, repo)
            self.store.insert_commit(conn, CommitRow(repository_id, commit_sha, tree_sha, cp.get("ref")
                                                     if valid_ref(cp.get("ref")) else None,
                                                     _parse_time(committer.get("date")), previous, mode))
            record_rates(conn, self.store, self.client, self.fp)

        tree_json = self._get(ctx, self.client.endpoints.git(repo.owner_login, repo.repo_name, "trees", tree_sha,
                                                             recursive=True), "tree",
                              max_bytes=self.config.max_tree_bytes)
        try:
            entries = parse_tree(tree_json, object_length=len(commit_sha), max_entries=self.config.max_tree_entries)
        except TreeError as exc:
            raise JobFailure(ErrorClass.DATA_INVALID, str(exc), code=exc.code) from None
        blobs = [e for e in entries if e.type == "blob"]
        metrics: Counter = Counter(tree_entries=len(entries), tree_blobs=len(blobs))
        candidates: list[TreeEntry] = []
        for entry in entries:
            reason = rejection_reason(entry, self.config.files, commit_sha=commit_sha)
            if reason is None:
                candidates.append(entry)
            elif entry.type != "tree":
                metrics[f"skipped_{reason}"] += 1
        candidates.sort(key=lambda e: priority(e, self.config.files))
        if len(candidates) > self.config.files.max_files_per_snapshot:
            metrics["skipped_over_file_limit"] = len(candidates) - self.config.files.max_files_per_snapshot
            candidates = candidates[: self.config.files.max_files_per_snapshot]
        self._write_snapshot_manifest(repo, repo_json, commit_json, commit_sha, tree_sha, previous, mode, blobs,
                                      len(candidates), cp.get("ref"))
        if position > len(candidates):
            raise _bad("checkpoint position is beyond the candidate list")

        self._renewed = self.monotonic()
        failures = {str(k): int(v) for k, v in (cp.get("item_failures") or {}).items()}
        with ThreadPoolExecutor(max_workers=self.config.fetch_concurrency, thread_name_prefix="gh-fetch") as pool:
            while True:
                if ctx.stop_requested:
                    return Yielded(checkpoint=dict(cp, next=position, item_failures=failures))
                batch = [FileItem(e, i, github_observation_id(repository_id, commit_sha, e.path, e.sha))
                         for i, e in enumerate(candidates[position:position + self.config.batch_size], start=position)]
                final = position + len(batch) >= len(candidates)
                done, failed = self._run_batch(ctx, repo, commit_sha, batch, failures, pool)
                position = self._commit(ctx, repo, commit_sha, previous, cp, done, failed, failures, metrics,
                                        final and failed is None and len(done) == len(batch), position)
                if failed is not None:
                    error = failed.unit_error
                    raise to_job_failure(error, f"file {failed.entry.path!r}") if isinstance(error, FetchError) \
                        else error
                if final:
                    break
        return Completed(metrics=dict(sorted(metrics.items())),
                         stage=Stage.SILVER if self.silver is not None else Stage.ACQUIRED)

    # -- API helpers ------------------------------------------------------------- #

    def _get(self, ctx: JobContext, url: str, what: str, *, max_bytes: int = MAX_METADATA_BYTES) -> Any:
        try:
            result = self.client.get_json(url, max_bytes=max_bytes)
        except FetchError as exc:
            with ctx.transaction() as conn:
                record_rates(conn, self.store, self.client, self.fp, exc)
            raise to_job_failure(exc, what) from None
        if result is None:
            raise JobFailure(ErrorClass.TRANSIENT, f"{what}: unexpected 304", code="unexpected_304")
        return result[0]

    # -- Bronze snapshot manifest -------------------------------------------------- #

    def _write_snapshot_manifest(self, repo: RepositoryRow, repo_json: dict, commit_json: dict, commit_sha: str,
                                 tree_sha: str, previous: str | None, mode: str, blobs: list[TreeEntry],
                                 qualified: int, ref: Any) -> None:
        path = self.bronze.snapshot_path(repo.repository_id, commit_sha)
        if path.exists():
            return
        current = {e.path for e in blobs}
        changes: dict[str, Any] = {"previous_snapshot_sha": previous}
        if previous:
            try:
                prev_manifest = self.bronze.read_json(self.bronze.snapshot_path(repo.repository_id, previous))
            except BronzeCorrupt:
                prev_manifest = None
            if prev_manifest and isinstance(prev_manifest.get("tree"), list):
                prev = {row[0]: row[2] for row in prev_manifest["tree"] if isinstance(row, list) and len(row) == 4}
                deleted = sorted(set(prev) - current)
                changes.update(added=len(current - set(prev)),
                               modified=sum(1 for e in blobs if e.path in prev and prev[e.path] != e.sha),
                               deleted_count=len(deleted), deleted=deleted[:MAX_DELETED_LISTED])
            else:
                changes["previous_manifest"] = "unavailable"
        committer = commit_json.get("committer") if isinstance(commit_json.get("committer"), dict) else {}
        author = commit_json.get("author") if isinstance(commit_json.get("author"), dict) else {}
        self.bronze.write_once(path, {
            "format": SNAPSHOT_FORMAT, "source_type": "github",
            "repository": {k: repo_json.get(k) for k in REPOSITORY_MANIFEST_FIELDS if k in repo_json},
            "owner_login": repo.owner_login, "repo_name": repo.repo_name, "html_url": repo.html_url,
            "license_spdx_id": repo.license_spdx_id,
            "commit": {"sha": commit_sha, "tree_sha": tree_sha, "ref_name": ref if valid_ref(ref) else None,
                       "committer_date": committer.get("date"), "author_date": author.get("date"),
                       "parents": [p.get("sha") for p in commit_json.get("parents", []) if isinstance(p, dict)],
                       "api_url": self.client.endpoints.git(repo.owner_login, repo.repo_name, "commits", commit_sha)},
            "snapshot_mode": mode, "changes": changes, "file_policy_version": self.config.files.version,
            "qualified_files": qualified, "retrieved_at": self.clock().isoformat(),
            "tree": [[e.path, e.mode, e.sha, e.size] for e in blobs],
        })

    # -- files ---------------------------------------------------------------------- #

    def _run_batch(self, ctx: JobContext, repo: RepositoryRow, commit_sha: str, batch: list[FileItem],
                   failures: dict[str, int], pool: ThreadPoolExecutor) -> tuple[list[FileItem], FileItem | None]:
        with ctx.transaction() as conn:
            seen = self.store.seen_files(conn, repo.repository_id, [i.entry.path for i in batch])
        for item in batch:
            if (item.entry.path, item.entry.sha) in seen:
                item.skip = "unchanged"
            elif failures.get(item.entry.path, 0) >= ITEM_TRANSIENT_LIMIT:
                item.item_error = (ErrorClass.TRANSIENT, "item_retry_limit",
                                   f"file caused {failures[item.entry.path]} unit retries")
            else:
                cached = self.bronze.rejected_reason(item.entry.sha)
                if cached is not None:
                    item.skip = f"rejected_{cached}"
        done: list[FileItem] = []
        work = ordered_bounded(pool, batch, lambda i: self._acquire(repo, commit_sha, i),
                               window=self.config.window, needs=lambda i: i.skip is None and i.item_error is None)
        try:
            for item, _ in work:
                if self.monotonic() - self._renewed >= ctx.lease_seconds / 3:
                    ctx.heartbeat()
                    self._renewed = self.monotonic()
                if item.unit_error is not None:
                    return done, item
                done.append(item)
        finally:
            work.close()
        return done, None

    def _acquire(self, repo: RepositoryRow, commit_sha: str, item: FileItem) -> None:
        """Pool thread. Never raises: the outcome is stored on ``item``."""
        entry = item.entry
        try:
            content = self.bronze.read_blob(entry.sha)
            if content is not None:
                item.counters["blob_reused"] += 1
            else:
                response = self.client.get(self.client.endpoints.git(repo.owner_login, repo.repo_name, "blobs",
                                                                     entry.sha),
                                           accept=ACCEPT_RAW, max_bytes=self.config.files.max_file_bytes)
                content = response.body
                item.counters["fetched"] += 1
                item.counters["bytes_fetched"] += len(content)
            verify_blob(content, entry.sha, entry.size)
            reason = text_rejection(content)
            if reason is not None:
                self.bronze.mark_rejected(entry.sha, reason)
                item.skip = f"rejected_{reason}"
                return
            item.counters[f"blob_{self.bronze.store_blob(entry.sha, content)}"] += 1
            manifest_path = self.bronze.observation_path(repo.repository_id, commit_sha, item.observation_id)
            existing = self.bronze.read_json(manifest_path)
            retrieved_at = _parse_time(existing.get("retrieved_at")) if existing else None
            if retrieved_at is None:
                retrieved_at = self.clock()
                self.bronze.write_once(manifest_path, self._file_manifest(repo, commit_sha, item, content,
                                                                          retrieved_at))
            item.row = FileRow(item.observation_id, repo.repository_id, commit_sha, entry.path, entry.sha,
                               len(content), self._source_url(repo, commit_sha, entry.path), retrieved_at,
                               self.run_id)
        except FetchError as exc:
            if is_item_error(exc):
                item.item_error = (exc.error_class, exc.code, str(exc))
            else:
                item.unit_error = exc
        except BlobIntegrityError as exc:
            item.item_error = (ErrorClass.DATA_INVALID, exc.code, str(exc))
        except (BronzeCorrupt, UnsafePath) as exc:
            item.item_error = (ErrorClass.DATA_INVALID, "bronze_corrupt", str(exc))
        except DiskSpaceError as exc:
            item.unit_error = JobFailure(ErrorClass.RESOURCE_EXHAUSTED, str(exc), code="disk_space")
        except OSError as exc:
            item.unit_error = JobFailure(ErrorClass.RESOURCE_EXHAUSTED, f"Bronze I/O error: {exc}", code="bronze_io")
        except Exception as exc:  # a bug: stop the unit, keep the committed prefix
            item.unit_error = JobFailure(ErrorClass.INTERNAL_BUG, f"{type(exc).__name__}: {exc}",
                                         code=type(exc).__name__)

    @staticmethod
    def _source_url(repo: RepositoryRow, commit_sha: str, path: str) -> str:
        """Commit-pinned URL built from the observation fields (schema §5.6); path percent-encoded."""
        return f"https://github.com/{repo.owner_login}/{repo.repo_name}/blob/{commit_sha}/{quote(path, safe='/')}"

    def _file_manifest(self, repo: RepositoryRow, commit_sha: str, item: FileItem, content: bytes,
                       retrieved_at: datetime) -> dict[str, Any]:
        entry = item.entry
        return {
            "format": OBSERVATION_FORMAT, "source_type": "github", "observation_id": str(item.observation_id),
            "identity": github_observation_payload(repo.repository_id, commit_sha, entry.path, entry.sha),
            "repository_id": repo.repository_id, "owner_login": repo.owner_login, "repo_name": repo.repo_name,
            "html_url": repo.html_url, "commit_sha": commit_sha, "blob_sha": entry.sha, "path": entry.path,
            "mode": entry.mode, "size": len(content), "content_sha256": hashlib.sha256(content).hexdigest(),
            "source_url": self._source_url(repo, commit_sha, entry.path),
            "api_url": self.client.endpoints.git(repo.owner_login, repo.repo_name, "blobs", entry.sha),
            "retrieved_at": retrieved_at.isoformat(), "file_policy_version": self.config.files.version,
        }

    def _commit(self, ctx: JobContext, repo: RepositoryRow, commit_sha: str, previous: str | None,
                cp: dict[str, Any], done: list[FileItem], failed: FileItem | None, failures: dict[str, int],
                metrics: Counter, final: bool, position: int) -> int:
        next_position = done[-1].index + 1 if done else position
        for item in done:
            failures.pop(item.entry.path, None)
        failure_error = None
        if failed is not None:
            error = failed.unit_error
            failure_error = error if isinstance(error, FetchError) else None
            error_class = error.error_class if isinstance(error, FetchError) else error.failure.error_class
            if error_class in (ErrorClass.TRANSIENT, ErrorClass.INTERNAL_BUG):
                failures[failed.entry.path] = failures.get(failed.entry.path, 0) + 1
        batch: Counter = Counter()
        prepared = None
        if self.silver is not None:
            refs = [SourceRef(i.observation_id, "github", self._source_url(repo, commit_sha, i.entry.path),
                              repository_id=repo.repository_id, commit_sha=commit_sha, path=i.entry.path,
                              blob_sha=i.entry.sha)
                    for i in done if i.row is not None or i.skip == "unchanged"]
            prepared = self.silver.prepare(ctx, refs)
        with ctx.transaction() as conn:
            for item in done:
                batch.update(item.counters)
                if item.skip is not None:
                    batch[item.skip] += 1
                elif item.row is not None:
                    batch["inserted" if self.store.persist_file(conn, item.row) else "already_present"] += 1
                elif item.item_error is not None:
                    error_class, code, message = item.item_error
                    batch["dead_lettered"] += 1
                    self.store.record_item_dead_letter(
                        conn, item_key=str(item.observation_id), stage=Stage.ACQUISITION_PENDING,
                        error_class=error_class, error_code=code, error_message=message,
                        attempt_count=ctx.job.attempt_count, job_id=ctx.job.job_id,
                        run_id=self.run_id or ctx.job.run_id,
                        payload_ref=github_observation_payload(repo.repository_id, commit_sha, item.entry.path,
                                                               item.entry.sha))
            if final:
                advanced = self.store.advance_snapshot(conn, repo.repository_id, commit_sha, previous)
                batch["snapshot_advanced" if advanced else "snapshot_superseded"] += 1
            if prepared is not None:
                batch.update(self.silver.persist(conn, prepared, job_id=ctx.job.job_id,
                                                 attempt_count=ctx.job.attempt_count))
            record_rates(conn, self.store, self.client, self.fp, failure_error)
            checkpoint = dict(cp, next=next_position, item_failures=dict(failures))
            if not failures:
                checkpoint.pop("item_failures")
            ctx.checkpoint_progress(checkpoint, stage=Stage.ACQUISITION_PENDING, conn=conn)
        metrics.update(batch)
        self._renewed = self.monotonic()
        return next_position

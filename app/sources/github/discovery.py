"""Automated public-repository discovery (``gh_repository_discovery`` jobs).

Method (no topic query, no hard-coded repository): the public repository
space is partitioned by **creation date**. The planner generates fixed
windows of ``window_days`` from ``created_from`` up to today (newest first)
and enqueues one discovery job per window and page::

    ghs1.<repository policy version>.<epoch>.<YYYYMMDD>-<YYYYMMDD>#page=<n>   (schema §16.2)

Each page runs the GitHub search API with the configured qualification
qualifiers only (``is:public fork:false stars:>=N size:<=K archived:false``)
plus ``created:<window>``. Page 1 splits a window whose result count exceeds
the API's 1000-result cap into two half windows, or enqueues the remaining
pages. Every item is re-qualified locally (public, not disabled, fork/archive,
size, stars, language, license, a safe default branch), upserted by its
numeric ``repository_id`` (owner/name are mutable metadata: renames and
transfers update the same row), and its default-branch HEAD is resolved with
a conditional request (``If-None-Match`` on the stored ``api_etag``: a 304
costs no quota). A snapshot job ``<repository_id>@<commit_sha>`` is enqueued
only when HEAD differs from ``last_snapshot_commit_sha``; unchanged
repositories are skipped.

Incremental operation: the epoch (default: ISO year-week) is part of the
query id, so running ``plan`` again in a new epoch re-walks the windows;
within an epoch, re-planning is a no-op (``ON CONFLICT DO NOTHING``).
"""

from __future__ import annotations

import json
import math
import re
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Protocol

from app.queue import identity
from app.queue.job_queue import EnqueueResult, JobSpec
from app.queue.retry import JobFailure
from app.queue.states import ErrorClass, JobSource, JobType, Stage
from app.sources.commoncrawl.http import FetchError
from app.sources.github.client import ACCEPT_SHA, GitHubClient, valid_owner, valid_ref, valid_repo_name
from app.sources.github.common import GH_POLICY, fingerprint, gate, is_item_error, record_rates, to_job_failure
from app.sources.github.config import DiscoveryConfig, GitHubCredentials, RepositoryPolicy
from app.sources.github.repository import GitHubStore, RepositoryRow
from app.workers.registry import Completed, JobContext

CHECKPOINT_VERSION = 1
SEARCH_CAP = 1000
PER_PAGE = 100
MAX_SEARCH_BYTES = 32 * 1024 * 1024
_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_QUERY_ID = re.compile(r"^ghs1\.(r1[0-9a-f]{10})\.([A-Za-z0-9]{1,20})\.([0-9]{8})-([0-9]{8})#page=([0-9]{1,2})$")


# --------------------------------------------------------------------------- #
# Windows and job identities
# --------------------------------------------------------------------------- #

def windows(config: DiscoveryConfig, today: date) -> list[tuple[date, date]]:
    """Fixed creation-date windows from ``created_from`` to ``today``, newest first (deterministic)."""
    out, start = [], config.created_from
    step = timedelta(days=config.window_days)
    while start <= today:
        out.append((start, min(start + step - timedelta(days=1), today)))
        start += step
    out.reverse()
    return out[: config.max_windows] if config.max_windows else out


def query_id(policy: RepositoryPolicy, epoch: str, start: date, end: date) -> str:
    return f"ghs1.{policy.version}.{epoch}.{start:%Y%m%d}-{end:%Y%m%d}"


def policy_params(policy: RepositoryPolicy) -> dict[str, Any]:
    return {"min_stars": policy.min_stars, "max_size_kb": policy.max_size_kb, "allow_forks": policy.allow_forks,
            "allow_archived": policy.allow_archived, "languages": list(policy.languages),
            "licenses": list(policy.licenses)}


def discovery_spec(policy: RepositoryPolicy, epoch: str, start: date, end: date, page: int,
                   run_id: uuid.UUID | None = None) -> JobSpec:
    return JobSpec(job_type=JobType.GH_REPOSITORY_DISCOVERY, source=JobSource.GITHUB,
                   source_identifier=identity.gh_repository_discovery(query_id(policy, epoch, start, end), page),
                   max_attempts=GH_POLICY.max_attempts, stage=Stage.DISCOVERED, run_id=run_id,
                   checkpoint={"v": CHECKPOINT_VERSION, "policy": policy_params(policy), "done": []})


def snapshot_spec(row: RepositoryRow, commit_sha: str, previous_sha: str | None,
                  run_id: uuid.UUID | None = None) -> JobSpec:
    return JobSpec(job_type=JobType.GH_REPOSITORY_SNAPSHOT, source=JobSource.GITHUB,
                   source_identifier=identity.gh_repository_snapshot(row.repository_id, commit_sha),
                   max_attempts=GH_POLICY.max_attempts, stage=Stage.DISCOVERED, run_id=run_id,
                   checkpoint={"v": CHECKPOINT_VERSION, "owner": row.owner_login, "name": row.repo_name,
                               "ref": row.default_branch, "previous": previous_sha, "next": 0})


# --------------------------------------------------------------------------- #
# Repository qualification (pure)
# --------------------------------------------------------------------------- #

def qualify_repository(item: Any, policy: RepositoryPolicy) -> tuple[RepositoryRow | None, str]:
    """(row, 'qualified') or (None, rejection reason). Missing metadata is never invented."""
    if not isinstance(item, dict):
        return None, "invalid_metadata"
    repo_id, owner = item.get("id"), item.get("owner")
    login = owner.get("login") if isinstance(owner, dict) else None
    name, html_url = item.get("name"), item.get("html_url")
    if isinstance(repo_id, bool) or not isinstance(repo_id, int) or not 1 <= repo_id < 2 ** 63:
        return None, "invalid_metadata"
    if item.get("private") is not False or item.get("visibility", "public") != "public":
        return None, "not_public"
    if not valid_owner(login) or not valid_repo_name(name) or not isinstance(html_url, str) \
            or html_url.lower() != f"https://github.com/{login}/{name}".lower():
        return None, "invalid_metadata"
    if item.get("disabled") is True:
        return None, "disabled"
    if item.get("fork") is True and not policy.allow_forks:
        return None, "fork"
    if item.get("archived") is True and not policy.allow_archived:
        return None, "archived"
    size, stars = item.get("size"), item.get("stargazers_count")
    if isinstance(size, int) and size > policy.max_size_kb:
        return None, "too_large"
    if isinstance(stars, int) and stars < policy.min_stars:
        return None, "stars"
    language = item.get("language")
    if policy.languages and (not isinstance(language, str) or language.lower() not in policy.languages):
        return None, "language"
    license_obj = item.get("license")
    spdx = license_obj.get("spdx_id") if isinstance(license_obj, dict) else None
    spdx = spdx if isinstance(spdx, str) and 0 < len(spdx) <= 100 else None
    if policy.licenses and (spdx is None or spdx.lower() not in policy.licenses):
        return None, "license"
    branch = item.get("default_branch")
    if not valid_ref(branch):
        return None, "unsupported_default_branch"
    node_id = item.get("node_id") if isinstance(item.get("node_id"), str) else None
    return RepositoryRow(repository_id=repo_id, node_id=node_id, owner_login=login, repo_name=name,
                         html_url=html_url, default_branch=branch, visibility="public", license_spdx_id=spdx,
                         is_fork=item.get("fork") if isinstance(item.get("fork"), bool) else None,
                         is_archived=item.get("archived") if isinstance(item.get("archived"), bool) else None), \
        "qualified"


# --------------------------------------------------------------------------- #
# Planner
# --------------------------------------------------------------------------- #

class PlannerQueue(Protocol):
    def enqueue_many(self, specs: Any) -> list[EnqueueResult]: ...


@dataclass
class DiscoveryPlanReport:
    epoch: str
    policy_version: str
    windows: int = 0
    created: int = 0
    existing: int = 0


def plan_discovery(queue: PlannerQueue, config: DiscoveryConfig, *, today: date | None = None,
                   run_id: uuid.UUID | None = None, batch_size: int = 200) -> DiscoveryPlanReport:
    """Enqueue page 1 of every window (idempotent). No network access."""
    today = today or datetime.now(timezone.utc).date()
    epoch = config.resolved_epoch(today)
    report = DiscoveryPlanReport(epoch, config.policy.version)
    specs = [discovery_spec(config.policy, epoch, start, end, 1, run_id) for start, end in windows(config, today)]
    report.windows = len(specs)
    for i in range(0, len(specs), batch_size):
        for result in queue.enqueue_many(specs[i:i + batch_size]):
            if result.created:
                report.created += 1
            else:
                report.existing += 1
    return report


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

@dataclass
class _Page:
    policy: RepositoryPolicy
    policy_version: str
    epoch: str
    start: date
    end: date
    page: int
    done: set[int] = field(default_factory=set)


def _load(ctx: JobContext) -> _Page:
    match = _QUERY_ID.match(ctx.job.source_identifier)
    bad = JobFailure(ErrorClass.DATA_INVALID, "invalid discovery job", code="invalid_discovery_job")
    if not match or ctx.checkpoint.get("v") != CHECKPOINT_VERSION:
        raise bad
    try:
        params = ctx.checkpoint["policy"]
        policy = RepositoryPolicy(min_stars=int(params["min_stars"]), max_size_kb=int(params["max_size_kb"]),
                                  allow_forks=bool(params["allow_forks"]), allow_archived=bool(params["allow_archived"]),
                                  languages=tuple(params["languages"]), licenses=tuple(params["licenses"]))
        start = datetime.strptime(match.group(3), "%Y%m%d").date()
        end = datetime.strptime(match.group(4), "%Y%m%d").date()
        done = {int(i) for i in ctx.checkpoint.get("done", [])}
    except (KeyError, TypeError, ValueError):
        raise bad from None
    page = int(match.group(5))
    if policy.version != match.group(1) or start > end or not 1 <= page <= SEARCH_CAP // PER_PAGE:
        raise bad
    return _Page(policy, match.group(1), match.group(2), start, end, page, done)


class RepositoryDiscoveryHandler:
    """Handler for ``gh_repository_discovery`` (register with GH_POLICY)."""

    def __init__(self, *, client: GitHubClient, store: GitHubStore, credentials: GitHubCredentials,
                 batch_size: int = 25, run_id: uuid.UUID | None = None) -> None:
        self.client, self.store, self.batch_size, self.run_id = client, store, batch_size, run_id
        self.fp = fingerprint(credentials)

    def __call__(self, ctx: JobContext) -> Completed:
        page = _load(ctx)
        gate(ctx, self.store, self.fp, ("search", "core"))
        q = f"{page.policy.search_qualifiers()} created:{page.start.isoformat()}..{page.end.isoformat()}"
        try:
            result = self.client.get_json(self.client.endpoints.search(q, page.page, PER_PAGE),
                                          max_bytes=MAX_SEARCH_BYTES, resource="search")
        except FetchError as exc:
            self._fail(ctx, exc, "search")
        body = result[0] if result else None
        if not isinstance(body, dict) or not isinstance(body.get("items"), list) \
                or isinstance(body.get("total_count"), bool) or not isinstance(body.get("total_count"), int):
            raise JobFailure(ErrorClass.TRANSIENT, "unexpected search response", code="bad_search_response")
        metrics: Counter = Counter(items=len(body["items"]))
        if body.get("incomplete_results") is True:
            metrics["incomplete_results"] += 1
        run_id = self.run_id or ctx.job.run_id
        if page.page == 1:
            split, follow_ups = self._follow_ups(page, body["total_count"], run_id, metrics)
            if split:  # the window was split: its halves carry the work
                with ctx.transaction() as conn:
                    for spec in follow_ups:
                        self.store.enqueue(conn, spec)
                    record_rates(conn, self.store, self.client, self.fp)
                return Completed(metrics=dict(metrics), stage=Stage.ACQUIRED)
        else:
            follow_ups = []

        items = [i for i in body["items"] if not (isinstance(i, dict) and i.get("id") in page.done)]
        for start in range(0, max(len(items), 1), self.batch_size):
            self._batch(ctx, page, items[start:start + self.batch_size], follow_ups if start == 0 else [],
                        run_id, metrics)
        return Completed(metrics=dict(sorted(metrics.items())), stage=Stage.ACQUIRED)

    def _follow_ups(self, page: _Page, total: int, run_id: uuid.UUID | None,
                    metrics: Counter) -> tuple[bool, list[JobSpec]]:
        """(True, two half windows) if the window exceeds the search cap, else (False, pages 2..n)."""
        if total > SEARCH_CAP and page.end > page.start:
            middle = page.start + (page.end - page.start) // 2
            metrics["window_split"] += 1
            return True, [discovery_spec(page.policy, page.epoch, page.start, middle, 1, run_id),
                          discovery_spec(page.policy, page.epoch, middle + timedelta(days=1), page.end, 1, run_id)]
        if total > SEARCH_CAP:
            metrics["window_saturated"] += 1  # one day with > 1000 qualifying repositories: first 1000 only
        pages = min(math.ceil(min(total, SEARCH_CAP) / PER_PAGE), SEARCH_CAP // PER_PAGE)
        return False, [discovery_spec(page.policy, page.epoch, page.start, page.end, n, run_id)
                       for n in range(2, pages + 1)]

    def _batch(self, ctx: JobContext, page: _Page, items: list[Any], follow_ups: list[JobSpec],
               run_id: uuid.UUID | None, metrics: Counter) -> None:
        qualified: list[RepositoryRow] = []
        done_ids: list[int] = []
        for item in items:
            row, reason = qualify_repository(item, page.policy)
            if row is None:
                metrics[f"rejected_{reason}"] += 1
                if isinstance(item, dict) and isinstance(item.get("id"), int):
                    done_ids.append(item["id"])
            else:
                qualified.append(row)
        states: dict[int, tuple[str | None, str | None]] = {}
        with ctx.transaction() as conn:
            for row in qualified:
                self.store.upsert_repository(conn, row)
                states[row.repository_id] = self.store.repository_state(conn, row.repository_id) or (None, None)
        outcomes: list[tuple[RepositoryRow, str, str | None, str | None]] = []  # row, outcome, sha, etag
        failure: FetchError | None = None
        for row in qualified:
            etag, last_sha = states[row.repository_id]
            try:
                response = self.client.get(self.client.endpoints.head(row.owner_login, row.repo_name,
                                                                      row.default_branch),
                                           accept=ACCEPT_SHA, max_bytes=256, etag=etag)
            except FetchError as exc:
                if is_item_error(exc):  # 404/409 (empty)/422/451: nothing to snapshot
                    outcomes.append((row, f"head_unavailable_{exc.code}", None, None))
                    continue
                failure = exc
                break
            if response.not_modified:
                outcomes.append((row, "unchanged", None, None))
                continue
            sha = response.body.decode("ascii", errors="replace").strip()
            if not _SHA.match(sha):
                outcomes.append((row, "head_invalid", None, None))
            elif sha == last_sha:
                outcomes.append((row, "unchanged", None, response.etag))
            else:
                outcomes.append((row, "snapshot", sha, response.etag))
        with ctx.transaction() as conn:
            for row, outcome, sha, new_etag in outcomes:
                if outcome == "snapshot":
                    created = self.store.enqueue(conn, snapshot_spec(row, sha, states[row.repository_id][1], run_id))
                    metrics["snapshots_enqueued" if created else "snapshot_exists"] += 1
                else:
                    metrics[outcome] += 1
                if new_etag:
                    self.store.set_etag(conn, row.repository_id, new_etag)
                done_ids.append(row.repository_id)
            for spec in follow_ups:
                metrics["pages_enqueued" if self.store.enqueue(conn, spec) else "pages_existing"] += 1
            page.done.update(done_ids)
            metrics["qualified"] += len(outcomes)
            record_rates(conn, self.store, self.client, self.fp, failure)
            checkpoint = dict(ctx.checkpoint, done=sorted(page.done))
            ctx.checkpoint_progress(checkpoint, stage=Stage.ACQUISITION_PENDING, conn=conn)
        if failure is not None:
            raise to_job_failure(failure, "branch HEAD")

    def _fail(self, ctx: JobContext, exc: FetchError, what: str) -> None:
        with ctx.transaction() as conn:
            record_rates(conn, self.store, self.client, self.fp, exc)
        raise to_job_failure(exc, what)

"""GitHub discovery + snapshot handlers through the Runtime Engine worker (offline).

Real: Worker, retry controller, handlers, GitHubClient, qualification, Bronze
(temp dir). Fakes: FakeQueue, FakeGitHub transport, InMemoryGitHubStore.
"""

import dataclasses
import json
import random
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

from app.config.settings import WorkerSettings
from app.queue.retry import RetryPolicy
from app.queue.states import JobStatus, JobType
from app.sources.github.bronze import GitHubBronze
from app.sources.github.client import GitHubClient, GitHubEndpoints
from app.sources.github.config import DiscoveryConfig, GitHubAcquisitionConfig, GitHubCredentials, GitHubHttpConfig
from app.sources.github.connector import register_github_handlers
from app.sources.github.config import GitHubSettings
from app.sources.github.discovery import plan_discovery
from app.utils.ids import github_observation_id
from app.workers.registry import HandlerRegistry
from app.workers.runtime import Worker

from tests.unit.gh_fixtures import SUBMODULE, SYMLINK, FakeGitHub, InMemoryGitHubStore
from tests.unit.test_runtime_worker import FakeQueue

POLICY = RetryPolicy(max_attempts=6, base_delay_seconds=1.0, max_delay_seconds=4.0, rate_limit_jitter_seconds=0.0)
TODAY = date(2024, 5, 31)
FILES = {
    "README.md": "# Project\nFacts about the project.\n",
    "docs/guide.rst": "Guide\n=====\nText.\n",
    "src/app.py": "print('hello')\n",
    "logo.png": b"\x89PNG\r\n\x1a\n",
    "node_modules/lib/index.js": "module.exports = 1;\n",
    ".env": "SECRET=1\n",
    "package-lock.json": "{}\n",
    "big.txt": "x" * 5000,
    "link.md": SYMLINK,
    "vendored": SUBMODULE,
    "notes.txt": b"looks like text\x00but is binary",
}
ACQUIRED = {"README.md", "docs/guide.rst", "src/app.py"}


class Crash(BaseException):
    pass


class Env:
    def __init__(self, test: unittest.TestCase, *, concurrency: int = 2, token: str | None = None) -> None:
        tmp = tempfile.TemporaryDirectory()
        test.addCleanup(tmp.cleanup)
        self.gh = FakeGitHub()
        self.head = self.gh.add_repo(101, "alice", "facts", FILES)
        self.gh.add_repo(102, "bob", "forked", {"README.md": "fork"}, fork=True)
        self.queue = FakeQueue()
        self.store = InMemoryGitHubStore(self.queue, clock=lambda: self.queue.now)
        files = dataclasses.replace(GitHubAcquisitionConfig().files, max_file_bytes=4096)
        self.config = GitHubAcquisitionConfig(bronze_root=Path(tmp.name) / "gh", fetch_concurrency=concurrency,
                                              batch_size=2, files=files)
        self.settings = GitHubSettings(discovery=DiscoveryConfig(created_from=date(2024, 5, 1), window_days=31,
                                                                 epoch="e1"),
                                       acquisition=self.config, credentials=GitHubCredentials(token=token))
        self.client = GitHubClient(GitHubHttpConfig(max_connections=concurrency), self.settings.credentials,
                                   transport=self.gh, wall_clock=lambda: self.queue.now)
        self.bronze = GitHubBronze(self.config.bronze_root)
        registry = HandlerRegistry()
        register_github_handlers(registry, self.settings, client=self.client, store=self.store, bronze=self.bronze)
        for reg in list(registry._registrations.values()):  # fast test retry timing
            registry._registrations[reg.job_type] = dataclasses.replace(reg, policy=POLICY)
        self.worker = Worker(self.queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0),
                             worker_id="w1", rng=random.Random(0))

    def drain(self, rounds: int = 80) -> None:
        for _ in range(rounds):
            if self.worker.run_once():
                continue
            waiting = [r for r in self.queue.jobs.values() if r["status"] in (JobStatus.PENDING, JobStatus.RETRY_PENDING)]
            if not waiting:
                return
            self.queue.now = max(self.queue.now, min(r["next_attempt_at"] for r in waiting)) + timedelta(seconds=1)
        raise AssertionError("queue did not settle")

    def jobs(self, job_type: JobType) -> list[dict]:
        return [j for j in self.queue.jobs.values() if j["job_type"] == job_type]

    def paths(self) -> set[str]:
        return {f.file_path for f in self.store.files.values()}


def _enqueue_many(store):
    class Q:
        def enqueue_many(self, specs):
            from app.queue.job_queue import EnqueueResult
            return [EnqueueResult(0, store.enqueue(None, s), "pending") for s in specs]
    return Q()


def plan(env: Env, epoch: str = "e1") -> None:
    plan_discovery(_enqueue_many(env.store), dataclasses.replace(env.settings.discovery, epoch=epoch), today=TODAY)


class EndToEndTests(unittest.TestCase):
    def test_discovery_snapshot_provenance_and_bronze(self) -> None:
        env = Env(self)
        plan(env)
        env.drain()
        (discovery,) = env.jobs(JobType.GH_REPOSITORY_DISCOVERY)
        self.assertEqual(discovery["status"], JobStatus.SUCCEEDED)
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        self.assertEqual((snapshot["source_identifier"], snapshot["status"]), (f"101@{env.head}", JobStatus.SUCCEEDED))
        self.assertEqual(set(env.store.repos), {101})  # the fork is rejected by policy
        repo = env.store.repos[101]
        self.assertEqual((repo["owner_login"], repo["visibility"], repo["license_spdx_id"], repo["last_snapshot"]),
                         ("alice", "public", "MIT", env.head))
        commit = env.store.commits[(101, env.head)]
        self.assertEqual((commit.snapshot_mode, commit.previous_snapshot_sha, commit.ref_name), ("full", None, "main"))
        self.assertEqual(env.paths(), ACQUIRED)
        self.assertEqual(env.gh.count("/git/blobs/"), 4)  # 3 text files + notes.txt (binary, rejected after download)

        row = next(f for f in env.store.files.values() if f.file_path == "docs/guide.rst")
        self.assertEqual(row.observation_id, github_observation_id(101, env.head, "docs/guide.rst", row.blob_sha))
        self.assertEqual(row.source_url, f"https://github.com/alice/facts/blob/{env.head}/docs/guide.rst")
        self.assertEqual(env.bronze.read_blob(row.blob_sha), FILES["docs/guide.rst"].encode())
        manifest = env.bronze.read_json(env.bronze.observation_path(101, env.head, row.observation_id))
        self.assertEqual((manifest["source_type"], manifest["repository_id"], manifest["owner_login"],
                          manifest["commit_sha"], manifest["blob_sha"], manifest["path"]),
                         ("github", 101, "alice", env.head, row.blob_sha, "docs/guide.rst"))
        snapshot_manifest = env.bronze.read_json(env.bronze.snapshot_path(101, env.head))
        self.assertEqual(snapshot_manifest["repository"]["stargazers_count"], 100)
        self.assertEqual(snapshot_manifest["commit"]["committer_date"], "2025-01-02T03:04:05Z")
        self.assertNotIn("published_at", json.dumps(snapshot_manifest))
        metrics = next(a for a in env.queue.attempts if a["job_id"] == snapshot["job_id"]
                       and a["outcome"] == "succeeded")["metrics"]
        for reason in ("skipped_extension", "skipped_excluded_dir", "skipped_secret_file", "skipped_generated",
                       "skipped_too_large", "skipped_symlink", "skipped_submodule", "rejected_binary"):
            self.assertEqual(metrics.get(reason), 1, reason)

    def test_rerun_is_idempotent_and_unchanged_repositories_cost_nothing(self) -> None:
        env = Env(self)
        plan(env)
        env.drain()
        blobs, files = env.gh.count("/git/blobs/"), dict(env.store.files)
        plan(env, epoch="e2")  # next discovery generation
        env.drain()
        self.assertEqual(len(env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)), 1)  # HEAD 304 -> no new snapshot
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        snapshot.update(status=JobStatus.PENDING, attempt_count=0, checkpoint=dict(snapshot["checkpoint"], next=0))
        env.drain()  # re-run the same repo@commit
        self.assertEqual(env.gh.count("/git/blobs/"), blobs)
        self.assertEqual(env.store.files, files)

    def test_incremental_commit_observes_only_changes_and_records_deletions(self) -> None:
        env = Env(self)
        plan(env)
        env.drain()
        old = set(env.store.files)
        files = {k: v for k, v in FILES.items() if k != "docs/guide.rst"}
        files.update({"README.md": "# Project\nUpdated facts.\n", "docs/new.md": "new page\n"})
        new_head = env.gh.commit(101, files)
        plan(env, epoch="e2")
        blobs = env.gh.count("/git/blobs/")
        env.drain()
        self.assertEqual(env.store.repos[101]["last_snapshot"], new_head)
        commit = env.store.commits[(101, new_head)]
        self.assertEqual((commit.snapshot_mode, commit.previous_snapshot_sha), ("incremental", env.head))
        new = {f.file_path for oid, f in env.store.files.items() if oid not in old}
        self.assertEqual(new, {"README.md", "docs/new.md"})  # src/app.py unchanged: not re-observed
        self.assertEqual(env.gh.count("/git/blobs/") - blobs, 2)
        self.assertTrue(old <= set(env.store.files))  # prior provenance kept
        changes = env.bronze.read_json(env.bronze.snapshot_path(101, new_head))["changes"]
        self.assertEqual((changes["deleted"], changes["added"], changes["modified"]), (["docs/guide.rst"], 1, 1))

    def test_rename_or_transfer_keeps_the_numeric_identity(self) -> None:
        env = Env(self)
        plan(env)
        env.drain()
        env.gh.rename(101, "carol", "facts-renamed")
        env.gh.commit(101, dict(FILES, **{"CHANGELOG.md": "v2\n"}))
        plan(env, epoch="e2")
        env.drain()
        self.assertEqual(set(env.store.repos), {101})
        self.assertEqual((env.store.repos[101]["owner_login"], env.store.repos[101]["repo_name"]),
                         ("carol", "facts-renamed"))
        self.assertTrue(any(f.source_url.startswith("https://github.com/carol/facts-renamed/")
                            for f in env.store.files.values()))


class FailureTests(unittest.TestCase):
    def test_primary_rate_limit_defers_without_consuming_and_is_shared(self) -> None:
        env = Env(self)
        env.gh.faults["/search/"].append({"status": 403, "remaining": 0, "body": b'{"message":"API rate limit"}'})
        plan(env)
        env.worker.run_once()
        (job,) = env.jobs(JobType.GH_REPOSITORY_DISCOVERY)
        self.assertEqual((job["status"], job["attempt_count"], job["deferral_count"]), (JobStatus.RETRY_PENDING, 0, 1))
        self.assertGreater(job["next_attempt_at"], env.queue.now)
        self.assertEqual([scope for scope, _fp in env.store.rate_limits], ["search"])
        requests = len(env.gh.requests)
        job["next_attempt_at"] = env.queue.now  # claimed early: the shared state defers it before any request
        env.worker.run_once()
        self.assertEqual(len(env.gh.requests), requests)
        env.queue.now = max(env.store.rate_limits.values()) + timedelta(seconds=1)
        env.drain()
        self.assertEqual(env.paths(), ACQUIRED)

    def test_secondary_limit_on_a_blob_commits_the_prefix_and_defers(self) -> None:
        env = Env(self, concurrency=1)
        env.gh.faults["/repos/alice/facts/git/blobs/"] = [{}, {"status": 429, "headers": {"Retry-After": "90"}}]
        plan(env)
        env.worker.run_once()  # discovery
        env.worker.run_once()  # snapshot: first file ok, second rate limited
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        self.assertEqual((snapshot["attempt_count"], snapshot["deferral_count"]), (0, 1))
        self.assertEqual(snapshot["checkpoint"]["next"], 1)  # the first file was committed
        self.assertIn("core", [resource for resource, _rem, retry_after in env.store.rate_events if retry_after == 90])
        env.drain()
        self.assertEqual(env.paths(), ACQUIRED)

    def test_5xx_and_timeout_retry_from_the_checkpoint(self) -> None:
        for fault in ({"status": 502}, {"timeout": True}):
            with self.subTest(fault=fault):
                env = Env(self, concurrency=1)
                env.gh.faults["/repos/alice/facts/git/blobs/"] = [{}, fault]
                plan(env)
                env.drain()
                (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
                self.assertEqual((snapshot["status"], snapshot["attempt_count"]), (JobStatus.SUCCEEDED, 2))
                self.assertEqual(env.paths(), ACQUIRED)
                self.assertEqual(len(env.store.files), 3)

    def test_permanent_404_and_integrity_failures_are_item_dead_letters(self) -> None:
        env = Env(self)
        readme_sha = next(e["sha"] for t in env.gh.trees.values() for e in t if e["path"] == "README.md")
        guide_sha = next(e["sha"] for t in env.gh.trees.values() for e in t if e["path"] == "docs/guide.rst")
        env.gh.persistent[f"/repos/alice/facts/git/blobs/{readme_sha}"] = {"status": 404}
        env.gh.persistent[f"/repos/alice/facts/git/blobs/{guide_sha}"] = {"body": b"Guide\n=====\nTEXT.\n"}  # same size
        plan(env)
        env.drain()
        self.assertEqual(env.paths(), {"src/app.py"})
        self.assertEqual(sorted(env.store.codes().values()), ["blob_hash_mismatch", "http_404"])
        self.assertIsNone(env.bronze.read_blob(guide_sha))  # conflicting content is never stored
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        self.assertEqual(snapshot["status"], JobStatus.SUCCEEDED)

    def test_binary_rejection_is_cached(self) -> None:
        env = Env(self)
        plan(env)
        env.drain()
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        snapshot.update(status=JobStatus.PENDING, attempt_count=0, checkpoint=dict(snapshot["checkpoint"], next=0))
        before = env.gh.count("/git/blobs/")
        env.drain()
        self.assertEqual(env.gh.count("/git/blobs/"), before)  # notes.txt not downloaded again

    def test_crash_recovery_reuses_bronze_and_never_duplicates(self) -> None:
        env = Env(self, concurrency=1)
        env.gh.faults["/repos/alice/facts/git/blobs/"] = [{}, {"raise": Crash()}]
        plan(env)
        env.worker.run_once()  # discovery
        with self.assertRaises(Crash):
            env.worker.run_once()
        env.queue.advance(61)
        env.worker.recover_expired_leases()
        env.drain()
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        self.assertEqual(snapshot["status"], JobStatus.SUCCEEDED)
        self.assertEqual(len(env.store.files), 3)
        self.assertEqual(env.gh.count("/git/blobs/"), 5)  # 4 blobs + the crashed request; none fetched twice

    def test_repository_no_longer_public_dead_letters_the_snapshot(self) -> None:
        env = Env(self)
        plan(env)
        env.worker.run_once()  # discovery enqueues the snapshot
        env.gh.repos[101]["meta"].update(private=True, visibility="private")
        env.drain()
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        self.assertEqual(snapshot["status"], JobStatus.DEAD_LETTER)
        self.assertEqual(env.queue.dead_letters[-1]["error_class"], "permanent_source")
        self.assertEqual(env.store.commits, {})

    def test_truncated_tree_is_not_guessed_at(self) -> None:
        env = Env(self)
        env.gh.truncate_trees = True
        plan(env)
        env.drain()
        (snapshot,) = env.jobs(JobType.GH_REPOSITORY_SNAPSHOT)
        self.assertEqual(snapshot["status"], JobStatus.DEAD_LETTER)
        self.assertEqual(env.store.files, {})

    def test_oversized_window_is_split(self) -> None:
        env = Env(self)
        env.gh.search_total_override = 1500
        plan(env)
        env.worker.run_once()
        ids = sorted(j["source_identifier"] for j in env.jobs(JobType.GH_REPOSITORY_DISCOVERY))
        self.assertEqual(len(ids), 3)
        self.assertTrue(any(".20240501-20240516#page=1" in i for i in ids))
        self.assertTrue(any(".20240517-20240531#page=1" in i for i in ids))

    def test_bounded_concurrency_and_token_never_leaks(self) -> None:
        token = "ghp_" + "S3cr3t" * 6
        env = Env(self, concurrency=2, token=token)
        seen = []
        env.gh.on_request = lambda _p: seen.append(threading.active_count())
        for path in ("/repos/alice/facts/git/blobs/",):
            env.gh.faults[path] = [{"status": 502}] + [{"delay": 0.05}] * 4  # the retry is logged
        plan(env)
        with self.assertLogs("app.workers.runtime", level="INFO") as logs:
            env.drain()
        self.assertLessEqual(env.gh.max_active, 2)
        self.assertTrue(all(h.get("Authorization") == f"Bearer {token}" for _p, h in env.gh.requests))
        self.assertNotIn(token, "\n".join(logs.output) + repr(env.client) + repr(env.settings))
        self.assertEqual(env.paths(), ACQUIRED)


if __name__ == "__main__":
    unittest.main()

"""Offline fixtures for GitHub Acquisition V1 tests (no network, no database).

* ``FakeGitHub``: an in-memory GitHub REST API (search, repositories/{id},
  commits/{ref} with ETag/304, git commits/trees/blobs) with real git blob
  hashes, rate-limit headers, fault injection and concurrency accounting. It is
  also a ``Transport`` (``open``).
* ``InMemoryGitHubStore``: a ``GitHubStore`` with the schema's keys, enqueueing
  into the runtime ``FakeQueue``.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import socket
import threading
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl

from app.queue.states import JobType
from app.sources.github.files import git_blob_hash
from app.sources.github.rate_limit import RateState

from tests.unit.cc_fixtures import FakeResponse

SYMLINK = "symlink"
SUBMODULE = "submodule"


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()


class FakeGitHub:
    def __init__(self) -> None:
        self.repos: dict[int, dict[str, Any]] = {}
        self.commits: dict[str, dict[str, Any]] = {}
        self.trees: dict[str, list[dict[str, Any]]] = {}
        self.blobs: dict[str, bytes] = {}
        self.requests: list[tuple[str, dict[str, str]]] = []
        self.faults: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.persistent: dict[str, dict[str, Any]] = {}
        self.remaining = 5000
        self.search_total_override: int | None = None
        self.truncate_trees = False
        self.lock = threading.Lock()
        self.active = self.max_active = 0
        self.on_request = None
        self._n = 0

    # -- building ---------------------------------------------------------------- #

    def add_repo(self, repo_id: int, owner: str, name: str, files: dict[str, Any], *, stars: int = 100,
                 created: date = date(2024, 5, 10), **meta: Any) -> str:
        self.repos[repo_id] = {"meta": {
            "id": repo_id, "node_id": f"R_{repo_id}", "name": name, "full_name": f"{owner}/{name}",
            "owner": {"login": owner}, "private": False, "visibility": "public",
            "html_url": f"https://github.com/{owner}/{name}", "fork": False, "archived": False, "disabled": False,
            "size": 120, "stargazers_count": stars, "language": "Python", "default_branch": "main",
            "license": {"spdx_id": "MIT"}, "created_at": f"{created.isoformat()}T00:00:00Z",
            "pushed_at": "2025-01-01T00:00:00Z", **meta}, "head": None}
        return self.commit(repo_id, files)

    def commit(self, repo_id: int, files: dict[str, Any]) -> str:
        entries = []
        for path, value in sorted(files.items()):
            if value == SUBMODULE:
                entries.append({"path": path, "mode": "160000", "type": "commit", "sha": _sha1(path)})
                continue
            content = value.encode() if isinstance(value, str) and value != SYMLINK else value
            mode = "100644"
            if value == SYMLINK:
                content, mode = b"../../etc/passwd", "120000"
            sha = git_blob_hash(content, 40)
            self.blobs[sha] = content
            entries.append({"path": path, "mode": mode, "type": "blob", "sha": sha, "size": len(content)})
        for directory in sorted({p.rsplit("/", 1)[0] for p in files if "/" in p}):
            entries.append({"path": directory, "mode": "040000", "type": "tree", "sha": _sha1("t" + directory)})
        self._n += 1
        tree_sha = _sha1(json.dumps(entries, sort_keys=True))
        self.trees[tree_sha] = entries
        parent = self.repos[repo_id]["head"]
        sha = _sha1(f"{repo_id}:{tree_sha}:{parent}:{self._n}")
        self.commits[sha] = {"sha": sha, "tree": {"sha": tree_sha}, "parents": [{"sha": parent}] if parent else [],
                             "committer": {"date": "2025-01-02T03:04:05Z"}, "author": {"date": "2025-01-01T00:00:00Z"}}
        self.repos[repo_id]["head"] = sha
        return sha

    def rename(self, repo_id: int, owner: str, name: str) -> None:
        meta = self.repos[repo_id]["meta"]
        meta.update(name=name, full_name=f"{owner}/{name}", owner={"login": owner},
                    html_url=f"https://github.com/{owner}/{name}")

    def count(self, part: str) -> int:
        return sum(1 for path, _h in self.requests if part in path)

    # -- transport ------------------------------------------------------------------ #

    def open(self, url, headers, *, connect_timeout, read_timeout):
        path, query = url.path, dict(parse_qsl(url.query))
        with self.lock:
            self.requests.append((path, dict(headers)))
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            key = next((k for k in self.faults if path.startswith(k) and self.faults[k]), None)
            fault = self.faults[key].pop(0) if key else next(
                (v for k, v in self.persistent.items() if path.startswith(k)), {})
            self.remaining = max(0, self.remaining - 1)
            remaining = self.remaining
        try:
            if self.on_request is not None:
                self.on_request(path)
            if fault.get("delay"):
                time.sleep(fault["delay"])
            if fault.get("raise") is not None:
                raise fault["raise"]
            if fault.get("timeout"):
                raise socket.timeout("timed out")
            resource = "search" if path.startswith("/search/") else "core"
            rate = {"x-ratelimit-limit": "5000", "x-ratelimit-remaining": str(fault.get("remaining", remaining)),
                    "x-ratelimit-reset": str(int(time.time()) + 3600), "x-ratelimit-resource": resource}
            if "status" in fault:
                return FakeResponse(fault["status"], {**rate, **fault.get("headers", {})}, fault.get("body", b"{}"))
            status, extra, body = self._route(path, query, headers)
            if fault.get("body") is not None:
                body = fault["body"]
            return FakeResponse(status, {**rate, **extra}, body)
        finally:
            with self.lock:
                self.active -= 1

    def _route(self, path: str, query: dict[str, str], headers: dict[str, str]) -> tuple[int, dict, bytes]:
        parts = path.strip("/").split("/")
        if path == "/search/repositories":
            created = next(t for t in query["q"].split() if t.startswith("created:"))[len("created:"):]
            lo, hi = (date.fromisoformat(d) for d in created.split(".."))
            items = sorted((r["meta"] for r in self.repos.values()
                            if lo <= date.fromisoformat(r["meta"]["created_at"][:10]) <= hi), key=lambda m: m["id"])
            page, per_page = int(query["page"]), int(query["per_page"])
            total = self.search_total_override if self.search_total_override is not None else len(items)
            return 200, {}, json.dumps({"total_count": total, "incomplete_results": False,
                                        "items": items[(page - 1) * per_page: page * per_page]}).encode()
        if parts[0] == "repositories":
            repo = self.repos.get(int(parts[1]))
            return (200, {}, json.dumps(repo["meta"]).encode()) if repo else (404, {}, b"{}")
        owner, name = parts[1], parts[2]
        repo = next((r for r in self.repos.values() if r["meta"]["full_name"] == f"{owner}/{name}"), None)
        if repo is None:
            return 404, {}, b"{}"
        if parts[3] == "commits":
            etag = f'"{repo["head"]}"'
            if headers.get("If-None-Match") == etag:
                return 304, {"ETag": etag}, b""
            return 200, {"ETag": etag}, repo["head"].encode()
        kind, sha = parts[4], parts[5]
        if kind == "commits":
            return (200, {}, json.dumps(self.commits[sha]).encode()) if sha in self.commits else (404, {}, b"{}")
        if kind == "trees":
            return 200, {}, json.dumps({"sha": sha, "tree": self.trees[sha], "truncated": self.truncate_trees}).encode()
        if kind == "blobs":
            return (200, {}, self.blobs[sha]) if sha in self.blobs else (404, {}, b"{}")
        return 404, {}, b"{}"


class InMemoryGitHubStore:
    def __init__(self, queue: Any, clock=None) -> None:
        self.queue = queue
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.repos: dict[int, dict[str, Any]] = {}
        self.commits: dict[tuple[int, str], Any] = {}
        self.files: dict[uuid.UUID, Any] = {}
        self.natural: set[tuple[int, str, str]] = set()
        self.dead_letters: dict[tuple[str, str], dict[str, Any]] = {}
        self.rate_limits: dict[tuple[str, str], datetime] = {}
        self.rate_events: list[tuple[str, int | None, int | None]] = []

    def transaction(self):
        return contextlib.nullcontext(self)

    def upsert_repository(self, conn, row) -> None:
        state = self.repos.setdefault(row.repository_id, {"api_etag": None, "last_snapshot": None})
        state.update(vars(row))

    def repository_state(self, conn, repository_id):
        state = self.repos.get(repository_id)
        return None if state is None else (state["api_etag"], state["last_snapshot"])

    def set_etag(self, conn, repository_id, etag) -> None:
        self.repos[repository_id]["api_etag"] = etag

    def advance_snapshot(self, conn, repository_id, commit_sha, previous_sha) -> bool:
        state = self.repos[repository_id]
        if state["last_snapshot"] != previous_sha:
            return False
        state["last_snapshot"] = commit_sha
        return True

    def insert_commit(self, conn, row) -> bool:
        assert row.repository_id in self.repos
        return self.commits.setdefault((row.repository_id, row.commit_sha), row) is row

    def seen_files(self, conn, repository_id, paths):
        paths = set(paths)
        return {(f.file_path, f.blob_sha) for f in self.files.values()
                if f.repository_id == repository_id and f.file_path in paths}

    def persist_file(self, conn, row) -> bool:
        assert (row.repository_id, row.commit_sha) in self.commits
        if row.observation_id in self.files:
            return False
        key = (row.repository_id, row.commit_sha, row.file_path)
        assert key not in self.natural, "natural key collision"
        self.natural.add(key)
        self.files[row.observation_id] = row
        return True

    def enqueue(self, conn, spec) -> bool:
        if any(j["source_identifier"] == spec.source_identifier and j["job_type"] == spec.job_type
               for j in self.queue.jobs.values()):
            return False
        self.queue.add(spec.source_identifier, checkpoint=spec.checkpoint, job_type=JobType(spec.job_type),
                       max_attempts=spec.max_attempts)
        return True

    def record_item_dead_letter(self, conn, *, item_key, stage, error_class, error_code, error_message,
                                attempt_count, job_id, run_id, payload_ref) -> None:
        entry = self.dead_letters.setdefault((item_key, str(stage)), {"occurrence_count": 0})
        entry.update(error_class=str(error_class), error_code=error_code, payload_ref=payload_ref, job_id=job_id)
        entry["occurrence_count"] += 1

    def blocked_until(self, conn, scope, fingerprint):
        until = self.rate_limits.get((scope, fingerprint))
        return until if until is not None and until > self.clock() else None

    def record_rate_limit(self, conn, state: RateState, fingerprint, *, retry_after_seconds=None) -> None:
        self.rate_events.append((state.resource, state.remaining, retry_after_seconds))
        candidates = []
        if state.remaining == 0 and state.reset_at is not None:
            candidates.append(state.reset_at)
        if retry_after_seconds is not None:
            candidates.append(self.clock() + timedelta(seconds=retry_after_seconds))
        if candidates:
            self.rate_limits[(state.resource, fingerprint)] = max(candidates)

    def codes(self) -> dict[str, str]:
        return {key: entry["error_code"] for (key, _stage), entry in self.dead_letters.items()}

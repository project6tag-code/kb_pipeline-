"""Offline end-to-end GitHub acquisition over a LOOPBACK HTTP server (runs by default).

A local ``http.server`` serves the synthetic GitHub API of tests.unit.gh_fixtures
with real HTTP; the production path (stdlib transport, client, discovery and
snapshot handlers, Runtime Engine worker, Bronze) runs unchanged except for the
loopback endpoint and the in-memory store. No external network, no database.
"""

import dataclasses
import http.server
import random
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from app.config.settings import WorkerSettings
from app.queue.job_queue import EnqueueResult
from app.queue.states import JobStatus
from app.sources.commoncrawl.http import StdlibTransport
from app.sources.github.bronze import GitHubBronze
from app.sources.github.client import GitHubClient, GitHubEndpoints
from app.sources.github.config import DiscoveryConfig, GitHubAcquisitionConfig, GitHubSettings
from app.sources.github.connector import register_github_handlers
from app.sources.github.discovery import plan_discovery
from app.workers.registry import HandlerRegistry
from app.workers.runtime import Worker

from tests.unit.gh_fixtures import FakeGitHub, InMemoryGitHubStore
from tests.unit.test_runtime_worker import FakeQueue


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        parts = urlsplit(self.path)
        gh: FakeGitHub = self.server.gh
        gh.requests.append((parts.path, dict(self.headers)))
        status, headers, body = gh._route(parts.path, dict(parse_qsl(parts.query)), dict(self.headers))
        self.send_response(status)
        for key, value in {"x-ratelimit-limit": "60", "x-ratelimit-remaining": "59", "x-ratelimit-reset": "0",
                           **headers}.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class GitHubLoopbackTests(unittest.TestCase):
    def test_discover_snapshot_rerun(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        gh = FakeGitHub()
        head = gh.add_repo(501, "octo", "docs", {"README.md": "# Docs\n", "guide/intro.md": "Intro\n",
                                                 "src/main.go": "package main\n", "img/a.png": b"\x89PNG"})
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        server.daemon_threads, server.gh = True, gh
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        queue = FakeQueue()
        store = InMemoryGitHubStore(queue, clock=lambda: queue.now)
        settings = GitHubSettings(discovery=DiscoveryConfig(created_from=date(2024, 5, 1), window_days=31, epoch="e1"),
                                  acquisition=GitHubAcquisitionConfig(bronze_root=Path(tmp.name) / "gh"))
        client = GitHubClient(settings.http, transport=StdlibTransport(2), wall_clock=lambda: queue.now,
                              endpoints=GitHubEndpoints.loopback_for_tests(f"http://127.0.0.1:{server.server_address[1]}"))
        self.addCleanup(client.close)
        registry = HandlerRegistry()
        register_github_handlers(registry, settings, client=client, store=store,
                                 bronze=GitHubBronze(settings.acquisition.bronze_root))
        worker = Worker(queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0),
                        worker_id="e2e", rng=random.Random(0))

        class Planner:
            def enqueue_many(self, specs):
                return [EnqueueResult(0, store.enqueue(None, s), "pending") for s in specs]

        plan_discovery(Planner(), settings.discovery, today=date(2024, 5, 31))
        worker.run(max_iterations=4)
        self.assertTrue(all(j["status"] == JobStatus.SUCCEEDED for j in queue.jobs.values()))
        self.assertEqual({f.file_path for f in store.files.values()}, {"README.md", "guide/intro.md", "src/main.go"})
        self.assertEqual(store.repos[501]["last_snapshot"], head)
        blob_requests = gh.count("/git/blobs/")
        self.assertEqual(blob_requests, 3)

        # New discovery generation: HEAD answers 304 (ETag) -> nothing new is fetched or stored.
        plan_discovery(Planner(), dataclasses.replace(settings.discovery, epoch="e2"), today=date(2024, 5, 31))
        queue.now += timedelta(seconds=1)
        worker.run(max_iterations=3)
        self.assertEqual(gh.count("/git/blobs/"), blob_requests)
        self.assertEqual(len(store.files), 3)
        self.assertEqual(len([j for j in queue.jobs.values() if j["source_identifier"].startswith("501@")]), 1)


if __name__ == "__main__":
    unittest.main()

"""Offline end-to-end Common Crawl acquisition over a LOOPBACK HTTP server (runs by default).

No external network and no database: a local ``http.server`` on 127.0.0.1
serves a synthetic Common Crawl (collinfo.json, cluster.idx, a zipnum CDX shard
and a WARC file) with real HTTP Range semantics. The production code path is
used unchanged (stdlib transport, planner, Runtime Engine worker, handler,
Bronze store), except that the endpoint allow-list is the loopback test origin
and persistence is the in-memory CaptureStore.
"""

import contextlib
import http.server
import random
import tempfile
import threading
import unittest
from pathlib import Path

from app.config.settings import WorkerSettings
from app.queue.job_queue import EnqueueResult
from app.queue.states import JobStatus, JobType
from app.sources.commoncrawl.acquisition import CC_PARTITION_POLICY, CCPartitionHandler
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.commoncrawl.config import AcquisitionConfig, FilterConfig, HttpConfig, PlanningConfig
from app.sources.commoncrawl.discovery import CrawlPlanner
from app.sources.commoncrawl.http import Endpoints, HttpClient, StdlibTransport
from app.workers.registry import HandlerRegistry
from app.workers.runtime import Worker

from tests.unit.cc_fixtures import CRAWL, FakeCommonCrawl, InMemoryCaptureStore, html_page, make_cdx_line, make_warc_record
from tests.unit.test_runtime_worker import FakeQueue


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        data = self.server.site.objects.get(self.path)
        self.server.requests.append((self.path, self.headers.get("Range")))
        if data is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        rng = self.headers.get("Range")
        if rng:
            start, end = (int(x) for x in rng[len("bytes="):].split("-"))
            body = data[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        else:
            body = data
            self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _QueueWithEnqueue(FakeQueue):
    """FakeQueue plus the planner's idempotent enqueue_many (UNIQUE job identity)."""

    def enqueue_many(self, specs):
        results = []
        for spec in specs:
            existing = next((j for j in self.jobs.values() if j["source_identifier"] == spec.source_identifier), None)
            if existing:
                results.append(EnqueueResult(existing["job_id"], False, existing["status"]))
                continue
            job_id = self.add(spec.source_identifier, checkpoint=spec.checkpoint, job_type=spec.job_type,
                              max_attempts=spec.max_attempts)
            results.append(EnqueueResult(job_id, True, "pending"))
        return results

    def transaction(self):
        return contextlib.nullcontext(self)


class LoopbackEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        site = FakeCommonCrawl()
        urls = [f"https://example.org/doc{i}" for i in range(12)]
        warcs = [make_warc_record(u, html_page(f"Doc {i}")) for i, u in enumerate(urls)]
        name, spans = site.add_warc(warcs, prefix=b"\x00" * 64)
        lines = [make_cdx_line(u, name, o, l, urlkey=f"org,example)/doc{i}")
                 for i, (u, (o, l)) in enumerate(zip(urls, spans))]
        lines.append(make_cdx_line("https://example.org/img", name, 0, 10, mime="image/png",
                                   mime_detected="image/png", urlkey="org,example)/img"))
        site.add_index([lines[0:4], lines[4:8], lines[8:]], cdx_file="cdx-00000.gz")
        site.add_index([[b"broken line"]], cdx_file="cdx-00001.gz")
        self.site = site

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.daemon_threads = True
        self.server.site, self.server.requests = site, []
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        origin = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.client = HttpClient(HttpConfig(max_connections_per_host=3), endpoints=Endpoints.loopback_for_tests(origin),
                                 transport=StdlibTransport(3))
        self.addCleanup(self.client.close)

    def test_plan_then_acquire_then_rerun(self) -> None:
        queue = _QueueWithEnqueue()
        store = InMemoryCaptureStore(clock=lambda: queue.now)
        filter_config = FilterConfig()
        planner = CrawlPlanner(self.client, queue, store, planning=PlanningConfig(blocks_per_partition=2),
                               filter_config=filter_config, policy=CC_PARTITION_POLICY,
                               max_block_bytes=16 * 1024 * 1024)
        report = planner.plan()
        self.assertEqual([(c.crawl_id, c.partitions, c.created) for c in report.crawls], [(CRAWL, 3, 3)])

        bronze_root = Path(self.tmp.name) / "bronze"
        handler = CCPartitionHandler(client=self.client, store=store, bronze=BronzeStore(bronze_root),
                                     config=AcquisitionConfig(bronze_root=bronze_root, fetch_concurrency=3,
                                                              batch_size=3))
        registry = HandlerRegistry()
        registry.register(JobType.CC_INDEX_PARTITION, handler, policy=CC_PARTITION_POLICY)
        worker = Worker(queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0),
                        worker_id="e2e", rng=random.Random(0))
        worker.run(max_iterations=5)

        statuses = sorted(str(j["status"]) for j in queue.jobs.values())
        self.assertEqual(statuses, [JobStatus.SUCCEEDED.value] * 3)
        self.assertEqual(len(store.observations), 12)
        self.assertEqual(len(list(bronze_root.rglob("*.warc.gz"))), 12)
        self.assertEqual(len(list(bronze_root.rglob("*.json"))), 12)
        self.assertEqual(list(store.codes().values()), ["index_bad_layout"])
        self.assertTrue(all(r is not None for p, r in self.server.requests if p.endswith(".warc.gz")))
        self.assertTrue(all(p != "/img" for p, _r in self.server.requests))

        # Re-plan and re-run: nothing new is enqueued, fetched or stored.
        self.assertEqual(planner.plan().crawls[0].created, 0)
        warc_requests = sum(1 for p, _r in self.server.requests if p.endswith(".warc.gz"))
        for job in queue.jobs.values():
            job.update(status=JobStatus.PENDING, attempt_count=0,
                       checkpoint=dict(job["checkpoint"], block=0, line=0))
        worker.run(max_iterations=5)
        self.assertEqual(sum(1 for p, _r in self.server.requests if p.endswith(".warc.gz")), warc_requests)
        self.assertEqual(len(store.observations), 12)


if __name__ == "__main__":
    unittest.main()

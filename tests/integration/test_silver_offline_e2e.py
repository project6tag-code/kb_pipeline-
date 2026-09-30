"""Offline end-to-end Bronze -> Silver over LOOPBACK HTTP (runs by default; no network, no database).

The production path runs unchanged: planner/discovery -> Runtime Engine worker
-> acquisition handler (stdlib transport, Bronze store) -> Silver stage in the
same unit (integrity, extraction, 22 gates) -> Silver persistence. Only the
endpoints (loopback) and the stores (in-memory, Schema V1 constraint
semantics) are test doubles.

Common Crawl: article -> SILVER accepted; mirror -> exact duplicate; French
page -> rejected_language; page leaking a private key -> quarantined.
GitHub: README -> SILVER accepted; French doc -> rejected_language; doc with a
private key -> quarantined; binary masquerading as text -> never observed.
A rerun of every unit adds zero rows.
"""

import base64
import hashlib
import http.server
import random
import shutil
import tempfile
import threading
import unittest
from datetime import date
from pathlib import Path

from app.config.settings import WorkerSettings
from app.processing.dedup.near import NearIndex
from app.processing.silver.config import SilverConfig
from app.processing.silver.processor import SilverProcessor
from app.queue.job_queue import EnqueueResult
from app.queue.states import JobStatus, JobType, Stage
from app.sources.commoncrawl.acquisition import CC_PARTITION_POLICY, CCPartitionHandler
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.commoncrawl.config import AcquisitionConfig, FilterConfig, HttpConfig, PlanningConfig
from app.sources.commoncrawl.discovery import CrawlPlanner
from app.sources.commoncrawl.http import Endpoints, HttpClient, StdlibTransport
from app.sources.github.bronze import GitHubBronze
from app.sources.github.client import GitHubClient, GitHubEndpoints
from app.sources.github.config import DiscoveryConfig, GitHubAcquisitionConfig, GitHubSettings
from app.sources.github.connector import register_github_handlers
from app.sources.github.discovery import plan_discovery
from app.workers.registry import HandlerRegistry
from app.workers.runtime import Worker

from tests.integration.test_cc_offline_e2e import _Handler as CCHandler
from tests.integration.test_cc_offline_e2e import _QueueWithEnqueue
from tests.integration.test_gh_offline_e2e import _Handler as GHHandler
from tests.unit.cc_fixtures import CRAWL, FakeCommonCrawl, InMemoryCaptureStore, make_cdx_line, make_warc_record
from tests.unit.gh_fixtures import FakeGitHub, InMemoryGitHubStore
from tests.unit.silver_fixtures import InMemorySilverStore, article_html
from tests.unit.test_runtime_worker import FakeQueue
from tests.unit.test_silver_processor import REFERENCE

FRENCH = ("La photosynthèse est le processus par lequel les plantes vertes convertissent l'énergie lumineuse en "
          "énergie chimique. Elle se déroule principalement dans les chloroplastes des cellules des feuilles, où "
          "la chlorophylle absorbe la lumière. Les réactions produisent de l'oxygène et des sucres pour la plante.")
KEY_BODY = base64.b64encode(b"".join(hashlib.sha256(bytes([i])).digest() for i in range(12))).decode()
PRIVATE_KEY = ("-----BEGIN RSA PRIVATE KEY-----\n" + "\n".join(KEY_BODY[i:i + 64] for i in range(0, 384, 64))
               + "\n-----END RSA PRIVATE KEY-----")


class LinkedSilverStore(InMemorySilverStore):
    """Silver store whose ``source_observations`` are the acquisition store's rows (one database)."""

    def __init__(self, rows) -> None:
        super().__init__()
        self._rows = rows

    def existing_observations(self, conn, observation_ids):
        found = set()
        for oid in observation_ids:
            row = self._rows().get(oid)
            if row is not None:
                path = getattr(row, "file_path", None)
                self.observations[oid] = (f"github:{row.repository_id}:{path}" if path is not None
                                          else f"url:{row.source_url}")
                found.add(oid)
        return found


def _serve(handler, test, **attrs) -> str:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    for name, value in attrs.items():
        setattr(server, name, value)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    test.addCleanup(server.server_close)
    test.addCleanup(server.shutdown)
    return f"http://127.0.0.1:{server.server_address[1]}"


def _processor(test, store, root: Path, **bronze) -> SilverProcessor:
    index = NearIndex(root / "near_index.sqlite3")
    test.addCleanup(index.close)
    return SilverProcessor(SilverConfig(near_index_path=root / "near_index.sqlite3"), store, index, **bronze)


class CommonCrawlBronzeToSilverTests(unittest.TestCase):
    def test_cc_bronze_to_silver_accepted_duplicate_and_rejections(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        site = FakeCommonCrawl()
        pages = {
            "https://science.example.org/photosynthesis": article_html(),
            "https://mirror.example.net/photosynthesis": article_html(),
            "https://exemple.fr/photosynthese": f"<html><body><h1>Photosynthèse</h1><p>{FRENCH}</p></body></html>"
                                                .encode(),
            "https://leak.example.com/deploy": article_html(
                "<h1>Deploy notes</h1><p>Binary search is an algorithm that finds a target value in a sorted "
                "array by halving the search interval at every step, so it runs in logarithmic time.</p>"
                f"<pre>{PRIVATE_KEY}</pre>"),
        }
        warcs = [make_warc_record(url, body) for url, body in pages.items()]
        name, spans = site.add_warc(warcs, prefix=b"\x00" * 64)
        lines = [make_cdx_line(url, name, o, n, urlkey=f"org,example)/{i}")
                 for i, (url, (o, n)) in enumerate(zip(pages, spans))]
        site.add_index([lines], cdx_file="cdx-00000.gz")
        origin = _serve(CCHandler, self, site=site, requests=[])
        client = HttpClient(HttpConfig(), endpoints=Endpoints.loopback_for_tests(origin), transport=StdlibTransport(2))
        self.addCleanup(client.close)

        queue = _QueueWithEnqueue()
        captures = InMemoryCaptureStore(clock=lambda: queue.now)
        CrawlPlanner(client, queue, captures, planning=PlanningConfig(blocks_per_partition=1),
                     filter_config=FilterConfig(), policy=CC_PARTITION_POLICY,
                     max_block_bytes=16 * 1024 * 1024).plan()
        silver = LinkedSilverStore(lambda: captures.observations)
        bronze = BronzeStore(root / "bronze")
        processor = _processor(self, silver, root, cc_bronze=bronze)
        handler = CCPartitionHandler(client=client, store=captures, bronze=bronze,
                                     config=AcquisitionConfig(bronze_root=root / "bronze", batch_size=2),
                                     silver=processor)
        registry = HandlerRegistry()
        registry.register(JobType.CC_INDEX_PARTITION, handler, policy=CC_PARTITION_POLICY)
        worker = Worker(queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0),
                        worker_id="e2e", rng=random.Random(0))
        worker.run(max_iterations=3)

        (job,) = queue.jobs.values()
        self.assertEqual((job["status"], job["stage"]), (JobStatus.SUCCEEDED, Stage.SILVER))
        self.assertEqual(len(captures.observations), 4)
        by_url = {row.source_url: oid for oid, row in captures.observations.items()}
        version = processor.extraction_version
        ext = {url: silver.extractions[(oid, version)] for url, oid in by_url.items()}
        article = ext["https://science.example.org/photosynthesis"]
        doc = silver.documents[article["document_id"]]
        self.assertEqual((doc["status"], doc["language"], doc["subject_domain"]), ("accepted", "en", "Biology"))
        self.assertTrue(doc["canonical_text"].startswith("# Photosynthesis\n\nPhotosynthesis is the biological"))
        self.assertNotIn("cookies", doc["canonical_text"])
        mirror = ext["https://mirror.example.net/photosynthesis"]
        self.assertEqual((mirror["document_id"], mirror["document_was_new"]), (article["document_id"], False))
        self.assertEqual(ext["https://exemple.fr/photosynthese"]["outcome"], "rejected_language")
        leak = silver.documents[ext["https://leak.example.com/deploy"]["document_id"]]
        self.assertEqual((leak["status"], leak["canonical_text"]), ("quarantined", None))
        self.assertNotIn(KEY_BODY[:40], repr(silver.documents) + repr(silver.dead_letters))
        self.assertEqual(len(silver.documents), 2)

        # Rerun the unit from the start: Bronze is reused, zero Silver rows are added.
        before = silver.snapshot()
        job.update(status=JobStatus.PENDING, attempt_count=0, checkpoint=dict(job["checkpoint"], block=0, line=0))
        worker.run(max_iterations=2)
        self.assertEqual(silver.snapshot(), before)
        self.assertEqual(job["status"], JobStatus.SUCCEEDED)


class GitHubBronzeToSilverTests(unittest.TestCase):
    def test_github_bronze_to_silver_accepted_and_rejections(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        gh = FakeGitHub()
        gh.add_repo(777, "octo", "algorithms", {
            "README.md": REFERENCE + "\n",
            "docs/fr.md": FRENCH + "\n",
            "docs/deploy.md": REFERENCE + "\n\n```\n" + PRIVATE_KEY + "\n```\n",
            "docs/logo.md": b"\x89PNG\r\n\x1a\n" + b"\x00" * 64,
        })
        queue = FakeQueue()
        store = InMemoryGitHubStore(queue, clock=lambda: queue.now)
        settings = GitHubSettings(discovery=DiscoveryConfig(created_from=date(2024, 5, 1), window_days=31, epoch="e1"),
                                  acquisition=GitHubAcquisitionConfig(bronze_root=root / "gh"))
        origin = _serve(GHHandler, self, gh=gh)
        client = GitHubClient(settings.http, transport=StdlibTransport(2), wall_clock=lambda: queue.now,
                              endpoints=GitHubEndpoints.loopback_for_tests(origin))
        self.addCleanup(client.close)
        bronze = GitHubBronze(settings.acquisition.bronze_root)
        silver = LinkedSilverStore(lambda: store.files)
        processor = _processor(self, silver, root, gh_bronze=bronze)
        registry = HandlerRegistry()
        register_github_handlers(registry, settings, client=client, store=store, bronze=bronze, silver=processor)
        worker = Worker(queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0),
                        worker_id="e2e", rng=random.Random(0))

        class Planner:
            def enqueue_many(self, specs):
                return [EnqueueResult(0, store.enqueue(None, s), "pending") for s in specs]

        plan_discovery(Planner(), settings.discovery, today=date(2024, 5, 31))
        worker.run(max_iterations=4)
        snapshot = next(j for j in queue.jobs.values() if j["source_identifier"].startswith("777@"))
        self.assertEqual((snapshot["status"], snapshot["stage"]), (JobStatus.SUCCEEDED, Stage.SILVER))
        by_path = {row.file_path: oid for oid, row in store.files.items()}
        self.assertNotIn("docs/logo.md", by_path)  # binary masquerading as text never became an observation
        version = processor.extraction_version
        readme = silver.extractions[(by_path["README.md"], version)]
        doc = silver.documents[readme["document_id"]]
        self.assertEqual((doc["status"], doc["language"]), ("accepted", "en"))
        self.assertTrue(doc["canonical_text"].startswith("# Binary search\n\nBinary search is an algorithm"))
        self.assertEqual(doc["subject_domain"], "Computer Science/Algorithms and Data Structures")
        self.assertEqual(silver.extractions[(by_path["docs/fr.md"], version)]["outcome"], "rejected_language")
        deploy = silver.documents[silver.extractions[(by_path["docs/deploy.md"], version)]["document_id"]]
        self.assertEqual((deploy["status"], deploy["canonical_text"]), ("quarantined", None))
        (dead_letter,) = silver.dead_letters.values()
        self.assertEqual((dead_letter["disposition"], dead_letter["error_code"]), ("quarantined", "secret_detected"))

        before = silver.snapshot()
        snapshot.update(status=JobStatus.PENDING, attempt_count=0, checkpoint=dict(snapshot["checkpoint"], next=0))
        worker.run(max_iterations=2)
        self.assertEqual(silver.snapshot(), before)


if __name__ == "__main__":
    unittest.main()

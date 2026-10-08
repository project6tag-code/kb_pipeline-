"""Offline end-to-end Bronze -> Silver -> Chunking over LOOPBACK HTTP (runs by default; no network, no DB).

Common Crawl page and GitHub README go through the real acquisition handlers,
the Silver stage and inline Chunking V1 (schema §10.3, same transaction), with
in-memory stores. Both documents share one identical section: it becomes ONE
chunk_registry row with an occurrence in each document (cross-source exact
chunk dedup). A rerun of both units adds nothing.
"""

import random
import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path

from app.config.settings import WorkerSettings
from app.processing.chunking import plain
from app.processing.chunking.chunker import ChunkingConfig
from app.processing.chunking.service import ChunkWriter
from app.processing.normalization.text import canonicalize_text
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
from app.utils.ids import chunk_id_from_text
from app.workers.registry import HandlerRegistry
from app.workers.runtime import Worker

from tests.integration.test_cc_offline_e2e import _Handler as CCHandler
from tests.integration.test_cc_offline_e2e import _QueueWithEnqueue
from tests.integration.test_gh_offline_e2e import _Handler as GHHandler
from tests.integration.test_silver_offline_e2e import LinkedSilverStore, _processor, _serve
from tests.unit.cc_fixtures import FakeCommonCrawl, InMemoryCaptureStore, make_cdx_line, make_warc_record
from tests.unit.chunk_fixtures import InMemoryChunkStore
from tests.unit.gh_fixtures import FakeGitHub, InMemoryGitHubStore
from tests.unit.test_runtime_worker import FakeQueue

SHARED = ("Glaciers form where the accumulation of snow exceeds its ablation over many years. The snow compacts "
          "into firn and then into dense glacial ice. A glacier is a persistent body of ice that moves under its "
          "own weight. Glacial ice covers about 10 percent of the land surface of the Earth today.")
WEB_ONLY = ("Ice sheets are glaciers that cover more than 50,000 square kilometres of land. The two modern ice "
            "sheets cover most of Greenland and Antarctica. The Antarctic ice sheet contains about 26.5 million "
            "cubic kilometres of ice, which is roughly 90 percent of the fresh water on the surface of the Earth.")
REPO_ONLY = ("Glacier mass balance is the difference between accumulation and ablation over one year. It is "
             "measured with stakes, snow pits and satellite altimetry. A negative balance means the glacier is "
             "losing mass, and most mountain glaciers have had a negative balance since the 1980s.")


class ChunkingEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.config = ChunkingConfig()

    def test_cc_and_github_documents_chunk_and_share_one_registry_row(self) -> None:
        # ---- Common Crawl unit (Silver + inline chunking) ----
        site = FakeCommonCrawl()
        url = "https://science.example.org/glaciers"
        page = (f"<html><head><title>Glaciers</title></head><body><main><article><h2>Formation</h2><p>{SHARED}</p>"
                f"<h2>Ice sheets</h2><p>{WEB_ONLY}</p></article></main></body></html>").encode()
        name, spans = site.add_warc([make_warc_record(url, page)])
        site.add_index([[make_cdx_line(url, name, *spans[0], urlkey="org,example)/glaciers")]], cdx_file="cdx-00000.gz")
        origin = _serve(CCHandler, self, site=site, requests=[])
        cc_client = HttpClient(HttpConfig(), endpoints=Endpoints.loopback_for_tests(origin), transport=StdlibTransport(2))
        self.addCleanup(cc_client.close)
        cc_queue = _QueueWithEnqueue()
        captures = InMemoryCaptureStore(clock=lambda: cc_queue.now)
        self.gh_store = InMemoryGitHubStore(FakeQueue())
        silver = LinkedSilverStore(lambda: {**captures.observations, **self.gh_store.files})  # one database
        chunks = InMemoryChunkStore(documents=silver.documents.keys())
        version = chunks.register(self.config)
        cc_bronze = BronzeStore(self.root / "bronze-cc")
        gh_bronze = GitHubBronze(self.root / "bronze-gh")
        processor = _processor(self, silver, self.root, cc_bronze=cc_bronze, gh_bronze=gh_bronze)
        processor.chunker = ChunkWriter(chunks, self.config)
        CrawlPlanner(cc_client, cc_queue, captures, planning=PlanningConfig(blocks_per_partition=1),
                     filter_config=FilterConfig(), policy=CC_PARTITION_POLICY, max_block_bytes=16 * 1024 * 1024).plan()
        registry = HandlerRegistry()
        registry.register(JobType.CC_INDEX_PARTITION, CCPartitionHandler(
            client=cc_client, store=captures, bronze=cc_bronze,
            config=AcquisitionConfig(bronze_root=self.root / "bronze-cc"), silver=processor),
            policy=CC_PARTITION_POLICY)
        cc_worker = Worker(cc_queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0),
                           worker_id="cc", rng=random.Random(0))
        cc_worker.run(max_iterations=2)

        # ---- GitHub unit (Silver + inline chunking) ----
        gh = FakeGitHub()
        gh.add_repo(4711, "octo", "glaciology", {"README.md": f"## Formation\n\n{SHARED}\n\n## Mass balance\n\n"
                                                              f"{REPO_ONLY}\n"})
        gh_queue = self.gh_store.queue
        self.gh_store.clock = lambda: gh_queue.now
        settings = GitHubSettings(discovery=DiscoveryConfig(created_from=date(2024, 5, 1), window_days=31, epoch="e1"),
                                  acquisition=GitHubAcquisitionConfig(bronze_root=self.root / "bronze-gh"))
        gh_origin = _serve(GHHandler, self, gh=gh)
        gh_client = GitHubClient(settings.http, transport=StdlibTransport(2), wall_clock=lambda: gh_queue.now,
                                 endpoints=GitHubEndpoints.loopback_for_tests(gh_origin))
        self.addCleanup(gh_client.close)
        gh_registry = HandlerRegistry()
        register_github_handlers(gh_registry, settings, client=gh_client, store=self.gh_store, bronze=gh_bronze,
                                 silver=processor)
        store = self.gh_store

        class Planner:
            def enqueue_many(self, specs):
                return [EnqueueResult(0, store.enqueue(None, s), "pending") for s in specs]

        plan_discovery(Planner(), settings.discovery, today=date(2024, 5, 31))
        gh_worker = Worker(gh_queue, gh_registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0),
                           worker_id="gh", rng=random.Random(0))
        gh_worker.run(max_iterations=4)

        # ---- both units reached SILVER and both documents were accepted and chunked ----
        (cc_job,) = cc_queue.jobs.values()
        gh_job = next(j for j in gh_queue.jobs.values() if j["source_identifier"].startswith("4711@"))
        self.assertEqual([(j["status"], j["stage"]) for j in (cc_job, gh_job)],
                         [(JobStatus.SUCCEEDED, Stage.SILVER)] * 2)
        accepted = {d: row for d, row in silver.documents.items() if row["status"] == "accepted"}
        self.assertEqual(len(accepted), 2)
        by_document = {d: chunks.ordered(d, version) for d in accepted}
        self.assertTrue(all(by_document.values()))
        for document_id, occurrences in by_document.items():
            text = accepted[document_id]["canonical_text"]
            self.assertEqual([o[0] for o in occurrences], list(range(len(occurrences))))
            for _ordinal, chunk_id, start, end, _path in occurrences:
                self.assertEqual(canonicalize_text(plain.render(text[start:end])), chunks.chunks[chunk_id]["chunk_text"])

        shared_id = chunk_id_from_text(f"Formation\n\n{SHARED}")  # c2: plain text, no "## " marker
        holders = [d for d, occ in by_document.items() if any(o[1] == shared_id for o in occ)]
        self.assertEqual(len(holders), 2)  # Common Crawl page and GitHub README
        self.assertEqual(sum(1 for c in chunks.chunks if c == shared_id), 1)
        self.assertEqual(len(chunks.chunks), 3)
        self.assertEqual(len(chunks.occurrences), 4)

        # ---- rerun both units from the start: nothing new ----
        before = (chunks.snapshot(), silver.snapshot())
        cc_job.update(status=JobStatus.PENDING, attempt_count=0, checkpoint=dict(cc_job["checkpoint"], block=0, line=0))
        gh_job.update(status=JobStatus.PENDING, attempt_count=0, checkpoint=dict(gh_job["checkpoint"], next=0))
        cc_worker.run(max_iterations=2)
        gh_worker.run(max_iterations=2)
        self.assertEqual((chunks.snapshot(), silver.snapshot()), before)


if __name__ == "__main__":
    unittest.main()

"""Opt-in PostgreSQL integration test of Common Crawl acquisition persistence (NO network).

Runs only when ALL of these hold (never by default):

    KB_RUN_PG_INTEGRATION=1
    KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    the connected database is named kb_pipeline_test (checked; any other database is refused)

    python -m unittest tests.integration.test_cc_postgres -v

Why the database-name guard: kb_pipeline_app can INSERT but never DELETE the
acquisition tables, so every row written here is permanent. Rows use a
synthetic crawl id ``CC-MAIN-1999-NN`` (not a real crawl) and random WARC and
CDX names, so reruns never collide and real work is never claimed. HTTP is the
in-memory FakeTransport: no Common Crawl request is made.
"""

import os
import random
import tempfile
import unittest
import uuid
from pathlib import Path

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_INTEGRATION_DATABASE_URL"))
REQUIRED_DATABASE = "kb_pipeline_test"


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_INTEGRATION_DATABASE_URL (kb_pipeline_test) to run")
class CommonCrawlPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from sqlalchemy import text

        from app.config.settings import DatabaseSettings
        from app.db.connection import Database
        from app.queue.job_queue import JobQueue

        cls.db = Database.from_settings(DatabaseSettings(url=os.environ["KB_INTEGRATION_DATABASE_URL"], pool_size=3,
                                                         application_name="kb_pipeline:cc_integration_test"))
        cls.db.verify_runtime_identity()
        with cls.db.engine.connect() as conn:
            name = conn.execute(text("SELECT current_database()")).scalar_one()
            conn.rollback()
        if name != REQUIRED_DATABASE:
            cls.db.dispose()
            raise unittest.SkipTest(f"refusing to write permanent test rows into database {name!r}; "
                                    f"only {REQUIRED_DATABASE!r} is allowed")
        cls.queue = JobQueue(cls.db)
        cls.crawl = f"CC-MAIN-1999-{random.randint(1, 52):02d}"

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "db", None) is not None:
            cls.db.dispose()

    def setUp(self) -> None:
        from app.sources.commoncrawl.bronze import BronzeStore
        from app.sources.commoncrawl.config import AcquisitionConfig, FilterConfig, PlanningConfig
        from app.sources.commoncrawl.http import HttpClient
        from app.sources.commoncrawl.repository import PostgresCaptureStore

        from tests.unit.cc_fixtures import FakeCommonCrawl, FakeTransport, html_page, make_cdx_line, make_warc_record

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tag = uuid.uuid4().hex[:12]
        self.site = FakeCommonCrawl()
        self.urls = [f"https://it-{tag}.example/page{i}" for i in range(4)]
        records = [make_warc_record(u, html_page(f"IT {i}")) for i, u in enumerate(self.urls)]
        self.warc, self.spans = self.site.add_warc(records, crawl=self.crawl, leaf=f"it-{tag}.warc.gz")
        lines = [make_cdx_line(u, self.warc, o, l, urlkey=f"example,it)/{i}")
                 for i, (u, (o, l)) in enumerate(zip(self.urls, self.spans))]
        self.cdx_file = f"cdx-{random.randint(10000, 99999):05d}.gz"
        self.site.add_index([lines[:2], lines[2:]], cdx_file=self.cdx_file, crawl=self.crawl)
        self.transport = FakeTransport(self.site)
        self.client = HttpClient(transport=self.transport)
        self.store = PostgresCaptureStore()
        self.bronze_root = Path(tmp.name) / "bronze"
        self.filter = FilterConfig()
        self.planning = PlanningConfig(blocks_per_partition=10)
        self.config = AcquisitionConfig(bronze_root=self.bronze_root, fetch_concurrency=2, batch_size=3)
        self.bronze = BronzeStore(self.bronze_root)
        self.prefix = f"{self.crawl}/cc-index/collections/{self.crawl}/indexes/{self.cdx_file}"

    def worker(self):
        from app.config.settings import WorkerSettings
        from app.sources.commoncrawl.acquisition import CC_PARTITION_POLICY, CCPartitionHandler
        from app.workers.registry import HandlerRegistry
        from app.workers.runtime import Worker

        registry = HandlerRegistry()
        registry.register("cc_index_partition", CCPartitionHandler(
            client=self.client, store=self.store, bronze=self.bronze, config=self.config),
            policy=CC_PARTITION_POLICY)
        return Worker(self.queue, registry, settings=WorkerSettings(
            lease_seconds=60, poll_interval_seconds=0, source_identifier_prefix=self.prefix), worker_id="cc-it")

    def plan(self):
        from app.sources.commoncrawl.acquisition import CC_PARTITION_POLICY
        from app.sources.commoncrawl.discovery import CrawlPlanner

        planner = CrawlPlanner(self.client, self.queue, self.store, planning=self.planning, filter_config=self.filter,
                               policy=CC_PARTITION_POLICY, max_block_bytes=16 * 1024 * 1024)
        return planner.plan([self.crawl]).crawls[0]

    def rows(self):
        from sqlalchemy import text

        with self.db.transaction() as conn:
            return [dict(r) for r in conn.execute(text(
                "SELECT o.observation_id, o.source_channel, o.source_url, o.url_host, o.source_title, "
                "o.source_published_at, c.warc_record_offset, c.warc_record_length, c.http_status, "
                "c.detected_mime, c.payload_digest, w.warc_filename, w.crawl_id "
                "FROM kbp.source_observations AS o JOIN kbp.cc_captures AS c USING (observation_id) "
                "JOIN kbp.cc_warc_files AS w USING (warc_file_id) WHERE w.warc_filename = :f "
                "ORDER BY c.warc_record_offset"), {"f": self.warc}).mappings()]

    def test_plan_acquire_persist_and_rerun(self) -> None:
        from app.utils.ids import cc_observation_id

        report = self.plan()
        self.assertEqual((report.partitions, report.created), (1, 1))
        self.assertEqual(self.plan().created, 0)  # re-plan: no duplicate job
        worker = self.worker()
        worker.run(max_iterations=2)
        rows = self.rows()
        self.assertEqual(len(rows), 4)
        first = rows[0]
        self.assertEqual((first["source_channel"], first["source_url"], first["url_host"], first["source_title"]),
                         ("common_crawl", self.urls[0], self.urls[0].split("/")[2], "IT 0"))
        self.assertIsNone(first["source_published_at"])
        self.assertEqual((first["crawl_id"], first["warc_filename"], first["http_status"], first["detected_mime"]),
                         (self.crawl, self.warc, 200, "text/html"))
        self.assertEqual(first["observation_id"], cc_observation_id(self.crawl, self.warc, *self.spans[0]))

        (job,) = [j for j in self._jobs() if j["source_identifier"].startswith(self.prefix)]
        self.assertEqual((job["status"], job["stage"]), ("succeeded", "ACQUIRED"))

        # Operator requeue: the rerun inserts nothing and fetches no WARC record.
        warc_requests = self.transport.count(self.warc)
        self.queue.requeue(job["job_id"])
        with self.db.transaction() as conn:
            from sqlalchemy import text

            conn.execute(text("UPDATE kbp.pipeline_jobs SET checkpoint = jsonb_set(jsonb_set(checkpoint, '{block}', "
                              "'0'), '{line}', '0') WHERE job_id = :j"), {"j": job["job_id"]})
        worker.run(max_iterations=2)
        self.assertEqual(len(self.rows()), 4)
        self.assertEqual(self.transport.count(self.warc), warc_requests)
        attempts = self.queue.attempts(job["job_id"])
        self.assertEqual(attempts[-1]["metrics"]["already_present"], 4)

    def test_natural_key_conflict_raises(self) -> None:
        from sqlalchemy.exc import IntegrityError

        from app.sources.commoncrawl.repository import CaptureRow
        from app.utils.time import utc_now

        self.plan()
        self.worker().run(max_iterations=2)
        offset, length = self.spans[0]
        forged = CaptureRow(uuid.uuid4(), self.crawl, self.warc, offset, length, self.urls[0], None, None, utc_now(),
                            None, None, None, 200, None, None, None, None, None)
        conn = self.db.engine.connect()
        conn.begin()
        try:
            with self.assertRaises(IntegrityError):
                self.store.persist_capture(conn, forged)
        finally:
            conn.rollback()
            conn.close()

    def _jobs(self):
        from sqlalchemy import text

        with self.db.transaction() as conn:
            return [dict(r) for r in conn.execute(text(
                "SELECT job_id, source_identifier, status, stage FROM kbp.pipeline_jobs "
                "WHERE job_type = 'cc_index_partition' AND starts_with(source_identifier, :p)"),
                {"p": self.prefix}).mappings()]


if __name__ == "__main__":
    unittest.main()

"""cc_index_partition handler end to end through the Runtime Engine worker (offline).

Real components: Worker, retry controller, handler, HttpClient, WARC/CDX
parsing, BronzeStore (temp dir). Fakes: FakeQueue (tests.unit.test_runtime_worker),
FakeTransport over an in-memory Common Crawl, InMemoryCaptureStore.
"""

import json
import random
import tempfile
import threading
import time
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

from app.config.settings import WorkerSettings
from app.queue.retry import RetryPolicy
from app.queue.states import JobStatus, JobType, Stage
from app.sources.commoncrawl.acquisition import DATA_SCOPE, CCPartitionHandler
from app.sources.commoncrawl.bronze import BronzeState, BronzeStore
from app.sources.commoncrawl.config import AcquisitionConfig, FilterConfig
from app.sources.commoncrawl.discovery import cdx_key
from app.sources.commoncrawl.http import HttpClient
from app.utils.ids import cc_observation_id, cc_observation_payload
from app.workers.registry import HandlerRegistry
from app.workers.runtime import Worker

from tests.unit.cc_fixtures import (
    CRAWL,
    FakeCommonCrawl,
    FakeTransport,
    InMemoryCaptureStore,
    html_page,
    make_cdx_line,
    make_warc_record,
    partitions,
)
from tests.unit.test_runtime_worker import FakeQueue

POLICY = RetryPolicy(max_attempts=6, base_delay_seconds=1.0, max_delay_seconds=4.0, rate_limit_jitter_seconds=0.0)
RUN_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
FILTER = FilterConfig()
LEAF_A = "CC-MAIN-20240805230130-20240806020130-00001.warc.gz"
LEAF_B = "CC-MAIN-20240805230130-20240806020130-00002.warc.gz"


class Crash(BaseException):
    """Simulates the process dying (not an Exception: nothing in the pipeline may catch it)."""


class Env:
    def __init__(self, test: unittest.TestCase, *, records: int = 6, per_block: int = 3, concurrency: int = 2,
                 batch_size: int = 4, extra_lines: dict[int, list[bytes]] | None = None, disk_free: int | None = None,
                 **config) -> None:
        tmp = tempfile.TemporaryDirectory()
        test.addCleanup(tmp.cleanup)
        self.site = FakeCommonCrawl()
        self.urls = [f"https://example.com/p{i}" for i in range(records)]
        self.bodies = [html_page(f"Title {i}") for i in range(records)]
        warcs = [make_warc_record(u, b) for u, b in zip(self.urls, self.bodies)]
        self.warc, self.spans = self.site.add_warc(warcs, leaf=LEAF_A, prefix=b"\x00" * 128)
        self.lines = [make_cdx_line(u, self.warc, o, l, urlkey=f"com,example)/p{i}")
                      for i, (u, (o, l)) in enumerate(zip(self.urls, self.spans))]
        blocks = [self.lines[i:i + per_block] for i in range(0, records, per_block)] or [[]]
        for block, lines in (extra_lines or {}).items():
            blocks[block] = blocks[block] + lines
        self.blocks = blocks
        self.site.add_index(blocks)
        self.transport = FakeTransport(self.site)
        self.queue = FakeQueue()
        self.store = InMemoryCaptureStore(clock=lambda: self.queue.now)
        usage = (lambda _p: SimpleNamespace(free=disk_free)) if disk_free is not None else None
        self.bronze = (BronzeStore(Path(tmp.name) / "bronze", min_free_bytes=1_000_000, disk_usage=usage)
                       if usage else BronzeStore(Path(tmp.name) / "bronze"))
        self.config = AcquisitionConfig(bronze_root=Path(tmp.name) / "bronze", fetch_concurrency=concurrency,
                                        batch_size=batch_size, **config)
        self.handler = CCPartitionHandler(client=HttpClient(transport=self.transport), store=self.store,
                                          bronze=self.bronze, config=self.config, run_id=RUN_ID,
                                          clock=lambda: self.queue.now)
        registry = HandlerRegistry()
        registry.register(JobType.CC_INDEX_PARTITION, self.handler, policy=POLICY)
        self.worker = Worker(self.queue, registry, settings=WorkerSettings(lease_seconds=60, poll_interval_seconds=0,
                                                                           claim_batch_size=1),
                             worker_id="w1", rng=random.Random(0))

    # -- helpers ---------------------------------------------------------------- #

    def enqueue(self, checkpoint_patch=None) -> list[int]:
        ids = []
        for spec in partitions(self.site, FILTER):
            checkpoint = spec.initial_checkpoint(FILTER)
            if checkpoint_patch:
                checkpoint_patch(checkpoint)
            ids.append(self.queue.add(spec.source_identifier(FILTER), checkpoint=checkpoint,
                                      job_type=JobType.CC_INDEX_PARTITION, max_attempts=POLICY.max_attempts))
        return ids

    def drain(self, rounds: int = 60) -> None:
        for _ in range(rounds):
            if self.worker.run_once():
                continue
            waiting = [r for r in self.queue.jobs.values()
                       if r["status"] in (JobStatus.PENDING, JobStatus.RETRY_PENDING)]
            if not waiting:
                return
            self.queue.now = max(self.queue.now, min(r["next_attempt_at"] for r in waiting)) + timedelta(seconds=1)
        raise AssertionError("queue did not settle")

    def job(self, job_id: int) -> dict:
        return self.queue.jobs[job_id]

    def oid(self, i: int) -> uuid.UUID:
        return cc_observation_id(CRAWL, self.warc, *self.spans[i])

    def warc_requests(self, warc: str | None = None) -> list[str | None]:
        return [rng for path, rng in self.transport.requests if path == "/" + (warc or self.warc)]

    def metrics(self, job_id: int) -> dict:
        return next(a for a in self.queue.attempts if a["job_id"] == job_id and a["outcome"] == "succeeded")["metrics"]


class HappyPathTests(unittest.TestCase):
    def test_end_to_end_capture_provenance_and_bronze(self) -> None:
        env = Env(self)
        (job_id,) = env.enqueue()
        env.drain()
        job = env.job(job_id)
        self.assertEqual((job["status"], job["stage"]), (JobStatus.SUCCEEDED, Stage.ACQUIRED))
        self.assertEqual(set(env.store.observations), {env.oid(i) for i in range(6)})
        self.assertEqual(len(env.warc_requests()), 6)
        self.assertEqual(env.store.crawls, set())  # crawls are registered by the planner, not the handler

        row = env.store.observations[env.oid(1)]
        offset, length = env.spans[1]
        self.assertEqual((row.crawl_id, row.warc_filename, row.warc_record_offset, row.warc_record_length),
                         (CRAWL, env.warc, offset, length))
        self.assertEqual((row.source_url, row.url_host, row.source_title, row.http_status, row.detected_mime),
                         (env.urls[1], "example.com", "Title 1", 200, "text/html"))
        self.assertEqual((row.http_content_type, row.run_id, row.acquired_at), ("text/html; charset=utf-8", RUN_ID,
                                                                                env.queue.now))
        self.assertTrue(row.payload_digest.startswith("sha1:"))
        self.assertIsNotNone(row.warc_record_id)
        self.assertIsNotNone(row.warc_date)

        data, manifest = env.bronze.read_verified(CRAWL, env.oid(1))
        self.assertEqual(data, env.site.objects["/" + env.warc][offset:offset + length])
        self.assertEqual((manifest["source_type"], manifest["crawl_id"], manifest["warc_filename"],
                          manifest["warc_offset"], manifest["warc_length"], manifest["source_url"]),
                         ("common_crawl", CRAWL, env.warc, offset, length, env.urls[1]))
        self.assertEqual(manifest["identity"], cc_observation_payload(CRAWL, env.warc, offset, length))
        self.assertEqual(manifest["index"]["filter_version"], FILTER.version)
        self.assertEqual(manifest["index"]["fields"]["status"], "200")
        self.assertEqual(manifest["retrieval"]["range"], f"bytes={offset}-{offset + length - 1}")
        self.assertTrue(manifest["warc"]["payload_digest_verified"])
        self.assertEqual(manifest["html_metadata"]["title"], "Title 1")
        # The fixture page declares no dates: the evidence is recorded as absent, never synthesized.
        self.assertEqual(manifest["html_metadata"]["dates"],
                         {"version": "ccdate-v1", "published_at": None, "published_evidence": None,
                          "modified_at": None, "modified_evidence": None, "http_last_modified": None})

        metrics = env.metrics(job_id)
        self.assertEqual((metrics["lines"], metrics["candidates"], metrics["inserted"], metrics["fetched"],
                          metrics["bronze_created"], metrics["blocks_done"]), (6, 6, 6, 6, 6, 2))
        self.assertEqual(job["checkpoint"]["block"], 2)
        self.assertEqual(job["checkpoint"]["totals"]["inserted"], 6)

    def test_rejected_candidates_are_counted_never_fetched(self) -> None:
        other = [
            make_cdx_line("https://example.com/404", "crawl-data/CC-MAIN-2024-33/segments/1.0/warc/z.warc.gz", 0, 10,
                          status="404", urlkey="a)/1"),
            make_cdx_line("https://example.com/img", "crawl-data/CC-MAIN-2024-33/segments/1.0/warc/z.warc.gz", 0, 10,
                          mime="image/png", mime_detected="image/png", urlkey="a)/2"),
            make_cdx_line("https://example.com/fr", "crawl-data/CC-MAIN-2024-33/segments/1.0/warc/z.warc.gz", 0, 10,
                          languages="fra", urlkey="a)/3"),
            make_cdx_line("https://example.com/r", "crawl-data/CC-MAIN-2024-33/segments/1.0/crawldiagnostics/z.warc.gz",
                          0, 10, status="301", urlkey="a)/4"),
        ]
        env = Env(self, extra_lines={0: other})
        (job_id,) = env.enqueue()
        env.drain()
        metrics = env.metrics(job_id)
        self.assertEqual((metrics["rejected_status"], metrics["rejected_mime"], metrics["rejected_language"],
                          metrics["rejected_record_type"]), (1, 1, 1, 1))
        self.assertEqual(env.transport.count("z.warc.gz"), 0)
        self.assertEqual(len(env.store.observations), 6)


class IdempotencyTests(unittest.TestCase):
    def test_rerun_is_a_no_op(self) -> None:
        env = Env(self)
        env.enqueue()
        env.drain()
        bronze_files = {p: p.stat().st_mtime_ns for p in env.bronze.root.rglob("*") if p.is_file()}
        before = len(env.warc_requests())
        (job_id,) = env.enqueue()  # the same partition again (e.g. an operator requeue)
        env.drain()
        self.assertEqual(len(env.warc_requests()), before)  # no WARC record fetched twice
        self.assertEqual(len(env.store.observations), 6)
        self.assertEqual(env.metrics(job_id)["already_present"], 6)
        self.assertEqual({p: p.stat().st_mtime_ns for p in env.bronze.root.rglob("*") if p.is_file()}, bronze_files)

    def test_duplicate_discovery_in_and_across_blocks(self) -> None:
        env = Env(self, records=4, per_block=2, batch_size=10)
        env.blocks[0].append(env.lines[0])      # same line twice in one batch
        env.blocks[1].append(env.lines[1])      # repeated in a later block
        env.site._cluster.clear()
        env.site.add_index(env.blocks)
        (job_id,) = env.enqueue()
        env.drain()
        metrics = env.metrics(job_id)
        self.assertEqual(len(env.store.observations), 4)
        self.assertEqual(metrics["duplicate_in_batch"], 1)
        self.assertEqual(metrics["already_present"], 1)
        self.assertEqual(sorted(env.warc_requests()), sorted(set(env.warc_requests())))  # each range once

    def test_missing_bronze_evidence_is_repaired_without_touching_the_database(self) -> None:
        env = Env(self)
        env.enqueue()
        env.drain()
        record_path, _ = env.bronze.paths(CRAWL, env.oid(2))
        record_path.unlink()
        env.enqueue()
        env.drain()
        self.assertEqual(env.bronze.inspect(CRAWL, env.oid(2)).state, BronzeState.COMPLETE)
        self.assertEqual(len(env.warc_requests()), 7)  # exactly one re-fetch
        self.assertEqual(len(env.store.observations), 6)


class RetryTests(unittest.TestCase):
    def test_429_retry_after_defers_without_consuming_an_attempt(self) -> None:
        env = Env(self, concurrency=1, batch_size=10)
        env.transport.faults["/" + env.warc].append({"status": 429, "headers": {"Retry-After": "120"}})
        (job_id,) = env.enqueue()
        start = env.queue.now
        env.worker.run_once()
        job = env.job(job_id)
        self.assertEqual((job["status"], job["attempt_count"], job["deferral_count"]),
                         (JobStatus.RETRY_PENDING, 0, 1))
        self.assertGreaterEqual(job["next_attempt_at"], start + timedelta(seconds=120))
        self.assertEqual(env.store.rate_limit_events, [(DATA_SCOPE, 429, 120)])
        self.assertEqual((job["checkpoint"]["block"], job["checkpoint"]["line"]), (0, 0))
        self.assertEqual(env.queue.outcomes(job_id), ["deferred_rate_limit"])

        # Claimed early anyway: the shared rate-limit state defers it before any request.
        requests = len(env.transport.requests)
        job["next_attempt_at"] = env.queue.now
        env.worker.run_once()
        self.assertEqual(len(env.transport.requests), requests)
        self.assertEqual((env.job(job_id)["deferral_count"], env.queue.outcomes(job_id)[-1]), (2, "deferred_rate_limit"))

        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
        self.assertEqual(len(env.store.observations), 6)

    def test_5xx_consumes_an_attempt_and_resumes_at_the_failing_record(self) -> None:
        env = Env(self, concurrency=1, batch_size=10)
        env.transport.faults["/" + env.warc] = [{}, {}, {"status": 500}]  # third record fails once
        (job_id,) = env.enqueue()
        env.worker.run_once()
        job = env.job(job_id)
        self.assertEqual((job["status"], job["attempt_count"], job["last_error_class"]),
                         (JobStatus.RETRY_PENDING, 1, "transient"))
        self.assertEqual((job["checkpoint"]["block"], job["checkpoint"]["line"]), (0, 2))
        self.assertEqual(job["checkpoint"]["item_failures"], {"0:2": 1})
        self.assertEqual(len(env.store.observations), 2)  # the prefix was committed
        env.drain()
        job = env.job(job_id)
        self.assertEqual((job["status"], job["attempt_count"]), (JobStatus.SUCCEEDED, 2))
        self.assertNotIn("item_failures", job["checkpoint"])
        self.assertEqual(len(env.store.observations), 6)
        self.assertEqual(len(env.warc_requests()), 7)

    def test_timeouts_and_bad_ranges_are_transient(self) -> None:
        for fault, code in (({"timeout": True}, "timeout"), ({"short": 5}, "short_read"),
                            ({"wrong_range": True}, "range_mismatch"), ({"ignore_range": True}, "range_not_honoured"),
                            ({"read_timeout": True}, "timeout")):
            with self.subTest(code=code):
                env = Env(self, concurrency=1)
                env.transport.faults["/" + env.warc].append(fault)
                (job_id,) = env.enqueue()
                env.worker.run_once()
                attempt = env.queue.attempts[-1]
                self.assertEqual((attempt["outcome"], attempt["error_class"]), ("failed", "transient"))
                env.drain()
                self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
                self.assertEqual(len(env.store.observations), 6)

    def test_permanent_4xx_record_is_dead_lettered_and_the_unit_continues(self) -> None:
        env = Env(self)
        other_body = html_page("gone")
        other = make_warc_record("https://example.com/gone", other_body)
        name, ((offset, length),) = env.site.add_warc([other], leaf=LEAF_B)
        env.transport.persistent["/" + name] = {"status": 404}
        env.blocks[0].append(make_cdx_line("https://example.com/gone", name, offset, length, urlkey="z)/gone"))
        env.site._cluster.clear()
        env.site.add_index(env.blocks)
        (job_id,) = env.enqueue()
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
        oid = str(cc_observation_id(CRAWL, name, offset, length))
        letter = env.store.dead_letters[(oid, "ACQUISITION_PENDING")]
        self.assertEqual((letter["error_class"], letter["error_code"], letter["job_id"]),
                         ("permanent_source", "http_404", job_id))
        self.assertEqual(json.loads(letter["payload_ref"])["warc_offset"], offset)
        self.assertEqual(env.transport.count(LEAF_B), 1)  # never retried
        self.assertEqual(len(env.store.observations), 6)

    def test_poisoned_record_is_dead_lettered_after_the_item_retry_limit(self) -> None:
        env = Env(self, concurrency=1, item_transient_limit=2)
        bad = make_warc_record("https://example.com/poison")
        name, ((offset, length),) = env.site.add_warc([bad], leaf=LEAF_B)
        env.transport.persistent["/" + name] = {"status": 500}
        env.blocks[0].insert(1, make_cdx_line("https://example.com/poison", name, offset, length, urlkey="p)/"))
        env.site._cluster.clear()
        env.site.add_index(env.blocks)
        (job_id,) = env.enqueue()
        env.drain()
        job = env.job(job_id)
        self.assertEqual((job["status"], job["attempt_count"]), (JobStatus.SUCCEEDED, 3))
        self.assertEqual(env.transport.count(LEAF_B), 2)
        letter = env.store.dead_letters[(str(cc_observation_id(CRAWL, name, offset, length)), "ACQUISITION_PENDING")]
        self.assertEqual((letter["error_class"], letter["error_code"]), ("transient", "item_retry_limit"))
        self.assertEqual(len(env.store.observations), 6)

    def test_index_block_failures(self) -> None:
        env = Env(self)
        path = "/" + cdx_key(CRAWL, "cdx-00000.gz")
        env.transport.faults[path].append({"status": 503, "headers": {"Retry-After": "30"}})
        (job_id,) = env.enqueue()
        env.worker.run_once()
        self.assertEqual((env.job(job_id)["deferral_count"], env.store.rate_limit_events),
                         (1, [(DATA_SCOPE, 503, 30)]))
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)

        env = Env(self)
        env.transport.persistent["/" + cdx_key(CRAWL, "cdx-00000.gz")] = {"status": 404}
        (job_id,) = env.enqueue()
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.DEAD_LETTER)
        self.assertEqual(env.queue.dead_letters[-1]["error_class"], "permanent_source")


class InvalidDataTests(unittest.TestCase):
    def test_malformed_index_rows_are_dead_lettered(self) -> None:
        bad = [b"garbage", b"k 20240805231234 {bad json",
               make_cdx_line("https://example.com/t", "crawl-data/CC-MAIN-2024-33/../../../etc/passwd", 0, 10,
                             urlkey="t)/"),
               make_cdx_line("javascript:alert(1)", "crawl-data/CC-MAIN-2024-33/segments/1.0/warc/a.warc.gz", 0, 10,
                             urlkey="j)/"),
               make_cdx_line("https://example.com/o", "crawl-data/CC-MAIN-2024-33/segments/1.0/warc/a.warc.gz", -5, 10,
                             urlkey="o)/")]
        env = Env(self, extra_lines={1: bad})
        (job_id,) = env.enqueue()
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
        codes = sorted(env.store.codes().values())
        self.assertEqual(codes, ["index_bad_json", "index_bad_layout", "index_invalid_field", "index_invalid_filename",
                                 "index_invalid_url"])
        self.assertTrue(all(key.startswith(f"cdx:{cdx_key(CRAWL, 'cdx-00000.gz')}@") for key, _s in env.store.dead_letters))
        self.assertEqual(env.metrics(job_id)["malformed"], 5)
        self.assertEqual(len(env.store.observations), 6)
        self.assertFalse(any("passwd" in path for path, _r in env.transport.requests))

    def test_invalid_warc_records_are_dead_lettered(self) -> None:
        env = Env(self, records=3, per_block=3)
        mismatched = make_warc_record("https://example.com/p9", target="https://evil.example/")
        request = make_warc_record("https://example.com/p8", warc_type="request")
        name, spans = env.site.add_warc([mismatched, request], leaf=LEAF_B)
        env.blocks[0] += [make_cdx_line("https://example.com/p9", name, *spans[0], urlkey="a)/9"),
                          make_cdx_line("https://example.com/p8", name, *spans[1], urlkey="a)/8")]
        env.site._cluster.clear()
        env.site.add_index(env.blocks)
        (job_id,) = env.enqueue()
        env.drain()
        self.assertEqual(sorted(env.store.codes().values()), ["warc_target_mismatch", "warc_unsupported_warc_type"])
        self.assertEqual(len(env.store.observations), 3)
        self.assertEqual(len(list(env.bronze.root.rglob("*.warc.gz"))), 3)  # invalid records never stored

    def test_oversized_record_protection(self) -> None:
        env = Env(self, records=2, per_block=2, max_decompressed_record_bytes=FILTER.max_record_bytes)
        huge = make_warc_record("https://example.com/big", b"<title>x</title>" + b"\x00" * (8 * 1024 * 1024))
        name, ((offset, length),) = env.site.add_warc([huge], leaf=LEAF_B)
        env.blocks[0].append(make_cdx_line("https://example.com/big", name, offset, length, urlkey="b)/"))
        env.site._cluster.clear()
        env.site.add_index(env.blocks)
        self.assertLess(length, FILTER.max_record_bytes)  # passes the index filter, fails decompression
        (job_id,) = env.enqueue()
        env.drain()
        self.assertEqual(list(env.store.codes().values()), ["warc_decompression_limit"])
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)

    def test_corrupt_index_block_is_dead_lettered_and_skipped(self) -> None:
        env = Env(self)
        env.site._cluster.clear()
        env.site.add_index(env.blocks, raw_blocks={0: b"this is not gzip at all, but has length"})
        (job_id,) = env.enqueue()
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
        self.assertEqual(list(env.store.codes().values()), ["index_block_not_gzip"])
        self.assertEqual(set(env.store.observations), {env.oid(i) for i in (3, 4, 5)})

    def test_tampered_checkpoint_dead_letters_the_unit(self) -> None:
        for patch in (lambda c: c["blocks"].pop(), lambda c: c["filter"].update(languages=[]),
                      lambda c: c.update(cdx_key="cc-index/collections/CC-MAIN-2024-33/indexes/cdx-00009.gz"),
                      lambda c: c["blocks"][1].__setitem__(0, 1), lambda c: c.update(block=99)):
            with self.subTest():
                env = Env(self)
                (job_id,) = env.enqueue(patch)
                env.drain()
                self.assertEqual(env.job(job_id)["status"], JobStatus.DEAD_LETTER)
                self.assertEqual(env.queue.dead_letters[-1]["error_class"], "data_invalid")
                self.assertEqual(env.transport.requests, [])


class RecoveryTests(unittest.TestCase):
    def test_crash_and_lease_recovery_never_duplicate(self) -> None:
        env = Env(self, concurrency=1, batch_size=10)
        env.transport.faults["/" + env.warc] = [{}, {}, {"raise": Crash()}]
        (job_id,) = env.enqueue()
        with self.assertRaises(Crash):
            env.worker.run_once()
        self.assertEqual(env.job(job_id)["status"], JobStatus.RUNNING)
        self.assertEqual(len(env.store.observations), 0)  # batch not committed
        self.assertEqual(len(list(env.bronze.root.rglob("*.warc.gz"))), 2)  # evidence already written

        env.queue.advance(61)
        (recovered,) = env.worker.recover_expired_leases()
        self.assertEqual(recovered.status, JobStatus.RETRY_PENDING)
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
        self.assertEqual(len(env.store.observations), 6)
        self.assertEqual(len(list(env.bronze.root.rglob("*.warc.gz"))), 6)
        ranges = env.warc_requests()
        self.assertEqual(len(ranges), len(set(ranges)) + 1)  # only the crashed request was repeated
        self.assertEqual(env.metrics(job_id)["bronze_reused"], 2)
        self.assertEqual(env.queue.outcomes(job_id), ["lease_expired", "succeeded"])

    def test_graceful_shutdown_returns_unfinished_work(self) -> None:
        env = Env(self, concurrency=2, batch_size=10)

        def stop_after_two(path, _rng):
            if path == "/" + env.warc and len(env.warc_requests()) == 2:
                env.worker.request_stop()

        env.transport.on_request = stop_after_two
        (job_id,) = env.enqueue()
        env.worker.run_once()
        job = env.job(job_id)
        self.assertEqual((job["status"], job["attempt_count"]), (JobStatus.RETRY_PENDING, 0))
        self.assertEqual(env.queue.outcomes(job_id), ["cancelled"])
        committed = len(env.store.observations)
        self.assertEqual(job["checkpoint"]["line"], committed)
        self.assertGreaterEqual(committed, 1)

        env.transport.on_request = None
        env.worker.stop_event.clear()
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
        self.assertEqual(len(env.store.observations), 6)
        ranges = env.warc_requests()
        self.assertEqual(len(ranges), len(set(ranges)))  # in-flight results were kept in Bronze, not re-fetched


class ResourceTests(unittest.TestCase):
    def test_shared_rate_limit_state_blocks_before_any_request(self) -> None:
        env = Env(self)
        env.store.rate_limits[DATA_SCOPE] = env.queue.now + timedelta(seconds=300)
        (job_id,) = env.enqueue()
        env.worker.run_once()
        job = env.job(job_id)
        self.assertEqual((job["status"], job["deferral_count"], job["attempt_count"]), (JobStatus.RETRY_PENDING, 1, 0))
        self.assertGreaterEqual(job["next_attempt_at"], env.queue.now + timedelta(seconds=300))
        self.assertEqual(env.transport.requests, [])

    def test_disk_space_guard(self) -> None:
        env = Env(self, disk_free=10)
        (job_id,) = env.enqueue()
        env.worker.run_once()
        self.assertEqual(env.queue.attempts[-1]["error_class"], "resource_exhausted")
        self.assertEqual(env.transport.requests, [])

    def test_bounded_concurrency_and_backpressure(self) -> None:
        env = Env(self, records=40, per_block=40, concurrency=3, batch_size=40)
        state = {"started": 0, "during_first": None, "threads": 0}
        lock = threading.Lock()

        def slow_first(path, _rng):
            if path != "/" + env.warc:
                return
            with lock:
                state["started"] += 1
                first = state["started"] == 1
                state["threads"] = max(state["threads"], sum(t.name.startswith("cc-fetch")
                                                              for t in threading.enumerate()))
            if first:
                time.sleep(0.3)
                with lock:
                    state["during_first"] = state["started"]
            else:
                time.sleep(0.005)

        env.transport.on_request = slow_first
        (job_id,) = env.enqueue()
        env.drain()
        self.assertEqual(env.job(job_id)["status"], JobStatus.SUCCEEDED)
        self.assertLessEqual(env.transport.max_active, 3)
        self.assertLessEqual(state["threads"], 3)
        self.assertLessEqual(state["during_first"], env.config.window)  # nothing runs ahead of the window
        self.assertGreaterEqual(state["during_first"], 2)               # but fetches do overlap
        self.assertEqual(len(env.store.observations), 40)


if __name__ == "__main__":
    unittest.main()

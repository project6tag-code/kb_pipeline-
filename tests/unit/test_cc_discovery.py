"""Crawl discovery, crawl-window selection, streamed cluster.idx planning and idempotent enqueue (offline)."""

import contextlib
import json
import unittest

from app.config.settings import SettingsError
from app.queue.job_queue import EnqueueResult
from app.queue.states import ErrorClass, JobSource, JobType, Stage
from app.sources.commoncrawl.acquisition import CC_PARTITION_POLICY
from app.sources.commoncrawl.config import CCSettings, FilterConfig, PlanningConfig
from app.sources.commoncrawl.discovery import (
    CrawlPlanner,
    DiscoveryError,
    cluster_idx_key,
    parse_collinfo,
    plan_partitions,
    select_crawls,
)
from app.sources.commoncrawl.http import FetchError, HttpClient

from tests.unit.cc_fixtures import CRAWL, FakeCommonCrawl, FakeTransport, InMemoryCaptureStore

MAX_BLOCK = 16 * 1024 * 1024


def cluster(entries: list[tuple[str, int, int]]) -> list[bytes]:
    return [f"com,x)/{n} 2024\t{f}\t{o}\t{l}\t{n + 1}".encode() for n, (f, o, l) in enumerate(entries)]


def contiguous(cdx_file: str, lengths: list[int]) -> list[tuple[str, int, int]]:
    out, offset = [], 0
    for length in lengths:
        out.append((cdx_file, offset, length))
        offset += length
    return out


def plan(lines, **planning):
    return list(plan_partitions(lines, CRAWL, PlanningConfig(**planning), max_block_bytes=MAX_BLOCK))


class CollinfoTests(unittest.TestCase):
    def test_newest_first_and_non_crawl_entries_ignored(self) -> None:
        data = json.dumps([{"id": "CC-MAIN-2023-50"}, {"id": "CC-MAIN-2024-33"}, {"id": "CC-MAIN-2024-10"},
                           {"id": "CC-NEWS-2024"}, {"id": "../../x"}, {"name": "no id"}, "junk"]).encode()
        self.assertEqual(parse_collinfo(data), ["CC-MAIN-2024-33", "CC-MAIN-2024-10", "CC-MAIN-2023-50"])

    def test_invalid_collinfo(self) -> None:
        for data in (b"not json", b"{}", b"[]", json.dumps([{"id": "CC-NEWS"}]).encode(), b"\xff"):
            with self.subTest(data=data), self.assertRaises(DiscoveryError):
                parse_collinfo(data)

    def test_window_selection(self) -> None:
        available = ["CC-MAIN-2024-33", "CC-MAIN-2024-30", "CC-MAIN-2024-26"]
        self.assertEqual(select_crawls(available, PlanningConfig(crawl_window=2)), available[:2])
        self.assertEqual(select_crawls(available, PlanningConfig(crawl_window=10)), available)
        self.assertEqual(select_crawls(available, PlanningConfig(explicit_crawls=("CC-MAIN-2024-26",))),
                         ["CC-MAIN-2024-26"])
        with self.assertRaises(DiscoveryError):
            select_crawls(available, PlanningConfig(explicit_crawls=("CC-MAIN-1999-01",)))


class PartitionTests(unittest.TestCase):
    def test_aligned_groups_per_shard(self) -> None:
        lines = cluster(contiguous("cdx-00000.gz", [10] * 7) + contiguous("cdx-00001.gz", [20] * 2))
        specs = plan(lines, blocks_per_partition=3)
        self.assertEqual([(s.cdx_file, s.first_block, s.last_block) for s in specs],
                         [("cdx-00000.gz", 0, 2), ("cdx-00000.gz", 3, 5), ("cdx-00000.gz", 6, 6),
                          ("cdx-00001.gz", 0, 1)])
        self.assertEqual(specs[1].blocks, ((30, 10), (40, 10), (50, 10)))
        self.assertEqual([s.ordinal for s in specs], [0, 1, 2, 3])

    def test_identity_and_checkpoint(self) -> None:
        spec = plan(cluster(contiguous("cdx-00007.gz", [5, 6])), blocks_per_partition=10)[0]
        filter_config = FilterConfig()
        self.assertEqual(spec.source_identifier(filter_config),
                         f"{CRAWL}/cc-index/collections/{CRAWL}/indexes/cdx-00007.gz"
                         f"#filter={filter_config.version}#range=0-1")
        checkpoint = spec.initial_checkpoint(filter_config)
        self.assertEqual((checkpoint["v"], checkpoint["blocks"], checkpoint["block"], checkpoint["line"]),
                         (1, [[0, 5], [5, 6]], 0, 0))
        self.assertEqual(json.dumps(checkpoint["filter"], sort_keys=True, separators=(",", ":")),
                         filter_config.canonical())

    def test_deterministic_sampling(self) -> None:
        lines = cluster(contiguous("cdx-00000.gz", [1] * 20))
        self.assertEqual([s.first_block for s in plan(lines, blocks_per_partition=2, partition_stride=3)],
                         [0, 6, 12, 18])
        self.assertEqual([s.first_block for s in plan(lines, blocks_per_partition=2, max_partitions_per_crawl=2)],
                         [0, 2])
        self.assertEqual(plan(lines, blocks_per_partition=2, partition_stride=3),
                         plan(lines, blocks_per_partition=2, partition_stride=3))

    def test_max_partitions_stops_streaming(self) -> None:
        consumed = []

        def lines():
            for line in cluster(contiguous("cdx-00000.gz", [1] * 1000)):
                consumed.append(line)
                yield line

        plan(lines(), blocks_per_partition=2, max_partitions_per_crawl=1)
        self.assertLess(len(consumed), 10)

    def test_malformed_or_inconsistent_cluster_idx_stops_planning(self) -> None:
        good = contiguous("cdx-00000.gz", [10, 10])
        cases = {
            "gap": cluster([good[0], ("cdx-00000.gz", 15, 10)]),
            "not contiguous files": cluster(good + contiguous("cdx-00001.gz", [5]) + [("cdx-00000.gz", 20, 10)]),
            "bad file name": cluster([("../cdx-00000.gz", 0, 10)]),
            "bad offset": [b"k 1\tcdx-00000.gz\t-1\t10\t1"],
            "too few fields": [b"k 1\tcdx-00000.gz"],
            "block too large": cluster([("cdx-00000.gz", 0, MAX_BLOCK + 1)]),
            "zero length": cluster([("cdx-00000.gz", 0, 0)]),
        }
        for name, lines in cases.items():
            with self.subTest(case=name), self.assertRaises(DiscoveryError):
                plan(lines)

    def test_blank_lines_ignored(self) -> None:
        lines = cluster(contiguous("cdx-00000.gz", [10, 10]))
        self.assertEqual(len(plan([b""] + lines + [b"  "], blocks_per_partition=1)), 2)


class FakePlannerQueue:
    def __init__(self) -> None:
        self.jobs: dict[tuple[str, str], object] = {}

    def transaction(self):
        return contextlib.nullcontext(self)

    def enqueue_many(self, specs):
        results = []
        for spec in specs:
            key = (spec.job_type.value, spec.source_identifier)
            created = key not in self.jobs
            self.jobs.setdefault(key, spec)
            results.append(EnqueueResult(len(self.jobs), created, "pending"))
        return results


class PlannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.site = FakeCommonCrawl()
        self.site.add_index([[b"a"], [b"b"], [b"c"]], cdx_file="cdx-00000.gz")
        self.site.add_index([[b"d"]], cdx_file="cdx-00001.gz")
        self.site.crawls.append("CC-MAIN-2024-30")
        self.site.put_collinfo()
        self.transport = FakeTransport(self.site)
        self.queue = FakePlannerQueue()
        self.store = InMemoryCaptureStore()
        self.planner = CrawlPlanner(HttpClient(transport=self.transport), self.queue, self.store,
                                    planning=PlanningConfig(blocks_per_partition=2), filter_config=FilterConfig(),
                                    policy=CC_PARTITION_POLICY, max_block_bytes=MAX_BLOCK)

    def test_plan_latest_crawl_and_replan_is_idempotent(self) -> None:
        report = self.planner.plan()
        (crawl,) = report.crawls
        self.assertEqual((crawl.crawl_id, crawl.crawl_registered, crawl.partitions, crawl.created, crawl.existing),
                         (CRAWL, True, 3, 3, 0))
        spec = next(iter(self.queue.jobs.values()))
        self.assertEqual((spec.job_type, spec.source, spec.stage, spec.max_attempts),
                         (JobType.CC_INDEX_PARTITION, JobSource.COMMON_CRAWL, Stage.DISCOVERED,
                          CC_PARTITION_POLICY.max_attempts))
        again = self.planner.plan().crawls[0]
        self.assertEqual((again.crawl_registered, again.created, again.existing), (False, 0, 3))
        self.assertEqual(len(self.queue.jobs), 3)
        self.assertEqual(self.store.crawls, {CRAWL})

    def test_unlisted_crawl_is_never_registered(self) -> None:
        with self.assertRaises(DiscoveryError):
            self.planner.plan(["CC-MAIN-1999-01"])
        self.assertEqual(self.store.crawls, set())
        self.assertEqual(self.transport.count("cluster.idx"), 0)

    def test_rate_limited_discovery_raises_classified(self) -> None:
        self.transport.faults["/collinfo.json"].append({"status": 503, "headers": {"Retry-After": "60"}})
        with self.assertRaises(FetchError) as ctx:
            self.planner.plan()
        self.assertEqual((ctx.exception.error_class, ctx.exception.retry_after_seconds), (ErrorClass.RATE_LIMITED, 60))
        self.assertEqual(self.queue.jobs, {})

    def test_cluster_idx_is_streamed_not_fetched_by_range(self) -> None:
        self.planner.plan()
        self.assertEqual([r for p, r in self.transport.requests if p.endswith(cluster_idx_key(CRAWL))], [None])


class ConfigTests(unittest.TestCase):
    def test_env_defaults_and_overrides(self) -> None:
        default = CCSettings.from_env({})
        self.assertEqual((default.planning.crawl_window, default.acquisition.fetch_concurrency,
                          default.acquisition.batch_size, default.acquisition.window), (1, 4, 50, 8))
        custom = CCSettings.from_env({"KB_CC_CRAWL_WINDOW": "3", "KB_CC_FETCH_CONCURRENCY": "2",
                                      "KB_CC_LANGUAGES": "", "KB_CC_MIME_TYPES": "TEXT/HTML, text/plain",
                                      "KB_CC_CRAWLS": "CC-MAIN-2024-33", "KB_CC_BRONZE_ROOT": "D:/bronze"})
        self.assertEqual((custom.planning.crawl_window, custom.acquisition.fetch_concurrency,
                          custom.http.max_connections_per_host), (3, 2, 2))
        self.assertEqual(custom.acquisition.filter.languages, ())
        self.assertEqual(custom.acquisition.filter.mime_types, ("text/html", "text/plain"))
        self.assertEqual(custom.planning.explicit_crawls, ("CC-MAIN-2024-33",))
        self.assertNotEqual(custom.acquisition.filter.version, default.acquisition.filter.version)

    def test_bounds_are_enforced(self) -> None:
        for env in ({"KB_CC_FETCH_CONCURRENCY": "0"}, {"KB_CC_FETCH_CONCURRENCY": "1000"},
                    {"KB_CC_CRAWL_WINDOW": "x"}, {"KB_CC_CRAWLS": "latest"}, {"KB_CC_LANGUAGES": "english"},
                    {"KB_CC_MIME_TYPES": "text"}, {"KB_CC_REJECT_TRUNCATED": "maybe"},
                    {"KB_CC_MAX_RECORD_BYTES": "999999999"}, {"KB_CC_READ_TIMEOUT_SECONDS": "0"},
                    {"KB_CC_USER_AGENT": "bad\nagent"}):
            with self.subTest(env=env), self.assertRaises(SettingsError):
                CCSettings.from_env(env)


if __name__ == "__main__":
    unittest.main()

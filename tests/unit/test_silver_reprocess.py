"""Silver reprocess operator path: scoping, safe default, force semantics, bounded batches, DB refusal."""

import contextlib
import unittest
import uuid
from collections import Counter

from app.processing.silver import reprocess as rp
from app.processing.silver.model import SourceRef


def ref(n: int) -> SourceRef:
    return SourceRef(uuid.UUID(int=n), "common_crawl", f"https://e.test/{n}", crawl_id="CC-MAIN-2013-20",
                     warc_filename="f.warc.gz", warc_offset=n, warc_length=10)


class FakeDb:
    @contextlib.contextmanager
    def transaction(self):
        yield object()


class FakeProcessor:
    extraction_version = "x-test"

    def __init__(self) -> None:
        self.persisted: list[int] = []
        self.evaluator = self

    def evaluate(self, source_ref):
        return source_ref

    def persist(self, conn, prepared, *, job_id, attempt_count):
        self.persisted.extend(int(ev.observation_id) for ev in prepared.evaluations)
        return Counter(silver_already_extracted=len(prepared.evaluations))


def pager(total: int, calls: list):
    def select(conn, channel, scope, version, after, limit):
        calls.append((channel, after, limit))
        if channel != "cc":
            return []
        start = int(uuid.UUID(after))
        return [ref(n) for n in range(start + 1, min(start + limit, total) + 1)]
    return select


class QueryTests(unittest.TestCase):
    def test_default_selects_only_observations_without_extraction(self) -> None:
        sql, params = rp.build_query("cc", rp.Scope(), "x1", rp.MIN_UUID, 50)
        self.assertIn("NOT EXISTS", sql)
        self.assertEqual(params["version"], "x1")

    def test_force_selects_all_and_scopes_apply(self) -> None:
        oid = uuid.uuid4()
        sql, params = rp.build_query("cc", rp.Scope(source="cc", force=True, observation_ids=(oid,),
                                                    crawl_id="CC-MAIN-2013-20"), "x1", rp.MIN_UUID, 5)
        self.assertNotIn("NOT EXISTS", sql)
        self.assertEqual(params["ids"], [str(oid)])
        self.assertEqual(params["crawl_id"], "CC-MAIN-2013-20")
        sql, params = rp.build_query("github", rp.Scope(source="github", repository_id=7), "x1", rp.MIN_UUID, 5)
        self.assertEqual(params["repository_id"], 7)
        self.assertNotIn("INSERT", sql.upper())

    def test_invalid_scopes_are_refused(self) -> None:
        for kwargs in ({"source": "x"}, {"batch_size": 0}, {"batch_size": rp.MAX_BATCH + 1}, {"limit": 0},
                       {"source": "github", "crawl_id": "CC-MAIN-2013-20"},
                       {"source": "cc", "repository_id": 1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(rp.ReprocessError):
                rp.Scope(**kwargs)

    def test_source_selection_channels(self) -> None:
        self.assertEqual(rp.Scope().channels(), ["cc", "github"])
        self.assertEqual(rp.Scope(source="cc").channels(), ["cc"])
        self.assertEqual(rp.Scope(crawl_id="CC-MAIN-2013-20").channels(), ["cc"])
        self.assertEqual(rp.Scope(repository_id=3).channels(), ["github"])


class RunTests(unittest.TestCase):
    def test_batches_are_bounded_and_every_observation_is_visited_once(self) -> None:
        calls: list = []
        processor = FakeProcessor()
        summary = rp.reprocess(FakeDb(), processor, rp.Scope(batch_size=4), page_selector=pager(10, calls))
        self.assertEqual(processor.persisted, list(range(1, 11)))
        self.assertTrue(all(limit <= 4 for _, _, limit in calls))
        self.assertEqual(summary["counts"]["evaluated"], 10)
        self.assertFalse(summary["force"])

    def test_limit_stops_the_run(self) -> None:
        processor = FakeProcessor()
        summary = rp.reprocess(FakeDb(), processor, rp.Scope(batch_size=4, limit=6), page_selector=pager(100, []))
        self.assertEqual(len(processor.persisted), 6)
        self.assertEqual(summary["counts"]["evaluated"], 6)

    def test_stop_request_ends_between_batches(self) -> None:
        processor = FakeProcessor()
        stops = iter([False, True, True, True])
        summary = rp.reprocess(FakeDb(), processor, rp.Scope(batch_size=2), should_stop=lambda: next(stops),
                               page_selector=pager(100, []))
        self.assertEqual(processor.persisted, [1, 2])
        self.assertTrue(summary["stopped_early"])

    def test_empty_selection_is_a_noop(self) -> None:
        processor = FakeProcessor()
        summary = rp.reprocess(FakeDb(), processor, rp.Scope(), page_selector=pager(0, []))
        self.assertEqual(processor.persisted, [])
        self.assertEqual(summary["counts"], {})


class DatabaseGuardTests(unittest.TestCase):
    def test_other_databases_are_refused(self) -> None:
        class Conn:
            def __init__(self, name):
                self.name = name

            def execute(self, *_a):
                return self

            def scalar_one(self):
                return self.name

        self.assertEqual(rp.verify_database(Conn("kb_pipeline_test"), "kb_pipeline_test"), "kb_pipeline_test")
        for actual, expect in (("data_collection_pipeline", "data_collection_pipeline"),
                               ("kb_pipeline_test", "other"), ("other", "kb_pipeline_test")):
            with self.subTest(actual=actual, expect=expect), self.assertRaises(rp.ReprocessError):
                rp.verify_database(Conn(actual), expect)


if __name__ == "__main__":
    unittest.main()

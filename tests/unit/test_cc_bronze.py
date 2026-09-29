"""Bronze evidence store: layout, atomic no-clobber writes, corruption, conflicts, traversal, disk guard."""

import json
import os
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

from app.sources.commoncrawl.bronze import (
    BRONZE_FORMAT,
    BronzeConflict,
    BronzeCorrupt,
    BronzeState,
    BronzeStore,
    DiskSpaceError,
    UnsafePath,
    sha256_hex,
)

CRAWL = "CC-MAIN-2024-33"
OID = uuid.UUID("afb1a4e1-acd0-5e13-96e2-94eb6aec8299")
RECORD = b"\x1f\x8b" + b"record bytes" * 100
MANIFEST = {"source_type": "common_crawl", "crawl_id": CRAWL, "retrieved_at": "2026-09-29T00:00:00+00:00"}


class BronzeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "bronze"
        self.store = BronzeStore(self.root)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_deterministic_layout(self) -> None:
        record_path, manifest_path = self.store.paths(CRAWL, OID)
        self.assertEqual(record_path, Path(os.path.abspath(self.root)) / CRAWL / "af" / "b1" / f"{OID}.warc.gz")
        self.assertEqual(manifest_path, record_path.with_name(f"{OID}.json"))

    def test_store_and_read_back(self) -> None:
        result = self.store.store(CRAWL, OID, RECORD, MANIFEST)
        self.assertTrue(result.created)
        self.assertEqual((result.record_sha256, result.record_bytes), (sha256_hex(RECORD), len(RECORD)))
        data, manifest = self.store.read_verified(CRAWL, OID)
        self.assertEqual(data, RECORD)
        self.assertEqual((manifest["format"], manifest["observation_id"], manifest["record_sha256"]),
                         (BRONZE_FORMAT, str(OID), sha256_hex(RECORD)))
        self.assertEqual(self.store.inspect(CRAWL, OID).state, BronzeState.COMPLETE)
        leftovers = [p for p in self.root.rglob("*") if ".tmp-" in p.name]
        self.assertEqual(leftovers, [])

    def test_repeat_store_is_idempotent_and_never_rewrites(self) -> None:
        self.store.store(CRAWL, OID, RECORD, MANIFEST)
        record_path, manifest_path = self.store.paths(CRAWL, OID)
        before = (record_path.stat().st_mtime_ns, manifest_path.read_bytes())
        again = self.store.store(CRAWL, OID, RECORD, dict(MANIFEST, retrieved_at="2030-01-01T00:00:00+00:00"))
        self.assertFalse(again.created)
        self.assertEqual((record_path.stat().st_mtime_ns, manifest_path.read_bytes()), before)

    def test_different_bytes_for_the_same_identity_conflict(self) -> None:
        self.store.store(CRAWL, OID, RECORD, MANIFEST)
        with self.assertRaises(BronzeConflict):
            self.store.store(CRAWL, OID, RECORD + b"!", MANIFEST)
        self.assertEqual(self.store.read_verified(CRAWL, OID)[0], RECORD)

    def test_damaged_record_is_detected_and_repaired_only_with_attested_bytes(self) -> None:
        self.store.store(CRAWL, OID, RECORD, MANIFEST)
        record_path, _ = self.store.paths(CRAWL, OID)
        record_path.write_bytes(RECORD[:-5])  # bit rot / partial overwrite
        self.assertEqual(self.store.inspect(CRAWL, OID).state, BronzeState.RECORD_DAMAGED)
        self.assertIsNone(self.store.read_verified(CRAWL, OID))
        with self.assertRaises(BronzeConflict):
            self.store.store(CRAWL, OID, b"other", MANIFEST)
        repaired = self.store.store(CRAWL, OID, RECORD, MANIFEST)
        self.assertTrue(repaired.repaired)
        self.assertEqual(self.store.read_verified(CRAWL, OID)[0], RECORD)
        record_path.unlink()
        self.assertEqual(self.store.inspect(CRAWL, OID).state, BronzeState.RECORD_DAMAGED)
        self.assertTrue(self.store.store(CRAWL, OID, RECORD, MANIFEST).repaired)

    def test_crash_between_record_and_manifest(self) -> None:
        record_path, manifest_path = self.store.paths(CRAWL, OID)
        record_path.parent.mkdir(parents=True)
        record_path.write_bytes(RECORD)
        self.assertEqual(self.store.inspect(CRAWL, OID).state, BronzeState.RECORD_ONLY)
        self.assertIsNone(self.store.read_verified(CRAWL, OID))  # not committed evidence
        with self.assertRaises(BronzeConflict):
            self.store.store(CRAWL, OID, RECORD + b"?", MANIFEST)
        self.assertTrue(self.store.store(CRAWL, OID, RECORD, MANIFEST).created)
        self.assertTrue(manifest_path.exists())

    def test_invalid_manifest_is_corrupt(self) -> None:
        self.store.store(CRAWL, OID, RECORD, MANIFEST)
        _, manifest_path = self.store.paths(CRAWL, OID)
        for content in (b"{not json", b"[]", json.dumps({"format": "other"}).encode(), b"x" * (2 * 1024 * 1024)):
            manifest_path.write_bytes(content)
            with self.subTest(content=content[:20]), self.assertRaises(BronzeCorrupt):
                self.store.inspect(CRAWL, OID)

    def test_manifest_for_another_observation_is_corrupt(self) -> None:
        other = uuid.uuid4()
        self.store.store(CRAWL, OID, RECORD, MANIFEST)
        _, manifest_path = self.store.paths(CRAWL, OID)
        body = json.loads(manifest_path.read_text())
        body["observation_id"] = str(other)
        manifest_path.write_text(json.dumps(body))
        with self.assertRaises(BronzeCorrupt):
            self.store.inspect(CRAWL, OID)

    def test_path_traversal_is_impossible(self) -> None:
        for crawl, oid in (("../../etc", OID), ("CC-MAIN-2024-33/../..", OID), ("CC-MAIN-2024-33\\..", OID),
                           ("", OID), (CRAWL, "../../x"), (CRAWL, "not-a-uuid"), (CRAWL, "/abs/path")):
            with self.subTest(crawl=crawl, oid=oid), self.assertRaises(UnsafePath):
                self.store.paths(crawl, oid)

    def test_disk_space_guard(self) -> None:
        store = BronzeStore(self.root, min_free_bytes=10_000,
                            disk_usage=lambda _p: SimpleNamespace(free=10_000 + len(RECORD) - 1))
        with self.assertRaises(DiskSpaceError):
            store.store(CRAWL, OID, RECORD, MANIFEST)
        self.assertFalse(self.root.exists() and any(self.root.rglob("*.warc.gz")))

    def test_sweep_removes_only_old_temporary_files(self) -> None:
        self.store.store(CRAWL, OID, RECORD, MANIFEST)
        record_path, _ = self.store.paths(CRAWL, OID)
        old = record_path.parent / f".{record_path.name}.tmp-deadbeef"
        new = record_path.parent / f".{record_path.name}.tmp-cafebabe"
        old.write_bytes(b"partial")
        new.write_bytes(b"partial")
        past = time.time() - 7200
        os.utime(old, (past, past))
        self.assertEqual(self.store.sweep_temp_files(older_than_seconds=3600), 1)
        self.assertFalse(old.exists())
        self.assertTrue(new.exists())
        self.assertEqual(self.store.read_verified(CRAWL, OID)[0], RECORD)

    def test_concurrent_writers_produce_one_object(self) -> None:
        results, errors = [], []

        def write() -> None:
            try:
                results.append(self.store.store(CRAWL, OID, RECORD, MANIFEST))
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=write) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(sum(r.created for r in results), 1)
        self.assertEqual(self.store.read_verified(CRAWL, OID)[0], RECORD)
        self.assertEqual(len(list(self.root.rglob("*.warc.gz"))), 1)


if __name__ == "__main__":
    unittest.main()

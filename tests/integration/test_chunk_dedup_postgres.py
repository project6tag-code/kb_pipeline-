"""Opt-in PostgreSQL validation of Global Chunk Near-Dedup V1 (NO network; kb_pipeline_test ONLY).

    KB_RUN_PG_INTEGRATION=1
    KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m unittest tests.integration.test_chunk_dedup_postgres -v

Prerequisites (once, migrator):
    python -m app.processing.silver.cli register-methods --expect-database kb_pipeline_test
    python -m app.processing.chunking.cli register-version --expect-database kb_pipeline_test
    python -m app.processing.chunk_dedup.cli register-method --expect-database kb_pipeline_test

Runs the deterministic near-dedup pilot (idempotent: permanent rows, a rerun
adds nothing), then verifies the persisted state. Every concurrency, crash and
guard probe runs in transactions that are ROLLED BACK: they leave no rows, and
side-index entries are compensated.
"""

import os
import threading
import unittest
import uuid

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_INTEGRATION_DATABASE_URL"))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_INTEGRATION_DATABASE_URL (kb_pipeline_test) to run")
class ChunkDedupPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from tests.integration import chunk_dedup_pilot as pilot
        from tests.integration.chunk_pilot import run_backfill_pilot

        cls.pilot = pilot
        cls.db = pilot.open_database()
        pilot.run_acquisition(cls.db)
        run_backfill_pilot(cls.db)
        pilot.run_dedup(cls.db)
        pilot.run_incremental_acquisition(cls.db)
        pilot.run_concurrent_dedup()
        cls.state = pilot.dedup_state(cls.db)

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "db", None) is not None:
            cls.db.dispose()

    def method_id(self) -> int:
        return self.state["dedup_method_id"]

    def row(self, case: str) -> dict:
        return self.state["cases"][case][0]

    def test_pilot_expectations_and_every_invariant(self) -> None:
        s = self.state
        self.assertEqual(s["expectations"], [])
        self.assertEqual((s["method_rows"], s["method_parameters_frozen"]), (1, True))
        self.assertEqual({k: v for k, v in s["audit"].items() if k.endswith("_violations")},
                         {k: 0 for k in s["audit"] if k.endswith("_violations")})
        self.assertEqual((s["audit"]["chunks_without_decision"], s["duplicate_decision_keys"],
                          s["duplicate_pairs_any_direction"], s["chunk_dedup_dead_letters"]), (0, 0, 0, 0))
        self.assertTrue(all(r["run_kind"] == "reprocess" and r["code_version"].startswith("chunk_minhash_lsh:")
                            for r in s["decision_runs"]))

    def test_rerun_adds_nothing(self) -> None:
        before = self.pilot.counts(self.state)
        self.assertEqual(self.pilot.run_dedup(self.db)["chunks"], 0)
        after_state = self.pilot.dedup_state(self.db)
        self.assertEqual(self.pilot.counts(after_state), before)
        self.assertEqual(after_state["representative_map"], self.state["representative_map"])
        self.assertEqual(after_state["edges"], self.state["edges"])

    def test_suppressed_chunk_keeps_full_provenance_and_maps_to_its_representative(self) -> None:
        from app.processing.chunk_dedup import eligibility

        suppressed = next(self.row(n) for n in self.pilot.NEAR_CLUSTER if self.row(n)["classification"] ==
                          "NEAR_DUPLICATE" and n == "cc_dam")
        representative = suppressed["representative"]
        cc = self.pilot.provenance(self.db, suppressed["chunk_id"])
        gh = self.pilot.provenance(self.db, representative)
        self.assertEqual([r["source_channel"] for r in cc], ["common_crawl"])
        self.assertTrue(cc[0]["warc_filename"].endswith(self.pilot.PILOT_WARC_LEAF))
        self.assertEqual(cc[0]["document_status"], "accepted")
        self.assertEqual({(r["repository_id"], r["file_path"]) for r in gh}, {(self.pilot.REPO_A, "README.md")})
        with self.db.transaction() as conn:
            status = eligibility.eligibility(conn, self.method_id(), [uuid.UUID(suppressed["chunk_id"]),
                                                                      uuid.UUID(representative)])
            eligible = set(eligibility.eligible_chunks(conn, self.method_id(), limit=100_000))
            suppressed_rows = {c: r for c, r, _s in eligibility.suppressed_chunks(conn, self.method_id(),
                                                                                  limit=100_000)}
        self.assertEqual(status[uuid.UUID(suppressed["chunk_id"])].embed_chunk_id, uuid.UUID(representative))
        self.assertTrue(status[uuid.UUID(representative)].eligible)
        self.assertIn(uuid.UUID(representative), eligible)
        self.assertNotIn(uuid.UUID(suppressed["chunk_id"]), eligible)
        self.assertEqual(suppressed_rows[uuid.UUID(suppressed["chunk_id"])], uuid.UUID(representative))

    def test_database_refuses_chains_and_duplicate_decisions(self) -> None:
        from sqlalchemy import text

        from app.processing.chunk_dedup import repository

        suppressed = self.row("cc_dam")["chunk_id"]
        target = self.row("unique_wheat")["chunk_id"]
        with self.db.engine.connect() as conn:
            try:
                run_id = conn.execute(text("SELECT run_id FROM kbp.chunk_dedup_decisions LIMIT 1")).scalar_one()
                chain = conn.execute(text(repository.INSERT_DECISION_SQL), {
                    "chunk_id": target, "method_id": self.method_id(), "classification": "NEAR_DUPLICATE",
                    "representative": suppressed, "similarity": 0.9, "run_id": run_id}).one_or_none()
                duplicate = conn.execute(text(repository.INSERT_DECISION_SQL), {
                    "chunk_id": target, "method_id": self.method_id(), "classification": "UNIQUE",
                    "representative": None, "similarity": None, "run_id": run_id}).one_or_none()
            finally:
                conn.rollback()
        self.assertIsNone(chain)  # representative is itself suppressed: refused (no chains, no cycles)
        self.assertIsNone(duplicate)  # PK: one decision per chunk per method

    def test_writers_serialize_on_the_method_lock(self) -> None:
        from sqlalchemy import text

        from app.processing.chunk_dedup.repository import PostgresChunkDedupStore

        store = PostgresChunkDedupStore()
        first, second = self.db.engine.connect(), self.db.engine.connect()
        result: dict = {}
        try:
            store.lock_method(first, self.method_id())

            def contender() -> None:
                second.execute(text("SET LOCAL lock_timeout = '20s'"))
                store.lock_method(second, self.method_id())
                result["acquired"] = True

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join(1.0)
            self.assertTrue(thread.is_alive(), "a second writer must wait for the first")
            first.rollback()
            thread.join(20)
            self.assertTrue(result.get("acquired"))
        finally:
            second.rollback()
            first.close()
            second.close()

    def test_crashed_batch_leaves_no_decision_and_no_index_entry(self) -> None:
        from sqlalchemy import text

        from app.processing.chunk_dedup.index import CandidateIndex, IndexIdentity
        from app.processing.chunk_dedup.repository import PostgresChunkDedupStore
        from app.processing.chunk_dedup.service import ChunkNearDeduplicator, run_backfill
        from app.utils.ids import chunk_content_hash, chunk_id_from_hash

        body = f"Crash probe {uuid.uuid4()} describes a temporary chunk that exists only inside a rolled back batch."
        digest = chunk_content_hash(body)
        chunk_id = chunk_id_from_hash(digest)
        with self.db.transaction() as conn:
            doc_id, obs_id = conn.execute(text("SELECT document_id, first_observation_id FROM kbp.document_registry "
                                               "WHERE status = 'accepted' LIMIT 1")).one()
            version_id = conn.execute(text("SELECT chunking_version_id FROM kbp.chunking_versions LIMIT 1")).scalar_one()
            run_id = conn.execute(text("SELECT run_id FROM kbp.chunk_dedup_decisions LIMIT 1")).scalar_one()

        class Crash(RuntimeError):
            pass

        index = CandidateIndex(self.pilot.index_path(), IndexIdentity(self.pilot.DATABASE, self.method_id()))
        items_before = index.item_count()
        dedup = ChunkNearDeduplicator(PostgresChunkDedupStore(), index, run_id=run_id)
        conn = self.db.engine.connect()

        class Tx:  # one transaction that inserts the probe chunk, is decided, then crashes before commit
            def __enter__(self):
                conn.execute(text("INSERT INTO kbp.chunk_registry (chunk_id, chunk_hash, chunk_text, char_length, "
                                  "token_count, chunking_version_id, first_document_id, representative_observation_id) "
                                  "VALUES (:c, :h, :t, :n, 20, :v, :d, :o)"),
                             {"c": chunk_id, "h": bytes.fromhex(digest), "t": body, "n": len(body), "v": version_id,
                              "d": doc_id, "o": obs_id})
                return conn

            def __exit__(self, exc_type, exc, tb):
                conn.rollback()
                return False

        original = dedup.process_batch

        def crash_after_deciding(c, **kwargs):
            chunks, counts = original(c, **kwargs)
            self.assertEqual(counts["decision_unique"], 1)
            self.assertEqual(index.item_count(), items_before + 1)  # indexed before the commit
            raise Crash("crash after the decision, before commit")

        dedup.process_batch = crash_after_deciding
        try:
            with self.assertRaises(Crash):
                run_backfill(lambda: Tx(), dedup, limit=1, batch_size=1, is_transient=lambda _e: False)
        finally:
            conn.close()
        self.assertEqual(index.item_count(), items_before)  # compensated
        index.close()
        with self.db.transaction() as check:
            self.assertEqual(check.execute(text("SELECT count(*) FROM kbp.chunk_dedup_decisions WHERE chunk_id = :c"),
                                           {"c": chunk_id}).scalar_one(), 0)
            self.assertEqual(check.execute(text("SELECT count(*) FROM kbp.chunk_registry WHERE chunk_id = :c"),
                                           {"c": chunk_id}).scalar_one(), 0)


if __name__ == "__main__":
    unittest.main()

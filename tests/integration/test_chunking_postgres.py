"""Opt-in PostgreSQL validation of Chunking V1 (NO network; kb_pipeline_test ONLY).

    KB_RUN_PG_INTEGRATION=1
    KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m unittest tests.integration.test_chunking_postgres -v

Prerequisites (once, migrator; see docs/chunking_v1.md):
    python -m app.processing.silver.cli register-methods --expect-database kb_pipeline_test
    python -m app.processing.chunking.cli register-version --expect-database kb_pipeline_test

Runs the deterministic chunk pilot (inline Silver+Chunking path and the backfill),
then verifies the persisted chunks. Concurrency and re-chunk checks run in
transactions that are ROLLED BACK: they leave no rows.
"""

import os
import threading
import unittest
import uuid

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_INTEGRATION_DATABASE_URL"))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_INTEGRATION_DATABASE_URL (kb_pipeline_test) to run")
class ChunkingPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from tests.integration import chunk_pilot

        cls.pilot = chunk_pilot
        cls.db = chunk_pilot.open_database()
        chunk_pilot.run_inline(cls.db)
        chunk_pilot.run_backfill_pilot(cls.db)
        cls.state = chunk_pilot.chunk_state(cls.db)

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "db", None) is not None:
            cls.db.dispose()

    def test_persisted_chunks_satisfy_every_invariant(self) -> None:
        s = self.state
        self.assertEqual(s["invariant_problems"], [])  # offsets, hashes, ids, token counts, recomputed order
        self.assertEqual((s["chunking_versions_with_this_name"], s["chunk_registry_duplicate_hashes"],
                          s["orphan_occurrences"], s["chunks_without_occurrence"]), (1, 0, 0, 0))
        # A database holding real data may contain accepted documents that legitimately yield no chunk; each one
        # must be explained by exactly one chunking dead letter (none in a pilot-only database).
        self.assertEqual(s["accepted_not_chunked"], s["chunking_dead_letters"])
        self.assertGreater(s["chunk_occurrences"], s["chunk_registry"] - 1)
        low, _avg, high = s["token_count_min_avg_max"]
        self.assertTrue(12 <= low and high <= 400)

    def test_cross_source_chunk_is_one_row_with_full_lineage(self) -> None:
        from sqlalchemy import text

        from app.utils.ids import chunk_id_from_text

        shared = chunk_id_from_text(self.pilot.SHARED_CHUNK)
        with self.db.transaction() as conn:
            rows = conn.execute(text(
                "SELECT o.document_id, so.source_channel, w.crawl_id, w.warc_filename, cc.warc_record_offset, "
                "g.repository_id, g.commit_sha, g.file_path, g.blob_sha "
                "FROM kbp.chunk_registry c "
                "JOIN kbp.chunk_occurrences o USING (chunk_id) "
                "JOIN kbp.observation_extractions e ON e.document_id = o.document_id "
                "JOIN kbp.source_observations so ON so.observation_id = e.observation_id "
                "LEFT JOIN kbp.cc_captures cc ON cc.observation_id = so.observation_id "
                "LEFT JOIN kbp.cc_warc_files w ON w.warc_file_id = cc.warc_file_id "
                "LEFT JOIN kbp.github_file_observations g ON g.observation_id = so.observation_id "
                "WHERE c.chunk_id = :c ORDER BY so.source_channel"), {"c": shared}).mappings().all()
            registry_rows = conn.execute(text("SELECT count(*) FROM kbp.chunk_registry WHERE chunk_id = :c"),
                                         {"c": shared}).scalar_one()
        self.assertEqual(registry_rows, 1)
        self.assertEqual([r["source_channel"] for r in rows], ["common_crawl", "github"])
        self.assertEqual(len({r["document_id"] for r in rows}), 2)
        cc, gh = rows
        self.assertTrue(cc["warc_filename"].endswith(self.pilot.PILOT_WARC_LEAF))  # Bronze coordinates reachable
        self.assertEqual((gh["repository_id"], gh["file_path"]), (self.pilot.PILOT_REPO, "README.md"))

    def test_rechunking_every_document_is_a_verified_no_op(self) -> None:
        from sqlalchemy import text

        from app.processing.chunking.repository import PostgresChunkStore, SilverDocument
        from app.processing.chunking.service import ChunkWriter

        writer = ChunkWriter(PostgresChunkStore())
        with self.db.engine.connect() as conn:
            try:
                docs = [SilverDocument.from_row(r) for r in conn.execute(text(
                    "SELECT document_id, content_hash, canonical_text, char_length, status, status_reason, "
                    "first_observation_id FROM kbp.document_registry WHERE status = 'accepted'")).mappings()]
                results = [writer.chunk_accepted(conn, d) for d in docs]
            finally:
                conn.rollback()
        self.assertTrue(docs)
        # chunk_rejected_* counters may accompany either outcome; only these two are the document outcomes.
        already = sum("chunk_document_already_chunked" in r for r in results)
        empty = sum("chunk_document_no_accepted_chunks" in r for r in results)
        self.assertEqual(already + empty, len(docs))
        self.assertEqual(empty, self.state["chunking_dead_letters"])

    def test_concurrent_insert_of_one_chunk_waits_and_never_duplicates(self) -> None:
        from sqlalchemy import text

        from app.processing.chunking.repository import ChunkRow, PostgresChunkStore
        from app.utils.ids import chunk_content_hash, chunk_id_from_hash

        store = PostgresChunkStore()
        body = f"Concurrency probe {uuid.uuid4()} for the chunk registry unique index."
        digest = chunk_content_hash(body)
        with self.db.transaction() as conn:
            doc_id, obs_id = conn.execute(text("SELECT document_id, first_observation_id FROM kbp.document_registry "
                                               "WHERE status = 'accepted' LIMIT 1")).one()
            version_id = conn.execute(text("SELECT chunking_version_id FROM kbp.chunking_versions LIMIT 1")).scalar_one()
        row = ChunkRow(chunk_id_from_hash(digest), bytes.fromhex(digest), body, 12, version_id, doc_id, obs_id)
        result: dict = {}
        first = self.db.engine.connect()
        second = self.db.engine.connect()
        try:
            self.assertTrue(store.insert_chunk(first, row))  # T1 holds the new key, uncommitted

            def contender() -> None:
                second.execute(text("SET LOCAL lock_timeout = '20s'"))
                result["inserted"] = store.insert_chunk(second, row)  # blocks on T1's key

            thread = threading.Thread(target=contender)
            thread.start()
            thread.join(1.0)
            self.assertTrue(thread.is_alive(), "T2 must wait for T1 on the unique index")
            first.rollback()  # T1 rolls back: T2 must not lose the chunk
            thread.join(20)
            self.assertTrue(result.get("inserted"))
            count = second.execute(text("SELECT count(*) FROM kbp.chunk_registry WHERE chunk_hash = :h"),
                                   {"h": row.chunk_hash}).scalar_one()
            self.assertEqual(count, 1)
        finally:
            second.rollback()  # leave nothing behind
            first.close()
            second.close()
        with self.db.transaction() as conn:
            self.assertEqual(conn.execute(text("SELECT count(*) FROM kbp.chunk_registry WHERE chunk_id = :c"),
                                          {"c": row.chunk_id}).scalar_one(), 0)


if __name__ == "__main__":
    unittest.main()

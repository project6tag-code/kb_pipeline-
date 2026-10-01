"""Opt-in PostgreSQL validation of Embeddings V1 (kb_pipeline_test ONLY; local model; no Gold).

    KB_RUN_PG_INTEGRATION=1
    KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m unittest tests.integration.test_embedding_postgres -v

Prerequisites: the model is in the local cache (``cli download``) and registered (``cli register-model``).
setUpClass runs the idempotent pilot: it embeds every eligible representative,
and a rerun adds nothing. The write-contract probes (same hash, different hash,
lost lease) run inside transactions that are ROLLED BACK.
"""

import os
import unittest
import uuid

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_INTEGRATION_DATABASE_URL"))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_INTEGRATION_DATABASE_URL (kb_pipeline_test) to run")
class EmbeddingPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from tests.integration import embedding_pilot as pilot

        cls.pilot = pilot
        cls.db = pilot.open_database()
        pilot.run(cls.db)
        cls.audit = pilot.audit(cls.db)

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "db", None) is not None:
            cls.db.dispose()

    def test_every_eligible_representative_is_embedded_and_audited(self) -> None:
        a = self.audit
        self.assertEqual(a["problems"], [])
        self.assertEqual(a["vectors"], a["eligible_representatives"])
        self.assertEqual(a["embedded_suppressed_near_duplicates"], 0)
        self.assertEqual(a["vector_hash_mismatch"] + a["vector_wrong_dimension"] + a["vector_norm_out_of_tolerance"], 0)

    def test_suppressed_chunks_resolve_to_an_embedded_representative(self) -> None:
        resolution = self.audit["suppressed_resolution"]
        self.assertEqual(len(resolution), self.audit["suppressed_near_duplicates"])
        self.assertTrue(resolution)
        for row in resolution:
            with self.subTest(chunk=row["chunk_id"]):
                self.assertEqual(row["status"], "suppressed")
                self.assertNotEqual(row["embed_chunk_id"], row["chunk_id"])
                self.assertTrue(row["representative_has_vector"])

    def test_rerun_adds_nothing(self) -> None:
        before = self.pilot.counts(self.db)
        result = self.pilot.run(self.db)
        self.assertEqual((result["enqueue"]["enqueued"], result["worker"]["claimed"]), (0, 0))
        self.assertEqual(self.pilot.counts(self.db), before)

    def _probe(self):
        """A rolled-back connection with a queue row leased to us for an embedded chunk, plus its text."""
        from sqlalchemy import text

        conn = self.db.engine.connect()
        conn.begin()
        model_id, chunk_id, chunk_text = conn.execute(text(
            "SELECT v.embedding_model_id, v.chunk_id, c.chunk_text FROM kbp.chunk_embedding_vectors v "
            "JOIN kbp.chunk_registry c USING (chunk_id) ORDER BY v.chunk_id LIMIT 1")).one()
        token = uuid.uuid4()
        conn.execute(text(
            "INSERT INTO kbp.embedding_queue (chunk_id, embedding_model_id, status, attempt_count, max_attempts, "
            "next_attempt_at, locked_at, lease_expires_at, worker_id, lease_token, enqueued_at) VALUES "
            "(:c, :m, 'running', 1, 5, now(), now(), now() + interval '5 minutes', 'probe', :t, now())"),
            {"c": chunk_id, "m": model_id, "t": token})
        return conn, model_id, chunk_id, chunk_text, token

    def test_write_contract_same_hash_different_hash_and_lost_lease(self) -> None:
        import numpy as np
        from sqlalchemy import text

        from app.processing.embedding.repository import Claim, PostgresEmbeddingStore
        from app.processing.embedding.service import EmbeddingWorker, LeaseLost, Nondeterministic
        from app.processing.embedding.validator import check, from_literal

        conn, model_id, chunk_id, _text, token = self._probe()
        try:
            stored = from_literal(conn.execute(text(
                "SELECT CAST(embedding AS text) FROM kbp.chunk_embedding_vectors WHERE chunk_id = :c "
                "AND embedding_model_id = :m"), {"c": chunk_id, "m": model_id}).scalar_one())
            worker = EmbeddingWorker(PostgresEmbeddingStore(), embedder=None, model_id=model_id, run_id=None,
                                     worker_id="probe")
            claim = Claim(chunk_id, 1, 5, token)
            savepoint = conn.begin_nested()
            self.assertEqual(worker.write(conn, claim, check(stored)), "already_embedded_same_hash")
            savepoint.rollback()
            shifted = stored.copy()
            shifted[0] = np.nextafter(shifted[0], np.float32(1))
            savepoint = conn.begin_nested()
            with self.assertRaises(Nondeterministic):
                worker.write(conn, claim, check(shifted))
            savepoint.rollback()
            unchanged = conn.execute(text("SELECT CAST(embedding AS text) FROM kbp.chunk_embedding_vectors WHERE "
                                          "chunk_id = :c AND embedding_model_id = :m"),
                                     {"c": chunk_id, "m": model_id}).scalar_one()
            self.assertEqual(from_literal(unchanged).tobytes(), stored.tobytes())  # never overwritten
            savepoint = conn.begin_nested()
            with self.assertRaises(LeaseLost):
                worker.write(conn, Claim(chunk_id, 1, 5, uuid.uuid4()), check(stored))
            savepoint.rollback()
        finally:
            conn.rollback()
            conn.close()

    def test_lost_lease_on_a_new_chunk_leaves_no_result_and_no_vector(self) -> None:
        from sqlalchemy import text

        from app.processing.embedding.repository import Claim, PostgresEmbeddingStore
        from app.processing.embedding.service import EmbeddingWorker, LeaseLost
        from app.processing.embedding.validator import check
        from app.utils.ids import chunk_content_hash, chunk_id_from_hash
        from tests.unit.embedding_fixtures import FakeEmbedder

        conn = self.db.engine.connect()
        conn.begin()
        try:
            body = f"Embedding lease probe {uuid.uuid4()} exists only inside a rolled back transaction."
            digest = chunk_content_hash(body)
            chunk_id = chunk_id_from_hash(digest)
            doc_id, obs_id = conn.execute(text("SELECT document_id, first_observation_id FROM kbp.document_registry "
                                               "WHERE status = 'accepted' LIMIT 1")).one()
            version_id = conn.execute(text("SELECT chunking_version_id FROM kbp.chunking_versions LIMIT 1")).scalar_one()
            model_id = conn.execute(text("SELECT embedding_model_id FROM kbp.embedding_models LIMIT 1")).scalar_one()
            conn.execute(text("INSERT INTO kbp.chunk_registry (chunk_id, chunk_hash, chunk_text, char_length, "
                              "token_count, chunking_version_id, first_document_id, representative_observation_id) "
                              "VALUES (:c, :h, :t, :n, 20, :v, :d, :o)"),
                         {"c": chunk_id, "h": bytes.fromhex(digest), "t": body, "n": len(body), "v": version_id,
                          "d": doc_id, "o": obs_id})
            worker = EmbeddingWorker(PostgresEmbeddingStore(), embedder=None, model_id=model_id, run_id=None,
                                     worker_id="probe")
            savepoint = conn.begin_nested()
            with self.assertRaises(LeaseLost):  # no queue row with this token: the fenced DELETE hits 0 rows
                worker.write(conn, Claim(chunk_id, 1, 5, uuid.uuid4()), check(FakeEmbedder().vector(body)))
            savepoint.rollback()
            left = conn.execute(text("SELECT (SELECT count(*) FROM kbp.chunk_embeddings WHERE chunk_id = :c), "
                                     "(SELECT count(*) FROM kbp.chunk_embedding_vectors WHERE chunk_id = :c)"),
                                {"c": chunk_id}).one()
            self.assertEqual(tuple(left), (0, 0))
        finally:
            conn.rollback()
            conn.close()


if __name__ == "__main__":
    unittest.main()

"""Opt-in PostgreSQL validation of Bronze -> Silver (NO network; kb_pipeline_test ONLY).

    KB_RUN_PG_INTEGRATION=1
    KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m unittest tests.integration.test_silver_postgres -v

Prerequisite (once, migrator):
    python -m app.processing.silver.cli register-methods --expect-database kb_pipeline_test

Runs the deterministic pilot (tests/integration/silver_pilot.py) through the real
Runtime Engine + acquisition handlers + Silver stage, twice, and verifies the
persisted Silver rows. Pilot rows are permanent and rerun-safe (fixed synthetic
identities); any database other than kb_pipeline_test is refused.
"""

import hashlib
import os
import unittest
import uuid

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_INTEGRATION_DATABASE_URL"))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_INTEGRATION_DATABASE_URL (kb_pipeline_test) to run")
class SilverPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from app.processing.silver.config import SilverConfig
        from tests.integration import silver_pilot

        cls.pilot = silver_pilot
        cls.db = silver_pilot.open_database()
        cls.version = SilverConfig().extraction_version
        cls.first = silver_pilot.run_pilot(cls.db)
        cls.state = silver_pilot.pilot_state(cls.db, cls.version)
        cls.second = silver_pilot.run_pilot(cls.db)
        cls.state_again = silver_pilot.pilot_state(cls.db, cls.version)

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "db", None) is not None:
            cls.db.dispose()

    def extraction(self, url_suffix: str) -> dict:
        (row,) = [e for e in self.state["extractions"]
                  if e["extraction_version"] == self.version and e["source_url"].endswith(url_suffix)]
        return row

    def document(self, document_id) -> dict:
        (row,) = [d for d in self.state["documents"] if d["document_id"] == document_id]
        return row

    def test_units_reach_silver_and_every_observation_has_one_extraction(self) -> None:
        self.assertEqual(sorted(self.state["jobs"], key=lambda j: j["job_type"]),
                         [{"job_type": "cc_index_partition", "status": "succeeded", "stage": "SILVER"},
                          {"job_type": "gh_repository_snapshot", "status": "succeeded", "stage": "SILVER"}])
        observations = {o["observation_id"] for o in self.state["observations"]}
        self.assertEqual(len(observations), 10)
        current = [e for e in self.state["extractions"] if e["extraction_version"] == self.version]
        self.assertEqual(len(current), 10)

    def test_accepted_documents_are_complete_and_identity_is_correct(self) -> None:
        from app.utils.ids import document_id_from_hash

        accepted = [d for d in self.state["documents"] if d["status"] == "accepted"]
        self.assertEqual(len(accepted), 3)
        for doc in accepted:
            with self.subTest(document=str(doc["document_id"])):
                self.assertTrue(doc["has_text"] and doc["hash_matches"])
                self.assertEqual(document_id_from_hash(doc["content_hash"]), doc["document_id"])
                self.assertEqual(doc["language"], "en")
                self.assertIsNotNone(doc["quality_score"])
                self.assertIsNotNone(doc["factuality_score"])
                self.assertEqual(doc["policy_version"], self.first["policy_version"])
                versions = doc["component_versions"]
                self.assertEqual((versions["extraction_version"], versions["language_detector"],
                                  versions["suitability_filter"]), (self.version, "lang2", "suit2"))
                self.assertEqual(doc["primary_reason"], "accepted")
        domains = sorted(d["subject_domain"] for d in accepted)
        self.assertEqual(domains, ["Biology", "Computer Science/Algorithms and Data Structures", "Earth Sciences"])
        from sqlalchemy import text

        with self.db.transaction() as conn:
            text_row = conn.execute(text(
                "SELECT canonical_text, char_length FROM kbp.document_registry WHERE document_id = :d"),
                {"d": self.extraction("www.silver-pilot.example/photosynthesis")["document_id"]}).one()
        self.assertTrue(text_row[0].startswith("# Photosynthesis\n\nPhotosynthesis is the biological process"))
        self.assertNotIn("\x00", text_row[0])
        self.assertEqual(len(text_row[0]), text_row[1])
        self.assertEqual(hashlib.sha256(text_row[0].encode("utf-8")).hexdigest(),
                         self.document(self.extraction("www.silver-pilot.example/photosynthesis")["document_id"])[
                             "content_hash"])

    def test_lineage_observation_extraction_document(self) -> None:
        from sqlalchemy import text

        with self.db.transaction() as conn:
            broken = conn.execute(text(
                "SELECT count(*) FROM kbp.observation_extractions e "
                "LEFT JOIN kbp.source_observations o USING (observation_id) "
                "LEFT JOIN kbp.document_registry d USING (document_id) "
                "WHERE e.extraction_version = :v AND (o.observation_id IS NULL "
                "OR (e.document_id IS NOT NULL AND d.document_id IS NULL))"), {"v": self.version}).scalar_one()
        self.assertEqual(broken, 0)
        for doc in self.state["documents"]:
            linked = {e["document_id"] for e in self.state["extractions"] if e["document_was_new"]}
            self.assertIn(doc["document_id"], linked)  # every document has its registering observation

    def test_exact_global_dedup_across_sources(self) -> None:
        web = self.extraction("www.silver-pilot.example/photosynthesis")
        mirror = self.extraction("mirror.silver-pilot.example/photosynthesis")
        repo = self.extraction("/docs/photosynthesis.txt")
        self.assertEqual(len({web["document_id"], mirror["document_id"], repo["document_id"]}), 1)
        self.assertEqual([web["document_was_new"], mirror["document_was_new"], repo["document_was_new"]].count(True), 1)
        self.assertEqual(sum(1 for d in self.state["documents"] if d["document_id"] == web["document_id"]), 1)

    def test_near_duplicate_decision_and_relationship(self) -> None:
        original = self.extraction("www.silver-pilot.example/glaciers")["document_id"]
        copy = self.extraction("copy.silver-pilot.example/glaciers")["document_id"]
        self.assertEqual(self.document(copy)["status"], "rejected_near_duplicate")
        (decision,) = [d for d in self.state["decisions"] if d["document_id"] == copy]
        self.assertEqual((decision["classification"], decision["canonical_document_id"], decision["algorithm"],
                          decision["algorithm_version"], decision["decision_status"]),
                         ("NEAR_DUPLICATE", original, "minhash_lsh", "oph-mh128-b32r4-w5-v1", "confirmed"))
        self.assertGreaterEqual(decision["similarity_score"], 0.8)
        (relationship,) = self.state["relationships"]
        self.assertEqual((relationship["document_id"], relationship["related_document_id"],
                          relationship["relationship_type"]), (copy, original, "NEAR_DUPLICATE"))
        unrelated = [d for d in self.state["decisions"] if d["classification"] == "UNIQUE"]
        self.assertEqual(len(unrelated), 3)  # photosynthesis, glaciers, binary search are not merged

    def test_rejections_and_quarantine_are_represented_without_leaks(self) -> None:
        self.assertEqual(self.extraction("www.silver-pilot.example/fr")["outcome"], "rejected_language")
        self.assertEqual(self.extraction("/docs/fr.md")["outcome"], "rejected_language")
        secret = self.document(self.extraction("www.silver-pilot.example/deploy")["document_id"])
        self.assertEqual((secret["status"], secret["has_text"]), ("quarantined", False))
        self.assertEqual(secret["primary_reason"], "secret:high_confidence")
        pii = self.document(self.extraction("/docs/customer.md")["document_id"])
        self.assertEqual((pii["status"], pii["has_text"], pii["primary_reason"]),
                         ("rejected_policy", False, "privacy:high_risk_identifier"))
        (dead_letter,) = self.state["dead_letters"]
        self.assertEqual((dead_letter["item_type"], dead_letter["disposition"], dead_letter["error_class"],
                          dead_letter["error_code"]), ("document", "quarantined", "policy_quarantine", "secret_detected"))
        stored = repr(self.state["documents"]) + repr(self.state["dead_letters"])
        for fragment in self.pilot.SENSITIVE_FRAGMENTS:
            self.assertNotIn(fragment, stored)

    def test_rerun_is_idempotent_and_deterministic(self) -> None:
        self.assertEqual(self.state_again, self.state)
        self.assertEqual(self.second["reprocess"], {"silver_already_extracted": 10})
        stored = {str(e["document_id"]) for e in self.state["extractions"] if e["document_id"]}
        recomputed = {v for v in self.second["recomputed_document_ids"].values() if v}
        self.assertEqual(recomputed, stored)
        self.assertTrue(all(uuid.UUID(v) for v in recomputed))


if __name__ == "__main__":
    unittest.main()

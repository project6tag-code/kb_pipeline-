"""Focused pure-logic tests for Gold Publisher V1 guards and conflict policy."""

import math
import unittest
import uuid

from app.processing.chunk_dedup import eligibility
from app.processing.embedding import model as embedding_model
from app.processing.gold import service


class ModelPromotionTests(unittest.TestCase):
    def model_row(self, status="candidate"):
        return {**embedding_model.registration_row(), "embedding_model_id": service.MODEL_ID, "status": status}

    def test_exact_candidate_and_active_model_configs_are_accepted(self) -> None:
        for status in ("candidate", "active_gold"):
            service.validate_model_row(self.model_row(status), allowed_statuses={"candidate", "active_gold"})

    def test_wrong_identity_dimension_config_and_status_are_refused(self) -> None:
        for field, value in (
            ("embedding_model_id", 3),
            ("model_name", "wrong/model"),
            ("model_revision", "0" * 40),
            ("embedding_version", "wrong-version"),
            ("embedding_dimension", 384),
            ("config", {"dimension": 384}),
            ("status", "retired"),
        ):
            row = self.model_row()
            row[field] = value
            with self.subTest(field=field), self.assertRaises(service.GoldPublisherError):
                service.validate_model_row(row, allowed_statuses={"candidate", "active_gold"})


class EligibilityTests(unittest.TestCase):
    def test_only_confirmed_non_near_chunks_are_eligible(self) -> None:
        chunk = uuid.uuid4()
        representative = uuid.uuid4()
        self.assertTrue(eligibility.classify(chunk, "UNIQUE", None, "confirmed").eligible)
        self.assertTrue(eligibility.classify(chunk, "RELATED", None, "confirmed").eligible)
        suppressed = eligibility.classify(chunk, "NEAR_DUPLICATE", representative, "confirmed")
        self.assertFalse(suppressed.eligible)
        self.assertEqual(suppressed.embed_chunk_id, representative)
        self.assertFalse(eligibility.classify(chunk, "UNIQUE", None, "proposed").eligible)
        self.assertFalse(eligibility.classify(chunk, None, None, None).eligible)


class VectorAndProvenanceTests(unittest.TestCase):
    def valid_vector_row(self):
        return {"embedding_status": "stored_shadow", "embedding_published_at": None,
                "embedding_text": "[1," + ",".join(["0"] * 767) + "]", "vector_sha256": b"x" * 32,
                "recomputed_sha256": b"x" * 32, "dimensions": 768, "norm": 1.0}

    def test_missing_or_invalid_vector_is_refused(self) -> None:
        with self.assertRaises(service.GoldPublisherError):
            service.validate_vector_row(None)
        cases = (
            {"dimensions": 384},
            {"vector_sha256": b"y" * 32},
            {"embedding_status": "published_gold"},
            {"embedding_text": "[NaN," + ",".join(["0"] * 767) + "]"},
            {"embedding_text": "[2," + ",".join(["0"] * 767) + "]", "norm": 2.0},
        )
        for changes in cases:
            row = {**self.valid_vector_row(), **changes}
            with self.subTest(changes=changes), self.assertRaises(service.GoldPublisherError):
                service.validate_vector_row(row)

    def test_provenance_rejects_missing_url_and_unknown_channel(self) -> None:
        base = {"chunk_id": uuid.uuid4(), "source_channel": "common_crawl", "source_url": "https://example.test/x",
                "source_title": None, "source_published_at": None, "subject_domain": None}
        record = service.validate_provenance(base)
        self.assertEqual(record["source_type"], "common_crawl")
        self.assertIsNone(record["source_title"])
        self.assertIsNone(record["published_at"])
        self.assertIsNone(record["updated_at"])
        for changes in ({"source_url": None}, {"source_channel": "kaggle"}):
            with self.subTest(changes=changes), self.assertRaises(service.GoldPublisherError):
                service.validate_provenance({**base, **changes})


class ConflictTests(unittest.TestCase):
    def setUp(self) -> None:
        self.chunk_id = uuid.uuid4()
        self.source_id = uuid.uuid4()
        self.expected_metadata = {"id": self.chunk_id, "source_url": "https://example.test/a",
                                  "source_title": "A", "source_type": "common_crawl", "domain": "Science",
                                  "published_at": None, "updated_at": None}
        self.chunk_text = "Canonical factual chunk"
        self.vector_wire = b"immutable vector wire bytes"
        self.metadata = {**self.expected_metadata, "source_id": self.source_id}
        self.kb = {"id": self.chunk_id, "chunk": self.chunk_text, "embedding_wire": self.vector_wire}
        self.ledger = {"chunk_id": self.chunk_id, "embedding_model_id": service.MODEL_ID,
                       "published_at": None, "run_id": uuid.uuid4()}

    def classify(self, metadata=None, kb=None, ledger=None):
        return service.classify_existing_gold(
            metadata, kb, ledger, expected_metadata=self.expected_metadata, source_id=self.source_id,
            chunk_id=self.chunk_id, chunk_text=self.chunk_text, vector_wire=self.vector_wire,
        )

    def test_absent_and_identical_complete_state(self) -> None:
        self.assertEqual(self.classify(), "absent")
        self.assertEqual(self.classify(self.metadata, self.kb, self.ledger), "reused")

    def test_partial_rows_and_all_immutable_conflicts_are_refused(self) -> None:
        partial = ((self.metadata, None, None), (None, self.kb, None), (None, None, self.ledger))
        for rows in partial:
            with self.subTest(rows=rows), self.assertRaises(service.GoldConflict):
                self.classify(*rows)
        metadata_changes = ("source_id", "source_url", "source_title", "source_type", "domain", "published_at")
        for field in metadata_changes:
            changed = dict(self.metadata)
            changed[field] = uuid.uuid4() if field == "source_id" else "changed"
            with self.subTest(field=field), self.assertRaises(service.GoldConflict):
                self.classify(changed, self.kb, self.ledger)
        for field, value in (("chunk", "changed chunk"), ("embedding_wire", b"changed vector")):
            changed = {**self.kb, field: value}
            with self.subTest(field=field), self.assertRaises(service.GoldConflict):
                self.classify(self.metadata, changed, self.ledger)
        changed_ledger = {**self.ledger, "embedding_model_id": 99}
        with self.assertRaises(service.GoldConflict):
            self.classify(self.metadata, self.kb, changed_ledger)


if __name__ == "__main__":
    unittest.main()
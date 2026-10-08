"""Focused pure-logic tests for Gold Publisher V2 guards and conflict policy."""

import hashlib
import math
import unittest
import uuid
from datetime import datetime, timezone

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
    WIRE = bytes([3, 0, 0, 0]) + b"vector-body"
    DIGEST = hashlib.sha256(WIRE[4:]).digest()

    def valid_vector_row(self):
        return {"embedding_status": "stored_shadow", "embedding_published_at": None,
                "embedding_text": "[1," + ",".join(["0"] * 767) + "]", "vector_sha256": self.DIGEST,
                "recomputed_sha256": self.DIGEST, "vector_wire": self.WIRE, "dimensions": 768, "norm": 1.0}

    def test_valid_vector_returns_text_and_full_wire_bytes(self) -> None:
        text, wire = service.validate_vector_row(self.valid_vector_row())
        self.assertTrue(text.startswith("[1,"))
        self.assertEqual(wire, self.WIRE)

    def test_missing_or_invalid_vector_is_refused(self) -> None:
        with self.assertRaises(service.GoldPublisherError):
            service.validate_vector_row(None)
        cases = (
            {"dimensions": 384},
            {"vector_sha256": b"y" * 32},
            {"vector_wire": bytes([3, 0, 0, 0]) + b"tampered"},
            {"embedding_status": "published_gold"},
            {"embedding_text": "[NaN," + ",".join(["0"] * 767) + "]"},
            {"embedding_text": "[2," + ",".join(["0"] * 767) + "]", "norm": 2.0},
        )
        for changes in cases:
            row = {**self.valid_vector_row(), **changes}
            with self.subTest(changes=changes), self.assertRaises(service.GoldPublisherError):
                service.validate_vector_row(row)

    def test_provenance_maps_source_document_and_dates(self) -> None:
        record_id, crawled = uuid.uuid4(), datetime(2024, 8, 5, 23, 12, 34, tzinfo=timezone.utc)
        base = {"chunk_id": uuid.uuid4(), "source_channel": "common_crawl", "source_url": "https://example.test/x",
                "source_title": None, "source_published_at": None, "subject_domain": None,
                "warc_record_id": record_id, "crawled_at": crawled, "page_published_at": None,
                "page_modified_at": None, "http_last_modified": None, "published_evidence": None,
                "modified_evidence": None}
        metadata, dataset = service.validate_provenance(base)
        self.assertEqual((metadata["source_type"], metadata["source_id"]), ("web_page", record_id))
        self.assertIsNone(metadata["source_title"])
        self.assertEqual((metadata["published_at"], metadata["updated_at"], metadata["crawled_at"]),
                         (crawled, crawled, crawled))
        self.assertEqual((dataset["id"], dataset["status"]), (record_id, "KB_SOURCE_DOCUMENT"))
        self.assertEqual(dataset["metadata"]["published_from"], "crawl_date_fallback")
        refused = ({"source_url": None}, {"source_channel": "kaggle"}, {"warc_record_id": None},
                   {"crawled_at": None}, {"warc_record_id": uuid.UUID("a7d8d2e1-8400-5916-9440-b492e8e692ce")})
        for changes in refused:
            with self.subTest(changes=changes), self.assertRaises(service.GoldPublisherError):
                service.validate_provenance({**base, **changes})


class ConflictTests(unittest.TestCase):
    def setUp(self) -> None:
        self.chunk_id = uuid.uuid4()
        self.source_id = uuid.uuid4()
        moment = datetime(2024, 8, 5, tzinfo=timezone.utc)
        self.expected_metadata = {"id": self.chunk_id, "source_id": self.source_id,
                                  "source_url": "https://example.test/a", "source_title": "A",
                                  "source_type": "common_crawl", "domain": "Science", "published_at": moment,
                                  "updated_at": moment, "crawled_at": moment}
        self.chunk_text = "Canonical factual chunk"
        self.vector_wire = b"immutable vector wire bytes"
        self.metadata = dict(self.expected_metadata)
        self.kb = {"id": self.chunk_id, "source_id": self.source_id, "chunk": self.chunk_text,
                   "embedding_wire": self.vector_wire}
        self.ledger = {"chunk_id": self.chunk_id, "embedding_model_id": service.MODEL_ID,
                       "published_at": None, "run_id": uuid.uuid4()}

    def classify(self, metadata=None, kb=None, ledger=None):
        return service.classify_existing_gold(
            metadata, kb, ledger, expected_metadata=self.expected_metadata,
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
        for field in service.METADATA_FIELDS:
            if field == "id":
                continue
            changed = dict(self.metadata)
            changed[field] = uuid.uuid4() if field == "source_id" else "changed"
            with self.subTest(field=field), self.assertRaises(service.GoldConflict):
                self.classify(changed, self.kb, self.ledger)
        for field, value in (("chunk", "changed chunk"), ("embedding_wire", b"changed vector"),
                             ("source_id", uuid.uuid4()), ("source_id", None)):
            changed = {**self.kb, field: value}
            with self.subTest(field=field), self.assertRaises(service.GoldConflict):
                self.classify(self.metadata, changed, self.ledger)
        changed_ledger = {**self.ledger, "embedding_model_id": 99}
        with self.assertRaises(service.GoldConflict):
            self.classify(self.metadata, self.kb, changed_ledger)

    def test_chunks_of_one_capture_share_source_id_but_keep_unique_ids(self) -> None:
        record_id, crawled = uuid.uuid4(), datetime(2024, 8, 5, tzinfo=timezone.utc)
        base = {"source_channel": "common_crawl", "source_url": "https://en.wikipedia.org/wiki/Oxygen",
                "source_title": "Oxygen", "source_published_at": None, "subject_domain": "Chemistry",
                "warc_record_id": record_id, "crawled_at": crawled, "page_published_at": None,
                "page_modified_at": None, "http_last_modified": None, "published_evidence": None,
                "modified_evidence": None}
        first, _ = service.validate_provenance({**base, "chunk_id": uuid.uuid4()})
        second, _ = service.validate_provenance({**base, "chunk_id": uuid.uuid4()})
        other, _ = service.validate_provenance({**base, "chunk_id": uuid.uuid4(), "warc_record_id": uuid.uuid4()})
        self.assertNotEqual(first["id"], second["id"])                       # each chunk id stays unique
        self.assertEqual(first["source_id"], second["source_id"])            # same capture -> same source_id
        self.assertEqual(first["source_id"], record_id)                      # CC: the WARC-Record-ID
        self.assertNotEqual(first["source_id"], other["source_id"])          # another capture -> another id
        kb = {"id": first["id"], "source_id": record_id, "chunk": "t", "embedding_wire": b"w"}
        self.assertTrue(service._same_kb(kb, first["id"], first["source_id"], "t", b"w"))
        self.assertFalse(service._same_kb(kb, first["id"], other["source_id"], "t", b"w"))

    def test_dataset_rows_compare_exactly(self) -> None:
        expected = {"id": self.source_id, "source_id": uuid.uuid4(), "dataset_name": "A",
                    "status": "KB_SOURCE_DOCUMENT", "metadata": {"warc_record_offset": 5, "crawl_id": "CC-MAIN-2024-33"}}
        self.assertTrue(service._same_dataset(dict(expected), expected))
        for field, value in (("dataset_name", "B"), ("status", "KB_REFERENCE"),
                             ("metadata", {"warc_record_offset": 6, "crawl_id": "CC-MAIN-2024-33"})):
            with self.subTest(field=field):
                self.assertFalse(service._same_dataset({**expected, field: value}, expected))


if __name__ == "__main__":
    unittest.main()
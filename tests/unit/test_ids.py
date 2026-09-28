"""Unit tests for app.utils.ids, including frozen identity regression vectors."""

import hashlib
import unittest
import uuid

from app.processing.normalization.text import canonicalize_text
from app.utils.hashing import InvalidHashError, sha256_text
from app.utils.ids import (
    KB_NAMESPACE,
    chunk_content_hash,
    chunk_id_from_hash,
    chunk_id_from_text,
    document_content_hash,
    document_id_from_hash,
    document_id_from_text,
)

# ---------------------------------------------------------------------------
# FROZEN REGRESSION VECTOR (Identity Specification V1 / Canonicalization V1).
# These values were computed once from the implemented specification.
# If this test fails, normalization, hashing, prefixes, or the namespace have
# changed, which would silently re-key the entire knowledge base. Do NOT update
# these values unless a deliberate, versioned identity migration is intended.
# ---------------------------------------------------------------------------
FROZEN_NAMESPACE = uuid.UUID("dd5af6b8-db26-4170-945d-557b16114d80")
RAW_TEXT = (
    "﻿  The  Café opened in 1998.\t\r\n\r\n\r\n\r\n"
    "Its owner,\tDr. Ana  Silva, said: \"Prices are NOT fixed!\"   \r"
    "End.  \n\n"
)
EXPECTED_CANONICAL_TEXT = (
    "The Café opened in 1998.\n\n"
    "Its owner, Dr. Ana Silva, said: \"Prices are NOT fixed!\"\n"
    "End."
)
EXPECTED_SHA256 = "af959c5a0629ec4c4c35a5802d9fb4e5edebea74c881b8c147cff8f89bd3a704"
EXPECTED_DOCUMENT_UUID = uuid.UUID("3b5c66fb-627b-5754-8e87-05221c2e1462")
EXPECTED_CHUNK_UUID = uuid.UUID("65a62e16-b229-5169-9ade-ffd0ac59412c")


class FrozenRegressionTests(unittest.TestCase):
    def test_namespace_frozen(self) -> None:
        self.assertEqual(KB_NAMESPACE, FROZEN_NAMESPACE)

    def test_canonical_text(self) -> None:
        self.assertEqual(canonicalize_text(RAW_TEXT), EXPECTED_CANONICAL_TEXT)

    def test_hashes(self) -> None:
        self.assertEqual(
            hashlib.sha256(EXPECTED_CANONICAL_TEXT.encode("utf-8")).hexdigest(), EXPECTED_SHA256
        )
        self.assertEqual(document_content_hash(RAW_TEXT), EXPECTED_SHA256)
        self.assertEqual(chunk_content_hash(RAW_TEXT), EXPECTED_SHA256)

    def test_document_uuid(self) -> None:
        self.assertEqual(document_id_from_text(RAW_TEXT), EXPECTED_DOCUMENT_UUID)
        self.assertEqual(document_id_from_hash(EXPECTED_SHA256), EXPECTED_DOCUMENT_UUID)
        self.assertEqual(
            uuid.uuid5(FROZEN_NAMESPACE, "document:v1:" + EXPECTED_SHA256), EXPECTED_DOCUMENT_UUID
        )

    def test_chunk_uuid(self) -> None:
        self.assertEqual(chunk_id_from_text(RAW_TEXT), EXPECTED_CHUNK_UUID)
        self.assertEqual(chunk_id_from_hash(EXPECTED_SHA256), EXPECTED_CHUNK_UUID)
        self.assertEqual(
            uuid.uuid5(FROZEN_NAMESPACE, "chunk:v1:" + EXPECTED_SHA256), EXPECTED_CHUNK_UUID
        )


class IdentityTests(unittest.TestCase):
    TEXT = "Water boils at 100 °C at sea level."

    def test_same_document_text_same_hash(self) -> None:
        self.assertEqual(document_content_hash(self.TEXT), document_content_hash(self.TEXT))

    def test_hash_is_of_canonical_text(self) -> None:
        self.assertEqual(document_content_hash(self.TEXT), sha256_text(canonicalize_text(self.TEXT)))

    def test_same_document_hash_same_uuid(self) -> None:
        h = document_content_hash(self.TEXT)
        self.assertEqual(document_id_from_hash(h), document_id_from_hash(h))

    def test_same_chunk_text_same_uuid(self) -> None:
        self.assertEqual(chunk_id_from_text(self.TEXT), chunk_id_from_text(self.TEXT))

    def test_ids_are_uuid_v5(self) -> None:
        self.assertEqual(document_id_from_text(self.TEXT).version, 5)
        self.assertEqual(chunk_id_from_text(self.TEXT).version, 5)

    def test_document_and_chunk_ids_differ_for_same_text(self) -> None:
        self.assertEqual(document_content_hash(self.TEXT), chunk_content_hash(self.TEXT))
        self.assertNotEqual(document_id_from_text(self.TEXT), chunk_id_from_text(self.TEXT))

    def test_equivalent_raw_text_same_ids(self) -> None:
        variants = [
            "Line  one.\r\nLine two.",
            "﻿Line one.\nLine two.  ",
            "  Line\tone.\rLine two.\n\n",
        ]
        doc_ids = {document_id_from_text(v) for v in variants}
        chunk_ids = {chunk_id_from_text(v) for v in variants}
        self.assertEqual(len(doc_ids), 1)
        self.assertEqual(len(chunk_ids), 1)

    def test_nfc_equivalent_text_same_ids(self) -> None:
        self.assertEqual(chunk_id_from_text("Café"), chunk_id_from_text("Café"))

    def test_different_meaningful_text_different_hashes(self) -> None:
        pairs = [
            ("The drug is safe.", "The drug is not safe."),
            ("Apple", "apple"),
            ("3.14", "314"),
            ("Let's eat, grandma.", "Let's eat grandma."),
            ("résumé", "resume"),
            ("a\nb", "a\n\nb"),
            ("a b", "ab"),
            ("Hel﻿lo", "Hello"),
        ]
        for a, b in pairs:
            with self.subTest(a=a, b=b):
                self.assertNotEqual(document_content_hash(a), document_content_hash(b))
                self.assertNotEqual(chunk_id_from_text(a), chunk_id_from_text(b))

    def test_ids_stable_across_repeated_calls(self) -> None:
        first_doc = document_id_from_text(self.TEXT)
        first_chunk = chunk_id_from_text(self.TEXT)
        for _ in range(100):
            self.assertEqual(document_id_from_text(self.TEXT), first_doc)
            self.assertEqual(chunk_id_from_text(self.TEXT), first_chunk)

    def test_uppercase_hash_normalized(self) -> None:
        h = chunk_content_hash(self.TEXT)
        self.assertEqual(chunk_id_from_hash(h.upper()), chunk_id_from_hash(h))
        self.assertEqual(document_id_from_hash(h.upper()), document_id_from_hash(h))

    def test_invalid_hashes_rejected(self) -> None:
        bad_values = ["", "abc", "z" * 64, "a" * 63, "a" * 65, " " + "a" * 63, "a" * 64 + "\n"]
        for bad in bad_values:
            with self.subTest(bad=bad):
                with self.assertRaises(InvalidHashError):
                    document_id_from_hash(bad)
                with self.assertRaises(InvalidHashError):
                    chunk_id_from_hash(bad)

    def test_invalid_input_types_rejected(self) -> None:
        for bad in (b"text", None, 7, ["x"]):
            with self.subTest(bad=bad):
                for fn in (
                    document_content_hash,
                    chunk_content_hash,
                    document_id_from_text,
                    chunk_id_from_text,
                    document_id_from_hash,
                    chunk_id_from_hash,
                ):
                    with self.assertRaises(TypeError):
                        fn(bad)  # type: ignore[arg-type]

    def test_empty_canonical_text_rejected(self) -> None:
        for empty in ("", "   ", "\r\n\t﻿ "):
            with self.subTest(text=empty):
                with self.assertRaises(ValueError):
                    document_content_hash(empty)
                with self.assertRaises(ValueError):
                    chunk_id_from_text(empty)


if __name__ == "__main__":
    unittest.main()

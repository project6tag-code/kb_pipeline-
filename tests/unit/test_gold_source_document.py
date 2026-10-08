"""Gold V2 source-document identity, date fallbacks and dataset rows (pure, offline)."""

import unittest
import uuid
from datetime import datetime, timezone

from app.processing.gold import source_document as sd
from app.processing.gold_bridge import spec as bridge_spec
from app.utils.ids import KB_NAMESPACE

UTC = timezone.utc
CRAWLED = datetime(2024, 8, 5, 23, 12, 34, tzinfo=UTC)
RECORD_ID = uuid.UUID("54d144a2-2c96-5d32-9411-66240f738f2d")
BLOB = "a" * 40


def cc_row(**changes):
    row = {"chunk_id": uuid.uuid4(), "source_channel": "common_crawl", "source_url": "https://en.wikipedia.org/wiki/X",
           "source_title": "X - Wikipedia", "warc_record_id": RECORD_ID, "crawled_at": CRAWLED,
           "page_published_at": None, "published_evidence": None, "page_modified_at": None,
           "modified_evidence": None, "http_last_modified": None, "crawl_id": "CC-MAIN-2024-33",
           "warc_filename": "crawl-data/x.warc.gz", "warc_record_offset": 10, "warc_record_length": 20,
           "payload_digest": "sha1:ABC"}
    row.update(changes)
    return row


def gh_row(**changes):
    row = {"chunk_id": uuid.uuid4(), "source_channel": "github", "source_url": "https://github.com/o/r/blob/c/a.md",
           "source_title": None, "blob_sha": BLOB, "repository_id": 7, "commit_sha": "c" * 40,
           "file_path": "a.md", "acquired_at": CRAWLED, "committed_at": datetime(2023, 1, 2, tzinfo=UTC)}
    row.update(changes)
    return row


class IdentityTests(unittest.TestCase):
    def test_common_crawl_uses_the_warc_record_id(self) -> None:
        self.assertEqual(sd.source_document_id(cc_row()), RECORD_ID)
        with self.assertRaises(sd.SourceDocumentError):
            sd.source_document_id(cc_row(warc_record_id=None))

    def test_github_uses_the_blob_sha(self) -> None:
        self.assertEqual(sd.source_document_id(gh_row()), uuid.uuid5(KB_NAMESPACE, "github-blob:v1:" + BLOB))
        self.assertNotEqual(sd.github_source_id("b" * 40), sd.github_source_id(BLOB))
        self.assertEqual(sd.github_source_id("c" * 64), uuid.uuid5(KB_NAMESPACE, "github-blob:v1:" + "c" * 64))
        for bad in (None, "xyz", "A" * 40, "a" * 39):
            with self.subTest(bad=bad), self.assertRaises(sd.SourceDocumentError):
                sd.github_source_id(bad)

    def test_two_chunks_of_one_document_share_the_id(self) -> None:
        self.assertEqual(sd.source_document_id(cc_row()), sd.source_document_id(cc_row()))
        self.assertNotEqual(sd.source_document_id(cc_row()), sd.source_document_id(cc_row(warc_record_id=uuid.uuid4())))


class DateTests(unittest.TestCase):
    def test_declared_dates_are_used(self) -> None:
        published, modified = datetime(2001, 11, 9, tzinfo=UTC), datetime(2024, 7, 30, tzinfo=UTC)
        dates = sd.gold_dates(cc_row(page_published_at=published, published_evidence="jsonld:datePublished",
                                     page_modified_at=modified, modified_evidence="jsonld:dateModified"))
        self.assertEqual((dates["published_at"], dates["updated_at"], dates["crawled_at"]), (published, modified, CRAWLED))
        self.assertEqual((dates["published_from"], dates["updated_from"]), ("jsonld:datePublished", "jsonld:dateModified"))

    def test_crawl_date_fallback_when_nothing_is_declared(self) -> None:
        dates = sd.gold_dates(cc_row())
        self.assertEqual((dates["published_at"], dates["updated_at"]), (CRAWLED, CRAWLED))
        self.assertEqual((dates["published_from"], dates["updated_from"]), ("crawl_date_fallback", "crawl_date_fallback"))

    def test_updated_falls_back_through_last_modified_then_published(self) -> None:
        published, last_modified = datetime(2010, 1, 1, tzinfo=UTC), datetime(2020, 1, 1, tzinfo=UTC)
        dates = sd.gold_dates(cc_row(page_published_at=published, published_evidence="meta:date",
                                     http_last_modified=last_modified))
        self.assertEqual((dates["updated_at"], dates["updated_from"]), (last_modified, "http:last-modified"))
        dates = sd.gold_dates(cc_row(page_published_at=published, published_evidence="meta:date"))
        self.assertEqual((dates["updated_at"], dates["updated_from"]), (published, "published_date"))

    def test_updated_is_never_before_published(self) -> None:
        # Only an HTTP Last-Modified older than the crawl-date fallback for published_at: it is skipped.
        dates = sd.gold_dates(cc_row(http_last_modified=datetime(2015, 1, 1, tzinfo=UTC)))
        self.assertGreaterEqual(dates["updated_at"], dates["published_at"])
        self.assertEqual(dates["updated_at"], CRAWLED)

    def test_missing_crawl_date_is_refused(self) -> None:
        with self.assertRaises(sd.SourceDocumentError):
            sd.gold_dates(cc_row(crawled_at=None))

    def test_github_dates(self) -> None:
        dates = sd.gold_dates(gh_row())
        self.assertEqual((dates["published_at"], dates["updated_at"], dates["crawled_at"]),
                         (datetime(2023, 1, 2, tzinfo=UTC), datetime(2023, 1, 2, tzinfo=UTC), CRAWLED))
        self.assertEqual(sd.gold_dates(gh_row(committed_at=None))["published_from"], "crawl_date_fallback")


class DatasetRowTests(unittest.TestCase):
    def test_common_crawl_dataset_row(self) -> None:
        row = cc_row()
        dataset = sd.dataset_row(row, RECORD_ID, sd.gold_dates(row))
        self.assertEqual(dataset["id"], RECORD_ID)
        self.assertEqual(dataset["source_id"], bridge_spec.dataset_source_id("common_crawl"))
        self.assertEqual((dataset["dataset_name"], dataset["status"]), ("X - Wikipedia", "KB_SOURCE_DOCUMENT"))
        self.assertEqual(dataset["metadata"]["warc_record_id"], str(RECORD_ID))
        self.assertEqual(dataset["metadata"]["crawled_at"], CRAWLED.isoformat())
        self.assertEqual(sd.dataset_row(row, RECORD_ID, sd.gold_dates(row)), dataset)  # deterministic

    def test_name_falls_back_to_url_and_is_bounded(self) -> None:
        row = cc_row(source_title=None, source_url="https://example.com/" + "a" * 400)
        self.assertEqual(len(sd.dataset_row(row, RECORD_ID, sd.gold_dates(row))["dataset_name"]), 255)

    def test_github_dataset_row_keeps_git_provenance(self) -> None:
        row = gh_row()
        source_id = sd.source_document_id(row)
        meta = sd.dataset_row(row, source_id, sd.gold_dates(row))["metadata"]
        self.assertEqual((meta["blob_sha"], meta["repository_id"], meta["file_path"]), (BLOB, 7, "a.md"))


if __name__ == "__main__":
    unittest.main()

"""Silver processor: final gate, persistence, idempotent reruns, crash recovery, nondeterminism guard."""

import dataclasses
import unittest

from app.processing.dedup.near import NearIndex
from app.processing.silver.config import SilverConfig
from app.processing.silver.processor import SilverProcessor
from app.queue.retry import JobFailure
from app.sources.commoncrawl.bronze import BronzeStore
from app.sources.github.bronze import GitHubBronze

from tests.unit.silver_fixtures import Ctx, InMemorySilverStore, article_html, store_cc, store_gh, tmpdir

REFERENCE = """# Binary search

Binary search is an algorithm that finds the position of a target value within a sorted array. It compares the
target value to the middle element of the array. If they are not equal, the half in which the target cannot lie is
eliminated and the search continues on the remaining half. The algorithm runs in logarithmic time, making O(log n)
comparisons, where n is the number of elements in the array.

Binary search is used to implement lookups in sorted arrays and database indexes. It was first described in 1946,
and a correct version for arrays of every length was published in 1960. The method requires random access to the
elements, so it is not used on linked lists. Variants include exponential search and interpolation search, which
are used when the size of the array is unknown or the values are uniformly distributed."""

MERGE_SORT = """# Merge sort

Merge sort is a divide-and-conquer sorting algorithm that splits an array into two halves, sorts each half
recursively and merges the sorted halves. It was invented by John von Neumann in 1945. The algorithm runs in
O(n log n) time in the worst case and requires additional memory proportional to the input size. Merge sort is
stable, which means that equal elements keep their relative order, and it is well suited to sorting linked lists
and very large files stored on disk."""

# Half factual statements, half personal remarks: suitability is UNRESOLVED (held, not promoted).
STATION = """# Central station

The station opened in 1998 and has two platforms. I think the new cafe on the upper level is nice.
Trains run every 15 minutes to the city centre and every hour to the airport. What do you think of it?
The ticket office is open from 6 am to 11 pm on weekdays. I visited last week with my friend and we loved it.
A bus terminal with 12 bays was added next to the main entrance in 2015. Honestly I would go back again soon.
Parking for 400 cars is available on the east side of the building. My friend says the coffee is amazing."""

# Varied (not repeated) forum chatter: it passes the noise gate and is rejected by suitability.
CHATTER = """# Weekend thread

lol I think this was the best weekend ever!!! Thanks everyone who came to the meetup, you guys are awesome.
Hey guys, did anyone else get soaked on the way home? I totally did, omg.
My friend says the pizza place was terrible but I loved it honestly.
Reply below if you want to join next time, and follow me for the photos.
I'm so happy with how it turned out, amazing crowd and great vibes all night.
Can't wait for the next one, who is in? I will bring snacks this time for sure."""


class ProcessorHarness:
    def __init__(self, test: unittest.TestCase, config: SilverConfig | None = None) -> None:
        root = tmpdir(test)
        self.cc, self.gh = BronzeStore(root / "cc"), GitHubBronze(root / "gh")
        self.store = InMemorySilverStore()
        self.index = NearIndex(":memory:")
        test.addCleanup(self.index.close)
        self.processor = SilverProcessor(config or SilverConfig(), self.store, self.index, cc_bronze=self.cc,
                                         gh_bronze=self.gh)
        self._offset = 1000

    def cc_doc(self, url: str, body: bytes, **kw):
        self._offset += 100_000
        ref = store_cc(self.cc, url, body, offset=kw.pop("offset", self._offset), **kw)
        self.store.observe(ref)
        return ref

    def gh_doc(self, path: str, content, **kw):
        ref = store_gh(self.gh, path, content, **kw)
        self.store.observe(ref)
        return ref

    def run(self, refs):
        prepared = self.processor.prepare(Ctx(), refs)
        with self.store.atomic():  # the acquisition batch transaction
            return self.processor.persist(None, prepared, job_id=7, attempt_count=1)

    def doc(self, ref):
        return self.store.document_for(ref, self.processor.extraction_version)

    def extraction(self, ref):
        return self.store.extraction(ref, self.processor.extraction_version)


class FinalGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ProcessorHarness(self)

    def test_only_a_fully_valid_document_becomes_silver_accepted(self) -> None:
        ref = self.h.cc_doc("https://example.com/photosynthesis", article_html())
        counts = self.h.run([ref])
        self.assertEqual(counts["silver_accepted"], 1)
        doc = self.h.doc(ref)
        self.assertEqual(doc["status"], "accepted")
        self.assertEqual(doc["language"], "en")
        self.assertTrue(doc["canonical_text"].startswith("# Photosynthesis"))
        self.assertTrue(doc["status_reason"].startswith("accepted|01_integrity=PASS"))
        for gate in ("13_language=PASS", "14_privacy=PASS", "15_secrets=PASS", "16_noise=PASS", "17_quality=PASS",
                     "18_suitability=PASS", "19_exact_dedup=PASS", "20_near_dedup=PASS", "21_domain=PASS",
                     "22_final=PASS"):
            self.assertIn(gate, doc["status_reason"])
        versions = doc["component_versions"]
        for key in ("extractor", "html_cleaner", "decoder", "canonicalizer", "language_detector",
                    "privacy_detector", "secret_detector", "quality_filter", "suitability_filter", "exact_dedup",
                    "near_dedup", "domain_classifier", "silver_gate", "extraction_version", "policy_version"):
            self.assertTrue(versions[key], key)
        self.assertEqual(doc["policy_version"], self.h.processor.policy_version)
        self.assertEqual(doc["subject_domain"], "Biology")
        self.assertEqual(self.h.extraction(ref), {"outcome": "document", "document_id": doc["document_id"],
                                                  "document_was_new": True})
        decision = self.h.store.decisions[(doc["document_id"], 1)]
        self.assertEqual(decision["classification"], "UNIQUE")

    def test_every_rejection_has_an_explicit_reason(self) -> None:
        h = self.h
        refs = {
            "spam": h.cc_doc("https://example.com/404", article_html(
                "<h1>404 Page not found</h1><p>Sorry, the page you requested does not exist or has been removed. "
                "Please check the address and try again, or return to the home page to continue browsing.</p>"
                "<p>The page may have been moved to a new location on this website.</p>")),
            "chatter": h.gh_doc("notes/chat.md", CHATTER),
            "ssn": h.gh_doc("docs/customer.md", REFERENCE + "\n\nCustomer SSN: 123-45-6789 is stored here."),
            "french": h.gh_doc("docs/fr.md", ("La photosynthèse est le processus par lequel les plantes vertes "
                                              "convertissent l'énergie lumineuse en énergie chimique. Elle se "
                                              "déroule dans les chloroplastes des cellules des feuilles. ") * 3),
            "empty": h.cc_doc("https://example.com/empty", b"<html><body><nav><a href='/'>Home</a></nav></body></html>"),
            "unresolved": h.gh_doc("docs/station.md", STATION),
        }
        h.run(list(refs.values()))
        self.assertEqual(h.doc(refs["spam"])["status"], "rejected_quality")
        self.assertTrue(h.doc(refs["spam"])["status_reason"].startswith("noise:"))
        self.assertEqual(h.doc(refs["chatter"])["status"], "rejected_factuality")
        ssn = h.doc(refs["ssn"])
        self.assertEqual((ssn["status"], ssn["canonical_text"]), ("rejected_policy", None))
        self.assertTrue(ssn["status_reason"].startswith("privacy:high_risk_identifier|"))
        self.assertNotIn("123-45-6789", ssn["status_reason"])
        self.assertEqual(h.extraction(refs["french"])["outcome"], "rejected_language")
        self.assertEqual(h.extraction(refs["empty"])["outcome"], "empty")
        unresolved = h.doc(refs["unresolved"])
        self.assertEqual(unresolved["status"], "quarantined")
        self.assertTrue(unresolved["status_reason"].startswith("unresolved:suitability|"))
        self.assertIsNotNone(unresolved["canonical_text"])  # O-14 default: non-sensitive held text is kept
        for doc in h.store.documents.values():
            self.assertNotEqual(doc["status"], "accepted")
            self.assertRegex(doc["status_reason"], r"^[a-z_]+(:[a-z_0-9.]+)*\|")

    def test_sensitive_quarantine_writes_a_safe_dead_letter(self) -> None:
        key = ("-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2"
               "gtZWQyNTUxOQAAACBqk9lX2sGp4mHtZqW8vYcR0dE3fN1LpQx7uJb5KaM2Tw\n-----END OPENSSH PRIVATE KEY-----")
        ref = self.h.gh_doc("docs/setup.md", REFERENCE + "\n\n```\n" + key + "\n```")
        self.h.run([ref])
        doc = self.h.doc(ref)
        self.assertEqual((doc["status"], doc["canonical_text"]), ("quarantined", None))
        (dl_key, dl), = self.h.store.dead_letters.items()
        self.assertEqual(dl_key[0], "document")
        self.assertEqual((dl["disposition"], dl["error_class"], dl["error_code"]),
                         ("quarantined", "policy_quarantine", "secret_detected"))
        self.assertIn("private_key=1", dl["error_message"])
        self.assertIn("fp=sha256:", dl["error_message"])
        self.assertNotIn("b3BlbnNzaC1rZXktdjEA", repr(self.h.store.dead_letters) + repr(self.h.store.documents))


class VersioningTests(unittest.TestCase):
    def test_component_versions_are_single_sourced_and_drive_the_labels(self) -> None:
        from unittest import mock

        from app.processing.factuality import classifier
        from app.processing.language import detector
        from app.processing.silver import config as silver_config

        config = SilverConfig()
        versions = config.component_versions()
        self.assertEqual((versions["language_detector"], versions["suitability_filter"]), ("lang2", "suit2"))
        self.assertEqual((detector.VERSION, classifier.VERSION), ("lang2", "suit2"))
        labels = (config.extraction_version, config.policy_version)
        with mock.patch.object(silver_config, "LANGUAGE_DETECTOR_VERSION", "lang-other"):
            self.assertNotEqual(config.extraction_version, labels[0])  # language decides extraction outcomes
            self.assertNotEqual(config.policy_version, labels[1])
        with mock.patch.object(silver_config, "SUITABILITY_FILTER_VERSION", "suit-other"):
            self.assertEqual(config.extraction_version, labels[0])  # suitability does not affect extraction
            self.assertNotEqual(config.policy_version, labels[1])


class IdempotencyTests(unittest.TestCase):
    def test_rerun_creates_zero_duplicate_rows_and_same_identity(self) -> None:
        h = ProcessorHarness(self)
        refs = [h.cc_doc("https://example.com/p", article_html()), h.gh_doc("README.md", REFERENCE),
                h.gh_doc("docs/fr.md", "Le processus est très important pour les plantes vertes. " * 8)]
        h.run(refs)
        before = h.store.snapshot()
        ids = {r.observation_id: h.extraction(r)["document_id"] for r in refs}
        counts = h.run(refs)
        self.assertEqual(h.store.snapshot(), before)
        self.assertEqual(counts["silver_already_extracted"], 3)
        # A fresh processor with the same versions reaches the same documents from the same Bronze.
        fresh = SilverProcessor(SilverConfig(), h.store, h.index, cc_bronze=h.cc, gh_bronze=h.gh)
        evaluations = [fresh.evaluator.evaluate(r) for r in refs]
        self.assertEqual({e.ref.observation_id: e.document_id for e in evaluations}, ids)

    def test_crash_mid_batch_then_retry_completes_without_duplicates(self) -> None:
        h = ProcessorHarness(self)
        refs = [h.gh_doc("a.md", REFERENCE), h.gh_doc("b.md", MERGE_SORT),
                h.cc_doc("https://example.com/p", article_html())]
        h.store.fail_on_extraction_insert = 2
        with self.assertRaises(ConnectionError):
            h.run(refs)  # crash after the first document of the batch was written
        self.assertEqual(h.store.snapshot(), (0, 0, 0, 0, 0))  # the batch transaction rolled back entirely
        self.assertGreater(h.index.size(), 0)  # stale near-index entries of the rolled-back batch remain ...
        h.store.fail_on_extraction_insert = None
        counts = h.run(refs)
        self.assertEqual(counts["silver_accepted"], 3)  # ... and are harmless: re-checked against the registry
        self.assertEqual((len(h.store.extractions), len(h.store.documents)), (3, 3))
        self.assertEqual({row["document_was_new"] for row in h.store.extractions.values()}, {True})
        self.assertEqual({d["classification"] for d in h.store.decisions.values()}, {"UNIQUE"})
        h.run(refs)
        self.assertEqual((len(h.store.extractions), len(h.store.documents)), (3, 3))

    def test_nondeterministic_rerun_is_reported_not_overwritten(self) -> None:
        h = ProcessorHarness(self)
        ref = h.gh_doc("README.md", REFERENCE)
        h.run([ref])
        key = (ref.observation_id, h.processor.extraction_version)
        h.store.extractions[key] = dict(h.store.extractions[key], outcome="empty", document_id=None,
                                        document_was_new=None)
        prepared = dataclasses.replace(h.processor.prepare(Ctx(), []),
                                       evaluations=[h.processor.evaluator.evaluate(ref)])
        counts = h.processor.persist(None, prepared, job_id=7, attempt_count=2)
        self.assertEqual(counts["silver_nondeterminism"], 1)
        (dl_key, dl), = h.store.dead_letters.items()
        self.assertEqual((dl["error_class"], dl["error_code"]), ("internal_bug", "silver_nondeterministic_extraction"))
        self.assertEqual(h.store.extractions[key]["outcome"], "empty")  # never overwritten

    def test_corrupt_bronze_is_dead_lettered_and_retried_after_repair(self) -> None:
        h = ProcessorHarness(self)
        ref = h.gh_doc("README.md", REFERENCE)
        blob = h.gh.blob_path(ref.blob_sha)
        good = blob.read_bytes()
        blob.write_bytes(b"corrupt")
        counts = h.run([ref])
        self.assertEqual(counts["silver_dead_letter_bronze_blob_missing"], 1)
        self.assertIsNone(h.extraction(ref))
        self.assertEqual(next(iter(h.store.dead_letters.values()))["error_class"], "data_invalid")
        blob.write_bytes(good)  # re-acquisition repaired the evidence
        self.assertEqual(h.run([ref])["silver_accepted"], 1)

    def test_unregistered_dedup_method_stops_the_unit(self) -> None:
        h = ProcessorHarness(self)
        h.store.methods.clear()
        ref = h.gh_doc("README.md", REFERENCE)
        with self.assertRaises(JobFailure) as ctx:
            h.run([ref])
        self.assertEqual(ctx.exception.failure.code, "dedup_method_not_registered")
        self.assertIsNone(h.extraction(ref))

    def test_observation_not_recorded_at_this_commit_is_skipped(self) -> None:
        h = ProcessorHarness(self)
        ref = store_gh(h.gh, "README.md", REFERENCE)  # Bronze exists, but no source_observations row
        self.assertEqual(h.run([ref])["silver_not_observed"], 1)
        self.assertEqual(h.store.snapshot(), (0, 0, 0, 0, 0))


if __name__ == "__main__":
    unittest.main()

"""Chunking V1 persistence: identity, global exact dedup, lineage, idempotency, crash, concurrency, backfill."""

import dataclasses
import json
import threading
import unittest
import uuid

from app.processing.chunking.chunker import ChunkingConfig
from app.processing.chunking.service import ChunkWriter, run_backfill, silver_kind
from app.processing.normalization.text import canonicalize_text
from app.queue.retry import JobFailure
from app.utils.ids import chunk_content_hash, chunk_id_from_text

from tests.unit.chunk_fixtures import BackfillStore, InMemoryChunkStore, silver_document
from tests.unit.test_chunking import section

S1 = section("Formation", 14)
S2 = section("Movement", 14, topic="ice sheets")
S3 = section("Retreat", 14, topic="mountain glaciers")


def writer_for(*documents, config: ChunkingConfig = ChunkingConfig()):
    store = InMemoryChunkStore()
    store.add_documents(*documents)
    version_id = store.register(config)
    return store, ChunkWriter(store, config), version_id


class IdentityAndDedupTests(unittest.TestCase):
    def test_chunks_use_locked_identity_and_record_lineage(self) -> None:
        doc = silver_document("# Glaciers\n\n" + S1 + "\n\n" + S2)
        store, writer, version = writer_for(doc)
        counts = writer.chunk_accepted(None, doc)
        self.assertEqual(counts["chunk_document_chunked"], 1)
        occurrences = store.ordered(doc.document_id, version)
        self.assertEqual([o[0] for o in occurrences], [0, 1])
        for ordinal, chunk_id, start, end, section_path in occurrences:
            text = store.chunks[chunk_id]["chunk_text"]
            self.assertEqual(chunk_id, chunk_id_from_text(text))                      # Identity V1
            self.assertEqual(canonicalize_text(doc.canonical_text[start:end]), text)  # schema §5.15 invariant
            self.assertEqual(store.chunks[chunk_id]["first_document_id"], doc.document_id)
            self.assertIn("Glaciers", json.loads(section_path)["headings"])

    def test_identical_chunks_within_one_document_are_one_registry_row(self) -> None:
        doc = silver_document(S1 + "\n\n" + S2 + "\n\n" + S1)
        store, writer, version = writer_for(doc)
        counts = writer.chunk_accepted(None, doc)
        ids = [o[1] for o in store.ordered(doc.document_id, version)]
        self.assertEqual(ids[0], ids[2])            # the repeated section is one chunk ...
        self.assertNotEqual(ids[0], ids[1])
        self.assertEqual(len(store.chunks), 2)      # ... registered once
        self.assertEqual(len(store.occurrences), 3)  # with an occurrence per position
        self.assertEqual((counts["chunk_new"], counts["chunk_existing"]), (2, 1))

    def test_identical_chunks_across_documents_and_sources_are_one_registry_row(self) -> None:
        web = silver_document(S1 + "\n\n" + S2, kind="html")          # a Common Crawl page
        repo = silver_document(S1 + "\n\n" + S3, kind="markdown")     # a GitHub README
        store, writer, version = writer_for(web, repo)
        writer.chunk_accepted(None, web)
        counts = writer.chunk_accepted(None, repo)
        self.assertEqual((counts["chunk_new"], counts["chunk_existing"]), (1, 1))
        shared = chunk_id_from_text(S1)
        self.assertEqual(store.ordered(web.document_id, version)[0][1], shared)
        self.assertEqual(store.ordered(repo.document_id, version)[0][1], shared)
        self.assertEqual(len(store.chunks), 3)
        self.assertEqual(len(store.occurrences), 4)
        self.assertEqual(store.chunks[shared]["first_document_id"], web.document_id)  # first writer kept

    def test_code_documents_are_parsed_as_code(self) -> None:
        self.assertEqual(silver_kind("accepted|01_integrity=PASS;03_mime=PASS:code;04_decode=PASS"), "code")
        self.assertEqual(silver_kind("accepted|01_integrity=PASS;03_mime=PASS:text/html"), "prose")
        self.assertEqual(silver_kind(None), "prose")


class IdempotencyAndSafetyTests(unittest.TestCase):
    def test_rerun_writes_nothing_and_returns_the_same_ordered_chunks(self) -> None:
        doc = silver_document("# Glaciers\n\n" + S1 + "\n\n" + S2 + "\n\n" + S3)
        store, writer, version = writer_for(doc)
        writer.chunk_accepted(None, doc)
        before, ordered = store.snapshot(), store.ordered(doc.document_id, version)
        counts = ChunkWriter(store, ChunkingConfig()).chunk_accepted(None, doc)  # a fresh writer
        self.assertEqual(counts["chunk_document_already_chunked"], 1)
        self.assertEqual((store.snapshot(), store.ordered(doc.document_id, version)), (before, ordered))

    def test_nondeterminism_is_reported_never_overwritten(self) -> None:
        doc = silver_document("# Glaciers\n\n" + S1 + "\n\n" + S2)
        store, writer, version = writer_for(doc)
        writer.chunk_accepted(None, doc)
        key = (doc.document_id, version, 1)
        store.occurrences[key] = dict(store.occurrences[key], char_end=store.occurrences[key]["char_end"] - 1)
        counts = writer.chunk_accepted(None, doc)
        self.assertEqual(counts["chunk_document_nondeterministic"], 1)
        (dead_letter,) = store.dead_letters.values()
        self.assertEqual((dead_letter["error_class"], dead_letter["error_code"]),
                         ("internal_bug", "chunking_nondeterministic"))

    def test_crash_mid_document_rolls_back_then_retry_completes(self) -> None:
        doc = silver_document("# Glaciers\n\n" + S1 + "\n\n" + S2 + "\n\n" + S3)
        store, writer, version = writer_for(doc)
        store.fail_on_occurrence = 2
        with self.assertRaises(ConnectionError):
            with store.atomic():
                writer.chunk_accepted(None, doc)
        self.assertEqual(store.snapshot(), (0, 0, 0))  # no chunk without its occurrence, nothing half-written
        store.fail_on_occurrence = None
        with store.atomic():
            writer.chunk_accepted(None, doc)
        self.assertEqual(len(store.ordered(doc.document_id, version)), 3)

    def test_concurrent_writers_share_chunks_without_duplicates(self) -> None:
        documents = [silver_document(S1 + "\n\n" + section(f"Part {i}", 14, topic=f"topic {i}")) for i in range(8)]
        store, _writer, version = writer_for(*documents)
        barrier = threading.Barrier(8)
        errors: list[BaseException] = []

        def work(document):
            try:
                barrier.wait()
                with store.atomic():  # one transaction per writer
                    ChunkWriter(store, ChunkingConfig()).chunk_accepted(None, document)
                    ChunkWriter(store, ChunkingConfig()).chunk_accepted(None, document)  # same doc twice
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=work, args=(d,)) for d in documents]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(store.chunks), 9)          # S1 once + 8 distinct parts
        self.assertEqual(len(store.occurrences), 16)    # 2 per document, no duplicates
        shared = chunk_id_from_text(S1)
        self.assertEqual(sum(1 for o in store.occurrences.values() if o["chunk_id"] == shared), 8)

    def test_a_new_chunking_version_adds_occurrences_and_reuses_identical_chunks(self) -> None:
        doc = silver_document("# Glaciers\n\n" + S1 + "\n\n" + S2)
        store, writer, v1 = writer_for(doc)
        writer.chunk_accepted(None, doc)
        other = ChunkingConfig(target_tokens=200)
        v2 = store.register(other)
        ChunkWriter(store, other).chunk_accepted(None, doc)
        self.assertNotEqual(v1, v2)
        self.assertEqual(len(store.ordered(doc.document_id, v1)), 2)
        self.assertEqual(len(store.ordered(doc.document_id, v2)), 2)
        self.assertEqual([o[1] for o in store.ordered(doc.document_id, v1)],
                         [o[1] for o in store.ordered(doc.document_id, v2)])  # same text -> same chunk rows
        self.assertEqual(len(store.chunks), 2)

    def test_corrupt_rows_and_unregistered_versions_are_refused(self) -> None:
        doc = silver_document("# Glaciers\n\n" + S1)
        store, writer, _v = writer_for(doc)
        tampered = dataclasses.replace(doc, canonical_text=doc.canonical_text.replace("glaciers", "glacierz"))
        counts = writer.chunk_accepted(None, tampered)
        self.assertEqual(counts["chunk_document_identity_mismatch"], 1)
        self.assertEqual(store.snapshot()[:2], (0, 0))
        with self.assertRaises(JobFailure) as ctx:
            ChunkWriter(InMemoryChunkStore(), ChunkingConfig()).chunk_accepted(None, doc)
        self.assertEqual(ctx.exception.failure.code, "chunking_version_not_registered")
        mismatched = InMemoryChunkStore()
        mismatched.versions[ChunkingConfig().name] = (1, "kbp-approx-bpe:1", {"tampered": True})
        with self.assertRaises(JobFailure) as ctx:
            ChunkWriter(mismatched, ChunkingConfig()).chunk_accepted(None, doc)
        self.assertEqual(ctx.exception.failure.code, "chunking_version_mismatch")


class BackfillTests(unittest.TestCase):
    def documents(self, n: int):
        return [silver_document(f"# Doc {i}\n\n" + section(f"Part {i}", 12, topic=f"subject {i}")) for i in range(n)]

    def test_bounded_resumable_and_rerunnable(self) -> None:
        docs = self.documents(5) + [silver_document("# Held\n\n" + S2, status="quarantined")]
        store = BackfillStore(docs)
        version = store.register(ChunkingConfig())
        writer = ChunkWriter(store, ChunkingConfig())
        first = run_backfill(store.atomic, writer, limit=3, batch_size=2)
        self.assertEqual((first.documents, first.batches), (3, 2))
        second = run_backfill(store.atomic, writer, limit=100, batch_size=2)
        self.assertEqual(second.documents, 2)  # resumes with the remaining accepted documents only
        before = store.snapshot()
        third = run_backfill(store.atomic, writer, limit=100, batch_size=2)
        self.assertEqual((third.documents, store.snapshot()), (0, before))
        chunked = {d for (d, v, _o) in store.occurrences if v == version}
        self.assertEqual(chunked, {d.document_id for d in docs[:5]})

    def test_transient_errors_retry_the_batch_and_stop_is_graceful(self) -> None:
        store = BackfillStore(self.documents(4))
        store.register(ChunkingConfig())
        writer = ChunkWriter(store, ChunkingConfig())
        failures = [ConnectionError("reset")]
        original = store.pending_documents

        def flaky(*args, **kwargs):
            if failures:
                raise failures.pop()
            return original(*args, **kwargs)

        store.pending_documents = flaky
        report = run_backfill(store.atomic, writer, limit=10, batch_size=2, sleep=lambda _s: None,
                              is_transient=lambda exc: isinstance(exc, ConnectionError))
        self.assertEqual((report.documents, report.retries), (4, 1))
        stop = threading.Event()
        stop.set()
        self.assertTrue(run_backfill(store.atomic, writer, limit=10, stop_event=stop).stopped)


class SilverInlineChunkingTests(unittest.TestCase):
    def test_accepted_documents_are_chunked_in_the_silver_transaction(self) -> None:
        from tests.unit.silver_fixtures import article_html
        from tests.unit.test_silver_processor import ProcessorHarness

        harness = ProcessorHarness(self)
        store = InMemoryChunkStore(documents=harness.store.documents.keys())
        version = store.register(ChunkingConfig())
        harness.processor.chunker = ChunkWriter(store, ChunkingConfig())
        web = harness.cc_doc("https://example.com/photosynthesis", article_html())
        mirror = harness.cc_doc("https://mirror.example.org/photosynthesis", article_html())
        french = harness.gh_doc("docs/fr.md", "Le processus est très important pour les plantes vertes. " * 8)
        counts = harness.run([web, mirror, french])
        self.assertEqual((counts["chunk_document_chunked"], counts["silver_exact_duplicate"]), (1, 1))
        document_id = harness.extraction(web)["document_id"]
        occurrences = store.ordered(document_id, version)
        self.assertEqual(len(occurrences), 3)  # the article's three sections
        text = harness.doc(web)["canonical_text"]
        for _o, chunk_id, start, end, _p in occurrences:
            self.assertEqual(store.chunks[chunk_id]["chunk_text"], text[start:end])
            self.assertEqual(store.chunks[chunk_id]["chunk_text"], canonicalize_text(text[start:end]))
            self.assertEqual(chunk_id, uuid.UUID(str(chunk_id_from_text(text[start:end]))))
            self.assertEqual(len(chunk_content_hash(text[start:end])), 64)
        self.assertEqual({d for (d, _v, _o) in store.occurrences}, {document_id})  # only the accepted doc


if __name__ == "__main__":
    unittest.main()

"""Focused tests for Global Chunk Near-Dedup V1 (offline; in-memory 001B store + real SQLite side index)."""

import random
import re
import shutil
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

from app.processing.chunk_dedup import eligibility, method
from app.processing.chunk_dedup.facts import fact_signature
from app.processing.chunk_dedup.index import (CandidateIndex, IndexDegraded, IndexIdentity, IndexIncompatible,
                                              IndexIncomplete, IndexMetrics, build_index)
from app.processing.chunk_dedup.service import (ChunkDedupError, ChunkNearDeduplicator, rebuild_index, replay,
                                                run_backfill)
from app.processing.chunk_dedup.verify import NEAR_DUPLICATE, RELATED, UNIQUE, Decision, decide
from app.queue.retry import JobFailure

from tests.unit.chunk_dedup_fixtures import InjectedCrash, InMemoryChunkDedupStore, chunk

DAM = ("The Kariba Dam was completed in 1959 on the Zambezi River between Zambia and Zimbabwe. Its hydroelectric "
       "station generates about 1,626 megawatts of electricity for both countries. The reservoir behind the dam is "
       "one of the largest artificial lakes in the world by volume. Engineers monitor the plunge pool below the "
       "spillway because erosion threatens the foundations of the wall.")
GLACIER = ("Glacier mass balance in the Alps decreased sharply after 1980 according to the survey. Most mountain "
           "glaciers lost more ice than they gained in each measured year. The researchers compared stake readings "
           "with satellite altimetry and found higher melt rates at lower elevations. The long records help "
           "hydrologists plan the water supply for the valleys below.")
TURBINES = ("The plant uses three turbines that were installed by the original contractor. Each unit spins at a "
            "steady speed and feeds the regional grid through a dedicated transformer. Operators rotate "
            "maintenance between the units so that the station keeps running during the dry season.")
UNRELATED = ("Sourdough bread relies on a culture of wild yeast and lactic acid bacteria kept alive by regular "
             "feeding with flour and water. Bakers adjust hydration and proofing time to control the open crumb and "
             "the sour flavour of the loaf.")


def markdown(text: str, heading: str = "Kariba Dam") -> str:
    sentences = re.split(r"(?<=[.!?])\s+", text)
    first, _, rest = sentences[0].partition(" ")
    return f"## {heading}\n\n" + "\n".join([f"- **{first}** {rest}"] + [f"- {s}" for s in sentences[1:]])


NEAR_VARIANTS = {
    "punctuation": DAM.replace(". ", "; ").replace(",", ""),
    "small_wording": DAM.replace("completed", "finished").replace("monitor", "inspect"),
    "heading_context": "## Zambezi hydropower\n\n" + DAM,
    "cross_source_markdown": markdown(DAM),
}
FACT_VARIANTS = {
    "number": DAM.replace("1,626", "1,470"),
    "unit": DAM.replace("megawatts", "kilowatts"),
    "year": DAM.replace("1959", "1961"),
    "entity": DAM.replace("Zambezi", "Limpopo"),
    "negation": DAM.replace("erosion threatens", "erosion does not threaten"),
    "increase_decrease": GLACIER.replace("decreased", "increased"),
    "higher_lower": GLACIER.replace("higher melt", "lower melt"),
    "more_less": GLACIER.replace("more ice", "less ice"),
    "before_after": GLACIER.replace("after 1980", "before 1980"),
    "gain_loss": GLACIER.replace("lost more ice than they gained", "gained more ice than they lost").replace(
        "gained more ice than they lost", "gained more ice than they lost"),
    "word_number": TURBINES.replace("three turbines", "four turbines"),
}
RELATED_SAME_TOPIC = (DAM.split(". ")[0] + ". " + DAM.split(". ")[1] + ". The dam wall is 128 metres high and "
                      "579 metres long. Floods in the wet season sometimes force operators to open several gates.")


class FactGuardTests(unittest.TestCase):
    def test_formatting_heading_and_cross_source_keep_facts_equal(self) -> None:
        for name, variant in {**NEAR_VARIANTS, "whitespace": DAM.replace(" ", "  ")}.items():
            with self.subTest(name):
                self.assertEqual(fact_signature(variant), fact_signature(DAM))

    def test_every_fact_change_is_detected(self) -> None:
        bases = {"increase_decrease": GLACIER, "higher_lower": GLACIER, "more_less": GLACIER,
                 "before_after": GLACIER, "word_number": TURBINES}
        for name, variant in FACT_VARIANTS.items():
            base = bases.get(name, DAM if name != "gain_loss" else GLACIER)
            with self.subTest(name):
                self.assertNotEqual(variant, base)
                self.assertFalse(fact_signature(variant).compatible(fact_signature(base)))

    def test_directional_pairs_and_synonyms(self) -> None:
        pairs = {("Revenue rose sharply.", "Revenue fell sharply."): False,
                 ("Revenue rose sharply.", "Revenue increased sharply."): True,
                 ("Output was higher than planned.", "Output was lower than planned."): False,
                 ("There were more visitors.", "There were fewer visitors."): False,
                 ("It closed before the war.", "It closed after the war."): False,
                 ("The club posted a gain.", "The club posted a loss."): False}
        for (left, right), compatible in pairs.items():
            with self.subTest(left=left, right=right):
                self.assertEqual(fact_signature(left).compatible(fact_signature(right)), compatible)

    def test_role_swaps_inside_a_sentence_are_detected_but_sentence_order_is_not(self) -> None:
        swaps = [("Company A acquired Company B in 2019.", "Company B acquired Company A in 2019."),
                 ("Output grew from 10 tonnes to 20 tonnes.", "Output grew from 20 tonnes to 10 tonnes."),
                 ("The team lost more games than it gained.", "The team gained more games than it lost.")]
        for left, right in swaps:
            with self.subTest(left):
                self.assertFalse(fact_signature(left).compatible(fact_signature(right)))
        reordered = " ".join(reversed(re.split(r"(?<=[.!?])\s+", DAM)))
        self.assertTrue(fact_signature(reordered).compatible(fact_signature(DAM)))

    def test_word_number_normalization(self) -> None:
        same = [("It employs ten people.", "It employs 10 people."),
                ("It holds twenty-five tonnes.", "It holds 25 tonnes."),
                ("Sales grew by ten percent.", "Sales grew by 10%."),
                ("It raised ten million dollars.", "It raised 10 million dollars."),
                ("It was the first site.", "It was the 1st site.")]
        for left, right in same:
            with self.subTest(left):
                self.assertEqual(fact_signature(left), fact_signature(right))
        self.assertNotEqual(fact_signature("It employs ten people."), fact_signature("It employs eleven people."))
        self.assertNotEqual(fact_signature("It was the first site."), fact_signature("It was the third site."))

    def test_entities_ignore_sentence_initial_capitals_but_keep_names_and_acronyms(self) -> None:
        signature = fact_signature("Revenue grew. The NASA team met Ada Lovelace in March 1843. It was not 10 km away.")
        self.assertEqual(signature.entities, ("ada lovelace", "nasa"))
        self.assertIn("month:march", signature.quantities)
        self.assertIn("10 km", signature.quantities)
        self.assertEqual(signature.negations, ("not",))


class VerifierTests(unittest.TestCase):
    def compare(self, text: str):
        return chunk(text).comparison()

    def test_near_variants_are_near_duplicates(self) -> None:
        base = self.compare(DAM)
        for name, variant in NEAR_VARIANTS.items():
            with self.subTest(name):
                d = decide(self.compare(variant), [base])
                self.assertEqual((d.classification, d.representative_chunk_id), (NEAR_DUPLICATE, base.chunk_id))
                self.assertGreaterEqual(d.similarity, method.NEAR_THRESHOLD)
        normalized_duplicate = decide(self.compare(DAM.replace(". ", "; ")), [base])  # distinct chunk_id, J = 1
        self.assertEqual((normalized_duplicate.classification, normalized_duplicate.similarity), (NEAR_DUPLICATE, 1.0))

    def test_fact_mismatches_stay_related_even_at_high_similarity(self) -> None:
        bases = {"increase_decrease": GLACIER, "higher_lower": GLACIER, "more_less": GLACIER, "before_after": GLACIER,
                 "gain_loss": GLACIER, "word_number": TURBINES}
        for name, variant in FACT_VARIANTS.items():
            with self.subTest(name):
                d = decide(self.compare(variant), [self.compare(bases.get(name, DAM))])
                self.assertEqual(d.classification, RELATED)
                self.assertGreaterEqual(d.similarity, method.NEAR_THRESHOLD)  # lexical twins, different facts
                self.assertEqual(d.fact_blocked, 1)
                self.assertIsNone(d.representative_chunk_id)

    def test_related_unique_and_deterministic_choice(self) -> None:
        base = self.compare(DAM)
        related = decide(self.compare(RELATED_SAME_TOPIC), [base])
        self.assertEqual(related.classification, RELATED)
        self.assertTrue(method.RELATED_THRESHOLD <= related.similarity < method.NEAR_THRESHOLD)
        self.assertEqual(decide(self.compare(UNRELATED), [base]).classification, UNIQUE)
        self.assertEqual(decide(self.compare(UNRELATED), []), Decision(UNIQUE, None, None))
        # two representatives at the same similarity: the tie breaks by chunk_hash, never by candidate order
        twins = [self.compare(DAM + " Extra alpha words here."), self.compare(DAM + " Extra gamma words here.")]
        query = self.compare(DAM)
        self.assertEqual(method.jaccard(query.features, twins[0].features),
                         method.jaccard(query.features, twins[1].features))
        choice = {decide(query, order).representative_chunk_id for order in (twins, twins[::-1])}
        self.assertEqual(choice, {min(twins, key=lambda t: t.chunk_hash).chunk_id})


class IndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.identity = IndexIdentity("kb_test", 1)

    def test_bucket_reads_are_bounded_and_truncation_is_measured(self) -> None:
        index = CandidateIndex(":memory:", self.identity, max_bucket_rows=5, max_candidates=3)
        self.addCleanup(index.close)
        keys = method.band_keys(method.features(DAM))
        for i in range(8):
            index.add(i + 1, keys)
        found, stats = index.candidates(keys)
        self.assertEqual(found, [1, 2, 3])  # capped, ranked by shared bands then key
        self.assertEqual((stats.probes, stats.truncated_probes, stats.rows_read), (10, 10, 50))
        metrics = IndexMetrics(min_probes=10)
        metrics.record(stats)
        self.assertTrue(metrics.degraded())
        with self.assertRaises(IndexDegraded):
            metrics.check()

    def test_identity_is_enforced_and_missing_file_is_incomplete(self) -> None:
        path = self.root / "idx.sqlite3"
        CandidateIndex(path, self.identity, create=True).close()
        with self.assertRaises(IndexIncompatible):
            CandidateIndex(path, IndexIdentity("kb_test", 2))
        with self.assertRaises(IndexIncompatible):
            CandidateIndex(path, IndexIdentity("other_db", 1))
        with self.assertRaises(IndexIncomplete):
            CandidateIndex(self.root / "missing.sqlite3", self.identity)


def near_dedup_corpus():
    texts = [DAM, GLACIER, TURBINES, UNRELATED, RELATED_SAME_TOPIC, *NEAR_VARIANTS.values(), *FACT_VARIANTS.values()]
    return [chunk(t) for t in texts]


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.path = self.root / "index.sqlite3"

    def dedup(self, store, *, path=None, create=True, **kwargs) -> ChunkNearDeduplicator:
        index = CandidateIndex(path or self.path, IndexIdentity("kb_test", 1), create=create, **kwargs)
        self.addCleanup(index.close)
        return ChunkNearDeduplicator(store, index, run_id=uuid.uuid4())

    def run_all(self, store, dedup, **kwargs):
        return run_backfill(store.transaction, dedup, limit=kwargs.pop("limit", 1000),
                            batch_size=kwargs.pop("batch_size", 4), sleep=lambda _s: None, **kwargs)

    def reference(self):
        store = InMemoryChunkDedupStore(near_dedup_corpus())
        path = self.root / "reference.sqlite3"
        dedup = self.dedup(store, path=path)
        self.run_all(store, dedup)
        return store, dedup.index.fingerprint()

    def test_backfill_classifies_the_corpus_and_keeps_invariants(self) -> None:
        store, _fp = self.reference()
        by_text = {c.chunk_text: c.chunk_id for c in store.registry.values()}
        d = {text: store.decisions[chunk_id] for text, chunk_id in by_text.items()}
        clusters = {DAM, *NEAR_VARIANTS.values()}
        representative = {d[t]["representative"] or by_text[t] for t in clusters}
        self.assertEqual(len(representative), 1)  # exactly one representative for the whole near cluster
        self.assertEqual(sum(d[t]["classification"] == NEAR_DUPLICATE for t in clusters), len(clusters) - 1)
        for name, variant in FACT_VARIANTS.items():
            with self.subTest(name):
                self.assertNotEqual(d[variant]["classification"], NEAR_DUPLICATE)  # facts never merged
        self.assertEqual(d[UNRELATED]["classification"], UNIQUE)
        self.assertEqual(set(store.audit().values()), {0})
        self.assertEqual(len(store.decisions), len(store.registry))

    def test_same_chunks_same_decisions_regardless_of_arrival_order_and_batch_size(self) -> None:
        reference, fingerprint = self.reference()
        for seed, batch in ((1, 1), (2, 3), (3, 50)):
            with self.subTest(seed=seed, batch=batch):
                corpus = near_dedup_corpus()
                random.Random(seed).shuffle(corpus)
                store = InMemoryChunkDedupStore(corpus)
                dedup = self.dedup(store, path=self.root / f"s{seed}.sqlite3")
                self.run_all(store, dedup, batch_size=batch)
                self.assertEqual(store.state(), reference.state())
                self.assertEqual(dedup.index.fingerprint(), fingerprint)

    def test_rerun_is_a_no_op(self) -> None:
        store = InMemoryChunkDedupStore(near_dedup_corpus())
        dedup = self.dedup(store)
        self.run_all(store, dedup)
        before = store.state()
        report = self.run_all(store, dedup)
        self.assertEqual((report.chunks, store.state()), (0, before))

    def test_later_better_arrival_never_rewrites_a_persisted_representative(self) -> None:
        texts = sorted([DAM, NEAR_VARIANTS["punctuation"]], key=lambda t: chunk(t).chunk_hash)
        late, early = chunk(texts[0]), chunk(texts[1])  # "late" sorts first: it would win in a joint run
        store = InMemoryChunkDedupStore([early])
        dedup = self.dedup(store)
        self.run_all(store, dedup)
        store.add_chunk(late)
        self.run_all(store, dedup)  # incremental arrival
        self.assertEqual(store.decisions[early.chunk_id]["classification"], UNIQUE)
        self.assertEqual((store.decisions[late.chunk_id]["classification"],
                          store.decisions[late.chunk_id]["representative"]), (NEAR_DUPLICATE, early.chunk_id))

    def test_representative_chain_is_refused(self) -> None:
        store, _fp = self.reference()
        suppressed = next(k for k, v in store.decisions.items() if v["classification"] == NEAR_DUPLICATE)
        target = chunk(DAM + " Additional sentence for a fresh chunk id.")
        store.add_chunk(target)
        with store.transaction():
            refused = store.insert_decision(None, 1, target.chunk_id, Decision(NEAR_DUPLICATE, suppressed, 0.9),
                                            uuid.uuid4())
        self.assertFalse(refused)  # a NEAR_DUPLICATE may never point at a suppressed chunk (no chains, no cycles)
        self.assertNotIn(target.chunk_id, store.decisions)

    def test_crashes_leave_no_partial_state_and_the_rerun_converges(self) -> None:
        reference, fingerprint = self.reference()
        for name, target in (("pending_chunks", 2), ("insert_decision", 6), ("insert_relationship", 2),
                             ("index_add", 3)):
            with self.subTest(crash=name):
                store = InMemoryChunkDedupStore(near_dedup_corpus())
                dedup = self.dedup(store, path=self.root / f"crash-{name}.sqlite3")
                if name == "index_add":
                    original, calls = dedup.index.add, {"n": 0}

                    def flaky(key, keys, original=original, calls=calls):
                        calls["n"] += 1
                        if calls["n"] == target:
                            raise InjectedCrash("injected crash in index add")
                        return original(key, keys)

                    dedup.index.add = flaky
                else:
                    store.fail[name] = target
                with self.assertRaises(InjectedCrash):
                    self.run_all(store, dedup, is_transient=lambda _e: False)
                self.assertEqual(set(store.audit().values()), {0})  # committed batches are whole
                self.run_all(store, dedup)  # resume
                self.assertEqual(store.state(), reference.state())
                self.assertEqual(dedup.index.fingerprint(), fingerprint)  # compensation left no stale items

    def test_transient_failure_retries_the_batch(self) -> None:
        reference, _fp = self.reference()
        store = InMemoryChunkDedupStore(near_dedup_corpus())
        store.fail["insert_relationship"] = 1
        report = self.run_all(store, self.dedup(store), is_transient=lambda e: isinstance(e, InjectedCrash))
        self.assertEqual((report.retries, store.state()), (1, reference.state()))

    def test_concurrent_workers_produce_the_sequential_result(self) -> None:
        reference, _fp = self.reference()
        store = InMemoryChunkDedupStore(near_dedup_corpus())
        workers = [self.dedup(store, create=True), self.dedup(store, create=False)]  # one shared index file
        errors: list[BaseException] = []

        def work(dedup):
            try:
                self.run_all(store, dedup, batch_size=2)
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=work, args=(w,)) for w in workers]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertEqual(errors, [])
        self.assertEqual(store.state(), reference.state())
        self.assertEqual(set(store.audit().values()), {0})

    def test_same_chunk_race_and_contradiction_never_overwrite(self) -> None:
        store = InMemoryChunkDedupStore([chunk(DAM), chunk(NEAR_VARIANTS["punctuation"])])
        dedup = self.dedup(store)
        self.run_all(store, dedup)
        before = store.state()
        target = next(iter(store.registry.values()))
        with store.transaction():
            counts = dedup.process_chunk(None, 1, target, [])  # a second worker evaluating the same chunk
        self.assertEqual((counts["chunk_already_decided"], store.state()), (1, before))
        store.decisions[target.chunk_id]["similarity"] = 0.123  # a stored decision that disagrees
        stored = dict(store.decisions[target.chunk_id])
        with store.transaction():
            counts = dedup.process_chunk(None, 1, target, [])
        self.assertEqual(counts["chunk_decision_conflict"], 1)
        self.assertEqual(store.decisions[target.chunk_id], stored)  # never overwritten
        self.assertIn(str(target.chunk_id), store.dead_letters)

    def test_lost_index_is_detected_and_rebuild_restores_identical_behavior(self) -> None:
        store = InMemoryChunkDedupStore(near_dedup_corpus()[:8])
        dedup = self.dedup(store)
        self.run_all(store, dedup)
        fingerprint = dedup.index.fingerprint()
        dedup.index.close()
        self.path.unlink()
        with self.assertRaises(IndexIncomplete):
            CandidateIndex(self.path, IndexIdentity("kb_test", 1))
        empty = self.dedup(store, path=self.root / "empty.sqlite3")
        with self.assertRaises(IndexIncomplete):
            self.run_all(store, empty)  # PostgreSQL has representatives the index does not
        built = rebuild_index(store.transaction, store, self.path, database="kb_test")
        self.assertEqual((built["fingerprint"], built["representatives"]), (fingerprint, built["pg_representatives"]))
        result = replay(store.decisions_in_order(), store.stored_relationships())
        self.assertEqual((result["decision_mismatch_count"], result["relationship_mismatch_count"]), (0, 0))
        for extra in near_dedup_corpus()[8:]:
            store.add_chunk(extra)
        self.run_all(store, self.dedup(store, create=False))  # incremental after the rebuild
        self.assertEqual(set(store.audit().values()), {0})
        self.assertEqual(len(store.decisions), len(store.registry))

    def test_degraded_index_stops_before_persisting_the_batch(self) -> None:
        store = InMemoryChunkDedupStore([chunk(f"{DAM} Variant sentence number {i} here.") for i in range(12)])
        dedup = self.dedup(store, max_bucket_rows=1)
        dedup.metrics = IndexMetrics(min_probes=10)
        with self.assertRaises(IndexDegraded):
            self.run_all(store, dedup, batch_size=12)
        self.assertEqual(store.decisions, {})  # the degraded batch rolled back
        self.assertEqual(dedup.index.item_count(), 0)  # and its index entries were compensated

    def test_method_must_be_registered_with_frozen_parameters(self) -> None:
        for store, code in ((InMemoryChunkDedupStore(registered=False), "chunk_dedup_method_not_registered"),
                            (InMemoryChunkDedupStore(parameters={"changed": True}), "chunk_dedup_method_mismatch")):
            with self.subTest(code), self.assertRaises(JobFailure) as caught:
                self.run_all(store, self.dedup(store, path=self.root / f"{code}.sqlite3"))
            self.assertEqual(caught.exception.failure.code, code)

    def test_provenance_is_never_touched_and_eligibility_maps_to_the_representative(self) -> None:
        store = InMemoryChunkDedupStore()
        cc, gh = chunk(DAM), chunk(markdown(DAM))
        store.add_chunk(cc, document="cc-page", ordinal=0)
        store.add_chunk(gh, document="gh-readme", ordinal=2)
        occurrences = {k: list(v) for k, v in store.occurrences.items()}
        registry = dict(store.registry)
        self.run_all(store, self.dedup(store))
        self.assertEqual((store.occurrences, store.registry), (occurrences, registry))
        status = {k: eligibility.classify(k, v["classification"], v["representative"], v["status"])
                  for k, v in store.decisions.items()}
        eligible = [s for s in status.values() if s.eligible]
        suppressed = [s for s in status.values() if s.status == eligibility.SUPPRESSED]
        self.assertEqual((len(eligible), len(suppressed)), (1, 1))
        self.assertEqual(suppressed[0].embed_chunk_id, eligible[0].chunk_id)
        self.assertEqual(eligibility.classify(cc.chunk_id, None, None, None).status, eligibility.UNDECIDED)
        self.assertEqual(eligibility.classify(cc.chunk_id, UNIQUE, None, "overridden").status,
                         eligibility.NOT_CONFIRMED)


class BuildTests(unittest.TestCase):
    def test_build_swaps_atomically_and_refuses_nothing_partial(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        path = root / "i.sqlite3"
        rows = [(c.chunk_hash, c.chunk_text) for c in near_dedup_corpus()[:3]]
        first = build_index(path, IndexIdentity("kb_test", 1), rows)
        second = build_index(path, IndexIdentity("kb_test", 1), rows)
        self.assertEqual((first["fingerprint"], first["items"]), (second["fingerprint"], 3))
        self.assertEqual(sorted(p.name for p in root.iterdir()), ["i.sqlite3"])  # no temp leftovers
        foreign = CandidateIndex(":memory:", IndexIdentity("kb_test", 9))  # built for another dedup_method_id
        self.addCleanup(foreign.close)
        with self.assertRaises(ChunkDedupError):
            ChunkNearDeduplicator(InMemoryChunkDedupStore(), foreign, run_id=uuid.uuid4()).method_id(None)


if __name__ == "__main__":
    unittest.main()

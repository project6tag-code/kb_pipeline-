"""Focused checks for the disposable chunk candidate-index benchmark prototypes."""

import unittest
import uuid

from scripts.benchmark_chunk_candidate_indexes import (
    MAX_CANDIDATES,
    BackgroundGenerator,
    BoundedSimHash,
    Item,
    OnePermutationMinHash,
    Query,
    ReducedMinHash,
    SQLitePostings,
    SparsePrefix,
    _markdown,
    fact_examples,
    fact_signature,
    features,
    make_corpus,
    verified_class,
)

SAMPLE = "A station opened in 1998 and supplies nearby farms and homes."


class PrototypePropertiesTests(unittest.TestCase):
    def test_band_indexes_have_fixed_postings(self) -> None:
        sample = features(SAMPLE)
        self.assertEqual(len(ReducedMinHash().postings(sample)), 8)
        self.assertEqual(len(BoundedSimHash().postings(sample)), 8)
        self.assertEqual(len(OnePermutationMinHash(10, 4).postings(sample)), 10)

    def test_one_permutation_minhash_is_deterministic_and_similarity_preserving(self) -> None:
        indexer = OnePermutationMinHash(10, 4)
        text = BackgroundGenerator(3).chunk()[0]
        self.assertEqual(indexer.postings(features(text)), OnePermutationMinHash(10, 4).postings(features(text)))
        shared = set(indexer.postings(features(text))) & set(indexer.postings(features(text.replace(". ", "; "))))
        self.assertEqual(len(shared), 10)  # comparison features ignore punctuation

    def test_sparse_prefix_postings_are_bounded_by_chunk_features(self) -> None:
        sample = features(SAMPLE)
        indexer = SparsePrefix()
        self.assertLessEqual(len(indexer.postings(sample)), len(sample))
        self.assertLessEqual(len(indexer.probes(sample)), 512)

    def test_candidate_output_is_hard_capped_and_excludes_the_query_item(self) -> None:
        store = SQLitePostings(OnePermutationMinHash(8, 2))
        try:
            items = [Item(uuid.UUID(int=i + 1), f"{SAMPLE} Copy {i}.", features(SAMPLE)) for i in range(300)]
            store.build(items)
            candidates, stats = store.lookup(Query(SAMPLE, items[1].item_id, "near", own_id=items[0].item_id))
            self.assertEqual(len(candidates), MAX_CANDIDATES)
            self.assertNotIn(items[0].item_id, candidates)
            # 300 rows per bucket > 256 read cap: which items survive depends on key order (recall risk at scale)
            self.assertEqual((stats["truncated_probes"], stats["rows_uncapped"]), (8, 8 * 300))
        finally:
            store.close()


class FactGuardTests(unittest.TestCase):
    def test_every_fact_mismatch_is_never_near_duplicate(self) -> None:
        for original, changed, kind in fact_examples():
            with self.subTest(kind=kind):
                self.assertNotEqual(fact_signature(original), fact_signature(changed))
                classification, score = verified_class(original, changed)
                self.assertNotEqual(classification, "NEAR_DUPLICATE")
                self.assertEqual(classification, "RELATED", score)  # same topic stays linked

    def test_formatting_and_heading_context_do_not_change_facts(self) -> None:
        for original, _changed, kind in fact_examples():
            with self.subTest(kind=kind):
                gh = _markdown(original, heading="Company History")
                self.assertEqual(fact_signature(gh), fact_signature(original))
                self.assertEqual(verified_class(original, gh)[0], "NEAR_DUPLICATE")

    def test_sentence_initial_words_are_not_entities_but_names_and_acronyms_are(self) -> None:
        numbers, entities, negations = fact_signature(
            "Revenue grew. The NASA team met Ada Lovelace in March 1843. It was not 10 km away.")
        self.assertEqual(entities, ("ada lovelace", "nasa"))
        self.assertIn("month:march", numbers)
        self.assertIn("10 km", numbers)
        self.assertEqual(negations, ("not",))

    def test_corpus_contains_every_required_case(self) -> None:
        _items, queries, counts = make_corpus([], template_count=16, background_count=200, queries_per_kind=50)
        for kind in ("near_distinct_normalized_duplicate", "near_cross_source_markdown",
                     "near_cross_source_markdown_edit", "near_wording_2_words", "fact_number", "fact_date",
                     "fact_entity", "fact_negation", "fact_unit_fixed", "novel"):
            self.assertGreater(counts.get(kind, 0), 0, kind)
        self.assertTrue(all(q.own_id != q.target_id for q in queries if q.own_id))


if __name__ == "__main__":
    unittest.main()

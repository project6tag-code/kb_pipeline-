"""Focused checks for disposable chunk candidate-index benchmark prototypes."""

import unittest
import uuid

from scripts.benchmark_chunk_candidate_indexes import (
    BoundedSimHash,
    Item,
    MAX_CANDIDATES,
    Query,
    ReducedMinHash,
    SQLitePostings,
    SparsePrefix,
    fact_examples,
    fact_signature,
    features,
    verified_class,
)


class PrototypePropertiesTests(unittest.TestCase):
    def test_reduced_minhash_and_simhash_use_eight_postings(self) -> None:
        sample = features("A station opened in 1998 and supplies nearby farms and homes.")
        self.assertEqual(len(ReducedMinHash().postings(sample)), 8)
        self.assertEqual(len(BoundedSimHash().postings(sample)), 8)

    def test_sparse_prefix_postings_are_bounded_by_chunk_features(self) -> None:
        sample = features("A station opened in 1998 and supplies nearby farms and homes.")
        indexer = SparsePrefix()
        self.assertLessEqual(len(indexer.postings(sample)), len(sample))
        self.assertLessEqual(len(indexer.probes(sample)), 512)

    def test_all_fact_examples_are_blocked_from_near_duplicate_class(self) -> None:
        for original, changed, kind in fact_examples():
            with self.subTest(kind=kind):
                self.assertNotEqual(fact_signature(original), fact_signature(changed))
                classification, _score = verified_class(original, changed)
                self.assertNotEqual(classification, "NEAR_DUPLICATE")

    def test_candidate_output_is_hard_capped(self) -> None:
        indexer = ReducedMinHash()
        store = SQLitePostings(indexer)
        try:
            text_value = "A station opened in 1998 and supplies nearby farms and homes."
            items = [Item(uuid.UUID(int=i + 1), text_value, features(text_value)) for i in range(300)]
            store.build(items)
            candidates, _probes = store.lookup(Query(text_value, items[0].item_id, "exact"))
            self.assertLessEqual(len(candidates), MAX_CANDIDATES)
            self.assertIn(items[0].item_id, candidates)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
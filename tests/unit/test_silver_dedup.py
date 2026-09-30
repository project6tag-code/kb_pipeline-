"""Gates 19-20: global exact dedup and near-duplicate / version analysis."""

import unittest
import uuid

from app.processing.dedup import near
from app.processing.dedup.near import NearIndex

from tests.unit.silver_fixtures import article_html
from tests.unit.test_silver_processor import REFERENCE, ProcessorHarness

SENTENCES = [
    "Glaciers are large masses of ice that form where snowfall exceeds melting over many years.",
    "They move slowly downhill under their own weight and reshape the landscape as they flow.",
    "Valley glaciers carve U-shaped valleys, while ice sheets cover entire continents such as Antarctica.",
    "The Antarctic ice sheet contains about 26.5 million cubic kilometres of ice.",
    "Glacial erosion produces landforms such as cirques, aretes, horns and fjords along coastlines.",
    "Moraines are ridges of rock debris deposited at the edges and snout of a glacier.",
    "Meltwater from glaciers supplies rivers used for drinking water and irrigation in many regions.",
    "Since the middle of the nineteenth century most mountain glaciers have been retreating.",
    "Scientists measure glacier mass balance by comparing accumulation of snow with ablation of ice.",
    "Ice cores drilled from glaciers preserve records of past atmospheric composition and temperature.",
    "Surging glaciers can advance several kilometres in a few months before returning to normal flow.",
    "Tidewater glaciers end in the sea and lose mass by calving icebergs from their fronts.",
]
REPLACEMENTS = [
    "Rock glaciers are mixtures of ice and debris that creep slowly down mountain slopes in cold climates.",
    "Glacier ice appears blue because it absorbs red light more strongly than blue light.",
    "The largest glacier outside the polar regions is the Fedchenko Glacier in Tajikistan.",
    "Permafrost is ground that remains frozen for at least two consecutive years.",
]


def glacier_doc(replace: dict[int, int] | None = None, *, extra: str = "") -> str:
    lines = list(SENTENCES)
    for index, replacement in (replace or {}).items():
        lines[index] = REPLACEMENTS[replacement]
    return "# Glaciers\n\n" + " ".join(lines[:6]) + "\n\n" + " ".join(lines[6:]) + extra


class ExactDedupTests(unittest.TestCase):
    def test_same_source_exact_duplicate_links_to_one_document(self) -> None:
        h = ProcessorHarness(self)
        first = h.cc_doc("https://example.com/photosynthesis", article_html())
        mirror = h.cc_doc("https://mirror.example.org/biology/photosynthesis", article_html())
        counts = h.run([first, mirror])
        self.assertEqual((counts["silver_accepted"], counts["silver_exact_duplicate"]), (1, 1))
        self.assertEqual(len(h.store.documents), 1)
        a, b = h.extraction(first), h.extraction(mirror)
        self.assertEqual(a["document_id"], b["document_id"])
        self.assertEqual((a["document_was_new"], b["document_was_new"]), (True, False))
        self.assertEqual(h.doc(first)["first_observation_id"], first.observation_id)

    def test_cross_source_exact_duplicate_common_crawl_vs_github(self) -> None:
        h = ProcessorHarness(self)
        web = h.cc_doc("https://example.com/photosynthesis", article_html())
        h.run([web])
        silver_text = h.doc(web)["canonical_text"]
        repo = h.gh_doc("docs/photosynthesis.txt", silver_text)
        counts = h.run([repo])
        self.assertEqual(counts["silver_exact_duplicate"], 1)
        self.assertEqual(len(h.store.documents), 1)
        self.assertEqual(h.extraction(repo)["document_id"], h.extraction(web)["document_id"])
        # both provenance chains stay linked to the one document
        linked = {oid for (oid, _v), row in h.store.extractions.items()
                  if row["document_id"] == h.extraction(web)["document_id"]}
        self.assertEqual(linked, {web.observation_id, repo.observation_id})


class NearDuplicateTests(unittest.TestCase):
    def test_near_duplicate_is_registered_rejected_and_linked(self) -> None:
        h = ProcessorHarness(self)
        original = h.gh_doc("docs/glaciers.md", glacier_doc())
        copy = h.cc_doc("https://copy.example.net/glaciers", f"<body>{_paragraphs(glacier_doc({11: 0}))}</body>"
                        .encode())
        h.run([original])
        h.run([copy])
        doc = h.doc(copy)
        self.assertEqual(doc["status"], "rejected_near_duplicate")
        decision = h.store.decisions[(doc["document_id"], 1)]
        self.assertEqual(decision["classification"], "NEAR_DUPLICATE")
        self.assertEqual(decision["canonical_document_id"], h.doc(original)["document_id"])
        self.assertGreaterEqual(decision["similarity_score"], 0.8)
        self.assertIn((doc["document_id"], h.doc(original)["document_id"], "NEAR_DUPLICATE", 1),
                      h.store.relationships)

    def test_version_of_the_same_source_path_is_kept_and_linked(self) -> None:
        h = ProcessorHarness(self)
        v1 = h.gh_doc("docs/glaciers.md", glacier_doc(), commit_sha="a" * 40)
        v2 = h.gh_doc("docs/glaciers.md", glacier_doc({2: 0, 5: 1, 9: 2}), commit_sha="b" * 40)
        h.run([v1])
        h.run([v2])
        self.assertEqual(h.doc(v2)["status"], "accepted")
        decision = h.store.decisions[(h.doc(v2)["document_id"], 1)]
        self.assertEqual(decision["classification"], "VERSION")
        self.assertEqual(decision["canonical_document_id"], h.doc(v1)["document_id"])
        self.assertTrue(0.5 <= decision["similarity_score"] < 0.8, decision)

    def test_related_but_not_duplicate_documents_are_not_merged(self) -> None:
        h = ProcessorHarness(self)
        a = h.gh_doc("docs/binary_search.md", REFERENCE)
        b = h.gh_doc("docs/glaciers.md", glacier_doc())
        c = h.cc_doc("https://example.com/photosynthesis", article_html())  # same broad science register
        h.run([a, b, c])
        self.assertEqual({h.doc(r)["status"] for r in (a, b, c)}, {"accepted"})
        self.assertEqual({d["classification"] for d in h.store.decisions.values()}, {"UNIQUE"})
        self.assertEqual(h.store.relationships, {})


class CandidateSearchTests(unittest.TestCase):
    def test_signature_is_deterministic_and_estimates_similarity(self) -> None:
        a, b = near.shingles(glacier_doc()), near.shingles(glacier_doc({11: 0}))
        self.assertEqual(near.signature(a), near.signature(frozenset(sorted(a))))
        sig_a, sig_b = near.signature(a), near.signature(b)
        estimate = sum(x == y for x, y in zip(sig_a, sig_b)) / near.BINS
        self.assertAlmostEqual(estimate, near.jaccard(a, b), delta=0.15)
        self.assertEqual(len(near.band_keys(sig_a)), near.BANDS)

    def test_candidate_search_is_bounded(self) -> None:
        index = NearIndex(":memory:")
        self.addCleanup(index.close)
        sig = near.signature(near.shingles(glacier_doc()))
        ids = [uuid.UUID(int=i + 1) for i in range(200)]
        for doc in ids:  # 200 identical signatures: every band bucket holds 200 entries
            index.add(doc, sig)
        found = index.candidates(sig, max_bucket_rows=16, max_candidates=5)
        self.assertEqual(found, sorted(found)[:5])
        self.assertEqual(len(found), 5)
        self.assertEqual(index.size(), 200)
        unrelated = near.signature(near.shingles(REFERENCE))
        self.assertEqual(index.candidates(unrelated, max_bucket_rows=16, max_candidates=5), [])

    def test_processor_checks_at_most_max_candidates(self) -> None:
        from dataclasses import replace

        from app.processing.silver.config import SilverConfig

        config = SilverConfig()
        config = replace(config, limits=replace(config.limits, max_near_candidates=2))
        h = ProcessorHarness(self, config)
        for i in range(4):  # distinct lineages, pairwise below the near-duplicate threshold
            h.run([h.gh_doc(f"docs/g{i}.md", glacier_doc({i: i}), repository_id=100 + i)])
        probe = h.gh_doc("docs/probe.md", glacier_doc({0: 1, 1: 2, 2: 3}), repository_id=999)
        h.run([probe])
        self.assertIn("candidates=2", h.doc(probe)["status_reason"])


def _paragraphs(markdown: str) -> str:
    blocks = markdown.split("\n\n")
    return f"<h1>{blocks[0].lstrip('# ')}</h1>" + "".join(f"<p>{b}</p>" for b in blocks[1:])


if __name__ == "__main__":
    unittest.main()

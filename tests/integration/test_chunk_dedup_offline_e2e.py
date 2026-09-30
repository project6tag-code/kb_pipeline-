"""Offline end-to-end: Chunking V1 -> chunk_registry -> Global Chunk Near-Dedup V1 (no network, no database).

A Common Crawl page and a GitHub README are chunked by the real Chunking V1
writer. Their chunks then go through the real side index, the exact verifier
and the fact guard, and into the in-memory 001B store:

* EXACT: one section appears verbatim in both documents. It is one
  chunk_registry row with two occurrences, and gets one decision;
* NEAR: the README renders a CC section as markdown bullets under another
  heading. That chunk is NEAR_DUPLICATE of the CC chunk (cross-source);
* RELATED: same topic, different facts. A year mismatch at J >= 0.70 is kept
  apart by the fact guard;
* UNIQUE: an unrelated section.

A rerun and an index rebuild change nothing.
"""

import shutil
import tempfile
import unittest
import uuid
from pathlib import Path

from app.processing.chunk_dedup import eligibility
from app.processing.chunk_dedup.index import CandidateIndex, IndexIdentity
from app.processing.chunk_dedup.service import ChunkNearDeduplicator, rebuild_index, replay, run_backfill
from app.processing.chunk_dedup.verify import NEAR_DUPLICATE, RELATED, UNIQUE, ChunkText
from app.processing.chunking.chunker import ChunkingConfig
from app.processing.chunking.service import ChunkWriter
from app.utils.ids import chunk_content_hash

from tests.unit.chunk_dedup_fixtures import InMemoryChunkDedupStore
from tests.unit.chunk_fixtures import InMemoryChunkStore, silver_document
from tests.unit.test_chunk_dedup import DAM, GLACIER, markdown

SHARED = ("Hydropower converts the energy of flowing water into electricity by driving turbines connected to "
          "generators. Storage schemes hold water in a reservoir and release it through penstocks when demand "
          "rises, while run-of-river schemes use the natural flow with little or no storage. Pumped storage "
          "plants move water uphill when power is cheap and generate when it is expensive.")
BREAD = ("Sourdough bread relies on a culture of wild yeast and lactic acid bacteria that bakers keep alive by "
         "feeding it with flour and water every day. Bakers adjust hydration, temperature and proofing time to "
         "control the open crumb, the thick crust and the sour flavour of the loaf, and many keep the same "
         "starter for decades.")
YEAR_CHANGED = DAM.replace("1959", "1961")


class ChunkDedupEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)

    def chunk_corpus(self):
        cc = silver_document(f"## Kariba Dam\n\n{DAM}\n\n## Hydropower\n\n{SHARED}\n\n## Glaciers\n\n{GLACIER}\n",
                             kind="html")
        gh = silver_document(f"{markdown(DAM, heading='Zambezi scheme')}\n\n## Hydropower\n\n{SHARED}\n\n"
                             f"## Revised history\n\n{YEAR_CHANGED}\n\n## Baking\n\n{BREAD}\n", kind="markdown")
        chunks = InMemoryChunkStore()
        chunks.add_documents(cc, gh)
        config = ChunkingConfig()
        chunks.register(config)
        writer = ChunkWriter(chunks, config)
        for document in (cc, gh):
            self.assertEqual(writer.chunk_accepted(None, document)["chunk_document_chunked"], 1)
        store = InMemoryChunkDedupStore()
        for (document_id, _v, ordinal), row in sorted(chunks.occurrences.items(), key=lambda kv: kv[0][2]):
            text = chunks.chunks[row["chunk_id"]]["chunk_text"]
            source = "common_crawl" if document_id == cc.document_id else "github"
            store.add_chunk(ChunkText(row["chunk_id"], bytes.fromhex(chunk_content_hash(text)), text),
                            document=source, ordinal=ordinal)
        return chunks, store

    def test_registry_to_decisions_relationships_and_representatives(self) -> None:
        chunks, store = self.chunk_corpus()
        index = CandidateIndex(self.root / "idx.sqlite3", IndexIdentity("offline", 1), create=True)
        self.addCleanup(index.close)
        dedup = ChunkNearDeduplicator(store, index, run_id=uuid.uuid4())
        report = run_backfill(store.transaction, dedup, limit=100, batch_size=2)

        def find(fragment: str, source: str) -> uuid.UUID:
            return next(k for k, c in store.registry.items() if fragment in c.chunk_text
                        and any(doc == source for doc, _o in store.occurrences[k]))

        shared = find("Pumped storage plants", "common_crawl")
        self.assertEqual(shared, find("Pumped storage plants", "github"))  # EXACT: one registry row
        self.assertEqual(sorted(d for d, _o in store.occurrences[shared]), ["common_crawl", "github"])
        cc_dam, gh_dam = find("completed in 1959", "common_crawl"), find("completed in 1959", "github")
        year = find("completed in 1961", "github")
        bread = find("Sourdough", "github")
        decisions = store.decisions
        near_pair = sorted([cc_dam, gh_dam], key=lambda k: store.registry[k].chunk_hash)
        self.assertEqual((decisions[near_pair[1]]["classification"], decisions[near_pair[1]]["representative"]),
                         (NEAR_DUPLICATE, near_pair[0]))  # NEAR, cross-source, first in hash order represents
        self.assertEqual(decisions[year]["classification"], RELATED)  # fact guard: 1959 vs 1961
        self.assertGreaterEqual(decisions[year]["similarity"], 0.70)
        self.assertEqual(decisions[bread]["classification"], UNIQUE)
        self.assertEqual(decisions[shared]["classification"], UNIQUE)
        self.assertEqual(len(decisions), len(store.registry))
        self.assertEqual(set(store.audit().values()), {0})
        edges = {tuple(sorted((a, b))): v["type"] for (a, b), v in store.relationships.items()}
        self.assertEqual(edges[tuple(sorted((cc_dam, gh_dam)))], NEAR_DUPLICATE)
        self.assertEqual(edges[tuple(sorted((near_pair[0], year)))], RELATED)
        status = eligibility.classify(near_pair[1], NEAR_DUPLICATE, near_pair[0], "confirmed")
        self.assertEqual((status.eligible, status.embed_chunk_id), (False, near_pair[0]))
        self.assertEqual(report.counts["decision_near_duplicate"], 1)
        self.assertGreaterEqual(report.counts["fact_guard_blocked_pairs"], 1)

        # rerun, index rebuild and replay: nothing changes
        before = store.state()
        self.assertEqual(run_backfill(store.transaction, dedup, limit=100).chunks, 0)
        fingerprint = index.fingerprint()
        index.close()
        built = rebuild_index(store.transaction, store, self.root / "idx.sqlite3", database="offline")
        self.assertEqual(built["fingerprint"], fingerprint)
        result = replay(store.decisions_in_order(), store.stored_relationships())
        self.assertEqual((result["decision_mismatch_count"], result["relationship_mismatch_count"]), (0, 0))
        self.assertEqual(store.state(), before)
        self.assertEqual(len(chunks.occurrences), sum(len(v) for v in store.occurrences.values()))


if __name__ == "__main__":
    unittest.main()

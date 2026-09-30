# Chunk Near-Dedup V1: Candidate-Index Benchmark and Design

Status: **the design is selected; production is not implemented.** This page records the benchmark and the chosen candidate-index design. Nothing here writes to PostgreSQL. Embeddings and Gold are not started.

- Benchmark: `scripts/benchmark_chunk_candidate_indexes.py`
- Tests: `tests/unit/test_chunk_candidate_benchmark.py`
- Raw JSON results are local scratch output; they are not committed.

## 1. What was benchmarked

Every approach ran with the same corpus, queries, caps (256 rows per bucket read, 64 candidates) and exact verifier.

| Approach | Postings/chunk | Origin |
|---|---|---|
| `reduced_minhash_lsh_8x2`: classic 16-hash MinHash | 8 | Copilot prototype |
| `minhash_oph_lsh_8x2`: one-permutation MinHash with rotation densification | 8 | added: the same LSH, fast signatures |
| `minhash_oph_lsh_10x4`: one-permutation MinHash, 10 bands × 4 rows | 10 | added: the staged-scale candidate |
| `simhash_64_multindex_8x8`: 64-bit SimHash, 8 exact 8-bit bands | 8 | Copilot prototype |
| `sparse_prefix_filter`: prefix filter at the near threshold, hash-ordered | ≈ 23 | Copilot prototype |

### Corpus

Measured runs used 6,073, 22,573 and 55,573 chunks for all five approaches, and 220,570 chunks for the two one-permutation variants. Each corpus has:

- **Pilot chunks.** Only 3 pilot chunks were loaded, read-only from `kb_pipeline_test` with `BEGIN READ ONLY` as `kb_pipeline_app`.
- **Templated families (512 chunks).** The same topic and wording with a different entity, year and quantity: the "related but different facts" stress case.
- **Diverse background.** A Zipfian content vocabulary (60k words) plus a Zipfian function-word stream, with years, quantities with units, and two-word names.
- **Fixed fact examples.**
- **Distinct normalized-duplicate items.** A different `chunk_text` (so a different `chunk_id`) with identical comparison features.

### Queries (4,207 per corpus)

Kinds with a (+300) marker were sampled at 300 queries each; the fixed fact examples add 7 more.

- **Near positives.** The target must be found and verified NEAR:
  - punctuation, whitespace and heading-context changes;
  - reordered sentences;
  - a 2-word edit;
  - a template 3-synonym rewrite;
  - **CC prose → GitHub markdown** (heading, bullets, bold), with and without a word edit;
  - **distinct normalized duplicates**.
- **Fact mismatches.** Must never be NEAR: number, date (year and month), entity (including an acronym), negation, and unit/quantity (+300 per kind, plus the fixed examples).
- **Novel.** Unindexed new text (+300). It must never be NEAR, and it measures the false-collision load.

## 2. What Copilot had done, and what was corrected

Kept as is: the three prototypes, the SQLite posting store, the caps, word-3-gram Jaccard verification, and fact-signature equality.

Corrected, because these were real defects:

1. **Overfit entity extractor.** It hard-coded the fixture's own words (`revenue`, `treaty`, `officials`) and missed acronyms and CamelCase names. It also let a sentence-final `.` join a name to the next sentence (`Gruthort. Of`), and lost units on double spaces. It is now general:
   - it ignores a sentence-initial capital or function word;
   - it strips markdown emphasis and list markers first;
   - it collapses horizontal whitespace;
   - it captures units from a lexicon, month names, and `n't` negations.

   Result: **0 signature changes across 15,000 formatting-only variants.**
2. **Saturated corpus.** With only 8 templates, every query, even an "unrelated" one, hit the 64-candidate cap, so recall and false-candidate numbers were uninformative. A diverse background was added.
3. **"Unrelated" queries were exact copies** of other indexed items. They are replaced by unindexed novel text.
4. **Exact duplicates were self-matches.** Now they are distinct indexed items, and the query item is excluded from its own candidates.
5. **No cross-source near pair.** CC→markdown pairs were added, with and without an edit.
6. **One corpus size only.** Bucket growth with N was never measured, and that growth is the 7B question. There are now 3–4 sizes, with uncapped bucket-row counts.
7. **Build timing ran under `tracemalloc`,** which inflated build time. It was removed.
8. **Postings stored 16-byte UUIDs.** Both encodings are now measured. The production form is an int64 chunk-hash prefix.

The previous verifier and false-candidate results are superseded by §3.

## 3. Measured results (largest run per approach)

| Approach (N) | Candidate recall | End-to-end recall | False NEAR | Cand. avg / p95 | False-candidate rate | Below RELATED | Build chunks/s | Lookup ms avg / p95 | Bytes/chunk int64 / uuid16 |
|---|---|---|---|---|---|---|---|---|---|
| reduced MinHash 8×2 (55,573) | 99.89% | 97.67% | 0 | 5.9 / 63 | 88.5% | 8.2% | 4,391 | 0.58 / 0.76 | 177.6 / 241.4 |
| one-permutation 8×2 (220,570) | 99.93% | 97.67% | 0 | 4.9 / 54 | 85.9% | 4.0% | 21,019 | 0.44 / 0.51 | 177.3 / 241.2 |
| **one-permutation 10×4 (220,570)** | **99.59%** | **97.67%** | **0** | **4.3 / 43** | **84.1%** | **0.5%** | **18,077** | **0.50 / 0.59** | **221.7 / 301.5** |
| SimHash 8×8 (55,573) | 89.70% | 89.26% | 0 | 64 / 64 | 99.0% | 97.3% | 3,015 | 2.08 / 2.49 | 128.4 / 192.7 |
| sparse prefix (55,573) | 100% | 97.67% | 0 | 58.7 / 64 | 98.8% | 90.0% | 15,021 | 3.30 / 5.16 | 510.8 / 694.7 |

- **False-candidate rate** is the share of candidates that are not verified NEAR. For the MinHash variants these are almost all same-topic template siblings, which are correctly classified **RELATED**; the "below RELATED" column is the share of truly irrelevant candidates.
- **Verification** takes about 0.02 ms per query on candidate features. Fetching candidate text from PostgreSQL is extra: about 4 primary-key reads per query.

### Growth: uncapped bucket rows per unrelated query ÷ N (measured)

This ratio should stay flat as N grows; a flat line is what makes linear extrapolation valid.

| Approach | 6k | 22k | 55k | 220k |
|---|---|---|---|---|
| one-permutation 10×4 | 0 | 0 | 5.4e-8 | 1.2e-7 |
| one-permutation 8×2 | 1.6e-6 | 1.6e-6 | 1.5e-6 | 1.5e-6 |
| reduced MinHash 8×2 | 3.5e-5 | 3.0e-5 | 3.2e-5 | — |
| sparse prefix | 7.9e-3 | 8.7e-3 | 8.8e-3 | — |
| SimHash 8×8 | 3.2e-2 | 3.2e-2 | 3.2e-2 | — |

- **SimHash** buckets grow as N/256 per band. At 55k, 17% of probes were already truncated and recall was falling: 92.4% → 90.9% → 89.3%.
- **Sparse prefix** buckets are keyed on common word trigrams. A real prefix filter needs a global frequency order, which cannot be maintained incrementally at 7B.
- **Classic 8×2** collides about 20× more than the one-permutation 8×2 at the same b × r. This was measured; it is likely row correlation in the linear mod-p hash family. Its builds are also 5× slower in pure Python.

### Verifier on fact mismatches (all sizes, all MinHash variants)

Each fact kind was tested 300 times, plus the fixed examples:

- number, unit, date (year and month), entity (including NASA→ESA), negation.

Results:

- The index **finds** 100% of them as candidates, so they get linked.
- The verifier classifies **100% as RELATED and 0 as NEAR**.
- Without the fact guard, about 1,203 of about 1,207 would be NEAR at J ≥ 0.70. **The guard is essential.**
- All CC↔GitHub markdown pairs (600) and distinct normalized duplicates (300) are NEAR.

**Threshold sweep** (end-to-end recall at 220k; with the guard there are 0 fact or cross-group false NEARs at every threshold):

| Threshold | 0.60 | 0.65 | 0.70 | 0.75 | 0.78 | 0.82 | 0.86 |
|---|---|---|---|---|---|---|---|
| Recall | 99.6% | 97.8% | 97.7% | 95.0% | 94.1% | 90.7% | 78.2% |

The misses at 0.70 are:

- 59 of 300 template 3-synonym rewrites: about 7% of the words changed, J 0.6–0.7;
- 4 of 300 two-word edits.

These are deliberately left RELATED: preserving factual diversity outranks recall.

## 4. Projections: EXTRAPOLATED, not measured

These are linear extrapolations from the 220,570-chunk run for the one-permutation variants and the 55,573-chunk run for the others.

- **Rebuild hours** use the measured single-process build rate. They assume a sorted bulk load, because random-order B-tree inserts at 1B+ rows are slower than in these in-cache runs.
- **Unrelated rows/query** is the growth ratio × N. It is the uncapped bucket-row count an unrelated query reads.

**Index size (int64 item keys)**

| Approach | 1M | 100M | 1B | 7B |
|---|---|---|---|---|
| **one-permutation 10×4** | 0.2 GiB | 20.6 GiB | 206 GiB | 1,445 GiB |
| one-permutation 8×2 | 0.2 GiB | 16.5 GiB | 165 GiB | 1,156 GiB |
| reduced MinHash 8×2 | 0.2 GiB | 16.5 GiB | 165 GiB | 1,158 GiB |
| SimHash 8×8 | 0.1 GiB | 12.0 GiB | 120 GiB | 837 GiB |
| sparse prefix | 0.5 GiB | 47.6 GiB | 476 GiB | 3,330 GiB |

With 16-byte UUID items, the 10×4 index is 0.3 / 28.1 / 281 / 1,966 GiB.

**Unrelated bucket rows read per query**

| Approach | 1M | 100M | 1B | 7B |
|---|---|---|---|---|
| **one-permutation 10×4** | 0.1 | 12 | 122 | 857 |
| one-permutation 8×2 | 1.5 | 153 | 1,528 | 10,695 |
| reduced MinHash 8×2 | 32 | 3,162 | 31,616 | 221k |
| SimHash 8×8 | 32k | 3.2M | 32M | 224M |
| sparse prefix | 8.8k | 879k | 8.8M | 62M |

**Other 10×4 projections**

- Postings: 10M at 1M chunks, 1B at 100M, 10B at 1B, 70B at 7B.
- Rebuild, 1 process: under 0.1 h / 1.5 h / 15 h / 108 h.
- Rebuild, 24 processes (ideal): under 0.1 h / 0.1 h / 0.6 h / 4.5 h.

**Caveat.** The background corpus is synthetic. Real web and GitHub text shares more boilerplate and licence text, which fattens some buckets. The 10×4 load figure also comes from a small count: 8 colliding rows over 300 queries. Before 100M, the load must be re-measured on real chunks.

## 5. Selected V1 design

**One-permutation MinHash LSH, 10 bands × 4 rows (40 bins), in a derived SQLite side index, with exact verification in the application.**

**Why this design.** It has the same end-to-end recall as every other credible option (97.67%), the fewest candidates, and the lowest irrelevant-candidate rate. Its measured unrelated-collision load per chunk is **12× lower than 8×2** and about 250× lower than classic 8×2. That load is what limits staged growth. The cost is 25% more bytes than 8×2.

- **SimHash 8×8 is rejected.** Its buckets grow as N/256, and recall is already falling at 55k.
- **Sparse prefix is rejected.** It has the largest index, heavy common-trigram buckets, and 3–5 ms lookups.
- **Classic 8×2 is rejected.** It collides 20× more and builds 5× slower than the one-permutation form.

**Proposed method registration** (production phase):

- `dedup_methods.algorithm`: `chunk_minhash_lsh`
- `algorithm_version`: `oph-w3-b10r4-j70-v1`
- **Frozen parameters:**
  - features: case-folded word 3-grams, BLAKE2b-64;
  - signature: one-permutation MinHash, 40 bins, splitmix64, rotation densification;
  - LSH: 10 bands × 4 rows;
  - caps: 256 bucket rows per probe, 64 candidates, ranked by shared bands then by item key;
  - NEAR: J ≥ 0.70 **and** an equal fact signature;
  - RELATED: J ≥ 0.30;
  - otherwise UNIQUE.

Document dedup thresholds are not reused.

**Fact guard.** Equality of three sorted multisets. Heading lines are ignored; markdown markup and whitespace are neutralized.

- **Numbers:** with currency, a unit from the unit lexicon, and month names.
- **Named entities:** capitalized runs, acronyms and CamelCase. A sentence-initial capital or function word does not count.
- **Negations:** `not`, `no`, `never`, `n't`, …

**Index.**

- **Layout:** a table `(band_key int64, item_key int64)` `WITHOUT ROWID`, where `item_key` is the first 8 bytes of `chunk_hash`. Verification resolves a key through the unique index on `chunk_registry.chunk_hash`, by prefix range. The chunk hash is the tie-breaker.
- **Derived and disposable:** rebuildable from `chunk_registry`. PostgreSQL, via 001B's `chunk_dedup_decisions` and `chunk_relationships`, stays the only durable truth.
- **Scope:** only representatives (non-NEAR chunks) are indexed, so suppressed duplicates add no postings.
- **Resolving candidates:** every candidate is re-checked against PostgreSQL before a decision.

**Representative.** Immutable per method version, as recorded in the 001B addendum. A new chunk is compared against the indexed representatives.

## 6. One-PC operating limits (16 GB RAM, 28 cores, about 322 GB free on E:)

| Stage | Chunks | Index (10×4) | Verdict |
|---|---|---|---|
| **A: V1 one-PC** | up to 100M | up to about 21 GiB | Supported. One SQLite file on local SSD. Rebuild takes about 1.5 h single process, or less with parallel signatures and a sorted load. Projected unrelated load is about 12 rows/query, far below the caps. |
| B: one-PC stretch | 100M–1B | 21–206 GiB | Only with the index sharded into band-partitioned files, a sorted bulk rebuild, and at least 250 GB of dedicated SSD. The index then exceeds RAM, so lookups become SSD-random. Gate: real-data load re-measured below the 256-row probe cap. |
| C: 7B production | about 7B | about 1.4 TiB, plus PostgreSQL chunk text of several TiB | **Not one-PC.** It needs a scale transition (§7). |

**Operating rules.**

- Refuse to build or extend when free disk is below index + 2× headroom, the same as the document index's `min_free_bytes`.
- Emit `truncated_probe_rate` and candidate p95 as metrics.
- **Stop and escalate** if truncation exceeds 0.1%, or if candidate p95 hits the cap on unrelated text.

## 7. Scale-transition requirements (before 1B, and before 7B)

1. **Re-measure on real chunks at ≥ 1M and ≥ 10M.** Measure bucket load and truncation on real data. If real boilerplate fattens buckets, move to 12×5 or add a per-bucket stop-list of hot keys (skip a posting whose bucket is over the cap, and log it), registered as a **new method version**.
2. **Shard the index** by `band_key` hash into P independent stores, one per worker or node. A lookup fans out to at most 10 shards. A rebuild is embarrassingly parallel: compute signatures, external-sort by key, then bulk-load each shard.
3. **Move to a distributed store at 7B:** about 1.4 TiB for the index alone. Options: a sharded key-value store, or a partitioned PostgreSQL table on dedicated nodes. The decision rows stay in PostgreSQL 001B unchanged.
4. **Implement the verifier outside pure Python,** or batch-verify in parallel. It is already cheap: about 0.02 ms per query plus about 4 text fetches.
5. **Cut to a new method version** for any parameter change. Old decisions are never rewritten (§5 representative policy).

## 8. Known gaps

- The synthetic background only approximates real text statistics. Only 3 real pilot chunks exist today.
- **Meaning changes outside numbers, units, dates, entities and negation are not guarded.** Antonyms and comparatives (`increase`/`decrease`) at J ≥ 0.70 would be NEAR. Word-form numbers (`ten` vs `10`) are also not normalized.
- **Heavier paraphrases** (more than about 7% of the words changed) stay RELATED by design.
- The 10×4 collision load at 220k rests on few events, so its 7B projection carries wide uncertainty.

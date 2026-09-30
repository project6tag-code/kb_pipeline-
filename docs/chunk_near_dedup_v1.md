# Global Chunk Near-Dedup V1

Status: **implemented and validated on `kb_pipeline_test`**. Validation covered:

- the offline tests and E2E;
- the PostgreSQL pilot and a rerun;
- concurrency, crash and guard probes;
- the side-index rebuild and a full replay.

Nothing is embedded or published. The design comes from `docs/chunk_near_dedup_candidate_index_v1.md`. Code lives in `app/processing/chunk_dedup/`.

Pipeline: `chunk_registry` → bounded candidate index → exact verification → fact guard → deterministic representative → 001B decisions and relationships → embedding eligibility.

## 0. Schema: Migration 001B (additive; applied and validated on kb_pipeline_test)

`kbp_001b_chunk_near_dedup_v1.py` follows `kbp_001a` and creates only two tables. Both have a NOT NULL `run_id` that references `kbp.pipeline_runs`.

- **`kbp.chunk_dedup_decisions`**:
  - PK `(chunk_id, dedup_method_id)`;
  - classification is `NEAR_DUPLICATE` / `RELATED` / `UNIQUE`;
  - `representative_chunk_id` is present exactly for NEAR_DUPLICATE, is never the chunk itself, and references `chunk_registry`;
  - a score is required unless the class is UNIQUE;
  - partial index on the representative.
- **`kbp.chunk_relationships`**:
  - PK `(chunk_id, related_chunk_id, dedup_method_id)` with CHECK `chunk_id < related_chunk_id`, so there is one canonical edge per pair and it cannot hold contradictory types;
  - reverse index.

`EXACT_DUPLICATE` is excluded because one `chunk_registry` row already is exact identity.

**Privileges and safety.**

- The runtime role has SELECT + INSERT only: no UPDATE, DELETE or DDL.
- `lock_timeout 5s` / `statement_timeout 60s`, and no data mutation.
- Downgrade drops only the two tables (`RESTRICT`).
- No `public.*` object is referenced.
- Online Alembic runs require `-x expect_database=<db>`.

## 1. Method

`kbp.dedup_methods`: `chunk_minhash_lsh` / `oph-w3-b10r4-j70-v1`. The full frozen parameters are in `method.PARAMETERS`, including a digest of every fact-guard lexicon (`python -m app.processing.chunk_dedup.cli method`).

| Part | Frozen value |
|---|---|
| comparison features | case-folded word 3-grams, BLAKE2b-64. Comparison only: `chunk_text`, `chunk_hash` and `chunk_id` are never changed |
| signature / LSH | one-permutation MinHash, 40 bins (splitmix64, rotation densification); 10 bands × 4 rows |
| candidate reads | at most 256 rows per bucket (a read of 257 detects truncation), at most 64 candidates, ranked by shared bands then item key |
| NEAR_DUPLICATE | exact Jaccard ≥ **0.70** **and** equal fact signatures |
| RELATED | exact Jaccard ≥ **0.30** that is not NEAR, including J ≥ 0.70 pairs with different facts |
| UNIQUE | everything else |

**Registration** is a migrator bootstrap. It is idempotent, refuses different stored parameters, and runs only on exactly `--expect-database` at `kbp_001b`:

```
python -m app.processing.chunk_dedup.cli register-method --expect-database kb_pipeline_test
```

## 2. Fact-preservation guard (`facts.py`, `fg-v1`)

A NEAR merge requires **equal** fact signatures. The signature has four components:

- **Quantities:** digits with currency, a normalized unit (`%`/`percent`, plural→singular) and ordinal suffixes. Number words are mapped to digits: `ten`→10, `twenty-five`→25, `one hundred`→100, `ten million`→`10 million`. Ordinal words are mapped too: `first`→1st, `third`→3rd; `second` is deliberately not mapped, because it is also a time unit. Month names are included.
- **Named entities:** capitalized runs, acronyms and CamelCase names. A capital at a sentence start is grammar, not a name.
- **Negations:** `not`, `no`, `never`, `n't`, …
- **Directions:** each directional or comparative word maps to an axis and a sign:
  - `change±`: increased/rose/grew/gained vs decreased/fell/declined/lost;
  - `level±`: higher/more/greater/above vs lower/less/fewer/below;
  - `time±`: after/later vs before/earlier;
  - `quality±`: better/win vs worse/fail.

  Synonyms stay compatible: `rose` ≈ `increased`, `10%` ≈ `ten percent`.

**Sentence structure.** The signature is **per sentence and ordered within the sentence**, with the sentences then sorted:

- **Role swaps inside a sentence are detected:** `A acquired B` / `B acquired A`, `from 10 to 20` / `from 20 to 10`, `lost more than it gained` / `gained more than it lost`.
- **Two different edits in two sentences cannot cancel out.**
- **Pure sentence reordering stays compatible.**

A bag-of-words guard fails all three: the first implementation did, and the tests caught it.

**What is neutralized.** Heading lines, markdown emphasis, list markers and whitespace, so CC prose and its GitHub markdown get the same signature. On the benchmark corpus, 0 of 15,000 formatting-only variants change the signature.

**Conservative by design.** Any difference, even a harmless one such as `km` vs `kilometres`, keeps the pair RELATED.

## 3. Representative policy

- **Processing order.** Writers of one method serialize on a transaction-scoped advisory lock (`method_lock_key`). Each batch processes pending chunks in `chunk_hash` order and sees every representative committed before it.
- **Only representatives are indexed.** A chunk is compared only against representatives: confirmed decisions that are not NEAR_DUPLICATE. A NEAR_DUPLICATE maps to the verified match with the highest similarity, ties by `chunk_hash`. Chain depth is therefore 1 by construction.
- **Deterministic.** The same chunk set gives the same decisions, whatever the arrival order into `chunk_registry` or the batch size (tested). The first chunk of a cluster in `chunk_hash` order represents it. The hash is content-derived and stable, not discovery order.
- **Immutable per method version.** A persisted decision is never rewritten. A later arrival that would have ranked first still maps to the existing representative (tested). Re-election requires a new method version.
- **Enforced in the database at write time.** The decision INSERT carries `WHERE representative IS NULL OR EXISTS (confirmed, non-NEAR decision of the representative under the same method)`, which prevents chains and cycles. The PK prevents a second decision.
- **Conflicts are never overwritten.** A recomputed decision that differs from a stored one becomes an `internal_bug` dead letter (item `chunk`, stage `CHUNK_DEDUPLICATED`).
- **Audited** (`cli audit`): every `*_violations` count must be 0. The checks are:
  - representative without a decision;
  - suppressed representative;
  - NEAR without its edge;
  - edge without decisions;
  - NEAR edge between two representatives;
  - decided chunk without an occurrence.

## 4. Persistence (Migration 001B; insert-only)

| Datum | Where |
|---|---|
| one decision per (chunk, method): class, representative, similarity, `confirmed`, `decided_at = clock_timestamp()`, `run_id` | `kbp.chunk_dedup_decisions` |
| NEAR / RELATED edges (ordered pair `chunk_id < related_chunk_id`), similarity, `run_id` | `kbp.chunk_relationships` |
| the run (`reprocess`, `code_version = chunk_minhash_lsh:oph-…`, parameters digest, metrics in `notes`) | `kbp.pipeline_runs` |

**Never touched:** `chunk_registry`, `chunk_occurrences`, documents, extractions and observations. Suppression only changes eligibility, and full provenance stays reachable:

chunk → occurrence → Silver document → extraction → source observation → CC WARC coordinates / GitHub repo, commit and path.

The pilot verifies this.

## 5. Candidate index (disposable)

**Location and identity.** The index lives at `data/chunk_dedup/<database>/<algorithm>-<version>.sqlite3`, which is git-ignored. Tables: `postings(band_key, item_key)`, `items`, and `meta` holding the identity (algorithm, version, parameter digest, database, `dedup_method_id`). A file that belongs to anything else is refused.

- **Keys.** `item_key` is the first 8 bytes of `chunk_hash`. Candidates are resolved in PostgreSQL by hash-prefix range to real `chunk_id`s, and only confirmed representatives are returned. Stale or colliding keys drop out there.
- **Write ordering.** Entries are written **before** the PostgreSQL commit. If the batch rolls back (exception, crash, retry), the items it added are removed. A hard kill can only leave stale entries, which are harmless and are removed on rebuild.
- **Completeness.** Each run starts with `ensure_complete`: index items must be ≥ PostgreSQL representatives, else stop with `rebuild-index`.
- **Rebuild.** `rebuild-index` rebuilds from PostgreSQL under the method lock into a temp file, then swaps it in with `os.replace`. Decisions are never touched.

## 6. Hot-bucket protection

Every lookup records probes, bucket rows read, truncated probes and candidates. The run summary and `pipeline_runs.notes` carry:

- truncation rate;
- candidates avg / max / histogram;
- bucket rows read.

`status` reports bucket size average and maximum, and buckets over the read cap.

**Degraded rule.** A run is degraded when **more than 0.1% of bucket reads are truncated** (after 1,000 probes). The run then raises `IndexDegraded` *before* committing the batch: that batch's decisions are not persisted, and the run stops. Decisions are never made silently while recall is being lost.

## 7. Operations

```
python -m app.processing.chunk_dedup.cli backfill --expect-database kb_pipeline_test [--limit N] [--batch-size N]
python -m app.processing.chunk_dedup.cli rebuild-index --expect-database kb_pipeline_test
python -m app.processing.chunk_dedup.cli status --expect-database kb_pipeline_test
python -m app.processing.chunk_dedup.cli audit --expect-database kb_pipeline_test [--replay]
```

`backfill` is also the **incremental path**. New chunks are simply the next pending set, and acquisition and chunking are never rerun. It is:

- bounded (`--limit`, `--batch-size`), with one transaction per batch;
- resumable and rerunnable, because the pending set is derived from PostgreSQL;
- retried on transient errors (up to 3 attempts);
- stopped cleanly on SIGINT/SIGTERM after the current batch;
- refused on any database other than `--expect-database`;
- given a disk-space guard of 2 GiB.

`audit --replay` re-derives every decision from scratch, in `decided_at` order, and compares it with the stored rows. It is bounded to 2M decisions and is an audit tool.

## 8. Embedding eligibility (for Embeddings V1; nothing is embedded)

`eligibility.py`: `resolve_method_id`, `eligibility(conn, method_id, chunk_ids)`, `eligible_chunks`, `suppressed_chunks`.

| Status | Rule | Embed |
|---|---|---|
| `eligible` | confirmed, not NEAR_DUPLICATE | this chunk |
| `suppressed` | confirmed NEAR_DUPLICATE | its `representative_chunk_id` (`embed_chunk_id`) |
| `undecided` / `not_confirmed` | no decision, or `proposed`/`overridden` | nothing yet |

## 9. Validation (kb_pipeline_test)

| Check | Result |
|---|---|
| unit (`test_chunk_dedup`, `test_chunk_dedup_sql`) | 30 OK. Full unit suite: 545 OK |
| offline E2E (`test_chunk_dedup_offline_e2e`) | Chunking V1 → decisions. Covers exact, cross-source near, fact-RELATED, unique, rerun, rebuild and replay |
| PostgreSQL (`test_chunk_dedup_postgres`, `chunk_dedup_pilot`) | 6 OK. Covers the method lock, DB chain refusal, crash rollback plus index compensation, provenance, eligibility and rerun |
| pilot | 31 decisions (3 NEAR, 8 RELATED, 20 UNIQUE), 27 edges, 0 audit violations, 0 undecided chunks, 0 dead letters. Two concurrent workers on the incremental set: serialized, no contradictions |
| rerun | decisions and relationships byte-identical, including `decided_at` and `run_id`; 0 new rows |
| index rebuild | same fingerprint (28 representatives, 280 postings); replay 0 decision and 0 relationship mismatches |

## 10. Known limits

- **RELATED recall below J 0.7 is best-effort.** LSH 10×4 is tuned for the NEAR threshold. The candidate probability is 1−(1−J⁴)¹⁰: 0.94 at J 0.70, 0.99 at 0.80, 0.48 at 0.50, about 0.09 at 0.31. A missed RELATED edge loses information only; it can never cause a merge. NEAR candidate recall at the threshold itself is 0.94; the benchmark measured 99.6% candidate recall on its near variants.
- **The first chunk of a family is UNIQUE.** Classification is relative to earlier representatives, so a family's first chunk in `chunk_hash` order is UNIQUE. Its later siblings carry the RELATED/NEAR classification and the edges.
- **The fact guard is lexical.** It does not detect:
  - antonyms outside its direction lexicon (for example `safe`/`unsafe` or `allowed`/`banned`);
  - changes to unnamed common nouns (`copper` → `nickel`);
  - paraphrased numbers such as `a dozen` or `half`.

  It errs toward RELATED, never toward merging, for everything it does recognize.
- **Representatives are not quality-ranked.** A representative is the first chunk in `chunk_hash` order, not the most complete or best-sourced one. A quality-aware rank would need an index on `chunk_registry` (a schema change) or a new method version.
- **Scale.** One PC supports up to about 100M chunks. 1B needs the partitioned index and bulk rebuild, and 7B needs a distributed candidate store (`chunk_near_dedup_candidate_index_v1.md` §6–7). The single-writer method lock is fine for one PC; a distributed deployment needs band-partitioned writers.
- **Live downgrade.** A live downgrade of 001B has not been run.

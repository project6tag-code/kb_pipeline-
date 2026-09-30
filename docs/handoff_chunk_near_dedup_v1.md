# Handoff: Global Near-Duplicate Chunk Dedup V1

Status: **Migration 001B is applied and validated on `kb_pipeline_test`. Chunk near-dedup implementation has not started.**

## 1. Repository state

- Branch at handoff: `chunk-near-dedup-v1`
- HEAD at handoff: `680808e Document chunk near-dedup schema blocker`
- The branch and HEAD above were verified before this work; the working tree contains the 001B migration, target guard, tests, and documentation changes. Migration 001B is now live on `kb_pipeline_test` at `kbp_001b`.

## 2. Completed and locked phases

- Database Foundation / Migration 001A (`kbp_001a`)
- Runtime Engine V1
- Common Crawl Acquisition V1
- GitHub Acquisition V1
- Bronze
- Bronze → Silver Preprocessing V1
- Exact and near-duplicate document dedup
- Silver PostgreSQL validation
- Chunking V1
- Global exact chunk dedup
- Chunking PostgreSQL validation

Do not redesign or restart these phases.

## 3. What has NOT been done

- Migration 001B is authored at `migrations/versions/kbp_001b_chunk_near_dedup_v1.py` and applied to `kb_pipeline_test`.
- **No chunk near-dedup implementation has started.** No code, tests or method registration exist for it.
- **No embeddings have started.**
- **No Gold work has started.** Nothing is published to `public.kb` or `public.metadata`.

## 4. Current blocker

Before 001B, Migration 001A could not durably represent chunk-level near-duplicate decisions: candidate chunk, representative chunk, class, similarity, method, status and run provenance. The additive schema now represents these fields, and its catalog/privilege validation passed on `kb_pipeline_test`.

| Table | Why it cannot be used |
|---|---|
| `document_dedup_decisions` | Its ids are foreign keys to `document_registry`, so chunk UUIDs would violate them and are forbidden. It holds one decision per document, but a document has many chunks. |
| `document_relationships` | Both ids are foreign keys to `document_registry`. |
| `chunk_registry` | Insert-only (§5.14). The runtime role has no UPDATE, and there are no decision or representative columns. |
| `chunk_occurrences` | There is no method in its key and no decision columns. Using `section_path` would be a forbidden JSON hack. |
| `chunk_embeddings`, `embedding_queue` | Belong to the embedding phase. The queue is transient. |
| `dead_letters`, `legacy_kb_reconciliation`, `pipeline_jobs.checkpoint` | Different purposes: failures, legacy reconciliation and job state. |
| A SQLite side file | Forbidden as the only durable record of decisions. |

Migration 001A already has:

- `dedup_methods` for registering the method;
- the stage `CHUNK_DEDUPLICATED`;
- the dead-letter item type `chunk`.

None of these needs to change.

## 5. Migration 001B Review Outcome

The initial proposal below is retained as historical context only. The reviewed implementation and authoritative schema are `migrations/versions/kbp_001b_chunk_near_dedup_v1.py` and `docs/chunk_near_dedup_v1.md`.

Amendments and rationale:

- Removed the composite self-FK from `(representative_chunk_id, dedup_method_id)` to `chunk_dedup_decisions`. A direct FK to `chunk_registry` preserves representative existence without imposing same-method decision insertion order or batch coupling.
- Made both tables' `run_id` NOT NULL, since each durable decision and relationship must identify its creating pipeline run.
- Changed relationship primary key to `(chunk_id, related_chunk_id, dedup_method_id)`. Including `relationship_type` would allow one pair to be both `NEAR_DUPLICATE` and `RELATED` for the same method.
- Added an explicit online Alembic database-name guard (`-x expect_database=...`). PostgreSQL validation must use only `kb_pipeline_test`.

The SQL sketch below is the superseded draft and must not be used for execution.

The design only adds tables and mirrors the document design (§5.24–5.25). It does not ALTER any existing table, runs no DML, and does not reference `public.*`.

```sql
CREATE TABLE kbp.chunk_dedup_decisions (
    chunk_id uuid NOT NULL REFERENCES kbp.chunk_registry (chunk_id),
    dedup_method_id smallint NOT NULL REFERENCES kbp.dedup_methods (dedup_method_id),
    classification text NOT NULL CHECK (classification IN ('NEAR_DUPLICATE', 'RELATED', 'UNIQUE')),
    representative_chunk_id uuid REFERENCES kbp.chunk_registry (chunk_id),
    similarity_score real CHECK (similarity_score IS NULL OR similarity_score BETWEEN 0 AND 1),
    decision_status text NOT NULL CHECK (decision_status IN ('proposed', 'confirmed', 'overridden')),
    decided_at timestamptz NOT NULL,
    run_id uuid REFERENCES kbp.pipeline_runs (run_id),
    PRIMARY KEY (chunk_id, dedup_method_id),
    CHECK ((classification = 'NEAR_DUPLICATE') = (representative_chunk_id IS NOT NULL)),
    CHECK (representative_chunk_id IS DISTINCT FROM chunk_id),
    FOREIGN KEY (representative_chunk_id, dedup_method_id)
        REFERENCES kbp.chunk_dedup_decisions (chunk_id, dedup_method_id)
);

CREATE INDEX chunk_dedup_decisions_representative_idx
    ON kbp.chunk_dedup_decisions (representative_chunk_id, dedup_method_id)
    WHERE representative_chunk_id IS NOT NULL;

CREATE TABLE kbp.chunk_relationships (
    chunk_id uuid NOT NULL REFERENCES kbp.chunk_registry (chunk_id),
    related_chunk_id uuid NOT NULL REFERENCES kbp.chunk_registry (chunk_id),
    relationship_type text NOT NULL CHECK (relationship_type IN ('NEAR_DUPLICATE', 'RELATED')),
    dedup_method_id smallint NOT NULL REFERENCES kbp.dedup_methods (dedup_method_id),
    similarity_score real NOT NULL CHECK (similarity_score BETWEEN 0 AND 1),
    decision_status text NOT NULL CHECK (decision_status IN ('proposed', 'confirmed', 'overridden')),
    run_id uuid REFERENCES kbp.pipeline_runs (run_id),          -- amendment: run provenance
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chunk_id, related_chunk_id, relationship_type, dedup_method_id),
    CHECK (chunk_id < related_chunk_id)
);

GRANT SELECT, INSERT ON TABLE kbp.chunk_dedup_decisions TO kb_pipeline_app;
GRANT SELECT, INSERT ON TABLE kbp.chunk_relationships TO kb_pipeline_app;
```

### What each rule guarantees

- **Primary key `(chunk_id, dedup_method_id)`.** Each chunk gets one decision per method version. Inserts use `ON CONFLICT DO NOTHING`, so reruns and concurrent writers never create duplicate or contradictory decisions.
- **Representative checks in the superseded draft.**
  - A representative is present exactly when the class is `NEAR_DUPLICATE`.
  - A chunk can never be its own representative.
  - The composite FK shown here was removed in the reviewed design to avoid insertion-order coupling.
- **Representative is not suppressed.** A representative must not itself be a `NEAR_DUPLICATE`. No CHECK can express this, so it is an application invariant plus a batch audit, as for documents.
- **Relationship pairs.** `chunk_id < related_chunk_id` gives each pair one fixed order, so (A,B) and (B,A) cannot both exist.
- **`EXACT_DUPLICATE` is excluded on purpose.** Identical text is already one `chunk_registry` row, through the chunk id (Identity V1).
- **Run provenance.** Both tables require non-null `run_id` referencing `pipeline_runs`.
- **Privileges.** The runtime role gets SELECT and INSERT only, with no UPDATE and no DELETE. The chunk method row in `dedup_methods` is registered by a migrator bootstrap with `--expect-database`.

### Future embedding eligibility (for Embeddings V1; not implemented)

- **Embed a chunk only when all of these hold** for the active chunk method:
  - a decision exists;
  - `classification <> 'NEAR_DUPLICATE'`;
  - `decision_status <> 'overridden'`.
- **Suppressed chunks** map to `representative_chunk_id`, which is the chunk to embed in their place.
- **Inline embedding enqueue.** The inline enqueue in schema §10.3 must apply this filter. There is no `active_gold` model today.

### Migration safety impact (reviewed implementation)

- **Statement kinds.** Only kinds the checker allows: `SET LOCAL lock_timeout/statement_timeout`, CREATE TABLE, CREATE INDEX and GRANT. All names are `kbp.`-qualified.
- **Downgrade.** Drops only the two new tables; their indexes and grants are table-owned dependencies.
- **Locks.** Creating the foreign keys takes brief locks on `chunk_registry`, `dedup_methods` and `pipeline_runs`. No existing rows are touched.
- **Files that change:**
  - new `migrations/versions/kbp_001b_*.py` (down_revision `kbp_001a`);
  - the `KBP_TABLES` and `RUNTIME_PRIVILEGES` manifests and their tests;
  - `docs/chunk_near_dedup_v1.md`, plus cross-references in the schema/privilege and migration documentation;
  - focused offline schema tests.

## 6. Algorithm Blocker (Not Implemented)

- **Candidate generation.** Uses chunk-specific MinHash/LSH signatures in a derived side index that can be rebuilt from `chunk_registry`; it is never the durable record. Bucket reads and candidate counts are capped, and there is no O(N²) comparison. Document parameters (`oph-mh128-b32r4-w5-v1`) and thresholds are not reused.
- **Normalization for comparison only.** `chunk_text`, `chunk_hash` and `chunk_id` are never changed.
- **Verification.** Exact similarity is checked before every decision. Chunk-specific NEAR and RELATED thresholds apply, with a numeric/entity-mismatch guard: the same topic with different facts must stay `RELATED`.
- **Representative.** Chosen by a stable rank over repository properties with a hash tie-break, never by discovery order alone.

The 7B scale requires a physical index design before implementation. The existing document LSH stores 32 postings per item; directly applying it would mean up to 224 billion chunk postings before index/WAL/rebuild overhead.

**Resolved for staged V1:** see `docs/chunk_near_dedup_candidate_index_v1.md`.

- **Five approaches benchmarked** on the same corpus and caps, from 6k to 220k chunks.
- **Selected design:** one-permutation MinHash LSH 10×4, which gives 10 postings per chunk.
- **Measured on the selected design:**
  - about 222 bytes per chunk with int64 keys;
  - end-to-end recall 97.7%;
  - 0 false NEAR decisions;
  - 0 fact-mismatch NEAR decisions with the fact guard.
- **Supported scale on one PC:** up to about 100M chunks.
- **Before 1B and 7B:** a scale transition is required (that document's §7). 7B is not a one-PC deployment.

The existing PostgreSQL 17 client and Windows password-file setup were usable. The guarded migration reached `kbp_001b`; the read-only verifier and effective runtime privilege checks passed. A live downgrade has not been run.

## 7. Representative Policy And Scale Question

Representative selection is deterministic for the set being evaluated. A later-arriving chunk that ranks better must **not** silently replace a representative that is already persisted. The table has no UPDATE, and suppressed chunks and future embeddings depend on the representative.

The selected V1 policy is **immutable per method version**. A persisted representative is final; a later chunk is recorded against it even if the later chunk would rank better. Re-election requires a new method version and new rows.

The derived signature index also needs its own physical design at the 7B-chunk scale.

## 8. Exact Next Actions

1. Done: the candidate-index benchmark and design (`docs/chunk_near_dedup_candidate_index_v1.md`). It needs review before implementation.
2. Implement Chunk Near-Dedup V1 with the selected design:
   - method registration;
   - candidates, verification and decisions;
   - representative policy (§7);
   - idempotency and concurrency;
   - tests;
   - a PostgreSQL pilot with a rerun proof.

Embeddings and Gold stay out of scope until chunk near-dedup is locked.

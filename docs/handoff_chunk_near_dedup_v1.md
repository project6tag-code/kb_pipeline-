# Handoff: Global Near-Duplicate Chunk Dedup V1

Status: **blocked at the schema check. Nothing has been implemented.**

## 1. Repository state

- Branch: `silver-preprocessing-wip`
- HEAD: `3bc2540 Complete and validate Chunking V1`
- The working tree was clean when this phase stopped. This handoff file is the only addition.

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

- **No Migration 001B has been created.**
- **No chunk near-dedup implementation has started.** No code, tests or method registration exist for it.
- **No embeddings have started.**
- **No Gold work has started.** Nothing is published to `public.kb` or `public.metadata`.

## 4. Current blocker

Migration 001A cannot durably represent chunk-level near-duplicate decisions: candidate chunk, representative chunk, class, similarity, method, status and run provenance. No existing table can hold them safely:

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

## 5. Proposed Migration 001B (not created; needs review and approval)

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
- **Representative checks.**
  - A representative is present exactly when the class is `NEAR_DUPLICATE`.
  - A chunk can never be its own representative.
  - The composite foreign key means the representative must already have a decision under the same method.
- **Representative is not suppressed.** A representative must not itself be a `NEAR_DUPLICATE`. No CHECK can express this, so it is an application invariant plus a batch audit, as for documents.
- **Relationship pairs.** `chunk_id < related_chunk_id` gives each pair one fixed order, so (A,B) and (B,A) cannot both exist.
- **`EXACT_DUPLICATE` is excluded on purpose.** Identical text is already one `chunk_registry` row, through the chunk id (Identity V1).
- **Run provenance.** Both tables carry a nullable `run_id` that references `pipeline_runs`.
- **Privileges.** The runtime role gets INSERT only, with no UPDATE and no DELETE. The chunk method row in `dedup_methods` is registered by a migrator bootstrap with `--expect-database`.

### Future embedding eligibility (for Embeddings V1; not implemented)

- **Embed a chunk only when all of these hold** for the active chunk method:
  - a decision exists;
  - `classification <> 'NEAR_DUPLICATE'`;
  - `decision_status <> 'overridden'`.
- **Suppressed chunks** map to `representative_chunk_id`, which is the chunk to embed in their place.
- **Inline embedding enqueue.** The inline enqueue in schema §10.3 must apply this filter. There is no `active_gold` model today.

### Migration safety impact

- **Statement kinds.** Only kinds the checker allows: `SET LOCAL lock_timeout/statement_timeout`, CREATE TABLE, CREATE INDEX and GRANT. All names are `kbp.`-qualified.
- **Downgrade.** Drops only the new index and the two new tables.
- **Locks.** Creating the foreign keys takes brief locks on `chunk_registry`, `dedup_methods` and `pipeline_runs`. No existing rows are touched.
- **Files that change:**
  - new `migrations/versions/kbp_001b_*.py` (down_revision `kbp_001a`);
  - the `KBP_TABLES` and `RUNTIME_PRIVILEGES` manifests and their tests;
  - `docs/database_schema_v1.md` and `docs/database_privileges_v1.md`;
  - a verification SQL script.

## 6. Planned algorithm (to be benchmarked; not implemented)

- **Candidate generation.** Uses chunk-specific MinHash/LSH signatures in a derived side index that can be rebuilt from `chunk_registry`; it is never the durable record. Bucket reads and candidate counts are capped, and there is no O(N²) comparison. Document parameters (`oph-mh128-b32r4-w5-v1`) and thresholds are not reused.
- **Normalization for comparison only.** `chunk_text`, `chunk_hash` and `chunk_id` are never changed.
- **Verification.** Exact similarity is checked before every decision. Chunk-specific NEAR and RELATED thresholds apply, with a numeric/entity-mismatch guard: the same topic with different facts must stay `RELATED`.
- **Representative.** Chosen by a stable rank over repository properties with a hash tie-break, never by discovery order alone.

## 7. Known limitation: incremental representative selection

Representative selection is deterministic for the set being evaluated. A later-arriving chunk that ranks better must **not** silently replace a representative that is already persisted. The table has no UPDATE, and suppressed chunks and future embeddings depend on the representative.

This needs an explicit policy before implementation. Options:

- **Immutable per method version.** A persisted representative is final. A better later chunk is recorded as `NEAR_DUPLICATE` of it.
- **Re-election in a new version.** A new method version re-evaluates the cluster and adds new rows, never overwriting old ones.

The policy must be written down and tested.

The derived signature index also needs its own physical design at the 7B-chunk scale.

## 8. Exact next action

1. Review and approve (or amend) the Migration 001B design in §5.
2. Create `kbp_001b` and run `python -m app.db.migration_safety`.
3. Apply it only to `kb_pipeline_test`, using the migrator with a `--expect-database kb_pipeline_test` guard. Never touch `data_collection_pipeline`.
4. Implement Chunk Near-Dedup V1:
   - method registration;
   - candidates, verification and decisions;
   - representative policy (§7);
   - idempotency and concurrency;
   - tests;
   - a PostgreSQL pilot with a rerun proof.

Embeddings and Gold stay out of scope until chunk near-dedup is locked.

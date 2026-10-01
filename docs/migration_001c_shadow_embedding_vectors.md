# Migration 001C — Shadow Embedding-Vector Storage

Status: **applied and validated on `kb_pipeline_test`** (revision `kbp_001c`). Additive only; 001A and 001B are unchanged. Nothing is embedded yet; Gold is not started.

## 1. Why

Schema §5.17 makes `kbp.chunk_embeddings` a result-state registry ("the vector itself is not stored here"). The only vector column was `public.kb.embedding` (Gold, §13.1). §12.4 step 3 / O-18 named the missing piece: "a per-model shadow vector table with PK `chunk_id`". 001C adds it, generically: one table keyed by `(chunk_id, embedding_model_id)`, so a new model never needs DDL.

## 2. Schema

```sql
CREATE TABLE kbp.chunk_embedding_vectors (
    chunk_id uuid NOT NULL,
    embedding_model_id smallint NOT NULL,
    embedding public.vector(768) NOT NULL,
    vector_sha256 bytea NOT NULL,
    CONSTRAINT chunk_embedding_vectors_pkey PRIMARY KEY (chunk_id, embedding_model_id),
    CONSTRAINT chunk_embedding_vectors_result_fkey
        FOREIGN KEY (chunk_id, embedding_model_id)
        REFERENCES kbp.chunk_embeddings (chunk_id, embedding_model_id),
    CONSTRAINT chunk_embedding_vectors_sha256_check
        CHECK (octet_length(vector_sha256) = 32)
);
GRANT SELECT ON TABLE kbp.chunk_embedding_vectors TO kb_pipeline_app;
GRANT INSERT ON TABLE kbp.chunk_embedding_vectors TO kb_pipeline_app;
```

- The composite FK targets the existing PK of `kbp.chunk_embeddings` (001A), so it is valid. It is NO ACTION and not deferrable, like every 001A FK.
- The table is owned by `kb_pipeline_migrator`.
- There is no ANN or vector index (§12.4 step 4 is separate), no trigger, and no other index besides the PK.
- Downgrade is `DROP TABLE kbp.chunk_embedding_vectors RESTRICT`. It has a lock and statement timeout, and does nothing else.

| Guarantee | Enforced by (verified live) |
|---|---|
| no vector without its `chunk_embeddings` row | composite FK |
| one vector per chunk/model | PK |
| exactly 768 dimensions | `vector(768)` typmod (767 and 769 refused) |
| no NaN or Inf | pgvector input validation |
| SHA-256 is exactly 32 bytes | CHECK |
| no overwrite | runtime role has SELECT + INSERT only (UPDATE, DELETE and TRUNCATE are denied); inserts use `ON CONFLICT DO NOTHING` |
| nondeterminism detectable | a recompute that differs inserts 0 rows; the stored `vector_sha256` differs from the recomputed one, so this must be reported, never replaced |

**`vector_sha256` definition (frozen):** SHA-256 of the 768 components as IEEE-754 float32, **big-endian**, in order. This is exactly pgvector's binary send payload after its 4-byte header, so an audit can recompute it in SQL with `sha256(substring(vector_send(embedding) FROM 5))`. This was verified live.

## 3. pgvector type resolution

**Decision:** the schema-qualified type `public.vector(768)`, guarded by a catalog preflight.

- **Preflight.** Before any DDL, the migration checks two things and raises otherwise:
  - extension `vector` is installed in schema `public` (`pg_extension.extnamespace`);
  - type `public.vector` is an extension member (`pg_depend`, `deptype = 'e'`).

  On `kb_pipeline_test` this holds: pgvector 0.8.1 in `public`.
- **Why not an unqualified `vector(768)` plus a pinned `search_path`.** An unqualified name resolves to the first `vector` type on the migrator's `search_path`, which depends on the session and environment. A qualified name cannot be shadowed by a type in another schema, and the preflight proves it is pgvector's.
- **`search_path`.** Neither the migration nor the runtime relies on it for the type.

## 4. Migration-safety checker change (deny-by-default kept)

`app/db/migration_safety.py` adds `ALLOWED_PUBLIC_TYPES = {"public.vector"}`. A statement mentioning `public.vector` passes the foreign-schema rule only when:

- it is `CREATE TABLE kbp.<name>`; **and**
- **every** `public.vector` occurrence has the column-type form `<column> public.vector(<integer>)`, followed by `NOT NULL`, `NULL`, `,` or `)`.

**Still rejected** (tested):

- `public.vector` without a dimension;
- a cast or function call;
- a `DEFAULT`;
- `ALTER TABLE`;
- a non-`kbp` table;
- `public.halfvec` or any other public type;
- public functions, tables or sequences;
- DML.

Protected legacy objects (`public.kb`, `public.metadata`, …) and the bridge-UUID rules are unchanged.

## 5. Revision pins

The runtime head moved to `kbp_001c`. `app.db.connection.EXPECTED_KBP_REVISION` is the single source of truth; the bootstrap guard (`silver.bootstrap.EXPECTED_REVISION`) and the Silver pilot now import it.

- `verify_migration_001b.sql` accepts `kbp_001b` or `kbp_001c`.
- `verify_migration_001a.sql` remains the exact-001A-baseline verifier. After 001B and 001C, its M03–M08, M10 and M16 report only the additive 001B/001C objects; this was checked item by item.

## 6. Future Embeddings V1 write transaction (contract; not implemented)

Per chunk, per batch, fenced by the queue lease. This mirrors §13.1 without Gold.

```
-- outside: claim embedding_queue rows (SKIP LOCKED, lease_token); read chunk_registry.chunk_text;
--          compute, validate (768 dims, finite), normalize; vector_sha256 = sha256(big-endian float32)
BEGIN
 1. INSERT kbp.chunk_embeddings (chunk_id, model, 'stored_shadow', attempt_count, embedded_at, NULL, run_id)
        ON CONFLICT (chunk_id, embedding_model_id) DO NOTHING
 2. INSERT kbp.chunk_embedding_vectors (chunk_id, model, embedding, vector_sha256)
        ON CONFLICT (chunk_id, embedding_model_id) DO NOTHING
 3. verify: both rows exist; stored vector_sha256 = computed (else: rerun nondeterminism ->
        dead_letters item 'chunk_embedding', internal_bug; never overwrite)
 4. DELETE kbp.embedding_queue WHERE chunk_id = ? AND embedding_model_id = ? AND lease_token = :token
        -- 0 rows => lease lost => ROLLBACK
COMMIT
```

- **Failure.** Any failure rolls back all four steps together. A `chunk_embeddings` row without its vector is impossible, because both are written in one transaction, and an audit query can confirm it.
- **Re-embedding.** A crashed worker's lease expires and the work is re-claimed. A rerun of a committed chunk is a no-op.
- **Gold later.** Gold publication will copy the vector from this table into `public.kb.embedding` (§13.1 step 3) instead of recomputing it.

## 7. Raw vector storage (planning figures, not measurements)

768 × float32 = **3,072 bytes per vector**, before the row header (~24 B), `chunk_id` + model (18 B), `vector_sha256` (32 B + 1 B header), the TOAST and page overhead (pgvector values are stored out of line past ~2 KB), and the PK index (~40–50 B per row).

| Embedded (eligible) chunks | Raw vectors (3,072 B) | Rough on-disk (~3.3 KB/row incl. PK, excl. bloat) |
|---|---|---|
| 1M | 2.9 GiB | ~3.1 GiB |
| 100M | 286 GiB | ~310 GiB |
| 1B | 2.8 TiB | ~3.0 TiB |
| 7B | 19.6 TiB | ~21 TiB |

- **One-PC deployment is staged.** This machine has about 200 GB free on C: and 300 GB on E:, so shadow vectors fit comfortably up to about 50M chunks. 100M is at the edge. Gold `public.kb.embedding` would hold a second copy unless shadow vectors are retired after publication.
- **Billion scale needs a different physical plan**: partitioned or sharded storage, or a dedicated vector store fed from this contract. **This migration does not claim one PC can store 7B float32 vectors.**
- **No `halfvec` in V1.** Gold is `vector(768)`, and the `active_gold` CHECK requires `vector`.

## 8. Validation (kb_pipeline_test)

| Check | Result |
|---|---|
| identity before write | `kb_pipeline_test` / `kb_pipeline_migrator` / `kbp_001b`; pgvector 0.8.1 in `public` |
| guards | no `-x expect_database` → refused; wrong name → refused; the revision stayed at `kbp_001b` |
| upgrade | `alembic -x expect_database=kb_pipeline_test upgrade head` → `kbp_001c` |
| `verify_migration_001c.sql` | PASSED: four columns, `vector(768)` (typmod 768), PK, FK to `chunk_embeddings` (NO ACTION, validated), SHA check, only the PK index, no triggers, owner, SELECT/INSERT only, no PUBLIC or grantable ACLs |
| `verify_migration_001b.sql` | PASSED at `kbp_001c` |
| unchanged | the `public` catalog fingerprint (11 relations, 118 functions, extensions), all 28 existing `kbp` table row counts, and the `chunk_registry` and chunk-dedup decision hashes |
| live integrity probe (rolled back) | every guarantee in §2 enforced; nothing persisted. The probe's rolled-back `embedding_models` INSERT consumed one identity value (harmless gap) |
| downgrade | rendered offline (`DROP TABLE … RESTRICT` plus the version update) and statically checked. **Not executed live**: there is no disposable database, and the only validated test state was not downgraded |

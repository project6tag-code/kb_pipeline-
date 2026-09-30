# Chunk Near-Duplicate Dedup V1 Schema Addendum

Status: Migration 001B is applied and validated on `kb_pipeline_test`. Offline safety/spec tests and live target guard, catalog verification, runtime privilege checks, and Alembic head verification passed. Global Chunk Near-Dedup V1 is not implemented or locked; the 7B-scale candidate-index design remains unresolved. This addendum extends the locked 001A baseline without changing any 001A or `public.*` object.

## Migration

`kbp_001b_chunk_near_dedup_v1.py` follows `kbp_001a` and creates only:

- `kbp.chunk_dedup_decisions`
- `kbp.chunk_relationships`

Both tables carry a non-null `run_id` foreign key to `kbp.pipeline_runs`. Classifications are `NEAR_DUPLICATE`, `RELATED`, and `UNIQUE`; `EXACT_DUPLICATE` is excluded because exact chunk identity is already represented by one `chunk_registry` row.

`chunk_dedup_decisions` has primary key `(chunk_id, dedup_method_id)`. Each row references its chunk, method, run, and (when near-duplicate) its representative chunk. A representative is required exactly for `NEAR_DUPLICATE` and cannot be the row's own chunk. The representative FK intentionally targets only `chunk_registry`: requiring the representative's same-method decision would create insertion-order and batch dependencies without improving chunk identity integrity. Whether a representative itself is suppressed remains a deterministic application invariant audited by the job.

`chunk_relationships` has primary key `(chunk_id, related_chunk_id, dedup_method_id)` and check `chunk_id < related_chunk_id`. The relationship type is not part of the key, so a pair cannot concurrently or successively have contradictory `NEAR_DUPLICATE` and `RELATED` rows for one method. The method writes one canonical ordered edge per pair.

## Privileges And Safety

The runtime role receives `SELECT` and `INSERT` only on both tables. No `UPDATE`, `DELETE`, `TRUNCATE`, sequence, or DDL privilege is added. The migrator owns the new objects by the existing schema ownership rule.

The migration uses `lock_timeout = 5s` and `statement_timeout = 60s`. Upgrade performs no data mutation. Downgrade drops only the two 001B tables with `RESTRICT`; their indexes and grants are table-owned dependencies. No `public.*` object is referenced or modified.

Online Alembic invocations require an explicit database identity guard:

```powershell
alembic -x expect_database=kb_pipeline_test upgrade head
```

The environment checks `current_database()` before running migrations and refuses a mismatch. Offline SQL rendering does not connect and does not need the guard. Upgrade, catalog, and effective runtime privilege validation passed on `kb_pipeline_test`. A live downgrade was not run.

## Representative Policy

Decisions are insert-only per `(chunk_id, dedup_method_id)`. Once confirmed, an existing representative mapping is immutable; later arrivals are compared to persisted representatives and cannot silently rewrite earlier mappings. Re-election requires a new registered method version. Representative selection within a candidate set must use stable chunk attributes and deterministic tie-breaking, not iteration order. A database constraint cannot guarantee that a referenced representative has a non-near decision under that method, so processing and audit checks must enforce that invariant.

## Unresolved Stop Conditions

The inherited document LSH configuration stores 32 postings per item. Directly applying that layout to 7 billion chunks implies 224 billion bucket postings before SQLite indexes, WAL, replication, or rebuild workspace. That is not an acceptable production-scale design assumption. No alternative has been selected or benchmarked; doing so requires explicit storage/latency constraints and a representative corpus/load test. Per the phase stop rule, no chunk near-dedup implementation is started until this is resolved.

Migration 001B was applied using the password-free migrator URL and existing Windows `pgpass.conf`. The read-only verifier passed as `kb_pipeline_migrator`; direct effective privilege checks passed as `kb_pipeline_app`. A live downgrade, decision-writer concurrency/rollback checks, chunk pilot, and idempotent rerun remain unverified because the chunk near-dedup writer is not implemented. Do not infer those results from the migration validation.
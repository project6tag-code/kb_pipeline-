# Gold Publication V1 Schema Support

Status: Migration 001D is applied and `public.metadata` has been patched and verified on `kb_pipeline_test`. Gold publication itself is not implemented.

## Audited Metadata Types

The committed `docs/audits/database_audit_v1.csv` confirms the historical types:

| Column | Type | Audited nullability |
|---|---|---|
| `source_url` | `text` | nullable |
| `source_title` | `text` | nullable |
| `source_type` | `varchar(50)` | nullable |
| `published_at` | `timestamptz` | nullable |
| `updated_at` | `timestamptz` | nullable |

The live `kb_pipeline_test` pre-patch schema was read-only inspected: `id uuid NOT NULL`, `source_id uuid NOT NULL`, `domain varchar(100) NULL`; no defaults; PK `metadata_pkey`; FK `metadata_source_id_fkey`; owner `postgres`; ACL `{postgres=arwdDxtm/postgres,kb_pipeline_app=a/postgres}`; zero rows.

## Admin-Only Patch

The only permitted public DDL is five nullable columns on `public.metadata`, with no defaults:

```text
source_url text
source_title text
source_type varchar(50)
published_at timestamptz
updated_at timestamptz
```

The patch is `python -m app.processing.gold_bridge.cli metadata-patch --expect-database kb_pipeline_test`. It uses `KB_ADMIN_DATABASE_URL`, refuses all other databases and pipeline roles, and requires a superuser or `public.metadata` owner. It checks the exact base schema before altering, adds only the five columns, then checks the final schema and before/after public inventory, owner/ACL, constraints, triggers, row counts, and the existing `id/source_id/domain` data digest. A second run returns `reused` and performs no DDL. Wrong-type, partial, or unexpected schemas refuse before ALTER.

Operator PowerShell command, using the operator's existing `pgpass.conf` credentials; do not put a password in the URL or repository:

```powershell
$env:KB_ADMIN_DATABASE_URL = 'postgresql+psycopg://postgres@localhost:5432/kb_pipeline_test'
python -m app.processing.gold_bridge.cli metadata-patch --expect-database kb_pipeline_test
python -m app.processing.gold_bridge.cli metadata-patch --expect-database kb_pipeline_test
```

If the normal admin login is unavailable, stop this portion and have the operator run it with their configured credentials. Do not change authentication or grant pipeline roles ALTER rights.

**Live patch result (2026-10-05):** The operator applied the five columns in a guarded transaction as `postgres`, reran the patch, and reports both runs succeeded. `verify_gold_metadata.sql` passed. A subsequent independent read-only check confirmed all eight columns in physical append order, the expected types/nullability, no defaults, zero `public.metadata` rows, and revision `kbp_001d`.

## Local Environment Note

Windows Application Control blocks SQLAlchemy's `_collections_cy` native extension in the local `.venv`. The operator applied the metadata patch with the PostgreSQL client in a guarded transaction instead. Do not weaken or bypass Windows Application Control; database administration should continue through the approved `psql`/operator path.

PostgreSQL appends columns physically. Thus the table's physical order after patch is the original three columns followed by these five; logical Gold metadata mapping remains `id`, `source_id`, `source_url`, `source_title`, `source_type`, `domain`, `published_at`, `updated_at`.

## Migration 001D

`kbp_001d` follows `kbp_001c` and creates only `kbp.gold_publications`:

| Column | Type | Nullability |
|---|---|---|
| `chunk_id` | `uuid` | NOT NULL |
| `embedding_model_id` | `smallint` | NOT NULL |
| `published_at` | `timestamptz` | NOT NULL |
| `run_id` | `uuid` | NOT NULL |

Primary key: `(chunk_id, embedding_model_id)`. Composite FK to the exact pair in `kbp.chunk_embeddings`; `run_id` FK to `kbp.pipeline_runs`. No vector, text, or mutable status is stored. The runtime receives `SELECT` and `INSERT` only. Downgrade drops only this table with `RESTRICT`.

## State Model

- `chunk_embeddings`: immutable embedding generation result state (`stored_shadow`).
- `chunk_embedding_vectors`: immutable shadow vector and hash.
- `gold_publications`: committed-publication ledger for a specific chunk/model pair.
- `public.metadata` and `public.kb`: actual Gold content.

The schema support does not implement a publisher, activate model 2, or write to either public Gold table. A Gold pair is not called successfully published unless metadata and KB rows exist, the public vector matches the shadow vector/hash, and the exact ledger pair exists in the same committed publication transaction.
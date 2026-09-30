# DATABASE SCHEMA SPECIFICATION V1 — LOCKED

| | |
|---|---|
| Status | **DATABASE SCHEMA V1 BASELINE — LOCKED.** Migration 001A (`migrations/versions/kbp_001a_schema_v1.py`) is the immutable baseline. The additive Migration 001B chunk-dedup schema is specified separately in `docs/chunk_near_dedup_v1.md`; it does not revise 001A or modify `public.*`. The Gold bridge bootstrap (§14.3) remains deferred because the semantics of `public.datasets.status` are unresolved (O-1 residual). |
| Target database | `data_collection_pipeline` (reused; no new database) |
| Authority for existing-database facts | `docs/audits/database_audit_v1.csv` (general audit) and `docs/audits/database_compatibility_audit_v1.csv` (targeted compatibility audit). Both are read-only. |
| Depends on | Identity Specification V1 (**LOCKED**): `app/utils/hashing.py`, `app/utils/ids.py`, `app/processing/normalization/text.py` |
| New objects | PostgreSQL schema `kbp`, plus two bootstrap **data rows** in `public.datasets` (section 14.3) |
| Existing table definitions modified by Schema V1 | **None** |

Classification labels used for decisions (section 27):

- **LOCKED**: a fixed architectural rule.
- **RESOLVED BY AUDIT**: answered by one of the two audits.
- **RESOLVED BY DESIGN**: decided in this document.
- **OPEN — BENCHMARK**: decided by measurement; the schema already stores the
  version or configuration.
- **OPEN — FUTURE IMPLEMENTATION**: decided when the relevant component is
  built; the schema already represents the needed state.
- **OPEN — REQUIRES USER DECISION**: a policy choice for the project owner.

---

## 1. Verified Existing Database State

Every fact in this section comes from the two audit CSVs:

- `database_audit_v1.csv`, cited as "A1".
- `database_compatibility_audit_v1.csv`, cited as "A2".

Notes on reading them:

- Row counts in A1 section `06` are planner estimates. For example,
  `alembic_version` shows 0 estimated rows although A1 section `10` shows a
  version row, and `dataset_sources` is estimated at 5 rows although A2 lists
  8.
- Exact counts exist only for `kb` and `metadata` (A1 section `11`).

### 1.1 Server

| Item | Value |
|---|---|
| Database | `data_collection_pipeline` |
| PostgreSQL | 17.10 |
| Server encoding | **UTF8** |
| Extensions | `plpgsql` 1.0, `vector` 0.8.1. No others (no `pgcrypto`). |
| Schemas audited | `public`. No `kbp` objects appear in the audit. |

### 1.2 Gold tables

**`public.kb`** has 383 rows (exact), about 1,880 kB.

| Column | Type | Nullable |
|---|---|---|
| id | uuid | NO |
| chunk | text | NO |
| embedding | **vector(768)** | YES |

- Constraints: `kb_pkey` PRIMARY KEY (`id`) and `kb_id_fkey` FOREIGN KEY
  `kb.id → metadata.id`.
- `kb_id_fkey` actions (A2): **ON UPDATE NO ACTION, ON DELETE CASCADE, NOT
  DEFERRABLE, NOT INITIALLY DEFERRED.**
- Indexes (A1 and A2): `kb_pkey` (B-tree on `id`) only. **There is no ANN
  index.**

**`public.metadata`** has 383 rows (exact), about 176 kB.

| Column | Type | Nullable |
|---|---|---|
| id | uuid | NO |
| source_id | uuid | NO |
| source_url | text | YES |
| source_title | text | YES |
| source_type | varchar(50) | YES |
| domain | varchar(100) | YES |
| published_at | timestamptz | YES |
| updated_at | timestamptz | YES |

- Constraints: `metadata_pkey` PRIMARY KEY (`id`) and `metadata_source_id_fkey`
  FOREIGN KEY `source_id → datasets.id`.
- `metadata_source_id_fkey` actions (A2): **ON UPDATE NO ACTION, ON DELETE NO
  ACTION, NOT DEFERRABLE.**
- Indexes (A1 and A2): `metadata_pkey` (B-tree on `id`) and
  `ix_metadata_source_id` (B-tree on `source_id`).

Consequences:

- `kb_id_fkey` is non-deferrable, so a `metadata` row must exist **before** its
  `kb` row is inserted, even inside one transaction.
- `ON DELETE CASCADE` means deleting a `metadata` row silently deletes its
  `kb` row.
- `metadata_source_id_fkey` means a `datasets` row cannot be deleted while any
  `metadata` row references it.

### 1.3 Legacy Gold content profile

| Check | Result |
|---|---|
| `kb_rows` / `metadata_rows` | 383 / 383 |
| `kb_without_metadata`, `metadata_without_kb` | 0, 0 |
| `null_chunks`, `null_embeddings`, `metadata_null_source_id` | 0, 0, 0 |
| `duplicate_chunk_text_groups` (exact raw text) | 0 |
| `metadata.source_type` values | `document` (216), `dataset_repository` (167) |
| `metadata.domain` values | `docs.python.org` (216), `github.com` (167). **These are web hostnames.** |
| Distinct `source_id` / `source_url` | 2 / 2 |

### 1.4 `public.datasets` and its parent

**`public.datasets`** has about 378 rows (estimate), 37 MB, and 34 columns.

- NOT NULL columns: `id uuid` (no default), `source_id uuid`,
  `dataset_name varchar(255)`, `status varchar(50)`, `metadata jsonb`,
  `created_at timestamptz DEFAULT now()`.
- All other columns are nullable.
- `datasets_source_id_fkey`: `source_id → dataset_sources.id`, with
  **ON UPDATE NO ACTION, ON DELETE NO ACTION, NOT DEFERRABLE** (A2).
- Constraints (A2 section 04): `datasets_pkey`, `datasets_source_id_fkey`.
- Unique indexes (A1): `uq_datasets_source_name (source_id, dataset_name)` and
  the partial `uq_datasets_source_url (source_id, dataset_url) WHERE
  dataset_url IS NOT NULL`.
  - These are unique **indexes**, not constraints (they do not appear in A2
    section 04).
  - `ON CONFLICT (source_id, dataset_name)` can still infer them;
    `ON CONFLICT ON CONSTRAINT` cannot.
- Status values seen in A2's 10-row sample: `ACCEPTED`, `DISCOVERED`,
  `REJECTED`. There is no CHECK constraint on `status`.

**`public.dataset_sources`** has **8 rows** (A2 section 05).

- Columns: `id uuid`, `name varchar(100)`, `source_type varchar(50)`,
  `api_endpoint`, `authentication_required boolean`, `rate_limit`,
  `status varchar(20)`, `created_at`.
- NOT NULL: `id`, `name`, `source_type`, `authentication_required`, `status`,
  `created_at`.
- Constraints (A2): `dataset_sources_pkey PRIMARY KEY (id)` and
  `uq_dataset_sources_name UNIQUE (name)`.

Existing rows (A2). All have `status = ACTIVE`.

| name | source_type | api_endpoint | authentication_required |
|---|---|---|---|
| **`common_crawl`** | `document` | NULL | false |
| **`github`** | `dataset_repository` | `https://api.github.com` | true |
| `pdf`, `web`, `api`, `rss` | `document` | NULL | false |
| `kaggle`, `huggingface` | `dataset_repository` | vendor URL | true |

The pipeline's two channels already exist as `dataset_sources` rows, so **no
`dataset_sources` row is created** (section 14.3). The UUIDs shown in A2 belong
to this database instance and are **not** treated as portable.

### 1.5 Other relevant tables (facts that decide reuse)

| Table | Decisive audited facts |
|---|---|
| `documents` | `url text NOT NULL` with **UNIQUE(url)**. URL-keyed identity. ~0 rows. |
| `processed_documents` | `document_id uuid NOT NULL` FK → `documents.id`, UNIQUE(`document_id`). ~0 rows. |
| `processing_jobs` | `document_id uuid NOT NULL` FK → `documents.id`. `retry_count`, `status`, `processing_config jsonb NOT NULL`. No lease or next-attempt columns. ~0 rows. |
| `storage_objects` | `document_id uuid NOT NULL` FK → `documents.id`. `object_path varchar(500)`, `storage_provider`, `storage_metadata jsonb NOT NULL`. ~0 rows. |
| `workflow_runs` | `request_id uuid NOT NULL` FK → `collection_requests.id`. `run_number`, `progress_percent NOT NULL`, `workflow_config jsonb NOT NULL`. ~0 rows. |
| `orchestration_jobs` | A legacy queue: `source`, `status` default `'QUEUED'`, `priority`, `retry_count`, `max_retries`, `worker_id`, `checkpoint jsonb`. Claim index `(status, priority, created_at)`. No lease expiry, next-attempt time or unique work key. ~2 rows. |
| `source_health` | PK `source varchar(50)`, `status` default `'HEALTHY'`, `consecutive_failures`, `cooldown_until`, `cooldown_reason`, `last_error_*`. No quota, remaining or reset fields. ~8 rows (legacy sources). |
| `alembic_version` | Single column `version_num`. Current value **`i4j5k6l7m8n9`**. |

### 1.6 Compatibility audit findings (A2)

| Item | Finding |
|---|---|
| User triggers on `kb`, `metadata`, `datasets`, `dataset_sources` | **None returned** |
| View or function dependencies on those tables | **None returned** |
| Table owners | `postgres` for all four tables |
| Table privileges | only `postgres` is listed: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES`, `TRIGGER` on all four |
| Column privileges | `postgres` only |
| Gold indexes | `kb_pkey`; `metadata_pkey`, `ix_metadata_source_id` |

### 1.7 Still not present in either audit

These facts are not in either audit, so the design does not depend on them:

- Which **applications** read or write `kb`, `metadata` or `datasets`, and
  how legacy code interprets `datasets.status`.
- The embedding model that produced the legacy `vector(768)` values.
- The intended semantics of `metadata.updated_at`.
- Whether the server supports `lz4` TOAST compression.
- `max_connections` and `search_path` settings.

---

## 2. Locked Architecture Decisions

| # | Decision |
|---|---|
| L1 | Reuse database `data_collection_pipeline`. No new database. |
| L2 | All new pipeline infrastructure lives in schema **`kbp`**. `public` stays compatible. |
| L3 | **Gold contract unchanged:** `public.kb(id, chunk, embedding)` and `public.metadata` (all 8 columns). **No column is added to `kb` in Schema V1** (no `embedding_model_id`). `metadata.source_id → datasets.id` is preserved. |
| L4 | **Identity V1 unchanged**: normalization, SHA-256, `KB_NAMESPACE`, the `document:v1:`/`chunk:v1:` prefixes and UUIDv5. Empty canonical documents and chunks are rejected. |
| L5 | For new-pipeline Gold rows, `kb.id = metadata.id = chunk_id`. |
| L6 | **Authoritative canonical content is durable in `kbp`**: document text in `kbp.document_registry`, chunk text in `kbp.chunk_registry`. `public.kb.chunk` is the Gold copy. Queues never hold authoritative content. |
| L7 | Source observation identity ≠ document identity ≠ chunk identity ≠ embedding execution. |
| L8 | Only two acquisition channels: **Common Crawl** and **public GitHub**. Wikipedia enters only as Common Crawl captures. |
| L9 | `metadata.domain` means **subject domain** for new-pipeline rows. Web hosts belong to provenance (`kbp.source_observations.url_host`). |
| L10 | Canonical documents and chunks containing **U+0000** are rejected or quarantined **before** PostgreSQL text persistence. Canonicalization is not changed. |
| L11 | Dedicated `kbp.pipeline_runs` and `kbp.pipeline_jobs`. No coupling to the legacy orchestration tables. |
| L12 | This project's Alembic history is isolated in **`kbp.alembic_version`**. `public.alembic_version` is never touched. |
| L13 | Legacy `kb`/`metadata` rows are never deleted, rewritten, re-keyed or normalized in place. |
| L14 | No ANN index is created or selected in V1. |
| L15 | Operational identifiers (runs, jobs, attempts, dead letters) may use random or sequence ids. The deterministic-identity rule applies to canonical content, source observations and bootstrap bridge rows. |
| L16 | **Gold source bridge:** reuse the existing `dataset_sources` rows named `common_crawl` and `github`; create no `dataset_sources` rows. Exactly **two** pipeline-owned `public.datasets` bridge rows exist, with **deterministic UUIDv5 ids** (section 26). They are internal compatibility records, not external datasets. |
| L17 | **Gold publication order:** `metadata` before `kb`, in one transaction. Conflicts on `id` are *verified*, never overwritten (section 13). |
| L18 | **Legacy embeddings:** new-pipeline embeddings are **not** assumed comparable with the 383 legacy vectors until the legacy model is identified and vector-space compatibility is demonstrated. A model with a dimension other than 768 requires a deliberate future Gold contract migration. |
| L19 | **Near-duplicates never delete provenance.** Relationships between canonical documents are recorded as versioned decisions (section 9.5). |
| L20 | **No constraints are added to any `public` table.** Controlled vocabularies for new rows (`source_type`, subject `domain`) are enforced by the pipeline, not by `public` CHECKs that could break legacy rows. |
| L21 | Gold rows are never deleted by this pipeline. `kb_id_fkey` is `ON DELETE CASCADE`, so deleting `metadata` would delete Gold; `DELETE` on `public.metadata`/`public.kb` is outside this pipeline's permitted operations. |

---

## 3. Existing Table Classification

| Existing table | Classification | Audit-based rationale |
|---|---|---|
| `public.kb` | **REUSE** (insert only, no schema change) | The Gold contract. New rows are inserted with `id = chunk_id`. Legacy rows are untouched. |
| `public.metadata` | **REUSE** (insert only, no schema change) | One compact representative row per new chunk, inserted before `kb` (because of `kb_id_fkey`). |
| `public.datasets` | **REUSE** (two deterministic bridge **rows**; no schema change) | Required target of `metadata.source_id`. Bridge rows are fully specified in section 14.3. No unrelated legacy dataset row is reused. |
| `public.dataset_sources` | **REUSE** (read-only lookup by UNIQUE `name`; **no rows created**) | Existing rows `common_crawl` and `github` (A2) are the parents of the two bridge rows. |
| `public.alembic_version` | **NOT USED** (never read or written by this project) | Owned by the legacy migration history (`i4j5k6l7m8n9`). This project uses `kbp.alembic_version`. |
| `public.documents` | **NOT USED** | Keyed by UNIQUE `url`. That is exactly the source/document identity confusion Identity V1 forbids. |
| `public.processed_documents` | **NOT USED** | Depends on `documents.id`. Superseded by `kbp.document_registry`. |
| `public.storage_objects` | **NOT USED** | Every row requires `document_id NOT NULL → documents.id`, so it cannot reference `kbp` documents without fake legacy `documents` rows. |
| `public.processing_jobs` | **NOT USED** | Every job requires `document_id NOT NULL → documents.id`. It has no lease or next-attempt fields and no unique work key. |
| `public.workflow_runs` | **NOT USED** | Every run requires `request_id NOT NULL → collection_requests.id`, which belongs to the legacy request system. |
| `public.orchestration_jobs` | **NOT USED** | A legacy queue with its own claimers. Rows inserted here could be claimed by legacy workers. It has no lease expiry, next-attempt time or unique work key. |
| `public.source_health` | **NOT USED** | Legacy-owned rows (~8). It has no quota, remaining or reset fields. Sharing `source` keys (for example a legacy `github`) would couple the two systems. |
| `public.agent_jobs`, `task_dependencies` | **NOT USED** | Legacy agent/DAG orchestration. |
| `public.collection_requests`, `collection_request_candidates`, `search_jobs`, `search_results`, `request_cache` | **NOT USED** | Search- and request-driven collection is outside the two-source policy. |
| `public.dataset_files`, `dataset_downloads`, `dataset_outputs`, `dataset_processing_jobs`, `processed_datasets`, `training_datasets` | **NOT USED** | Dataset download and export workflows. They have no WARC or commit/blob provenance fields. |

**Existing public tables modified by Schema V1: none.**

---

## 4. `kbp` Schema Overview

### 4.1 Tables

| Group | Table | Growth |
|---|---|---|
| Runs, jobs, reliability | `pipeline_runs`, `pipeline_jobs`, `processing_attempts`, `dead_letters`, `source_rate_limits` | small, bounded |
| Gold bridge | `source_dataset_map` | tiny |
| Common Crawl provenance | `cc_crawls`, `cc_warc_files`, `cc_captures` | captures: very large |
| GitHub provenance | `github_repositories`, `github_commits`, `github_file_observations` | observations: large |
| Shared provenance | `source_observations`, `observation_extractions` | very large |
| Canonical documents | `document_registry` | large |
| Document relationships (near-duplicate, version, related) | `dedup_methods`, `document_dedup_decisions`, `document_relationships` | methods tiny; decisions ≈ documents × methods; relationships ≈ related pairs |
| Canonical chunks | `chunking_versions`, `chunk_registry`, `chunk_occurrences` | very large (≥7×10⁹) |
| Embeddings | `embedding_models`, `chunk_embeddings`, `embedding_queue` | registry very large; queue bounded |
| Legacy | `legacy_kb_reconciliation` | = legacy `kb` row count (383) |
| Migrations | `alembic_version` (managed by Alembic) | tiny |

That is 25 pipeline tables plus the Alembic version table.

### 4.2 Relational model

```
 public (unchanged)                                    kbp
 ─────────────────                                     ───
 dataset_sources <── datasets <────────── source_dataset_map
                        ^
                        | metadata.source_id (existing FK)
                     metadata <── kb.id (existing FK) ── kb
                        ^  id = chunk_id                  ^ id = chunk_id
                        :  (logical, no FK)               : (logical, no FK)
                        :                                 :
 cc_crawls ─< cc_warc_files ─< cc_captures ──1:1──┐       :
                                                  v       :
 github_repositories ─< github_commits ─< github_file_observations ──1:1──> source_observations
                                                                              |  ^   ^
                                                     observation_extractions >┘  |   | representative_
                                                          | document_id          |   | observation_id
                                                          v                      |   |
                        document_registry (first_observation_id) ───────────────┘   |
                          ^        ^                                                 |
                          |        └─< chunk_occurrences >── chunk_registry ─────────┘
                          |              (document_id,          ^    ^    (first_document_id ─> document_registry)
                          |               chunking_version_id,  |    |
                          |               ordinal)              |    └─< chunk_embeddings >── embedding_models
                          |                                     └─< embedding_queue >──────┘
                          ├─< document_dedup_decisions (document_id, dedup_method_id) ─> canonical_document_id
                          └─< document_relationships (document_id, related_document_id, type, dedup_method_id)
                                         both ─> dedup_methods

 pipeline_runs ─< pipeline_jobs ─< processing_attempts
       ^               ^
       └──── dead_letters, source_observations.run_id, chunk_embeddings.run_id, legacy_kb_reconciliation.run_id
 legacy_kb_reconciliation : legacy_kb_id = public.kb.id (logical, no FK)
```

The complete provenance path (section 11):

```
source_observations → observation_extractions → document_registry
    → chunk_occurrences → chunk_registry → public.kb / public.metadata
```

---

## 5. Complete Proposed Table Specifications

Conventions that apply to every table:

- Schema `kbp`; `timestamptz` for all times.
- `created_at`-style columns are `NOT NULL DEFAULT now()` unless stated.
- **Hashes are `bytea`** with `CHECK (octet_length(col) = 32)`. That is
  32 bytes, against 64+ for hex text. The lowercase hex from Identity V1 is the
  external textual form; conversion uses `bytes.fromhex()` and `.hex()`.
- Status fields are `text` with CHECK lists.
- `jsonb` is used only where the shape varies by job type or component: job
  checkpoints, attempt metrics, component-version bundles and model configs.
- Every **text-hash CHECK** below uses PostgreSQL's built-in
  `sha256(bytea)` (available since PostgreSQL 11; no extension needed) on
  `convert_to(text, 'UTF8')`. The server encoding is UTF8 (verified), so this
  is exactly the UTF-8 byte sequence Identity V1 hashes.

### 5.1 `kbp.pipeline_runs`

| Column | Type | Null | Rationale |
|---|---|---|---|
| run_id | uuid | NO | PK. Random UUID; operational (L15). |
| run_kind | text | NO | CHECK IN (`'ingest'`, `'reprocess'`, `'reconcile_legacy'`, `'reembed'`, `'maintenance'`) |
| status | text | NO | CHECK IN (`'running'`, `'succeeded'`, `'failed'`, `'aborted'`) |
| started_at | timestamptz | NO | |
| finished_at | timestamptz | YES | NULL while running. CHECK `finished_at IS NULL OR finished_at >= started_at`. |
| code_version | text | YES | git commit of the pipeline code, when available |
| config_sha256 | bytea | YES | CHECK 32 bytes. Hash of the effective configuration. |
| identity_spec_version | text | NO | CHECK IN (`'v1'`) |
| identity_extension_version | text | YES | `'v1'` once section 26 is frozen |
| host | text | YES | |
| notes | text | YES | |

### 5.2 `kbp.source_dataset_map` (Gold bridge)

This table is necessary. `metadata.source_id` is NOT NULL and must reference
`public.datasets.id`. The map gives the pipeline an explicit, FK-protected
record of the only two `datasets` rows it owns, and it is the only way the
publication path resolves `metadata.source_id`. There are no string lookups
into legacy tables at publication time.

| Column | Type | Null | Rationale |
|---|---|---|---|
| source_channel | text | NO | PK. CHECK IN (`'common_crawl'`, `'github'`). The pipeline's channel enum, equal to `dataset_sources.name`. |
| dataset_source_name | text | NO | CHECK `dataset_source_name = source_channel`. Records which existing `dataset_sources.name` the bridge row hangs under. |
| bridge_identity_name | text | NO | UNIQUE. The exact UUIDv5 name string (section 26), e.g. `dataset-bridge:v1:common_crawl`. CHECK `bridge_identity_name = 'dataset-bridge:v1:' \|\| source_channel`. |
| dataset_id | uuid | NO | UNIQUE; FK → `public.datasets(id)`. Equals `UUIDv5(KB_NAMESPACE, bridge_identity_name)`. |
| created_at | timestamptz | NO | |

V1 granularity is **exactly one `datasets` row per channel** (section 14.3).
Pages, repositories and files never get `datasets` rows; their provenance
lives in `kbp`.

### 5.3 `kbp.source_rate_limits`

| Column | Type | Null | Rationale |
|---|---|---|---|
| source_channel | text | NO | CHECK IN (`'common_crawl'`, `'github'`) |
| limit_scope | text | NO | e.g. `rest_core`, `graphql`, `search`, `cc_data_https` |
| credential_fingerprint | text | NO | `''` for unauthenticated use. Otherwise a non-reversible fingerprint of the credential identity. **Never a token.** |
| limit_total | integer | YES | e.g. from `x-ratelimit-limit` |
| remaining | integer | YES | CHECK `>= 0` |
| reset_at | timestamptz | YES | e.g. from `x-ratelimit-reset` |
| retry_after_until | timestamptz | YES | `now()` + `Retry-After` (secondary limits, 429/503) |
| last_http_status | smallint | YES | |
| updated_at | timestamptz | NO | |

- PK: `(source_channel, limit_scope, credential_fingerprint)`.

### 5.4 `kbp.cc_crawls`

| Column | Type | Null | Rationale |
|---|---|---|---|
| crawl_id | text | NO | PK. CHECK `crawl_id ~ '^CC-MAIN-[0-9]{4}-[0-9]{2}$'`. |
| index_kind | text | YES | CHECK IN (`'cdx'`, `'columnar'`). The index used for index-first discovery (O-15). |
| registered_at | timestamptz | NO | |

### 5.5 `kbp.cc_warc_files`

A compact dimension for captures. It is created on demand by UPSERT when a
capture references the file. This is **not** a work unit: acquisition is
index-first with byte-range fetches.

| Column | Type | Null | Rationale |
|---|---|---|---|
| warc_file_id | bigint GENERATED ALWAYS AS IDENTITY | NO | PK. Keeps billions of captures at 8 bytes instead of ~120-byte paths. |
| crawl_id | text | NO | FK → `cc_crawls` |
| warc_filename | text | NO | UNIQUE. The exact relative key as it appears in the index, e.g. `crawl-data/CC-MAIN-…/…warc.gz`. CHECK `starts_with(warc_filename, 'crawl-data/' \|\| crawl_id \|\| '/')`. |
| registered_at | timestamptz | NO | |

### 5.6 `kbp.source_observations` (supertype for all acquired source records)

One row per source record **actually fetched** (a WARC record range, or a
GitHub file blob at a commit). Insert-only.

| Column | Type | Null | Rationale |
|---|---|---|---|
| observation_id | uuid | NO | PK. Identity Extension V1 (section 26). |
| source_channel | text | NO | CHECK IN (`'common_crawl'`, `'github'`). This enforces the two-source policy (L8). |
| source_url | text | NO | CC: `WARC-Target-URI` as recorded. GitHub: commit-pinned `https://github.com/<owner>/<repo>/blob/<commit_sha>/<path>`, built from the observation fields. |
| url_host | text | YES | Lowercased host of `source_url` (web provenance, L9) |
| source_title | text | YES | Only if present in the source (e.g. HTML `<title>`). Never synthesized. |
| source_published_at | timestamptz | YES | Only from explicit source metadata (O-9). Never guessed. |
| acquired_at | timestamptz | NO | When the pipeline fetched it. **Not** part of identity. |
| run_id | uuid | YES | FK → `pipeline_runs` |

### 5.7 `kbp.cc_captures` (Common Crawl subtype)

| Column | Type | Null | Rationale |
|---|---|---|---|
| observation_id | uuid | NO | PK; FK → `source_observations` |
| warc_file_id | bigint | NO | FK → `cc_warc_files` |
| warc_record_offset | bigint | NO | CHECK `>= 0` |
| warc_record_length | integer | NO | CHECK `> 0`. Compressed record length used for the byte-range fetch. |
| warc_record_id | uuid | YES | `WARC-Record-ID` when parseable |
| warc_date | timestamptz | YES | `WARC-Date`: the capture time recorded by Common Crawl |
| http_status | smallint | YES | CHECK `BETWEEN 100 AND 599` |
| http_content_type | text | YES | HTTP `Content-Type` as recorded |
| detected_mime | text | YES | Index-provided detected MIME, if available |
| redirect_location | text | YES | `Location` header, if present |
| canonical_url | text | YES | `rel=canonical` if present in the HTML. Informational; never identity. |
| payload_digest | text | YES | `WARC-Payload-Digest` / index digest as recorded |

- UNIQUE `(warc_file_id, warc_record_offset)`. This natural-key guard is
  independent of `observation_id` derivation (two WARC records cannot start at
  the same offset).
- `crawl_id` is reachable through `cc_warc_files`.

### 5.8 `kbp.github_repositories`

| Column | Type | Null | Rationale |
|---|---|---|---|
| repository_id | bigint | NO | PK. GitHub numeric repository id, stable across renames and transfers. |
| node_id | text | YES | GraphQL node id, if fetched |
| owner_login | text | NO | Last observed. Mutable. |
| repo_name | text | NO | Last observed. Mutable. |
| html_url | text | NO | Last observed |
| default_branch | text | YES | NULL when unknown |
| visibility | text | NO | CHECK IN (`'public'`, `'private'`, `'internal'`, `'unknown'`). Last observed. |
| license_spdx_id | text | YES | As reported by GitHub. Never inferred (O-7). |
| is_fork | boolean | YES | |
| is_archived | boolean | YES | |
| api_etag | text | YES | Conditional requests save rate-limit quota |
| last_snapshot_commit_sha | text | YES | Last fully processed snapshot, used for incremental processing |
| first_seen_at | timestamptz | NO | |
| metadata_refreshed_at | timestamptz | YES | |

- There is no UNIQUE on `(owner_login, repo_name)`, because names can be
  reused after renames.

### 5.9 `kbp.github_commits` (repository versions / snapshots)

| Column | Type | Null | Rationale |
|---|---|---|---|
| repository_id | bigint | NO | FK → `github_repositories` |
| commit_sha | text | NO | CHECK `~ '^[0-9a-f]{40}([0-9a-f]{24})?$'` (SHA-1 or SHA-256 object format) |
| tree_sha | text | YES | Same hex CHECK |
| ref_name | text | YES | The branch or tag resolved to this commit |
| committed_at | timestamptz | YES | Commit metadata. **Not** a publication date. |
| visibility_at_snapshot | text | NO | CHECK `= 'public'`. The database enforces the public-only policy at snapshot time. |
| previous_snapshot_sha | text | YES | The snapshot diffed against, for incremental processing |
| snapshot_mode | text | NO | CHECK IN (`'full'`, `'incremental'`) |
| resolved_at | timestamptz | NO | |

- PK `(repository_id, commit_sha)`.

### 5.10 `kbp.github_file_observations` (GitHub subtype)

| Column | Type | Null | Rationale |
|---|---|---|---|
| observation_id | uuid | NO | PK; FK → `source_observations` |
| repository_id | bigint | NO | Composite FK `(repository_id, commit_sha)` → `github_commits` |
| commit_sha | text | NO | |
| file_path | text | NO | Repository-relative path exactly as in the git tree (section 26) |
| blob_sha | text | NO | Hex CHECK (40 or 64). Always available: from the tree entry, or computed locally as the git blob hash. |
| file_size_bytes | bigint | YES | CHECK `>= 0` |

- UNIQUE `(repository_id, commit_sha, file_path)`, the natural-key guard.

### 5.11 `kbp.observation_extractions` (observation → document link)

| Column | Type | Null | Rationale |
|---|---|---|---|
| observation_id | uuid | NO | FK → `source_observations` |
| extraction_version | text | NO | Label for extractor + preprocessing + canonicalization (e.g. `x1-c1`) |
| outcome | text | NO | CHECK IN (`'document'`, `'empty'`, `'rejected_language'`, `'rejected_prefilter'`, `'rejected_nul'`, `'extraction_failed'`) |
| document_id | uuid | YES | FK → `document_registry` |
| document_was_new | boolean | YES | TRUE if this extraction registered the document. FALSE means it resolved to an existing canonical document, i.e. an **exact duplicate observation** (section 9.5). |
| extracted_at | timestamptz | NO | |

- PK `(observation_id, extraction_version)`.
- CHECK `(outcome = 'document') = (document_id IS NOT NULL)`.
- CHECK `(document_id IS NULL) = (document_was_new IS NULL)`.

### 5.12 `kbp.document_registry` (global canonical documents)

| Column | Type | Null | Rationale |
|---|---|---|---|
| document_id | uuid | NO | PK. `UUIDv5(KB_NAMESPACE, "document:v1:" + content_hash_hex)`. |
| content_hash | bytea | NO | UNIQUE; CHECK 32 bytes |
| canonical_text | text | YES | **Durable authoritative canonical text** (Silver). Required when `status = 'accepted'`. |
| char_length | integer | NO | CHECK `> 0`. Unicode code points of the canonical text. |
| language | text | YES | |
| language_confidence | real | YES | CHECK `BETWEEN 0 AND 1` |
| quality_score | real | YES | |
| factuality_score | real | YES | |
| subject_domain | text | YES | Subject-domain label (L9). CHECK `char_length <= 100`, so it fits `metadata.domain`. Taxonomy is O-5. |
| status | text | NO | CHECK IN (`'accepted'`, `'rejected_quality'`, `'rejected_near_duplicate'`, `'rejected_factuality'`, `'rejected_policy'`, `'quarantined'`) |
| status_reason | text | YES | Machine-readable reason code |
| policy_version | text | NO | Version of the filter and policy bundle that set `status` |
| component_versions | jsonb | NO | Language, quality, factuality and domain classifier versions (the set varies) |
| first_observation_id | uuid | NO | FK → `source_observations` |
| created_at | timestamptz | NO | |
| status_updated_at | timestamptz | NO | |

CHECKs:

- `canonical_text IS NULL OR sha256(convert_to(canonical_text,'UTF8')) = content_hash`
  (the database verifies content identity);
- `canonical_text IS NULL OR char_length(canonical_text) = char_length`;
- `status <> 'accepted' OR (canonical_text IS NOT NULL AND language = 'en')`
  (English-only Gold).

Near-duplicate relationships are **not** stored on this row. They live in the
versioned tables `document_dedup_decisions` and `document_relationships`
(5.23–5.25, section 9.5). `status = 'rejected_near_duplicate'` is only the
acceptance outcome. The invariant "a document with that status has a
confirmed `NEAR_DUPLICATE` decision naming its canonical document" is
enforced in the application and verified by an audit query, because a CHECK
cannot span tables.

Registration rule (resolves D13):

- A document is registered once it has survived extraction, canonicalization
  (non-empty), the U+0000 rule and English validation.
- Non-English, empty and NUL outcomes are recorded only in
  `observation_extractions.outcome`.
- Rejected documents keep their hash and decision, but `canonical_text` may be
  NULL.
- Retention of quarantined text is O-14.

### 5.13 `kbp.chunking_versions`

| Column | Type | Null | Rationale |
|---|---|---|---|
| chunking_version_id | smallint GENERATED ALWAYS AS IDENTITY | NO | PK |
| name | text | NO | UNIQUE, e.g. `chunker-v1` |
| tokenizer | text | YES | |
| parameters | jsonb | NO | Frozen chunker configuration |
| created_at | timestamptz | NO | |

### 5.14 `kbp.chunk_registry` (global canonical chunks; authoritative)

| Column | Type | Null | Rationale |
|---|---|---|---|
| chunk_id | uuid | NO | PK. `UUIDv5(KB_NAMESPACE, "chunk:v1:" + chunk_hash_hex)`. Equals `kb.id` after publication. |
| chunk_hash | bytea | NO | UNIQUE; CHECK 32 bytes |
| chunk_text | text | NO | **Authoritative canonical chunk text** (L6) |
| char_length | integer | NO | CHECK `> 0 AND char_length = char_length(chunk_text)` |
| token_count | integer | YES | Tokenizer-dependent |
| chunking_version_id | smallint | NO | FK → `chunking_versions`. The version that **first** produced this chunk. Identity does not depend on it. |
| first_document_id | uuid | NO | FK → `document_registry` |
| representative_observation_id | uuid | NO | FK → `source_observations`. Source of the compact `metadata` row (section 14). |
| created_at | timestamptz | NO | |

- CHECK `sha256(convert_to(chunk_text,'UTF8')) = chunk_hash`.
- Only chunks that pass chunk-level acceptance are registered.
- Insert-only. Publication state is tracked in `chunk_embeddings` and `kb`,
  not by updating this table.

### 5.15 `kbp.chunk_occurrences` (chunk provenance)

| Column | Type | Null | Rationale |
|---|---|---|---|
| document_id | uuid | NO | FK → `document_registry` |
| chunking_version_id | smallint | NO | FK → `chunking_versions` |
| ordinal | integer | NO | CHECK `>= 0`. Chunker position; gaps are allowed where chunks were rejected. |
| chunk_id | uuid | NO | FK → `chunk_registry` |
| char_start | integer | NO | Code-point offset into `document_registry.canonical_text` |
| char_end | integer | NO | CHECK `0 <= char_start AND char_start < char_end` |
| section_path | text | YES | Only when the extractor provides real structure |

- PK `(document_id, chunking_version_id, ordinal)`.
- Invariant (enforced in the application, audited in batch):
  `canonicalize_text(canonical_text[char_start:char_end])` equals the chunk
  text.

### 5.16 `kbp.embedding_models`

| Column | Type | Null | Rationale |
|---|---|---|---|
| embedding_model_id | smallint GENERATED ALWAYS AS IDENTITY | NO | PK |
| model_name | text | NO | |
| model_revision | text | NO | Immutable revision (e.g. model repository commit) |
| embedding_version | text | NO | Pipeline-side configuration version (prefix, truncation, normalization) |
| embedding_dimension | integer | NO | CHECK `> 0` |
| vector_type | text | NO | CHECK IN (`'vector'`, `'halfvec'`) |
| distance_metric | text | NO | CHECK IN (`'cosine'`, `'l2'`, `'inner_product'`) |
| is_normalized | boolean | NO | |
| max_input_tokens | integer | YES | |
| config | jsonb | NO | Full frozen configuration |
| status | text | NO | CHECK IN (`'candidate'`, `'shadow'`, `'active_gold'`, `'retired'`) |
| created_at | timestamptz | NO | |

- UNIQUE `(model_name, model_revision, embedding_version)`.
- Partial UNIQUE index on `(status) WHERE status = 'active_gold'`: at most one
  model publishes to Gold.
- CHECK `status <> 'active_gold' OR (vector_type = 'vector' AND embedding_dimension = 768)`.
  This is a guard that mirrors the **current** Gold column type
  `vector(768)`. It does not select a model (section 12).

### 5.17 `kbp.chunk_embeddings` (per-chunk embedding result registry)

A durable record that chunk X was embedded by model/version M, with the result
metadata. Insert-once on success. **The vector itself is not stored here.**
For Gold it lives in `public.kb.embedding`; shadow vector storage is future
work (section 12.4).

| Column | Type | Null | Rationale |
|---|---|---|---|
| chunk_id | uuid | NO | FK → `chunk_registry` |
| embedding_model_id | smallint | NO | FK → `embedding_models` (model + revision + embedding version + dimension) |
| status | text | NO | CHECK IN (`'published_gold'`, `'stored_shadow'`) |
| attempt_count | smallint | NO | CHECK `> 0`. Attempts it took to succeed. |
| embedded_at | timestamptz | NO | |
| published_at | timestamptz | YES | CHECK `(status = 'published_gold') = (published_at IS NOT NULL)` |
| run_id | uuid | YES | FK → `pipeline_runs` |

- PK `(chunk_id, embedding_model_id)`. This is the required
  `chunk_id + model/version` uniqueness.
- Pending and failed embedding work is **not** kept here. It lives in
  `embedding_queue` and `dead_letters`, so this table is never updated through
  a lifecycle.

### 5.18 `kbp.embedding_queue` (transient embedding work)

Contains **no chunk text**. Workers read `chunk_registry.chunk_text`. Rows are
deleted when the result is committed.

| Column | Type | Null | Rationale |
|---|---|---|---|
| chunk_id | uuid | NO | FK → `chunk_registry` |
| embedding_model_id | smallint | NO | FK → `embedding_models` |
| status | text | NO | CHECK IN (`'pending'`, `'running'`, `'retry_pending'`) |
| priority | smallint | NO | default 100 |
| attempt_count | smallint | NO | default 0 |
| max_attempts | smallint | NO | CHECK `max_attempts > 0 AND attempt_count BETWEEN 0 AND max_attempts` |
| next_attempt_at | timestamptz | NO | |
| locked_at | timestamptz | YES | |
| lease_expires_at | timestamptz | YES | |
| worker_id | text | YES | |
| lease_token | uuid | YES | Fencing token |
| last_error_class | text | YES | Same CHECK list as section 17.2 |
| last_error_message | text | YES | Truncated by the application |
| enqueued_at | timestamptz | NO | |

- PK `(chunk_id, embedding_model_id)`.
- CHECK `(status = 'running') = (worker_id IS NOT NULL AND lease_token IS NOT NULL AND locked_at IS NOT NULL AND lease_expires_at IS NOT NULL)`.

### 5.19 `kbp.pipeline_jobs` (persistent bounded work units)

| Column | Type | Null | Rationale |
|---|---|---|---|
| job_id | bigint GENERATED ALWAYS AS IDENTITY | NO | PK |
| job_type | text | NO | CHECK IN (`'cc_index_partition'`, `'gh_repository_discovery'`, `'gh_repository_snapshot'`, `'embedding_enqueue'`, `'policy_reevaluation'`, `'legacy_reconciliation'`) |
| source | text | NO | CHECK IN (`'common_crawl'`, `'github'`, `'internal'`) |
| source_identifier | text | NO | Deterministic work-unit key (section 16.2) |
| status | text | NO | CHECK IN (`'pending'`, `'running'`, `'retry_pending'`, `'succeeded'`, `'dead_letter'`, `'quarantined'`, `'cancelled'`) |
| stage | text | NO | Last completed stage. CHECK in the state list of section 16.3. |
| checkpoint | jsonb | NO | default `'{}'`. Resume position inside the unit (shape varies by `job_type`). |
| priority | smallint | NO | default 100 |
| attempt_count | smallint | NO | default 0 |
| max_attempts | smallint | NO | CHECK `max_attempts > 0 AND attempt_count BETWEEN 0 AND max_attempts` |
| deferral_count | integer | NO | default 0. Rate-limit deferrals, which do **not** consume `attempt_count`. |
| next_attempt_at | timestamptz | NO | available_at |
| locked_at | timestamptz | YES | |
| lease_expires_at | timestamptz | YES | |
| worker_id | text | YES | |
| lease_token | uuid | YES | |
| last_error_class | text | YES | CHECK list in section 17.2 |
| last_error_code | text | YES | e.g. HTTP status or exception type |
| last_error_message | text | YES | |
| run_id | uuid | YES | FK → `pipeline_runs` |
| created_at | timestamptz | NO | |
| started_at | timestamptz | YES | First time claimed |
| completed_at | timestamptz | YES | CHECK `(status IN ('succeeded','dead_letter','quarantined','cancelled')) = (completed_at IS NOT NULL)` |
| updated_at | timestamptz | NO | |

- UNIQUE `(job_type, source_identifier)`.
- CHECK `(status = 'running') = (worker_id IS NOT NULL AND lease_token IS NOT NULL AND locked_at IS NOT NULL AND lease_expires_at IS NOT NULL)`.

### 5.20 `kbp.processing_attempts`

| Column | Type | Null | Rationale |
|---|---|---|---|
| attempt_id | bigint GENERATED ALWAYS AS IDENTITY | NO | PK |
| job_id | bigint | NO | FK → `pipeline_jobs` |
| attempt_no | integer | NO | CHECK `> 0`. Counts every claim, including deferrals. |
| worker_id | text | NO | |
| lease_token | uuid | NO | |
| started_at | timestamptz | NO | |
| finished_at | timestamptz | YES | NULL means in flight or crashed |
| outcome | text | YES | CHECK IN (`'succeeded'`, `'failed'`, `'deferred_rate_limit'`, `'lease_expired'`, `'cancelled'`) |
| error_class | text | YES | |
| error_code | text | YES | |
| error_message | text | YES | |
| retry_after_seconds | integer | YES | Server-provided `Retry-After`, if any |
| scheduled_next_attempt_at | timestamptz | YES | The retry time computed at failure |
| metrics | jsonb | YES | Per-unit counters: fetched, new/existing documents, new/existing chunks, outcomes |

- UNIQUE `(job_id, attempt_no)`.

### 5.21 `kbp.dead_letters`

| Column | Type | Null | Rationale |
|---|---|---|---|
| dead_letter_id | bigint GENERATED ALWAYS AS IDENTITY | NO | PK |
| item_type | text | NO | CHECK IN (`'job'`, `'observation'`, `'document'`, `'chunk'`, `'chunk_embedding'`, `'legacy_kb_row'`) |
| item_key | text | NO | Canonical key: `job_id`, `observation_id`, `document_id`, `chunk_id`, `"<chunk_id>:<model_id>"`, legacy `kb.id` |
| stage | text | NO | Stage where it failed (section 16.3 list) |
| disposition | text | NO | CHECK IN (`'dead_letter'`, `'quarantined'`) |
| job_id | bigint | YES | FK → `pipeline_jobs` |
| run_id | uuid | YES | FK → `pipeline_runs` |
| error_class | text | NO | |
| error_code | text | YES | |
| error_message | text | YES | |
| attempt_count | integer | NO | |
| occurrence_count | integer | NO | default 1 |
| payload_ref | text | YES | Path under `data/quarantine/` for payloads that cannot or must not be stored in PostgreSQL (e.g. text containing U+0000) |
| first_seen_at | timestamptz | NO | |
| last_seen_at | timestamptz | NO | |
| resolved_at | timestamptz | YES | |
| resolution | text | YES | CHECK IN (`'requeued'`, `'ignored'`, `'fixed'`) and `(resolution IS NULL) = (resolved_at IS NULL)` |

- UNIQUE `(item_type, item_key, stage)`.

### 5.22 `kbp.legacy_kb_reconciliation`

| Column | Type | Null | Rationale |
|---|---|---|---|
| legacy_kb_id | uuid | NO | PK. Equals `public.kb.id`. **No FK**: an FK from `kbp` would restrict legacy deletes and change legacy behaviour. |
| legacy_source_id | uuid | YES | Copied from `public.metadata.source_id` at reconciliation time |
| raw_chunk_sha256 | bytea | NO | CHECK 32 bytes. SHA-256 of `kb.chunk` exactly as stored, to detect later legacy edits. |
| canonical_chunk_hash | bytea | YES | CHECK 32 bytes. NULL if the canonical text is empty or contains U+0000. |
| canonical_chunk_id | uuid | YES | Identity V1 chunk id of the canonical text. **Computed, not registered.** |
| canonical_differs | boolean | YES | TRUE if canonicalization changed the text |
| reconciliation_status | text | NO | CHECK IN (`'pending'`, `'computed'`, `'empty_after_canonicalization'`, `'contains_nul'`, `'unique'`, `'collides_with_legacy'`, `'collides_with_new_chunk'`, `'error'`) |
| collision_chunk_id | uuid | YES | A new-pipeline `chunk_id` with the same canonical hash |
| collision_legacy_kb_id | uuid | YES | Another legacy `kb.id` with the same canonical hash. For a group, the member with the smallest `legacy_kb_id` is recorded. |
| decision | text | NO | default `'undecided'`. CHECK IN (`'undecided'`, `'keep_legacy_only'`, `'alias_new_to_legacy'`, `'publish_new_mark_legacy_superseded'`, `'publish_both'`, `'exclude'`). This is the publication policy for a collision (O-4). |
| decision_reason | text | YES | |
| review_state | text | NO | default `'not_required'`. CHECK IN (`'not_required'`, `'needs_review'`, `'reviewed'`). |
| reviewed_by | text | YES | |
| reviewed_at | timestamptz | YES | CHECK `(review_state = 'reviewed') = (reviewed_at IS NOT NULL)` |
| error_message | text | YES | Set when `reconciliation_status = 'error'` |
| reconciliation_version | text | NO | |
| run_id | uuid | YES | FK → `pipeline_runs` |
| first_reconciled_at | timestamptz | NO | |
| reconciled_at | timestamptz | NO | Last computation |
| decided_at | timestamptz | YES | CHECK `(decision = 'undecided') = (decided_at IS NULL)` |

Additional CHECKs:

- `reconciliation_status NOT IN ('collides_with_new_chunk') OR collision_chunk_id IS NOT NULL`;
- `reconciliation_status NOT IN ('collides_with_legacy') OR collision_legacy_kb_id IS NOT NULL`;
- `reconciliation_status NOT IN ('unique','collides_with_legacy','collides_with_new_chunk') OR (canonical_chunk_hash IS NOT NULL AND canonical_chunk_id IS NOT NULL)`.

### 5.23 `kbp.dedup_methods`

A registry of the deduplication and relationship methods.

| Column | Type | Null | Rationale |
|---|---|---|---|
| dedup_method_id | smallint GENERATED ALWAYS AS IDENTITY | NO | PK |
| algorithm | text | NO | e.g. `exact_content_hash`, `minhash_lsh`, `simhash`, `url_lineage` |
| algorithm_version | text | NO | |
| parameters | jsonb | NO | Frozen thresholds and settings (O-17: benchmark) |
| created_at | timestamptz | NO | |

- UNIQUE `(algorithm, algorithm_version)`.

### 5.24 `kbp.document_dedup_decisions`

One classification per document per method version.

| Column | Type | Null | Rationale |
|---|---|---|---|
| document_id | uuid | NO | FK → `document_registry`. The document being classified. |
| dedup_method_id | smallint | NO | FK → `dedup_methods` |
| classification | text | NO | CHECK IN (`'EXACT_DUPLICATE'`, `'NEAR_DUPLICATE'`, `'VERSION'`, `'RELATED'`, `'UNIQUE'`) |
| canonical_document_id | uuid | YES | FK → `document_registry`. The representative document it duplicates, or is a version of or related to. |
| similarity_score | real | YES | CHECK `similarity_score IS NULL OR similarity_score BETWEEN 0 AND 1` |
| decision_status | text | NO | CHECK IN (`'proposed'`, `'confirmed'`, `'overridden'`) |
| decided_at | timestamptz | NO | |
| run_id | uuid | YES | FK → `pipeline_runs` |

- PK `(document_id, dedup_method_id)`.
- CHECK `(classification = 'UNIQUE') = (canonical_document_id IS NULL)`.
- CHECK `canonical_document_id IS DISTINCT FROM document_id`.

### 5.25 `kbp.document_relationships`

Additional pairwise relationships, when one document relates to several others.

| Column | Type | Null | Rationale |
|---|---|---|---|
| document_id | uuid | NO | FK → `document_registry` |
| related_document_id | uuid | NO | FK → `document_registry` |
| relationship_type | text | NO | CHECK IN (`'EXACT_DUPLICATE'`, `'NEAR_DUPLICATE'`, `'VERSION'`, `'RELATED'`) |
| dedup_method_id | smallint | NO | FK → `dedup_methods` |
| similarity_score | real | YES | CHECK `similarity_score IS NULL OR similarity_score BETWEEN 0 AND 1` |
| decision_status | text | NO | CHECK IN (`'proposed'`, `'confirmed'`, `'overridden'`) |
| created_at | timestamptz | NO | |

- PK `(document_id, related_document_id, relationship_type, dedup_method_id)`.
- CHECK `document_id <> related_document_id`.
- For `VERSION`, the direction is "`document_id` is a later version of
  `related_document_id`".

---

## 6. PK / FK / UNIQUE / CHECK Constraints (consolidated)

### 6.1 Primary and unique keys

| Table | PK | Additional UNIQUE |
|---|---|---|
| pipeline_runs | run_id | — |
| source_dataset_map | source_channel | dataset_id; bridge_identity_name |
| source_rate_limits | (source_channel, limit_scope, credential_fingerprint) | — |
| cc_crawls | crawl_id | — |
| cc_warc_files | warc_file_id | warc_filename |
| source_observations | observation_id | — |
| cc_captures | observation_id | (warc_file_id, warc_record_offset) |
| github_repositories | repository_id | — |
| github_commits | (repository_id, commit_sha) | — |
| github_file_observations | observation_id | (repository_id, commit_sha, file_path) |
| observation_extractions | (observation_id, extraction_version) | — |
| document_registry | document_id | content_hash |
| chunking_versions | chunking_version_id | name |
| chunk_registry | chunk_id | chunk_hash |
| chunk_occurrences | (document_id, chunking_version_id, ordinal) | — |
| embedding_models | embedding_model_id | (model_name, model_revision, embedding_version); partial (status) WHERE status='active_gold' |
| chunk_embeddings | (chunk_id, embedding_model_id) | — |
| embedding_queue | (chunk_id, embedding_model_id) | — |
| pipeline_jobs | job_id | (job_type, source_identifier) |
| processing_attempts | attempt_id | (job_id, attempt_no) |
| dead_letters | dead_letter_id | (item_type, item_key, stage) |
| legacy_kb_reconciliation | legacy_kb_id | — |
| dedup_methods | dedup_method_id | (algorithm, algorithm_version) |
| document_dedup_decisions | (document_id, dedup_method_id) | — |
| document_relationships | (document_id, related_document_id, relationship_type, dedup_method_id) | — |

### 6.2 Foreign keys

All foreign keys use `ON DELETE NO ACTION`: parents are never deleted in
normal operation.

- **Within `kbp`:**
  - `cc_warc_files.crawl_id → cc_crawls`
  - `cc_captures.observation_id → source_observations`
  - `cc_captures.warc_file_id → cc_warc_files`
  - `github_commits.repository_id → github_repositories`
  - `github_file_observations.observation_id → source_observations`
  - `github_file_observations.(repository_id, commit_sha) → github_commits`
  - `observation_extractions.observation_id → source_observations`
  - `observation_extractions.document_id → document_registry`
  - `document_registry.first_observation_id → source_observations`
  - `document_dedup_decisions.document_id → document_registry`
  - `document_dedup_decisions.canonical_document_id → document_registry`
  - `document_dedup_decisions.dedup_method_id → dedup_methods`
  - `document_relationships.document_id → document_registry`
  - `document_relationships.related_document_id → document_registry`
  - `document_relationships.dedup_method_id → dedup_methods`
  - `chunk_registry.chunking_version_id → chunking_versions`
  - `chunk_registry.first_document_id → document_registry`
  - `chunk_registry.representative_observation_id → source_observations`
  - `chunk_occurrences.document_id → document_registry`
  - `chunk_occurrences.chunking_version_id → chunking_versions`
  - `chunk_occurrences.chunk_id → chunk_registry`
  - `chunk_embeddings.chunk_id → chunk_registry`
  - `chunk_embeddings.embedding_model_id → embedding_models`
  - `embedding_queue.chunk_id → chunk_registry`
  - `embedding_queue.embedding_model_id → embedding_models`
  - `processing_attempts.job_id → pipeline_jobs`
  - `dead_letters.job_id → pipeline_jobs`
  - all `run_id → pipeline_runs`
- **From `kbp` into `public`: exactly one.**
  `source_dataset_map.dataset_id → public.datasets(id)`, with `ON DELETE NO
  ACTION`.
  - It references only the two pipeline-owned bridge rows, and those rows are
    already protected by `metadata_source_id_fkey` once any new-pipeline
    `metadata` row references them.
  - `datasets` is small (~378 rows), so the brief lock taken when the FK is
    created is harmless.
- **Deliberately absent** (logical links only):
  - `public.kb.id ↔ kbp.chunk_registry.chunk_id`;
  - `kbp.chunk_embeddings → public.kb`;
  - `kbp.legacy_kb_reconciliation → public.kb`.

  These avoid constraining legacy writers and deleters.
- **Not added in V1: subtype ↔ `source_channel` consistency.** Enforcing it
  with a composite FK would need an extra UNIQUE `(observation_id,
  source_channel)` index on the very large supertype. The channel is already
  encoded in the `observation_id` derivation prefix, and consistency is
  verified by an audit query (optional hardening: section 21.3).

### 6.3 Cross-cutting CHECK rules

| Rule | Where |
|---|---|
| Hash columns are 32 bytes | every `*_hash` and `*_sha256` bytea column |
| Stored text hashes to its identity | `document_registry.canonical_text`, `chunk_registry.chunk_text` |
| Stored length matches text | same tables |
| Accepted documents are English and have text | `document_registry` |
| Only the two channels | `source_observations`, `source_dataset_map`, `source_rate_limits` |
| Public GitHub only | `github_commits.visibility_at_snapshot = 'public'` |
| Lease consistency | `pipeline_jobs`, `embedding_queue` |
| Bounded retries | `attempt_count BETWEEN 0 AND max_attempts` |
| Gold model compatible with Gold column type | `embedding_models` (`vector(768)`) |
| U+0000 | Structurally guaranteed: PostgreSQL `text` cannot contain it. The **pipeline** rejects such content before any insert (L10), so the failure is a classified outcome, not a database error. |
| Bridge naming is deterministic | `source_dataset_map` (`bridge_identity_name` derived from `source_channel`) |
| Dedup decisions are well formed | `document_dedup_decisions` (canonical target iff not UNIQUE; no self-reference); `document_relationships` (no self-pair) |
| Reconciliation state is consistent | `legacy_kb_reconciliation` (collision targets present; decision and review timestamps) |
| **No CHECK on `public` tables** | L20 |

---

## 7. Common Crawl Provenance

### 7.1 Flow (index-first, byte-range fetch)

1. **Discovery.** A `pipeline_jobs` row with `job_type = 'cc_index_partition'`
   is created for each index partition (a CDX shard, or a columnar-index file
   plus row-group range) and **filter version**.
   - Candidates that fail index-level filters (for example status, MIME or
     language hints) are never fetched and never stored. They are only counted
     in `processing_attempts.metrics`.
   - Candidate lists are **not persisted**. They are re-derivable
     deterministically from the index partition and filter version, and the
     checkpoint records the position (e.g. `{"row": 183422}`).
2. **Acquisition.** Each candidate is fetched by range
   `[warc_record_offset, warc_record_offset + warc_record_length)` from
   `warc_filename`.
3. **Persistence in one transaction per batch:**
   - UPSERT `cc_crawls`, `cc_warc_files`;
   - insert `source_observations` and `cc_captures`;
   - process to a document (section 9) and write `observation_extractions`;
   - advance the job checkpoint.

### 7.2 Preserved fields

| Requirement | Column |
|---|---|
| crawl_id | `cc_warc_files.crawl_id` (→ `cc_crawls`) |
| WARC filename | `cc_warc_files.warc_filename` |
| offset / length | `cc_captures.warc_record_offset` / `warc_record_length` |
| original URL | `source_observations.source_url` (WARC-Target-URI) |
| canonical URL | `cc_captures.canonical_url` (informational) |
| redirect | `cc_captures.redirect_location` |
| HTTP status / content type | `cc_captures.http_status`, `http_content_type`, `detected_mime` |
| capture time (CC) | `cc_captures.warc_date` |
| acquisition time (ours) | `source_observations.acquired_at` |
| capture identity | `observation_id` (section 26) + UNIQUE `(warc_file_id, warc_record_offset)` |

**Reproduction:** from `warc_filename`, offset and length, re-fetch the exact
record, re-run `extraction_version`, canonicalize, and compare the result with
`content_hash`. Wikipedia pages are ordinary captures and get no special
channel.

---

## 8. GitHub Provenance

### 8.1 Flow

1. **Repository discovery job** (`gh_repository_discovery`).
   - UPSERT `github_repositories` from the API, including `repository_id`,
     `visibility`, `default_branch` and license.
   - Only `visibility = 'public'` repositories proceed.
2. **Snapshot job** (`gh_repository_snapshot`, key
   `"<repository_id>@<commit_sha>"`).
   - Resolve the commit (default policy: default-branch HEAD, O-8).
   - Insert `github_commits` with `visibility_at_snapshot = 'public'`.
     The CHECK rejects anything else.
3. **Incremental processing.** Compare the new tree with
   `last_snapshot_commit_sha`.
   - Files whose `(path, blob_sha)` is unchanged are **not re-observed**. The
     earlier observation remains their provenance.
   - New or changed files are fetched and observed.
   - `snapshot_mode` records `'full'` or `'incremental'`.
4. **Per file** (in batches, one transaction):
   - insert `source_observations` and `github_file_observations`;
   - process to a document and write `observation_extractions`;
   - advance the checkpoint (e.g. `{"last_path": "docs/z.md"}`).
5. **On snapshot success:** update
   `github_repositories.last_snapshot_commit_sha` in the same transaction that
   marks the job `succeeded`.

### 8.2 Preserved fields

| Requirement | Column |
|---|---|
| repository identity | `github_repositories.repository_id` (numeric, rename-stable) |
| repository URL, owner/name | `html_url`, `owner_login`, `repo_name` (last observed) |
| default branch | `default_branch` (nullable) |
| commit SHA / tree | `github_commits.commit_sha`, `tree_sha` |
| file path / blob | `github_file_observations.file_path`, `blob_sha` |
| acquired_at | `source_observations.acquired_at` |
| public-only | `github_commits.visibility_at_snapshot` CHECK |

File history never creates duplicate documents. The same content at any commit
or path hashes to the same `content_hash`, so the same `document_id` is linked
through `observation_extractions`.

---

## 9. Global Document Dedup Model

### 9.1 Logical requirement

There is exactly one `document_registry` row per `content_hash`, and exactly
one per `document_id`. This holds across both channels and all time.

### 9.2 Pilot and initial physical implementation

- PK(`document_id`) plus UNIQUE(`content_hash`).
- A CHECK binds stored text to its hash.
- `document_id` is computed from `content_hash` in the application (Identity
  V1, with regression-tested vectors). An audit job re-derives it periodically.
  SQL cannot recompute UUIDv5 without SHA-1, and `pgcrypto` is not installed.

### 9.3 Processing an observation (one transaction per batch)

```
insert source_observations + subtype                       (ON CONFLICT (observation_id) DO NOTHING)
extract -> canonicalize -> [empty? U+0000? non-English?]   -> observation_extractions(outcome=...) ; stop
compute content_hash, document_id
INSERT document_registry ... ON CONFLICT (document_id) DO NOTHING RETURNING document_id
   new      -> run quality / near-dup / factuality / subject-domain decisions (computed before insert;
               the row is written once with its final status); if accepted, chunk (section 10)
   existing -> no re-evaluation, no re-chunking
insert document_dedup_decisions / document_relationships for the new document  (ON CONFLICT DO NOTHING)
insert observation_extractions(outcome='document', document_id, document_was_new)   (ON CONFLICT DO NOTHING)
advance job checkpoint
```

Near duplicates are **distinct** canonical documents. They are registered with
`status = 'rejected_near_duplicate'`, and their relationship is recorded in
the versioned dedup tables (section 9.5).

### 9.4 Very large scale

See section 25. The logical requirement does not change. The physical
enforcement (partitioned or sharded uniqueness) must be benchmarked.

### 9.5 Near-duplicate and relationship model

The draft's single `duplicate_of_document_id` column was **insufficient**. It
had no algorithm version, no score, no decision status, and no way to express
VERSION, RELATED or several relationships. It is replaced by 5.23–5.25.

| Classification | Where it is recorded | Canonical relationship |
|---|---|---|
| `EXACT_DUPLICATE` (identical canonical text) | An observation that resolves to an existing document: `observation_extractions.document_was_new = FALSE`. A duplicate canonical document cannot exist, because the PK and UNIQUE(`content_hash`) make it impossible. | the document itself |
| `EXACT_DUPLICATE` under a stricter-than-Identity-V1 equivalence defined by a dedup method (e.g. boilerplate-stripped hash) | `document_dedup_decisions` | `canonical_document_id` |
| `NEAR_DUPLICATE` | `document_dedup_decisions` (+ optional `document_relationships` for further matches) | `canonical_document_id` |
| `VERSION` (same URL across crawls, or same repository path across commits, with changed content) | `document_dedup_decisions` / `document_relationships` | the earlier or representative version |
| `RELATED` | `document_relationships` (many allowed) | `related_document_id` |
| `UNIQUE` | `document_dedup_decisions` with `canonical_document_id = NULL` | none |

Properties:

- **Provenance is never deleted.** Every document, including rejected near
  duplicates, keeps its registry row, observations and extractions.
- **Versioned decisions.** Each decision records `dedup_method_id` (algorithm
  plus version plus frozen parameters), `similarity_score` and
  `decision_status`. A new method version adds rows and never overwrites
  earlier decisions.
- **Acceptance link.** `document_registry.status = 'rejected_near_duplicate'`
  is set only together with a `confirmed` `NEAR_DUPLICATE` decision (an
  application invariant, verified by audit).
- **Thresholds and algorithm are not chosen here** (O-17). The schema stores
  whatever method is benchmarked.
- **Scale note.** Signature storage for large-scale MinHash/LSH candidate
  generation (bands, buckets) is part of the near-duplicate implementation and
  its physical design. Only the **decisions** are part of the V1 relational
  model.

---

## 10. Global Chunk Dedup Model

### 10.1 Logical requirement

There is exactly one `chunk_registry` row per `chunk_hash` and per `chunk_id`,
and at most one Gold row per `chunk_id`.

### 10.2 Pilot and initial physical implementation

- PK(`chunk_id`) plus UNIQUE(`chunk_hash`).
- CHECK `sha256(chunk_text) = chunk_hash`.
- The application derives `chunk_id` (Identity V1). A batch audit re-derives
  it.

### 10.3 Chunking an accepted document (same transaction as 9.3)

```
for each accepted chunk (ordinal, span, canonical chunk text):
   reject empty / U+0000 / chunk-level policy failures (not registered)
   INSERT chunk_registry (...) ON CONFLICT (chunk_id) DO NOTHING RETURNING chunk_id  -> new_chunks
   INSERT chunk_occurrences (...) ON CONFLICT DO NOTHING
IF an embedding model has status 'active_gold':
   INSERT embedding_queue (new_chunks x active_gold model) ON CONFLICT DO NOTHING
ELSE:
   nothing is enqueued; an embedding_enqueue job backfills after a model is activated
```

Chunk registration does not depend on the embedding-model decision (O-2).

- The first writer's `first_document_id`, `chunking_version_id` and
  `representative_observation_id` are kept.
- Concurrent writers of the same chunk serialize on the unique index.
  `ON CONFLICT` waits for the first transaction's outcome, so a rolled-back
  first writer never leaves the chunk missing.

---

## 11. Chunk Provenance Model

```
Document A ──┐  chunk_occurrences(A, cv, 3, X)
Document B ──┼─ chunk_occurrences(B, cv, 0, X) ──> chunk_registry(X)  ──> public.kb(X), public.metadata(X)
Document C ──┘  chunk_occurrences(C, cv, 7, X)
```

| Query | Path | Initially indexed? |
|---|---|---|
| Chunks of document D | `chunk_occurrences` PK prefix `(D, …)` | Yes |
| Documents containing chunk X | `chunk_occurrences WHERE chunk_id = X` | **No.** Optional index (21.3). Batch scan until needed. |
| Sources of document D | `observation_extractions WHERE document_id = D` → `source_observations` → subtype | Yes (21.2) |
| Full sources of Gold row X | the two rows above combined | partial |
| Representative source of Gold row X | `chunk_registry.representative_observation_id` (PK lookup) | Yes |

Re-chunking with a new `chunking_version_id` adds occurrences. It reuses
existing chunks whenever the text is identical, and never deletes old
occurrences.

---

## 12. Embedding Version Model

### 12.1 Facts and constraints

- The audited Gold column is **`vector(768)`**, nullable (A1).
- All 383 legacy rows have non-NULL embeddings.
- There is no ANN index on `kb` (A1, A2).
- The model that produced the legacy vectors is **not identified by either
  audit**.
- **The 768 dimension does not select the future model.** Model selection is
  **OPEN — BENCHMARK** (O-2).
- Schema V1 does **not** add `embedding_model_id` to `public.kb` (L3).
- **Locked rule (L18):**
  - New-pipeline embeddings are not assumed comparable with legacy embeddings
    until the legacy model is identified and vector-space compatibility is
    demonstrated.
  - Until then, any search that mixes legacy and new rows is unsupported;
    consumers can separate them by `kbp.chunk_registry` membership.
  - A model with a dimension other than 768 requires a deliberate future Gold
    contract migration.
  - No ANN index is created by the Schema V1 migration.

### 12.2 Tables

| Concern | Table |
|---|---|
| Model/version identity, dimension, metric, config, lifecycle | `kbp.embedding_models` |
| Which model/version produced the embedding for chunk X (the result record) | `kbp.chunk_embeddings` (PK `chunk_id, embedding_model_id`) |
| Pending work, leases, attempts, errors | `kbp.embedding_queue` (transient) |
| Exhausted failures | `kbp.dead_letters` (`item_type = 'chunk_embedding'`) |
| Gold vectors | `public.kb.embedding` |

### 12.3 Rules

- `chunk_id` is independent of the model. A model change never changes
  `chunk_id`, `kb.id` or `metadata`.
- Exactly zero or one model is `active_gold`. Only that model publishes into
  `public.kb.embedding`, and the CHECK requires `vector` and dimension 768
  while Gold is `vector(768)`.
- **Mixed vector spaces (L18):** new and legacy vectors are treated as
  non-comparable until proven otherwise (O-3). Schema V1 records exact
  provenance (`chunk_embeddings`) so that new rows are always distinguishable.
  Legacy rows have no `chunk_embeddings` row.

### 12.4 Controlled re-embedding (future; not created in V1)

1. Register the new model as `candidate`.
2. Enqueue `(chunk_id, new_model)` in resumable batches
   (`embedding_enqueue` jobs). Text comes from `chunk_registry`.
3. Store results in a **future** per-model shadow vector table with PK
   `chunk_id`, and record `chunk_embeddings(status = 'stored_shadow')`.
4. Benchmark, and build the ANN index on the shadow data (separate exercise).
5. Cutover is **OPEN — FUTURE IMPLEMENTATION** (O-18): a Gold contract change, a
   consumer switch, or a batched in-place replacement (only if the dimension
   and vector space are compatible).

Re-running any step is a no-op because of the PKs.

---

## 13. Gold Publication Model

### 13.1 Publication transaction (LOCKED order; per batch, fenced by lease token)

```
-- outside the transaction
claim embedding_queue rows for the active_gold model (FOR UPDATE SKIP LOCKED, set lease)
read chunk_registry.chunk_text / chunk_hash ; compute embeddings
pre-check legacy collisions via legacy_kb_reconciliation(canonical_chunk_hash)       (15.2)

BEGIN
 1. resolve bridge: SELECT dataset_id FROM kbp.source_dataset_map
                    WHERE source_channel = <channel of representative observation>
                    -- missing row => abort; publication is blocked until bootstrap (14.3)
 2. INSERT public.metadata (id=chunk_id, source_id=<bridge dataset_id>, source_url, source_title,
                            source_type, domain, published_at, updated_at=NULL)
         ON CONFLICT (id) DO NOTHING
    if 0 rows inserted -> VERIFY existing metadata row (13.2)
 3. INSERT public.kb (id=chunk_id, chunk=chunk_registry.chunk_text, embedding)
         ON CONFLICT (id) DO NOTHING
    if 0 rows inserted -> VERIFY existing kb row (13.2)
 4. INSERT kbp.chunk_embeddings (chunk_id, model, 'published_gold', attempt_count, embedded_at,
                                 published_at, run_id)  ON CONFLICT (chunk_id, embedding_model_id) DO NOTHING
    DELETE kbp.embedding_queue WHERE chunk_id = ? AND embedding_model_id = ? AND lease_token = :token
         -- 0 rows => lease lost => ROLLBACK
COMMIT
```

- **Conflict targets:** `public.metadata (id)`, `public.kb (id)` (both
  existing primary keys), and `kbp.chunk_embeddings (chunk_id,
  embedding_model_id)`.
- **No `DO UPDATE` is ever used on `public.metadata` or `public.kb`.**

### 13.2 Conflict verification (no destructive overwrite)

When an insert hits an existing row with the same deterministic `chunk_id`,
the row is **compared, never overwritten**:

| Existing row | Compatible if | Otherwise |
|---|---|---|
| `metadata(id = chunk_id)` | `source_id` is one of the two bridge `dataset_id`s in `kbp.source_dataset_map` (it is a pipeline-owned row). Other representative fields may differ; they stay as first written (insert-once, D16). | ROLLBACK the item; `dead_letters(item_type='chunk', stage='STORED', error_class='data_invalid', error_code='gold_metadata_conflict', disposition='quarantined')` |
| `kb(id = chunk_id)` | `sha256(convert_to(kb.chunk,'UTF8')) = chunk_registry.chunk_hash` **and** `kb.embedding IS NOT NULL`. If `kbp.chunk_embeddings` already has a `published_gold` row for this model, the retry is a no-op success. If there is no such row, this transaction cannot have written the existing `kb` row (both commit atomically). The existing row is then **kept unchanged** and flagged for review (`dead_letters`, `error_code='gold_unattributed_row'`). | ROLLBACK the item; dead letter with `error_code='gold_kb_conflict'`, `disposition='quarantined'` |

A conflicting row can only come from a bug or an external writer, because
`chunk_id` is UUIDv5 over the content hash and legacy ids are not in this
namespace. Such rows are surfaced for review. They are never silently
replaced.

### 13.3 Invariants

- `metadata` is inserted before `kb`, because `kb_id_fkey` is **not
  deferrable** (A2).
- The pipeline never deletes `metadata` or `kb` rows (L21), because
  `kb_id_fkey` is `ON DELETE CASCADE`.
- If a legacy or external process deletes a new-pipeline `metadata` row, its
  `kb` row disappears with it. A periodic **Gold drift audit** compares
  `chunk_embeddings(status='published_gold')` with `public.kb` and re-enqueues
  missing chunks through `embedding_enqueue`.
- New-pipeline `kb.chunk` is byte-identical to `chunk_registry.chunk_text`
  (verifiable: `sha256(kb.chunk) = chunk_hash`).
- New-pipeline Gold rows always have a non-NULL embedding: no
  NULL-then-UPDATE.
- A kb row is new-pipeline exactly when `kb.id` ∈ `kbp.chunk_registry`.
- If a lease is lost, the fenced `DELETE` affects 0 rows. The transaction
  aborts and all its inserts roll back.
- Legacy collision handling happens **before** publication (section 15.2).

---

## 14. Metadata Compatibility Strategy

### 14.1 One representative row, complete provenance in `kbp`

- `public.metadata` stays one row per `kb.id`: the compact representative
  record. **No constraint is added to `public.metadata`** (L20).
- Complete multi-source provenance is in `kbp` (section 11).
- The representative is `chunk_registry.representative_observation_id`: the
  observation whose processing first registered the chunk. `metadata` is
  **inserted once and never rewritten** (resolves D16). The choice depends on
  processing order; the complete provenance set does not.

### 14.2 Field mapping for new-pipeline rows

| Column | Value | Notes |
|---|---|---|
| id | `chunk_id` | |
| source_id | `source_dataset_map.dataset_id` for the representative observation's channel | Preserves `metadata_source_id_fkey` |
| source_url | representative `source_observations.source_url` | |
| source_title | representative `source_title`, or NULL | Never invented |
| source_type | `'common_crawl'` or `'github'` (pipeline-controlled vocabulary, **LOCKED**) | Fits `varchar(50)`. Enforced by the pipeline, not a `public` CHECK. |
| domain | `document_registry.subject_domain` of `chunk_registry.first_document_id`, or NULL | **Subject domain** (L9, LOCKED). Fits `varchar(100)`. Hostnames never go here; they are in `source_observations.url_host`. |
| published_at | representative `source_published_at`, or NULL | O-9 |
| updated_at | **NULL** (LOCKED for V1) | Legacy semantics are not in either audit. Not guessed. |

### 14.3 Gold source bridge (RESOLVED BY AUDIT + DESIGN)

**What the bridge rows are.** They are two pipeline-owned
**internal compatibility records** in `public.datasets`. They exist only so
that `public.metadata.source_id` can satisfy `metadata_source_id_fkey` for
new-pipeline Gold rows.

- They are **not external datasets**.
- They do not describe downloadable data.
- They carry no invented source facts.
- Complete provenance lives in `kbp`.

**Parents.** The existing `public.dataset_sources` rows named `common_crawl`
and `github` (A2) are reused.

- No `dataset_sources` row is created.
- No unrelated legacy `datasets` row is reused.
- Pages, repositories and files never get `datasets` rows.

**Deterministic ids** (Identity Extension V1, section 26; frozen in `docs/identity_extension_v1.md`):

| Channel | UUIDv5 name (in `KB_NAMESPACE`) | `datasets.id` |
|---|---|---|
| common_crawl | `dataset-bridge:v1:common_crawl` | `a7d8d2e1-8400-5916-9440-b492e8e692ce` (frozen) |
| github | `dataset-bridge:v1:github` | `b487f559-5f8e-56ba-9746-28658b9d09d1` (frozen) |

**Bootstrap column values.** Only audited `public.datasets` columns are used.

| Column | Common Crawl bridge | GitHub bridge | Rationale |
|---|---|---|---|
| id | bridge UUID (above) | bridge UUID (above) | deterministic, portable |
| source_id | `dataset_sources.id` looked up **by `name = 'common_crawl'`** | looked up **by `name = 'github'`** | UUIDs are not portable across databases; `name` is UNIQUE (A2) |
| dataset_name | `kb_pipeline_common_crawl` | `kb_pipeline_public_github` | immutable; fits `varchar(255)`; unique per `source_id` (`uq_datasets_source_name`) |
| status | `ACCEPTED` | `ACCEPTED` | NOT NULL; a value already used in this column (A2 sample). See the pre-bootstrap check below. |
| metadata (jsonb, NOT NULL) | see below | see below | identifies the row as a bridge |
| description | `Internal KB pipeline compatibility record (Common Crawl channel). Not an external dataset.` | `Internal KB pipeline compatibility record (public GitHub channel). Not an external dataset.` | a human-readable disclaimer |
| created_at | column default `now()` | column default `now()` | not part of identity |
| dataset_url | **NULL** | **NULL** | there is no dataset URL, and a URL could trigger URL-driven legacy tooling; the partial unique index ignores NULL |
| all other columns | **NULL** | **NULL** | no meaningful value; nothing is invented |

The "all other columns" set is: `author`, `license`, `domain`, `language`,
`task_type`, `file_format`, `size_bytes`, `row_count`, `quality_score`,
`revision_id`, `revision_kind`, `reproducibility_status`, `spdx_id`,
`use_class`, `license_source`, `license_confidence`, `capability_flags`,
`metadata_confidence`, `confidence_overall`, `size_precision`, `modality`,
`modality_raw`, `languages_raw`, `selection_state`, `dedup_group_id`,
`rejection_reason`.

`metadata` jsonb, for example for Common Crawl:

```
{"kind": "kb_pipeline_gold_source_bridge",
 "bridge_version": "v1",
 "source_channel": "common_crawl",
 "bridge_identity_name": "dataset-bridge:v1:common_crawl",
 "external_dataset": false,
 "purpose": "Compatibility record so public.metadata.source_id satisfies metadata_source_id_fkey for KB pipeline Gold rows. Provenance is in schema kbp."}
```

**Idempotent bootstrap procedure** (one transaction; an explicitly approved
bootstrap operation, section 22):

1. `SELECT id, source_type FROM public.dataset_sources WHERE name = 'common_crawl'`
   (and likewise for `'github'`).
   - If a row is missing: **abort**. Never create one.
   - If `source_type` differs from A2 (`document` for common_crawl,
     `dataset_repository` for github): **abort** for review, because the
     environment is not the audited one.
2. `INSERT INTO public.datasets (…) VALUES (…) ON CONFLICT (id) DO NOTHING`.
   - A clash on `uq_datasets_source_name` with a **different** id raises an
     error, and the transaction aborts. An existing foreign row is never
     adopted.
3. Re-read the row by `id` and verify `source_id` and `dataset_name`.
   Mismatch: abort.
4. `INSERT INTO kbp.source_dataset_map (…) ON CONFLICT (source_channel) DO NOTHING`,
   then verify `dataset_id` and `bridge_identity_name`.
5. Commit.

Re-running changes nothing. **Gold publication is blocked** (13.1, step 1)
until both map rows exist.

**Pre-bootstrap check (non-structural).** Neither audit shows how legacy
application code consumes `datasets.status`. Before bootstrap, confirm that
no legacy job would act on an `ACCEPTED` dataset whose `dataset_url` is NULL
and whose `metadata.kind` is `kb_pipeline_gold_source_bridge` (O-1).

- If a legacy consumer would act on it, choose a different `status` value.
  This changes a data value only, not the schema.

**Downgrade.**

- Once any `public.metadata` row references a bridge row,
  `metadata_source_id_fkey` (NO ACTION) prevents deleting it. That is correct:
  Gold rows depend on it.
- A downgrade therefore removes the bridge rows only if no `metadata` row
  references them. Otherwise it leaves them in place and reports that.

### 14.4 Semantic incompatibility with legacy rows (documented, not fixed)

| Column | Legacy rows (audit) | New-pipeline rows |
|---|---|---|
| source_type | `document`, `dataset_repository`. These mirror `dataset_sources.source_type`. | `common_crawl`, `github` (pipeline-controlled) |
| domain | web hostnames (`docs.python.org`, `github.com`) | subject domains (e.g. `science`, `law`, `software`) |
| updated_at | unknown semantics | NULL |
| source_id | 2 legacy `datasets` rows | the 2 bridge `datasets` rows |

- Legacy rows remain untouched.
- No constraint is added that could reject them (L20).
- Consumers that filter or group on `domain` or `source_type` will see two
  vocabularies. New-pipeline rows are identifiable through `kbp.chunk_registry`
  membership. Communicating this to consumers is a release prerequisite.

---

## 15. Legacy Reconciliation Strategy (future procedure; not performed now)

Run once, as a `legacy_reconciliation` job under a `reconcile_legacy` run,
**before large production ingestion**. It is read-only on `public`.

1. Read every `public.kb(id, chunk)` with `public.metadata.source_id`, in
   batches ordered by `id`, with the checkpoint `{"last_id": …}`.
2. Compute `raw_chunk_sha256 = SHA256(chunk)` as stored.
3. Compute `canonical = canonicalize_text(chunk)` (Identity V1).
   - If the result is empty: `reconciliation_status =
     'empty_after_canonicalization'`.
   - If it contains U+0000: `reconciliation_status = 'contains_nul'`. This
     cannot occur in stored text; the status is listed for completeness.
   - Any processing error: `reconciliation_status = 'error'`, with
     `error_message` set.
4. Compute `canonical_chunk_hash`, `canonical_chunk_id` (Identity V1
   `chunk:v1:`) and `canonical_differs`.
5. Detect collisions:
   - legacy ↔ legacy on the same `canonical_chunk_hash`:
     `collides_with_legacy` with `collision_legacy_kb_id`. A1 shows 0 raw-text
     duplicates, but canonical duplicates are still possible.
   - legacy ↔ new: `canonical_chunk_hash` present in `kbp.chunk_registry`,
     giving `collides_with_new_chunk` with `collision_chunk_id`.
   - otherwise: `unique`.
   - Every collision sets `review_state = 'needs_review'`, with `decision`
     left at `'undecided'`.
6. UPSERT `kbp.legacy_kb_reconciliation` (`ON CONFLICT (legacy_kb_id) DO
   UPDATE` of the computed columns only if `raw_chunk_sha256` or
   `reconciliation_version` changed).
   - `decision`, `review_state`, `reviewed_*` and `decided_at` are never reset
     by recomputation, unless the raw hash changed; in that case the row
     returns to `needs_review`.
7. Never modify, delete or re-key legacy rows. Never insert legacy text into
   `chunk_registry`, because legacy rows have not passed the new policies.

### 15.1 Guarantees

- Legacy ids are preserved.
- The mapping `legacy_kb_id → canonical_chunk_hash → canonical_chunk_id` is
  explicit and queryable.
- The procedure is re-runnable: it UPSERTs by `legacy_kb_id`.
- A legacy row edited later is detected through `raw_chunk_sha256`.

### 15.2 Publication-time collision rule (LOCKED behaviour; policy OPEN)

When a chunk about to be published has a `chunk_hash` equal to a reconciled
legacy `canonical_chunk_hash` whose `decision = 'undecided'`, the chunk is
**not** published. Two independently published Gold representations of the
same canonical content are never created silently.

- The chunk stays unpublished: it is dead-lettered with `stage = 'STORED'`,
  `error_class = 'data_invalid'`, `error_code = 'legacy_collision'`,
  `disposition = 'quarantined'`.
- It is requeued once a decision is recorded.

The merge or alias policy itself is **OPEN — FUTURE IMPLEMENTATION** (O-4).
The schema can already record each option through `decision`:

- `keep_legacy_only` (Gold continues to serve the legacy row);
- `alias_new_to_legacy` (the new chunk maps to the legacy id and gets no new
  Gold row);
- `publish_new_mark_legacy_superseded` (tracked in `kbp` only; legacy is not
  modified);
- `publish_both` (only by explicit decision);
- `exclude`.

**Precondition:** reconciliation must have run (all 383 rows `computed` or
beyond) **before large production ingestion**.

---

## 16. Job Queue Persistence Model

### 16.1 Principles

- **Bounded work units:** a CC index partition, a GitHub repository discovery
  page set, a repository snapshot, an embedding-enqueue range, or a policy
  re-evaluation range.
- There are never jobs per document or per chunk.
- Item-level progress is made durable by the idempotent registries and by the
  job `checkpoint`, committed in the same transaction as the items.
- Embedding work is item-level, but only in the **transient, bounded**
  `embedding_queue`. Rows are deleted on success.
- Completed `pipeline_jobs` rows are retained. They are small in number and
  give UNIQUE-key idempotency. A deliberate reprocess resets `status` and adds
  attempts.

### 16.2 `source_identifier` formats (deterministic)

| job_type | source_identifier |
|---|---|
| cc_index_partition | `<crawl_id>/<index partition path>#filter=<filter_version>[#range=<a>-<b>]` |
| gh_repository_discovery | `<discovery query id>#page=<n>` |
| gh_repository_snapshot | `<repository_id>@<commit_sha>` |
| embedding_enqueue | `<embedding_model_id>#chunk_id=<from>..<to>` |
| policy_reevaluation | `<policy_version>#document_id=<from>..<to>` |
| legacy_reconciliation | `<reconciliation_version>` |

### 16.3 Stages (`pipeline_jobs.stage`, `dead_letters.stage`)

The allowed stage values are:

`DISCOVERED`, `ACQUISITION_PENDING`, `ACQUIRED`, `PARSED`, `NORMALIZED`,
`LANGUAGE_VALIDATED`, `QUALITY_VALIDATED`, `EXACT_DEDUP_VALIDATED`,
`NEAR_DEDUP_VALIDATED`, `FACTUALITY_VALIDATED`, `SOURCE_CLASSIFIED`,
`DOMAIN_CLASSIFIED`, `POLICY_VALIDATED`, `SILVER`, `CHUNKED`,
`CHUNK_DEDUPLICATED`, `EMBEDDED`, `STORED`.

The brief's failure states map to columns, not stages:

| Brief state | Maps to |
|---|---|
| RETRY_PENDING | `status = 'retry_pending'` |
| FAILED | `processing_attempts.outcome = 'failed'`. A single failed attempt; not terminal. |
| QUARANTINED | `status = 'quarantined'` (unit) or `dead_letters.disposition = 'quarantined'` (item) |
| DEAD_LETTER | `status = 'dead_letter'` (unit) or `dead_letters` (item) |

Intermediate item stages run in memory inside a unit. The durable item outcome
is `observation_extractions.outcome` or `document_registry.status`.

### 16.4 Atomic claiming (required capability; SQL not written)

- A single statement selects eligible rows
  (`status IN ('pending','retry_pending') AND next_attempt_at <= now()`),
  ordered by `(priority, next_attempt_at)`, `FOR UPDATE SKIP LOCKED LIMIT n`.
- The same statement sets `status = 'running'`, `worker_id`, a new
  `lease_token`, `locked_at`, `lease_expires_at`, `attempt_count + 1` and
  `started_at` (if NULL).
- The worker then inserts the matching `processing_attempts` row.
- Every later write by the worker (checkpoint, completion, lease renewal) is
  fenced with `WHERE job_id = ? AND lease_token = ?`.

---

## 17. Retry / Attempt / Dead-Letter Model

### 17.1 Retry scheduling (computed by the application; stored in `next_attempt_at`)

- Base backoff: `delay = min(cap, base × 2^(attempt_count − 1))`.
- Jitter: "full jitter", i.e. `next_attempt_at = now() + random_uniform(0, delay)`.
  The per-job-type values of `base`, `cap` and `max_attempts` are
  configuration.
- **Retry-After:** `next_attempt_at = max(backoff_time, now() + Retry-After)`.
  The value is stored in `processing_attempts.retry_after_seconds`.
- **GitHub primary rate limit** (`remaining = 0`): `next_attempt_at =
  x-ratelimit-reset` (plus small jitter). Update `kbp.source_rate_limits`.
  Increment `deferral_count` and record the attempt as `deferred_rate_limit`.
  **`attempt_count` is not consumed.**
- **GitHub secondary rate limit / 429:** honour `Retry-After`, and
  `source_rate_limits.retry_after_until`.
- Claimers skip work for a `(source_channel, limit_scope)` while
  `remaining = 0 AND reset_at > now()` or `retry_after_until > now()`.

### 17.2 Error classes (`last_error_class`, `error_class` CHECK list)

| Class | Examples | Action |
|---|---|---|
| `transient` | network reset, 5xx, timeout | retry with backoff |
| `rate_limited` | GitHub 403/429 with rate headers, CC 503 slow-down | defer (no attempt consumed) |
| `resource_exhausted` | out of memory, disk full, GPU OOM | retry with backoff; operator alert |
| `permanent_source` | 404/410, repository gone or private | item: dead letter; unit: dead letter |
| `data_invalid` | undecodable content, U+0000, legacy collision | item: dead letter or quarantine; the unit continues |
| `policy_quarantine` | PII or secret suspicion | item: `disposition = 'quarantined'` |
| `internal_bug` | assertion or invariant violation (e.g. nondeterministic extraction) | retry up to `max_attempts`, then dead letter |
| `lease_expired` | worker crash | reclaim; attempt closed as `lease_expired` |

### 17.3 Crash recovery

- A sweeper finds `status = 'running' AND lease_expires_at < now()` in
  `pipeline_jobs` and `embedding_queue`.
- It closes the open `processing_attempts` row (`outcome = 'lease_expired'`)
  and sets `status = 'retry_pending'`.
- If `attempt_count` is exhausted, it sets `status = 'dead_letter'` and adds a
  `dead_letters` row.
- Resumption starts from `stage` and `checkpoint`. Replaying items after the
  checkpoint is harmless because of the UPSERTs.

### 17.4 Transitions

```
pending/retry_pending --claim--> running --success--> succeeded
running --retryable failure, attempts left--> retry_pending
running --rate limited--> retry_pending (deferral_count+1, attempt not consumed)
running --attempts exhausted | permanent--> dead_letter (+ dead_letters row, disposition='dead_letter')
running --policy quarantine--> quarantined (+ dead_letters row, disposition='quarantined')
running --lease expired--> retry_pending | dead_letter
dead_letter/quarantined --operator resolution--> pending (dead_letters.resolution='requeued') | stays
```

Item-level failures inside a unit (one capture or file) go to `dead_letters`
without failing the unit. The UNIQUE `(item_type, item_key, stage)`
deduplicates repeats by incrementing `occurrence_count` and updating
`last_seen_at`.

---

## 18. Pipeline Run Model

- `kbp.pipeline_runs` is dedicated to this pipeline. `workflow_runs` is not
  used (it requires `collection_requests`).
- A run groups jobs, observations, embeddings and reconciliation.
- A run records the code version, a configuration hash, and the identity
  versions (`v1`, plus the extension version) used.
- Runs are operational records with random UUIDs. Correctness never depends on
  run identity.

---

## 19. Idempotency Matrix

| Entity | Identity key | Unique protection | Behaviour on duplicate |
|---|---|---|---|
| Crawl | crawl_id | PK | no-op |
| WARC file | warc_filename | UNIQUE | no-op; existing id reused |
| Source observation | observation_id (section 26) | PK | no-op |
| CC capture | observation_id; (warc_file_id, offset) | PK + UNIQUE | no-op. A natural-key conflict under a different id raises an error (derivation bug). |
| Repository | repository_id | PK | update mutable fields only when changed |
| Commit snapshot | (repository_id, commit_sha) | PK | no-op |
| GitHub file observation | observation_id; (repository_id, commit_sha, file_path) | PK + UNIQUE | as CC capture |
| Observation extraction | (observation_id, extraction_version) | PK | no-op. A differing `document_id` is a nondeterminism bug → `dead_letters` (`internal_bug`). |
| Canonical document | document_id; content_hash | PK + UNIQUE | no-op; observation linked; no re-chunking |
| Chunking version | name | UNIQUE | no-op |
| Canonical chunk | chunk_id; chunk_hash | PK + UNIQUE | no-op; first writer's representative kept; not re-enqueued |
| Chunk occurrence | (document_id, chunking_version_id, ordinal) | PK | no-op |
| Embedding model | (model_name, model_revision, embedding_version) | UNIQUE | no-op |
| Embedding work | (chunk_id, embedding_model_id) | PK | no-op |
| Embedding result | (chunk_id, embedding_model_id) | PK | no-op |
| Gold metadata | id = chunk_id | `metadata_pkey` | no-op (insert-once) |
| Gold kb | id = chunk_id | `kb_pkey` | no-op |
| Job | (job_type, source_identifier) | UNIQUE | no-op |
| Attempt | (job_id, attempt_no) | UNIQUE | no-op |
| Dead letter | (item_type, item_key, stage) | UNIQUE | increment counters |
| Rate-limit state | (source_channel, limit_scope, credential_fingerprint) | PK | overwrite with latest |
| Dataset bridge map | source_channel | PK (+ UNIQUE dataset_id, bridge_identity_name) | no-op, then verify |
| Bridge `public.datasets` row | id = UUIDv5(`dataset-bridge:v1:<channel>`) | `datasets_pkey` (+ `uq_datasets_source_name`) | no-op, then verify. A name clash under another id raises. |
| Dedup method | (algorithm, algorithm_version) | UNIQUE | no-op |
| Dedup decision | (document_id, dedup_method_id) | PK | no-op (a new method version adds rows) |
| Document relationship | (document_id, related_document_id, relationship_type, dedup_method_id) | PK | no-op |
| Legacy reconciliation | legacy_kb_id | PK | recompute only if raw hash or version changed; never resets decisions unless the raw hash changed |
| Run | run_id | PK | n/a (created once) |

**Re-running the same source** gives 0 new canonical documents, 0 new chunks, 0
new occurrences, 0 new Gold rows and 0 new observations. New rows appear only
in `processing_attempts`, and in `observation_extractions` if
`extraction_version` changed.

---

## 20. UPSERT Conflict Targets

| Statement | Conflict target | Action |
|---|---|---|
| INSERT cc_crawls | `(crawl_id)` | DO NOTHING |
| INSERT cc_warc_files | `(warc_filename)` | DO NOTHING, then SELECT id (or a no-op `DO UPDATE … RETURNING`) |
| INSERT source_observations | `(observation_id)` | DO NOTHING |
| INSERT cc_captures | `(observation_id)` | DO NOTHING. UNIQUE `(warc_file_id, warc_record_offset)` is left to raise. |
| INSERT github_repositories | `(repository_id)` | DO UPDATE SET mutable columns WHERE any `IS DISTINCT FROM` |
| INSERT github_commits | `(repository_id, commit_sha)` | DO NOTHING |
| INSERT github_file_observations | `(observation_id)` | DO NOTHING. The natural UNIQUE is left to raise. |
| INSERT observation_extractions | `(observation_id, extraction_version)` | DO NOTHING, then compare `document_id` |
| INSERT document_registry | `(document_id)` | DO NOTHING RETURNING |
| INSERT chunking_versions | `(name)` | DO NOTHING |
| INSERT chunk_registry | `(chunk_id)` | DO NOTHING RETURNING |
| INSERT chunk_occurrences | `(document_id, chunking_version_id, ordinal)` | DO NOTHING |
| INSERT embedding_models | `(model_name, model_revision, embedding_version)` | DO NOTHING |
| INSERT embedding_queue | `(chunk_id, embedding_model_id)` | DO NOTHING |
| INSERT chunk_embeddings | `(chunk_id, embedding_model_id)` | DO NOTHING |
| INSERT public.metadata | `(id)` | DO NOTHING, then verify the existing row (13.2). Never `DO UPDATE`. |
| INSERT public.kb | `(id)` | DO NOTHING, then verify the existing row (13.2). Never `DO UPDATE`. |
| INSERT public.datasets (bridge bootstrap only) | `(id)` | DO NOTHING, then verify (14.3) |
| INSERT dedup_methods | `(algorithm, algorithm_version)` | DO NOTHING |
| INSERT document_dedup_decisions | `(document_id, dedup_method_id)` | DO NOTHING. Operator overrides are explicit updates of `decision_status`. |
| INSERT document_relationships | `(document_id, related_document_id, relationship_type, dedup_method_id)` | DO NOTHING |
| INSERT pipeline_jobs | `(job_type, source_identifier)` | DO NOTHING |
| INSERT processing_attempts | `(job_id, attempt_no)` | DO NOTHING |
| INSERT dead_letters | `(item_type, item_key, stage)` | DO UPDATE occurrence_count+1, last_seen_at, error_* |
| INSERT source_rate_limits | `(source_channel, limit_scope, credential_fingerprint)` | DO UPDATE latest values |
| INSERT source_dataset_map | `(source_channel)` | DO NOTHING |
| INSERT legacy_kb_reconciliation | `(legacy_kb_id)` | DO UPDATE WHERE raw hash or version differs |

**Why `ON CONFLICT (pk) DO NOTHING` does not hide a second unique violation:**

- `document_registry` and `chunk_registry` use the PK as the conflict target.
  A row whose hash exists under a *different* id (a derivation bug) still
  violates UNIQUE(`content_hash`) or UNIQUE(`chunk_hash`), and **raises**.
- The same holds for the natural keys of `cc_captures` and
  `github_file_observations`.

This is intentional: bugs fail loudly instead of silently duplicating.

---

## 21. Index Classification

"PK" and "UNIQUE" imply their index. Sizes marked "est." are
order-of-magnitude estimates for planning.

### 21.1 REQUIRED FOR CORRECTNESS (identity and idempotency guards)

| Index | Protects |
|---|---|
| All PKs in 6.1 | entity identity; UPSERT targets |
| document_registry UNIQUE(content_hash) | global exact document uniqueness |
| chunk_registry UNIQUE(chunk_hash) | global exact chunk uniqueness (pilot physical form, section 25) |
| cc_captures UNIQUE(warc_file_id, warc_record_offset) | capture uniqueness independent of id derivation |
| github_file_observations UNIQUE(repository_id, commit_sha, file_path) | file observation uniqueness |
| cc_warc_files UNIQUE(warc_filename) | WARC dimension idempotency |
| chunking_versions UNIQUE(name); embedding_models UNIQUE(model, revision, version) | version registries |
| embedding_models partial UNIQUE(status) WHERE status='active_gold' | single Gold-publishing model |
| pipeline_jobs UNIQUE(job_type, source_identifier) | idempotent enqueue |
| processing_attempts UNIQUE(job_id, attempt_no) | attempt idempotency |
| dead_letters UNIQUE(item_type, item_key, stage) | idempotent dead-lettering |
| source_dataset_map UNIQUE(dataset_id), UNIQUE(bridge_identity_name) | one channel per bridge dataset |
| dedup_methods UNIQUE(algorithm, algorithm_version); PKs of document_dedup_decisions and document_relationships | versioned, idempotent dedup decisions |

### 21.2 REQUIRED FOR INITIAL OPERATIONS

| Index | Query | Cost |
|---|---|---|
| pipeline_jobs `(job_type, priority, next_attempt_at) WHERE status IN ('pending','retry_pending')` | SKIP LOCKED claim | small table |
| pipeline_jobs `(lease_expires_at) WHERE status = 'running'` | crash-recovery sweep | tiny |
| embedding_queue `(embedding_model_id, priority, next_attempt_at) WHERE status IN ('pending','retry_pending')` | batch claim | bounded by backlog |
| embedding_queue `(lease_expires_at) WHERE status = 'running'` | sweep | tiny |
| observation_extractions `(document_id) WHERE document_id IS NOT NULL` | document → sources provenance | ≈ observations × 16 B |
| dead_letters `(last_seen_at) WHERE resolved_at IS NULL` | operator triage | tiny |
| legacy_kb_reconciliation `(canonical_chunk_hash)` | publication-time legacy collision check (15.2) | 383 rows |

Deliberately **not** indexed:

- FK child columns on large tables (`chunk_occurrences.chunk_id`,
  `chunk_registry.first_document_id`,
  `chunk_registry.representative_observation_id`,
  `document_registry.first_observation_id`,
  `chunk_embeddings.embedding_model_id`). FK enforcement on child inserts uses
  the *parent's* index, and parents are never deleted.
- Status columns on large tables.

### 21.3 OPTIONAL AFTER BENCHMARKING

| Index / object | Serves | Why deferred |
|---|---|---|
| chunk_occurrences `(chunk_id)` | chunk → documents online | est. hundreds of GB at 7B; one more random B-tree insert per occurrence |
| source_observations expression index on `sha256(convert_to(source_url,'UTF8'))` | URL → observations | wide keys; ad-hoc use only |
| github_file_observations `(repository_id, blob_sha)` | skip unchanged blobs across paths | incremental diff already uses the UNIQUE prefix `(repository_id, commit_sha)` |
| github_repositories `(lower(owner_login), lower(repo_name))` | lookup by name | small; add when needed |
| document_registry partial `WHERE status = 'quarantined'` | review tooling | add with tooling |
| BRIN on `source_observations.acquired_at`, `chunk_registry.created_at` | time-range audits | cheap, but only if used |
| UNIQUE `(observation_id, source_channel)` + composite subtype FKs | database-enforced subtype/channel consistency | extra index on a very large table; audit query instead |
| document_dedup_decisions `(canonical_document_id) WHERE canonical_document_id IS NOT NULL` | "which documents duplicate D" | add when cluster-review tooling exists |
| document_relationships `(related_document_id)` | reverse relationship lookup | add when needed |

### 21.4 VERY-LARGE-SCALE PHYSICAL DESIGN (benchmark before adopting)

- Partitioning of `chunk_registry`, `chunk_occurrences`, `chunk_embeddings`,
  `source_observations`, `observation_extractions` and `document_registry`.
- The global uniqueness strategy under partitioning (section 25.2).
- Text compression (lz4 availability is O-12), external durable text storage,
  and archival of cold Silver text.
- Sharding by `chunk_id`.
- **ANN indexing of vectors: not selected (L14).**

---

## 22. Alembic / Migration Ownership Strategy

- **Ownership.** `public.alembic_version` (`i4j5k6l7m8n9`) belongs to the legacy
  migration history, which this repository does not contain. This project
  **never reads, stamps, upgrades or downgrades it.**
- **Isolated lineage.** This project's Alembic environment uses
  `version_table = 'alembic_version'` with
  `version_table_schema = 'kbp'`, which gives **`kbp.alembic_version`**. It has
  its own base revision, with no `down_revision` link to the legacy head.
- **Bootstrap of the version table.** Alembic creates its version table before
  running revisions, so the environment must ensure the `kbp` schema exists
  first. That is an idempotent `CREATE SCHEMA IF NOT EXISTS kbp` executed by
  the migration environment.
  - Owner: `postgres`, the role that owns the Gold tables per A2, unless a
    dedicated role is chosen (O-10).
- **Autogenerate scoping (critical, LOCKED).**
  - Without scoping, autogenerate would see every `public` table as "not in
    the model" and propose dropping it.
  - The environment must use `include_schemas=True` with an
    `include_name`/`include_object` filter that admits **only schema `kbp`**.
  - A CI or pre-commit guard rejects any revision containing `drop_table`,
    `drop_column`, `drop_constraint`, `drop_index` or `alter_column` that
    targets a non-`kbp` schema.
  - Every generated migration is reviewed by hand.
  - **Autogenerate must never emit DROP operations for `public` objects.**
- **Scope of this lineage.**
  - It creates and alters only `kbp` objects, plus the single FK
    `kbp.source_dataset_map → public.datasets`.
  - It never issues DDL against `public` tables.
- **Explicitly approved bootstrap operations** (data, not DDL, on `public`).
  Only these are permitted:
  1. the idempotent Gold bridge procedure of section 14.3: read
     `public.dataset_sources` by name, insert or verify two `public.datasets`
     rows, insert `kbp.source_dataset_map`;
  2. seeding `kbp` reference rows (`chunking_versions`, `embedding_models`,
     `dedup_methods`) once their configurations are decided.

  These may run as dedicated, reviewed data revisions in the `kbp` lineage or
  as a standalone bootstrap command. Either way, they follow the same
  idempotent procedure.
- This document creates no Alembic configuration.

---

## 23. Migration Safety Strategy

1. **Additive only.**
   - Schema V1 creates `kbp` objects.
   - There are no `ALTER`, `DROP`, `RENAME` or `TRUNCATE` statements on
     `public` objects.
   - No legacy row is updated or deleted.
2. **Preserve existing rows.**
   - Backfills (such as legacy reconciliation) are resumable pipeline jobs, not
     migrations.
   - They are read-only on `public`.
3. **Locks.**
   - Creating new tables in `kbp` does not lock `public` tables.
   - The one cross-schema FK (`→ public.datasets`) briefly takes a
     `SHARE ROW EXCLUSIVE` lock on `datasets` (~378 rows). Create it
     `NOT VALID` and `VALIDATE` it separately.
   - Every migration sets `lock_timeout` (e.g. `5s`) and a bounded
     `statement_timeout`. On timeout, retry; never queue behind long
     transactions.
4. **Indexes created after data exists** use `CREATE INDEX CONCURRENTLY` in an
   autocommit block. Afterwards, check for and drop any `INVALID` index.
5. **Constraints added to populated `kbp` tables** use `NOT VALID` followed by
   `VALIDATE CONSTRAINT`.
6. **Rollback.**
   - Each downgrade drops only the objects its revision created.
   - The downgrade of the base revision drops `kbp` objects **only**, never
     `public` data.
   - Data in `kbp` is lost on downgrade, so take a `pg_dump --schema=kbp`
     before any production downgrade.
7. **Pre-flight.**
   - Full backup.
   - Confirm the role the pipeline connects as (O-10). A2 lists only
     `postgres` with privileges on the Gold tables.
   - Dry-run on a restored copy of `data_collection_pipeline`.
   - The Identity V1 tests must pass in CI.
   - Before the **bridge bootstrap and source ingestion**, the frozen Identity
     Extension V1 vectors (section 26), including the bridge ids, must also
     pass. They are not needed to create empty `kbp` tables.
8. **Legacy data.** Never re-keyed, re-normalized or re-validated in place.
   Distinguished from new rows by `kbp.chunk_registry` membership.

---

## 24. One-PC Considerations

- **Only PostgreSQL:**
  - queues via `FOR UPDATE SKIP LOCKED`;
  - no Kafka, RabbitMQ, Redis, Elasticsearch or Kubernetes;
  - Silver text durable inside PostgreSQL for V1.
- **Bounded concurrency.** A small, fixed number of workers per job type, and a
  connection count well below `max_connections`. The current setting is not in
  either audit; worker counts are O-20 (**OPEN — BENCHMARK**).
- **Backpressure.** Chunk producers pause while the `embedding_queue` backlog
  is above a configured bound. Claim batch sizes are bounded.
- **Transactions.** Keep them short. Do network fetches and model inference
  outside the writing transaction; commit per batch together with the
  checkpoint.
- **Queue-table maintenance.** `pipeline_jobs` and `embedding_queue` churn.
  Tune per-table autovacuum thresholds after benchmarking.
- **Disk.** Chunk text exists twice by design (`chunk_registry` and
  `public.kb`). Document text exists once. Watch WAL volume during bulk
  ingestion, and size disks from measured bytes per chunk.
- **Backups.** `pg_dump --schema=kbp` for pipeline state; the legacy `public`
  backup follows the existing practice. The Gold tables are recoverable from
  `kbp` plus the embeddings.
- **Pilot sizing.** Measure average chunk bytes, document bytes, index bytes
  per row and rows per hour on the pilot before projecting.

---

## 25. 7B Physical-Scale Considerations

### 25.1 Order-of-magnitude storage (estimates; not measured)

| Component | Per row | At 7×10⁹ chunks |
|---|---|---|
| Gold vectors, `vector(768)` float32 | 768×4 + 8 ≈ 3.1 kB | ≈ 21 TB |
| Chunk text | measured average T bytes | stored **twice** (registry + Gold): ≈ 2 × 7×10⁹ × T, before TOAST compression |
| `chunk_registry` PK + UNIQUE(chunk_hash) | ≈ 30 B + ≈ 45 B | ≈ 0.5 TB of index |
| `chunk_occurrences` (≥ 7×10⁹ rows) | ≈ 70 B heap + ≈ 40 B PK | ≈ 0.8 TB+ |
| `chunk_embeddings` | ≈ 60 B heap + ≈ 30 B PK | ≈ 0.6 TB |

**Conclusion:** the eventual 7B target cannot fit a typical single PC. Schema
V1 is correct at pilot scale and avoids choices that block later physical
redesign.

### 25.2 Logical uniqueness vs physical enforcement

- The **logical requirement** is fixed: global uniqueness of `document_id`,
  `content_hash`, `chunk_id` and `chunk_hash`, together with idempotent
  UPSERTs.
- The **pilot physical form** is monolithic PK + UNIQUE B-trees.
- This is **not** claimed to be the final 7B design. Constraints to weigh:
  - PostgreSQL requires a UNIQUE constraint on a partitioned table to include
    the partition key.
    - Partitioning by `hash(chunk_id)` keeps PK(`chunk_id`) global, but
      UNIQUE(`chunk_hash`) cannot stay global.
    - Partitioning by `hash(chunk_hash)` keeps UNIQUE(`chunk_hash`) global, but
      the PK must include `chunk_hash`, which affects FKs to `chunk_id`.
  - Candidate designs to benchmark:
    - partitioning by `chunk_id` with deterministic-derivation audits;
    - partition-local uniqueness combined with a routing function (the id and
      hash map to the same partition when the partition key is derived from
      the hash);
    - application-level sharding by hash prefix.
- A decision is required before `chunk_registry` exceeds a benchmark-derived
  threshold (O-13). **The idempotency requirement is not weakened in any
  option.**

### 25.3 Other scale levers (future, benchmark-gated)

- Compression and external durable storage for Silver document text and
  registry chunk text (without losing authoritative representation).
- Archival of cold data.
- Partitioned attempt history.
- ANN index selection.
- Separate Gold serving storage.

---

## 26. Identity Extension V1 — Source Observation and Bootstrap Identities (LOCKED)

Status: **LOCKED.** The authoritative specification is
`docs/identity_extension_v1.md`. It is implemented in `app/utils/ids.py` and
frozen by `tests/unit/test_identity_extension.py`. This section is only a
summary; if the two ever differ, the specification document wins.

| Identity | UUIDv5 name (in `KB_NAMESPACE`) | Used for |
|---|---|---|
| Common Crawl observation | `observation:v1:common_crawl:` + canonical JSON `{"crawl_id","warc_filename","warc_offset","warc_length"}` | `kbp.source_observations.observation_id` / `kbp.cc_captures` |
| GitHub observation | `observation:v1:github:` + canonical JSON `{"repository_id","commit_sha","path","blob_sha"}` | `kbp.source_observations.observation_id` / `kbp.github_file_observations` |
| Gold bridge dataset | `dataset-bridge:v1:` + `common_crawl` or `github` | `public.datasets.id` of the two bridge rows, `kbp.source_dataset_map.dataset_id` |

- **Canonical JSON:** a keyed object in a fixed key order, serialized as
  `json.dumps(obj, ensure_ascii=True, separators=(",", ":"), allow_nan=False)`.
  The result is pure ASCII.
- **GitHub repository identity:** the numeric **repository database id**
  (bigint). It is never the node id or owner/name.
- **Commit and blob SHAs:** 40 or 64 lowercase hex characters, of equal
  length. `blob_sha` is mandatory.
- **Paths:** verbatim git tree paths. Leading or trailing `/`, empty, `.` and
  `..` segments, U+0000 and surrogates are rejected. Nothing is normalized.
- **Frozen bridge ids:**
  - `common_crawl` → `a7d8d2e1-8400-5916-9440-b492e8e692ce`
  - `github` → `b487f559-5f8e-56ba-9746-28658b9d09d1`
- **Column mapping:** the identity field names `warc_offset`/`warc_length` and
  `path` correspond to the columns `cc_captures.warc_record_offset` /
  `warc_record_length` and `github_file_observations.file_path`.
- Any change requires Identity Extension V2.

---

## 27. Decision Register

### 27.1 Draft decisions D1–D21

| # | Topic | Classification | Resolution |
|---|---|---|---|
| D1 | Schema for new objects | **LOCKED** | `kbp` (L2) |
| D2 | Alembic lineage | **LOCKED** | isolated `kbp.alembic_version`; legacy head `i4j5k6l7m8n9` untouched (section 22) |
| D3 | Observation identity | **RESOLVED BY DESIGN** | Identity Extension V1 (section 26). Frozen and LOCKED (`docs/identity_extension_v1.md`). |
| D4 | `datasets` bridge granularity and values | **RESOLVED BY AUDIT + DESIGN** | two deterministic bridge rows under the existing `common_crawl`/`github` sources (14.3) |
| D5 | `kb.embedding_model_id` | **LOCKED** | not added (L3); `kbp.chunk_embeddings` instead |
| D6 | Reuse `processing_jobs`/`workflow_runs` | **RESOLVED BY AUDIT** | both unusable (required FKs to `documents`/`collection_requests`); `orchestration_jobs` is a live legacy queue; new `kbp.pipeline_jobs`/`kbp.pipeline_runs` |
| D7 | Global UNIQUE vs partitioning | **RESOLVED BY DESIGN** (pilot) | PK + UNIQUE. The 7B physical form is O-13. |
| D8 | Document text storage | **RESOLVED BY AUDIT** | `storage_objects` requires `documents.id`; text durable in `document_registry.canonical_text` |
| D9 | Rate-limit state | **RESOLVED BY AUDIT** | `source_health` is legacy-owned with no quota fields; `kbp.source_rate_limits` |
| D10 | `published_at` | **RESOLVED BY DESIGN** | NULL unless explicit. Extraction rules: O-9. |
| D11 | GitHub license policy | **OPEN — REQUIRES USER DECISION** | O-7 |
| D12 | GitHub snapshot policy | **RESOLVED BY DESIGN** (default); confirmation O-8 | default-branch HEAD, incremental |
| D13 | Register early rejections | **RESOLVED BY DESIGN** | not registered; `observation_extractions.outcome` |
| D14 | Server encoding | **RESOLVED BY AUDIT** | UTF8 |
| D15 | Legacy metadata semantics | **RESOLVED BY AUDIT + LOCKED** | legacy `source_type`/`domain` vocabularies documented; new semantics locked (14.2, 14.4) |
| D16 | Representative selection | **LOCKED** | insert-once |
| D17 | Legacy dedup | **RESOLVED BY DESIGN** | procedure and state (section 15). Merge/alias policy: O-4. |
| D18 | Re-embedding cutover | **OPEN — FUTURE IMPLEMENTATION** | O-18 |
| D19 | U+0000 | **LOCKED** | L10 |
| D20 | kb/metadata physical facts, FK actions, triggers | **RESOLVED BY AUDIT** | A1 + A2 (sections 1.2, 1.6) |
| D21 | Multi-node scale | **OPEN — BENCHMARK** | O-13 |

### 27.2 Open items O-1 to O-20

| # | Item | Classification | Blocks |
|---|---|---|---|
| O-1 | Gold bridge rows | **RESOLVED BY AUDIT + DESIGN** (14.3), except the bridge `status` value. The residual `ACCEPTED` safety check was reviewed against local code: **UNRESOLVED**. No local code touches `datasets.status`, and the legacy application's code is not in this workspace (`docs/database_privileges_v1.md` §11). The `status` value stays open until the legacy consumer behaviour is confirmed. | bridge bootstrap execution only |
| O-2 | Final embedding model | **OPEN — BENCHMARK** | Gold publication (not the schema) |
| O-3 | Legacy embedding model identity and vector-space compatibility | **OPEN — REQUIRES USER DECISION** (not in either audit). The rule is locked meanwhile (L18). | mixed legacy/new vector search only |
| O-4 | Legacy collision merge/alias policy | **OPEN — FUTURE IMPLEMENTATION** (state represented in 5.22; unpublished-until-decided rule locked in 15.2) | publication of colliding chunks only |
| O-5 | Subject-domain taxonomy details and classifier | **OPEN — FUTURE IMPLEMENTATION** (column and versioning exist) | nothing structural |
| O-6 | `metadata.updated_at` semantics | **RESOLVED BY DESIGN**: NULL for new rows in V1 (not in either audit) | — |
| O-7 | GitHub license acceptance policy | **OPEN — REQUIRES USER DECISION** | GitHub ingestion (not the schema) |
| O-8 | GitHub snapshot scope and discovery criteria | **OPEN — REQUIRES USER DECISION** (a default is defined) | GitHub ingestion |
| O-9 | `source_published_at`/`source_title` extraction rules | **OPEN — FUTURE IMPLEMENTATION** | nothing structural |
| O-10 | Runtime role and privilege policy | **RESOLVED BY DESIGN**: `docs/database_privileges_v1.md` (LOCKED). The runtime role is `kb_pipeline_app` and the owner/migration role is `kb_pipeline_migrator`; `postgres` is used for provisioning only. | — |
| O-11 | Triggers, dependencies, FK actions | **RESOLVED BY AUDIT** (A2: none; actions recorded). Residual: identify application consumers of Gold, to notify them of the new vocabularies. | release communication only |
| O-12 | `lz4` TOAST support | **OPEN — BENCHMARK** | nothing structural |
| O-13 | Very-large-scale physical design: partitioning or sharding, global-uniqueness enforcement, text storage and compression, multi-node | **OPEN — BENCHMARK** | scale-out beyond the pilot |
| O-14 | Retention of text for quarantined documents | **OPEN — REQUIRES USER DECISION** (`canonical_text` is nullable either way) | nothing structural |
| O-15 | Common Crawl index form and filter set | **OPEN — FUTURE IMPLEMENTATION** (`index_kind`, `filter_version` in the job key) | nothing structural |
| O-16 | Quality, factuality and chunk-acceptance thresholds | **OPEN — BENCHMARK** (versions stored) | nothing structural |
| O-17 | Near-duplicate algorithm, parameters and thresholds | **OPEN — BENCHMARK** (`dedup_methods`) | nothing structural |
| O-18 | Re-embedding cutover and shadow vector storage | **OPEN — FUTURE IMPLEMENTATION** | the first model change |
| O-19 | Frozen regression vectors and unit tests for Identity Extension V1 (observation and bridge ids) | **RESOLVED BY DESIGN**: Identity Extension V1 LOCKED (`docs/identity_extension_v1.md`) | — |
| O-20 | Worker concurrency, batch sizes, lease durations, retry base/cap, chunk-size configuration | **OPEN — BENCHMARK** (configuration and versions stored) | nothing structural |

### 27.3 Self-review record (lock review)

| Check | Result |
|---|---|
| Duplicate canonical storage | Document text only in `document_registry`. Chunk text in `chunk_registry` (authoritative) plus `public.kb.chunk` (Gold copy, deliberate). `embedding_queue` holds no text. |
| Provenance gaps | observation → extraction → document → occurrence → chunk → Gold is complete. Exact-duplicate observations are flagged by `document_was_new`. Near-duplicate, version and related links are versioned. |
| Source vs document identity | Observations are keyed by location; documents by content; bridge rows by channel. Three separate prefix domains. |
| Chunk vs document identity | Separate registries and prefixes; many-to-many via `chunk_occurrences` |
| Uniqueness protection | PK + UNIQUE on all four canonical keys; natural-key guards on observations; PK-targeted UPSERTs still raise on hash conflicts |
| Unsafe legacy changes | No DDL or constraints on `public`. Two data rows inserted into `public.datasets` by an approved idempotent bootstrap. No `dataset_sources` rows. No DELETE or UPDATE on Gold. |
| Gold overwrite risk | Conflicts on `id` are verified and quarantined; there is no `DO UPDATE` on `public` |
| CASCADE risk | Documented (L21); drift audit re-enqueues missing Gold rows |
| Retry and recovery fields | Leases, fencing tokens, attempts, deferrals, next-attempt time, checkpoints, attempt history, dead letters |
| Embedding coupling | Model identity only in `kbp`; Gold untouched; legacy comparability not assumed (L18) |
| Unsupported assumptions | Every existing-database fact is cited from A1 or A2. The bridge `status` is labelled with its residual check. |
| Unnecessary 7B-scale indexes | Reverse-lookup and FK-child indexes on large tables are deferred |
| Two-source policy | CHECKs on `source_channel`; public-only CHECK on GitHub snapshots; bridge map limited to the two channels |
| Draft defects corrected in this review | See the list below |

Draft defects corrected in this review:

- Near-duplicate state: the `duplicate_of_document_id` column was replaced by
  5.23–5.25.
- Channel token aligned to `common_crawl`.
- Chunks are enqueued for embedding only when an active model exists.
- The bridge is now fully specified.
- Gold conflict verification added.
- The CASCADE handling rule added.

---

## 28. SCHEMA V1 LOCK READINESS

### Verdict: **READY TO LOCK** → **LOCKED** (Phase 1). The bridge bootstrap is deferred (see Status).

There is no structural blocker. Every remaining open item is a benchmark,
policy or implementation choice that the schema already represents through
nullable columns, versioned configuration tables or explicit state.

| Lock criterion | Status | Where |
|---|---|---|
| No destructive public-table changes | ✅ No DDL on `public`; no DELETE or UPDATE of legacy rows; two approved bridge data rows only | 3, 4, 14.3, 22, 23 |
| Public Gold contract preserved | ✅ `kb(id, chunk, embedding vector(768))` and the 8 `metadata` columns are unchanged; no constraints added | L3, L20 |
| Deterministic identity supported | ✅ Identity V1 (locked) plus Identity Extension V1 design | 5.12, 5.14, 26 |
| Source provenance supported | ✅ `source_observations` + subtypes + `observation_extractions` | 5.6–5.11 |
| Global exact dedup supported | ✅ PK + UNIQUE on `document_id`/`content_hash` and `chunk_id`/`chunk_hash`; hash-binding CHECKs | 9, 10 |
| Near-duplicate relationship state supported | ✅ `dedup_methods`, `document_dedup_decisions`, `document_relationships`, `document_was_new` | 5.23–5.25, 9.5 |
| Chunk provenance supported | ✅ `chunk_occurrences` | 5.15, 11 |
| Embedding versioning supported | ✅ `embedding_models`, `chunk_embeddings`, `embedding_queue`; no Gold change | 5.16–5.18, 12 |
| Persistent retry/job/dead-letter state supported | ✅ `pipeline_jobs`, `processing_attempts`, `dead_letters`, `source_rate_limits` | 16, 17 |
| Common Crawl provenance supported | ✅ `cc_crawls`, `cc_warc_files`, `cc_captures` (index-first, byte-range) | 7 |
| GitHub provenance supported | ✅ `github_repositories`, `github_commits` (public-only CHECK), `github_file_observations` | 8 |
| Deterministic bridge datasets designed | ✅ `dataset-bridge:v1:<channel>` UUIDv5, lookup by UNIQUE name, idempotent bootstrap | 14.3, 26 |
| Gold publication order defined | ✅ bridge → `metadata` → `kb` → `kbp` state, one transaction, verify-not-overwrite | 13 |
| Legacy reconciliation represented | ✅ `legacy_kb_reconciliation` with hash, id, status, decision, review, error, timestamps | 5.22, 15 |
| Isolated Alembic ownership defined | ✅ `kbp.alembic_version`; `kbp`-only autogenerate filter; no-DROP-on-`public` guard | 22 |
| One-PC initial operation supported | ✅ PostgreSQL-only queues, bounded work units, backpressure | 16, 24 |
| Future physical scaling not blocked | ✅ The logical keys are independent of the physical layout; partitioning and sharding options are documented | 21.4, 25 |

### Non-structural gates (these do not block locking the schema)

| Gate | Required before |
|---|---|
| ~~O-19: frozen vectors and unit tests for Identity Extension V1~~ **Done**: Identity Extension V1 LOCKED | — |
| ~~O-10: connection role decision~~ **Done**: `docs/database_privileges_v1.md` LOCKED; provisioning per its §13 | — |
| O-1 residual: `ACCEPTED` status **UNRESOLVED** by local code review; confirm legacy consumer behaviour, then decide the bridge `status` value | bridge bootstrap |
| Legacy reconciliation run (section 15) | large production ingestion |
| O-2: embedding model chosen by benchmark and registered as `active_gold` (768-dimensional, or a Gold contract migration) | Gold publication |
| O-7, O-8: GitHub license and snapshot policy | GitHub ingestion |

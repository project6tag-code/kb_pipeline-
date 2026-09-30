# DATABASE RUNTIME ROLE & PRIVILEGE SPECIFICATION V1 — LOCKED

| | |
|---|---|
| Status | **LOCKED** (see section 14 for scope and residual items) |
| Scope | Roles, ownership, privileges and connection policy for the `kbp` pipeline in `data_collection_pipeline` |
| Depends on | `docs/database_schema_v1.md` (Schema V1, READY TO LOCK), `docs/identity_extension_v1.md` (LOCKED) |
| Audit authority | `docs/audits/database_audit_v1.csv` (A1), `docs/audits/database_compatibility_audit_v1.csv` (A2) |
| Executed? | **No.** Nothing here has been run: no `CREATE ROLE`, `GRANT`, `ALTER DEFAULT PRIVILEGES` or migration. |

---

## 1. Purpose

This specification defines **who** may do **what** in the database for the
new KB pipeline. The goals are:

- the pipeline never runs as the superuser `postgres`;
- schema ownership (DDL) is separated from runtime data access (DML);
- the runtime role gets only the privileges Schema V1's operations need;
- existing `public` tables keep their owner and are exposed minimally;
- the one-time Gold bridge bootstrap does not leave permanent write access to
  `public.datasets` with the runtime role.

This resolves Schema V1 open item **O-10**.

## 2. Role separation

| Identity | Kind | Purpose | Used by |
|---|---|---|---|
| `postgres` | existing superuser; owner of all audited `public` tables (A2) | **One-time provisioning only**: create the two roles, create schema `kbp`, and issue the few grants on `public` tables. Never used by the pipeline afterwards. | DBA, manually |
| **`kb_pipeline_migrator`** | new LOGIN role, owner of `kbp` | Creates, upgrades and downgrades `kbp` objects (Alembic). Runs **approved bootstrap operations** (Gold bridge rows, reference-row seeding) and approved operator procedures. Grants `kbp` privileges to the runtime role. | Migration and bootstrap commands, operators |
| **`kb_pipeline_app`** | new LOGIN role, no ownership | All runtime DML: workers, acquisition, processing, retry/job engine, embedding workers, Gold publication, legacy reconciliation computation | The application |

No other roles are required for V1. A future read-only analytics role is out of
scope; it would be a separate, additive decision.

## 3. Migration/admin identity: `kb_pipeline_migrator`

### 3.1 Role attributes

```
LOGIN  NOSUPERUSER  NOCREATEDB  NOCREATEROLE  NOREPLICATION  NOBYPASSRLS  INHERIT
CONNECTION LIMIT 2
```

- It is not a superuser, so DDL mistakes cannot reach other schemas' owners'
  objects.
- The connection limit is 2: migrations and bootstrap are serial, one-off
  operations.

### 3.2 Privileges

| Object | Privilege | Why |
|---|---|---|
| database `data_collection_pipeline` | `CONNECT` | run migrations |
| schema `kbp` | **owner** (created by `postgres` with `AUTHORIZATION kb_pipeline_migrator`) | owns all `kbp` DDL, including `kbp.alembic_version` |
| all `kbp` objects | **owner** (it creates them) | DDL; grants to the runtime role |
| schema `public` | `USAGE` (no `CREATE`) | resolve `public` tables |
| `public.datasets` | `REFERENCES (id)` | create the FK `kbp.source_dataset_map.dataset_id → public.datasets(id)` (Schema V1 §6.2) |
| `public.datasets` | `SELECT`, `INSERT` | idempotent bridge bootstrap: insert with `ON CONFLICT (id) DO NOTHING`, then re-read and verify (Schema V1 §14.3) |
| `public.dataset_sources` | `SELECT` | look up `common_crawl` / `github` by UNIQUE `name` during bootstrap |
| `public.kb`, `public.metadata` | **none** | migrations and bootstrap never touch Gold |

Explicitly **not** granted:

- `UPDATE`, `DELETE` or `TRUNCATE` on any `public` table;
- `CREATE` on schema `public` or on the database;
- ownership of any `public` object.

**Refinement of Schema V1 §14.3 (downgrade).** The migrator has no `DELETE`
on `public.datasets`, so a `kbp` downgrade **never removes the two bridge
rows**. They are inert compatibility records. Removing them, if ever
required, is a manual DBA action by `postgres`, and it is only possible when
no `public.metadata` row references them (`metadata_source_id_fkey`, NO
ACTION).

**Refinement of Schema V1 §22 (schema bootstrap).** The migrator has no
`CREATE` on the database, and PostgreSQL checks that privilege even for
`CREATE SCHEMA IF NOT EXISTS`. The Alembic environment must therefore
**verify** that `kbp` exists (catalog lookup) and fail with a clear message
if it does not, instead of creating it. The schema is created once at
provisioning (section 13).

## 4. Runtime identity: `kb_pipeline_app`

### 4.1 Role attributes

```
LOGIN  NOSUPERUSER  NOCREATEDB  NOCREATEROLE  NOREPLICATION  NOBYPASSRLS  INHERIT
CONNECTION LIMIT <bounded; = sum of configured worker pools + small margin>
```

- It owns **no** objects and is a member of no roles.
- Recommended role-level settings, with values tuned by benchmark (O-20):
  - `statement_timeout`: bounded;
  - `lock_timeout`: bounded (for example 5s);
  - `idle_in_transaction_session_timeout`: bounded, so a crashed worker
    cannot hold locks;
  - `search_path = kbp, public`.
- All SQL must still **schema-qualify** `public.kb`, `public.metadata`,
  `public.datasets` and `public.dataset_sources`.
- The schema that holds the `vector` type (from the pgvector extension) is
  **not in either audit**. If it is not `public`, add it to `search_path`, or
  qualify the type, at implementation time.

### 4.2 General rules

- There is no DDL, no `CREATE` on any schema, and no ownership.
- `TRUNCATE`, `REFERENCES` and `TRIGGER` are never granted.
- `DELETE` is granted on exactly **one** table: `kbp.embedding_queue`, whose
  rows are removed when work completes (Schema V1 §5.18).
- `UPDATE` is granted only where Schema V1 defines a mutable lifecycle. Where
  only some columns change, `UPDATE` is **column-level**, so identity and
  content columns stay immutable even against application bugs.
- `INSERT … ON CONFLICT DO NOTHING` needs only `INSERT`. `SELECT` is granted
  separately because verification and `RETURNING` read the rows.
  `ON CONFLICT DO UPDATE` additionally needs `UPDATE` on the assigned columns
  and `SELECT` on the columns read.
- **Identity columns** (`GENERATED ALWAYS AS IDENTITY`) need no sequence
  grant for `INSERT`. `USAGE` on the internal sequences is not granted, and
  `setval` (sequence `UPDATE`) is never granted.
- Foreign-key checks run with the privileges of the table owner, not the
  inserting role. So the runtime role needs no privileges on `public.datasets`
  to insert `public.metadata`, and none on `public.metadata` beyond its own
  needs to insert `public.kb`.

## 5. `kbp` schema privileges (runtime)

Schema: `GRANT USAGE ON SCHEMA kbp TO kb_pipeline_app` (no `CREATE`). There
are no `PUBLIC` privileges on `kbp`.

### 5.1 Exact runtime privilege matrix for `kbp` tables

| Table | SELECT | INSERT | UPDATE | DELETE | Justification (Schema V1 operation) |
|---|:-:|:-:|:-:|:-:|---|
| `pipeline_runs` | ✅ | ✅ | ✅ `(status, finished_at, notes)` | — | runs are opened, then closed (§5.1, §18) |
| `pipeline_jobs` | ✅ | ✅ | ✅ table-level | — | claim, lease renewal, checkpoint, retry scheduling, completion (§16, §17); completed rows are retained, so no `DELETE` |
| `processing_attempts` | ✅ | ✅ | ✅ `(finished_at, outcome, error_class, error_code, error_message, retry_after_seconds, scheduled_next_attempt_at, metrics)` | — | an attempt is opened, then closed (§5.20) |
| `dead_letters` | ✅ | ✅ | ✅ `(occurrence_count, last_seen_at, attempt_count, error_class, error_code, error_message, payload_ref, disposition, resolved_at, resolution)` | — | the `ON CONFLICT DO UPDATE` counters (§20); resolution/requeue |
| `source_rate_limits` | ✅ | ✅ | ✅ table-level | — | `ON CONFLICT DO UPDATE` latest values (§20) |
| `source_dataset_map` | ✅ | — | — | — | read-only at runtime; written only by the bootstrap (section 7) |
| `cc_crawls` | ✅ | ✅ | — | — | insert-once dimension |
| `cc_warc_files` | ✅ | ✅ | — | — | insert-once dimension (`RETURNING` / select id) |
| `source_observations` | ✅ | ✅ | — | — | insert-only (§5.6) |
| `cc_captures` | ✅ | ✅ | — | — | insert-only |
| `github_repositories` | ✅ | ✅ | ✅ `(node_id, owner_login, repo_name, html_url, default_branch, visibility, license_spdx_id, is_fork, is_archived, api_etag, last_snapshot_commit_sha, metadata_refreshed_at)` | — | mutable last-observed metadata (§5.8, §20). `repository_id` and `first_seen_at` are immutable. |
| `github_commits` | ✅ | ✅ | — | — | insert-only |
| `github_file_observations` | ✅ | ✅ | — | — | insert-only |
| `observation_extractions` | ✅ | ✅ | — | — | insert-only (§5.11) |
| `document_registry` | ✅ | ✅ | ✅ `(status, status_reason, policy_version, component_versions, language, language_confidence, quality_score, factuality_score, subject_domain, status_updated_at)` | — | versioned policy re-evaluation (§5.12). **Not updatable:** `document_id`, `content_hash`, `canonical_text`, `char_length`, `first_observation_id`, `created_at`. |
| `dedup_methods` | ✅ | — | — | — | reference rows seeded by the migrator (approved bootstrap) |
| `document_dedup_decisions` | ✅ | ✅ | — | — | insert-only at runtime; operator overrides of `decision_status` use the migrator identity (section 7.3) |
| `document_relationships` | ✅ | ✅ | — | — | insert-only at runtime; overrides as above |
| `chunking_versions` | ✅ | — | — | — | reference rows seeded by the migrator |
| `chunk_registry` | ✅ | ✅ | — | — | insert-only, authoritative (§5.14). No UPDATE at all. |
| `chunk_occurrences` | ✅ | ✅ | — | — | insert-only |
| `embedding_models` | ✅ | — | — | — | model registration and lifecycle (`candidate` → `active_gold`) are operator decisions via the migrator (section 7.3) |
| `chunk_embeddings` | ✅ | ✅ | — | — | insert-once result records (§5.17) |
| `embedding_queue` | ✅ | ✅ | ✅ table-level | ✅ | transient work: claim, lease, retry; the row is deleted on success or when moved to dead letters (§5.18, §13.1) |
| `legacy_kb_reconciliation` | ✅ | ✅ | ✅ table-level | — | computation UPSERT and the reset to `needs_review` (§15). Collision **decisions** (`decision`, `decision_reason`, `reviewed_*`, `decided_at`) are written only through the operator procedure (section 7.3). The table has 383 rows. |
| `alembic_version` (kbp) | ✅ | — | — | — | startup check that the database is at the expected migration head |

- `TRUNCATE`, `REFERENCES` and `TRIGGER` are **never** granted on any `kbp`
  table.
- Schema V1 defines **no functions or procedures**. If a future migration
  adds one, `EXECUTE` is granted explicitly per function, and `SECURITY
  DEFINER` functions require separate review.

### 5.2 Why the runtime role has no DDL

- Schema V1 has no runtime DDL requirement. Queues are ordinary tables, and
  partitions and re-embedding shadow tables are future migrations.
- Any future need, such as creating partitions, is added through a migration
  run by the migrator, not by widening the runtime role.

## 6. `public` table privileges (runtime)

Only these four `public` tables are exposed. There is no `public.*` grant, no
schema-wide grant, and no ownership change.

| Table | Runtime privilege | Justification | Not granted |
|---|---|---|---|
| `public.dataset_sources` | `SELECT (id, name, source_type)` | startup self-check that each `kbp.source_dataset_map` bridge still points to a `datasets` row whose parent `dataset_sources.name` equals the channel | INSERT, UPDATE, DELETE, TRUNCATE, other columns |
| `public.datasets` | `SELECT (id, source_id, dataset_name, metadata)` | the same startup bridge self-check (the row exists, the parent matches, and `metadata.kind` is the bridge marker) | **INSERT**, UPDATE, DELETE, TRUNCATE, REFERENCES, other columns |
| `public.metadata` | `SELECT (id, source_id)`, `INSERT` (all columns) | Gold publication step 2: `INSERT … ON CONFLICT (id) DO NOTHING`; conflict verification reads `source_id` (§13.2); legacy reconciliation reads `source_id` (§15) | **UPDATE, DELETE**, TRUNCATE, REFERENCES, TRIGGER |
| `public.kb` | `SELECT`, `INSERT` | Gold publication step 3: insert-once; verification reads `chunk` and `embedding` (§13.2); drift audit reads `id` (§13.3); legacy reconciliation reads `chunk` (§15) | **UPDATE, DELETE**, TRUNCATE, REFERENCES, TRIGGER |

Additional rules:

- **Schema `public`:** the runtime role needs `USAGE`. Whether `USAGE` is
  already granted to `PUBLIC` is **not in either audit**; verify at
  implementation time. If it is not, grant `USAGE` only.
  `CREATE ON SCHEMA public` is never granted.
- **Why no `UPDATE`/`DELETE` on Gold:**
  - publication is insert-once and never overwrites (Schema V1 §13, L17);
  - the pipeline never deletes Gold rows, because `kb_id_fkey` is `ON DELETE
    CASCADE` (L21);
  - Schema V1 never updates `metadata`, since representatives are
    insert-once (D16).
- **Future `UPDATE` on `public.kb.embedding`.** A future in-place re-embedding
  cutover would need `UPDATE (embedding)`. That is **not** granted in V1. It
  requires its own reviewed decision (Schema V1 O-18) and, if approved, a
  column-level grant.
- **Current grants on these tables.** A2 lists only `postgres` as holding
  privileges on the four tables, so there are no existing `PUBLIC` grants to
  revoke. The grants above are purely additive.

## 7. Bridge bootstrap privilege policy

### 7.1 Who performs it

The one-time, idempotent Gold bridge bootstrap (Schema V1 §14.3) is
performed by **`kb_pipeline_migrator`**, as an explicitly approved bootstrap
operation (Schema V1 §22).

- `kb_pipeline_app` has **no** `INSERT`/`UPDATE` on `public.datasets` and
  **no** write access to `kbp.source_dataset_map`.
- It only reads them for its startup self-check. A bootstrap that happens once
  does not justify a permanent runtime write privilege on a shared legacy
  table.

### 7.2 Bootstrap steps and required privileges

All steps run as `kb_pipeline_migrator` in one transaction.

| Step (Schema V1 §14.3) | Privilege used |
|---|---|
| Look up `public.dataset_sources` by `name IN ('common_crawl','github')`; verify `source_type` | `SELECT` on `dataset_sources` |
| Insert the bridge rows with the frozen ids `a7d8d2e1-8400-5916-9440-b492e8e692ce` (common_crawl) and `b487f559-5f8e-56ba-9746-28658b9d09d1` (github), `ON CONFLICT (id) DO NOTHING` | `INSERT` on `datasets` |
| Re-read and verify `source_id`, `dataset_name` | `SELECT` on `datasets` |
| Insert and verify `kbp.source_dataset_map` | owner of `kbp` |

- No `dataset_sources` row is ever created. The migrator has no `INSERT` on
  it.
- **Execution is blocked** until the `ACCEPTED` status question (section 11)
  is resolved.

### 7.3 Operator procedures (same identity)

Operator decisions that Schema V1 keeps out of the runtime path are executed
as `kb_pipeline_migrator` through reviewed procedures, not by widening the
runtime role:

- registering `embedding_models` and changing their `status`;
- seeding `chunking_versions` and `dedup_methods`;
- overriding `decision_status` in the dedup decision tables;
- recording legacy collision `decision`/review columns.

## 8. Ownership policy

| Objects | Owner | Rule |
|---|---|---|
| schema `kbp` and **every** object in it (tables, identity sequences, indexes, `kbp.alembic_version`, future functions) | `kb_pipeline_migrator` | All `kbp` DDL runs as the migrator. Objects created by any other role (for example `postgres`) are a policy violation, detected by the drift check (section 13, step 8). |
| `public.kb`, `public.metadata`, `public.datasets`, `public.dataset_sources` (and every other `public` object) | `postgres` (A2), **unchanged** | No `ALTER … OWNER`. No ownership transfer, ever. |
| `kb_pipeline_app` | owns nothing | |

## 9. Default privileges

`ALTER DEFAULT PRIVILEGES` applies **only to objects created later by the
role named in `FOR ROLE`**, and only in the named schema. Default privileges
for `kb_pipeline_migrator` therefore do **not** apply to objects created by
`postgres` or any other role. This is one more reason all `kbp` DDL must run
as the migrator.

Conceptual definition. **Not executed.** It is run once as, or on behalf of,
`kb_pipeline_migrator`:

```
ALTER DEFAULT PRIVILEGES FOR ROLE kb_pipeline_migrator IN SCHEMA kbp
    GRANT SELECT ON TABLES TO kb_pipeline_app;

ALTER DEFAULT PRIVILEGES FOR ROLE kb_pipeline_migrator
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

-- No default privileges on SEQUENCES: identity columns need none.
-- No default INSERT/UPDATE/DELETE: they differ per table.
```

- **The baseline is `SELECT` only.** The per-table write privileges of
  section 5.1 are **not** uniform, so they cannot be expressed as defaults.
  Each migration that creates a `kbp` table must contain the explicit `GRANT`
  statements for that table's row in the matrix (for example `INSERT`, or
  column-level `UPDATE`, or `DELETE` for `embedding_queue`).
- A table therefore never starts with more than `SELECT` for the runtime role,
  even if a migration forgets its grants. Forgetting a grant fails safely, as
  a permission error in tests, rather than open.
- The `EXECUTE` revoke means future functions are not callable by `PUBLIC`
  until explicitly granted. PostgreSQL's built-in default grants `EXECUTE` on
  functions to `PUBLIC`.
- The `EXECUTE` revoke is written **without** `IN SCHEMA`. PostgreSQL applies
  per-schema default privileges only additively on top of the global
  defaults, so an `IN SCHEMA kbp … REVOKE EXECUTE … FROM PUBLIC` would have no
  effect. The global form applies only to functions created by
  `kb_pipeline_migrator`, which creates objects only in `kbp`, so the scope is
  unchanged. (Corrected in Live Database Phase 1A, F1; matches
  `scripts/db/provision_kbp_roles.sql`.)

## 10. Connection and secrets policy

- **Runtime:** the application connects **only** as `kb_pipeline_app`.
- **Migrations and bootstrap:** only as `kb_pipeline_migrator`.
- **`postgres`:** used only for the one-time provisioning (section 13) and
  exceptional DBA actions. Never by application code or migrations.
- **Credentials** come from environment variables or a secrets store at
  runtime. The standard variable **names** (values are never committed) are
  one SQLAlchemy connection URL per role:

  | Variable | Role | Used by |
  |---|---|---|
  | `KB_MIGRATION_DATABASE_URL` | `kb_pipeline_migrator` | Alembic migrations and approved bootstrap commands only |
  | `KB_DATABASE_URL` | `kb_pipeline_app` | the application (workers, Gold publication) |

  - Format: `postgresql+psycopg://<role>[:<password>]@<host>:<port>/<database>`.
    The preferred form omits the password and lets libpq read it from a
    `pgpass` file outside the repository.
  - The two values must differ. The migration environment refuses to run
    when `KB_MIGRATION_DATABASE_URL` equals `KB_DATABASE_URL`.
  - (Standardized in Live Database Phase 1A; this replaces the earlier
    proposal of split `KB_DB_*` host, port, name, user and password
    variables. The role separation is unchanged.)

- **Git rules:**
  - Credentials must never be committed.
  - `.env.example` may list the variable names above with **placeholder
    values only**, never real passwords.
  - Real `.env` files must be listed in `.gitignore`. As of Phase 1,
    `.gitignore` excludes `.env` and `.env.*` (except `.env.example`).
- **Logging:** connection strings and passwords are never logged. Set
  `application_name` per worker type (for example
  `kb_pipeline:embedding_worker`) for observability.
- **Authentication:** password-based roles use `scram-sha-256`. The
  `pg_hba.conf` entries are **not in either audit**; they are configured at
  provisioning, restricting both roles to the expected hosts (localhost for
  the one-PC deployment).
- This task inspected `.env.example`, `config/pipeline.yaml`,
  `app/config/settings.py` and `docker-compose.yml`. All are currently
  **empty**. No `.env` file exists. No secrets were read or changed.

## 11. `ACCEPTED` status code-review result

**Result: UNRESOLVED.**

### What was searched

The whole local project (excluding `docs/`) was searched, case-insensitively,
for:

- `ACCEPTED`, `datasets`, `.status`, `dataset_sources`, `selection_state`,
  `postgres`;
- every non-empty source file.

### Evidence

- The only non-empty source files in the repository are:
  - `app/processing/normalization/text.py`;
  - `app/utils/hashing.py`;
  - `app/utils/ids.py`;
  - `tests/unit/test_hashing.py`, `tests/unit/test_identity_extension.py`,
    `tests/unit/test_ids.py`, `tests/unit/test_text_normalization.py`.
- Every other skeleton file (including `app/db/*`, `app/queue/*`,
  `app/workers/*`, `app/sources/*`, `app/config/settings.py`) is **empty**.
- Matches found:
  - `app/utils/ids.py:14`, `:153`, `:155`: docstrings mentioning the bridge
    `public.datasets` / `dataset_sources.name`. No query, no status logic.
  - `app/utils/ids.py:128`: a comment ("must not be accepted as an integer").
    Unrelated.
  - `tests/unit/test_identity_extension.py:1`, `:172`, `:315`: test names and
    docstrings. Unrelated.
- **No local code reads, writes, filters or branches on `datasets.status`, or
  on `ACCEPTED` in any form.**
- The drive containing the project holds no other code.

### Why this is UNRESOLVED, not SAFE

- The 378 `public.datasets` rows (statuses `ACCEPTED`, `DISCOVERED` and
  `REJECTED` per A2) and the legacy workflow tables (`dataset_downloads`,
  `processed_datasets`, `collection_request_candidates`,
  `orchestration_jobs`, …) were produced by a **legacy application whose code
  is not in this workspace**.
- The absence of references in this repository says nothing about whether
  that application processes `ACCEPTED` datasets.

### Consequence

- The bridge bootstrap `status` value stays **open** (Schema V1 O-1 residual).
  No alternative status is invented here.
- Bridge bootstrap execution is blocked until the legacy application's code or
  owner confirms how `datasets.status = 'ACCEPTED'` is consumed.
- Privilege design is unaffected: whatever value is chosen, the same roles and
  privileges apply.

## 12. Security invariants

1. The pipeline never connects as a superuser. Neither pipeline role has
   `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION` or `BYPASSRLS`.
2. `kb_pipeline_app` owns nothing and holds no DDL privilege anywhere.
3. `kb_pipeline_app` holds no `UPDATE`, `DELETE` or `TRUNCATE` on any
   `public` table, and no privilege on any `public` table other than the four
   in section 6.
4. The only `DELETE` the runtime role holds is on `kbp.embedding_queue`.
5. Identity and content columns (`document_id`, `content_hash`,
   `canonical_text`, `chunk_id`, `chunk_hash`, `chunk_text`, and all
   observation identity columns) are **not updatable** by the runtime role.
6. `kb_pipeline_migrator` holds no `UPDATE`, `DELETE` or `TRUNCATE` on any
   `public` table and no privilege on `public.kb`/`public.metadata`. Its only
   `public` write is `INSERT` on `public.datasets`, used by the bridge
   bootstrap.
7. No ownership of any existing `public` object changes.
8. All `kbp` objects are owned by `kb_pipeline_migrator`, and default
   privileges never grant more than `SELECT` to the runtime role.
9. No `PUBLIC` privileges are granted on `kbp` or its objects.
10. No credentials are stored in the repository.

## 13. Future implementation procedure

This is the order of events for the implementation task. **Nothing here is
performed now.**

1. **Preconditions:**
   - Schema V1 locked.
   - A full database backup taken.
   - `.env` added to `.gitignore`.
   - Credentials provisioned in the secrets store.
2. **Provisioning, as `postgres`, once, reviewed:**
   - create `kb_pipeline_migrator` and `kb_pipeline_app` with the attributes
     in sections 3.1 and 4.1;
   - `CREATE SCHEMA kbp AUTHORIZATION kb_pipeline_migrator`;
   - grant `CONNECT` on the database to both roles;
   - verify or grant `USAGE ON SCHEMA public` to both roles;
   - grant the `public` table privileges of sections 3.2 (migrator) and 6
     (runtime);
   - configure `pg_hba.conf` (scram-sha-256, local hosts);
   - set the role-level timeouts and `search_path` for `kb_pipeline_app`.
3. **As `kb_pipeline_migrator`:**
   - `GRANT USAGE ON SCHEMA kbp TO kb_pipeline_app`;
   - set the default privileges of section 9.
4. **Migrations, as `kb_pipeline_migrator`:**
   - create the Schema V1 tables;
   - each revision includes the explicit per-table grants of section 5.1;
   - the Alembic environment verifies that `kbp` exists; it does not create it.
5. **Bridge bootstrap, as `kb_pipeline_migrator`:** only after the section 11
   question is resolved and the `status` value decided.
6. **Seed reference rows, as `kb_pipeline_migrator`:** `chunking_versions`,
   `dedup_methods` and `embedding_models`, once their configurations are
   decided.
7. **Application start, as `kb_pipeline_app`:**
   - check `kbp.alembic_version`;
   - run the bridge self-check.
8. **Privilege drift check** (CI or startup; read-only catalog queries):
   - every `kbp` object is owned by `kb_pipeline_migrator`;
   - the runtime role's effective privileges equal the matrices in sections 5
     and 6, no more and no less;
   - no `PUBLIC` grants on `kbp`;
   - the ownership of the four `public` tables is still `postgres`.
9. **Tests:** permission tests confirm that the runtime role is **denied**:
   - `UPDATE`/`DELETE` on `public.kb` and `public.metadata`;
   - `INSERT` on `public.datasets`;
   - `UPDATE` of `chunk_registry` and identity columns;
   - any DDL.

## 14. Lock status

**DATABASE RUNTIME ROLE & PRIVILEGE SPECIFICATION V1 — LOCKED.**

The role separation, attributes, ownership, privilege matrices, default
privileges, bootstrap policy and connection policy are complete, and they are
consistent with Schema V1 and Identity Extension V1. No privilege decision
depends on unresolved information.

### Residual items (these do not block locking this specification)

| Item | Nature | Blocks |
|---|---|---|
| `ACCEPTED` bridge status (section 11): **UNRESOLVED** | a data value, Schema V1 O-1 residual | bridge bootstrap execution only |
| `USAGE` on schema `public` for `PUBLIC`; the extension schema of `vector`; `pg_hba.conf` | environment facts not in either audit | verified during provisioning (section 13, step 2) |
| Timeout values, connection limit for the runtime role | tuning (Schema V1 O-20) | nothing structural |

### Refinements to Schema V1 recorded here

These are not schema redesigns:

- The bridge rows are never deleted by a `kbp` downgrade (section 3.2).
- The Alembic environment verifies, rather than creates, schema `kbp`
  (section 3.2).

## 15. Additive Migration 001B Privileges

Migration 001B adds `kbp.chunk_dedup_decisions` and
`kbp.chunk_relationships`, specified in `docs/chunk_near_dedup_v1.md`. The
runtime role receives `SELECT` and `INSERT` on each table, and no `UPDATE`,
`DELETE`, `TRUNCATE`, `REFERENCES`, or `TRIGGER`. This is an additive extension
to the locked 001A privilege matrix; ownership remains with
`kb_pipeline_migrator`.

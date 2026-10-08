# KB Pipeline: Production Deployment Guide (Common Crawl V2)

Builds an English factual knowledge base from **Common Crawl** and publishes chunks plus embeddings into PostgreSQL (`public.kb`, `public.metadata`). Design details: [README.md](README.md), [docs/cc_fact_kb_v2.md](docs/cc_fact_kb_v2.md), [docs/local_runbook.md](docs/local_runbook.md).

> **This release is Common Crawl only.** GitHub ingestion is unfinished and **refuses to start**: `app.pipeline.cli run --sources github`, the GitHub worker and the GitHub planner raise an error unless `KB_ENABLE_GITHUB=1` (development builds only). `run_cc.ps1` and `check_setup.ps1` also refuse when it is set. Leave `KB_ENABLE_GITHUB` and `KB_GH_*` unset.

> **Target database.** Every command needs `--expect-database <name>`, which must equal `KB_EXPECTED_DATABASE` (default `kb_pipeline_test`, for local testing) **and** the database the connection actually reaches; otherwise the command refuses (exit 2). Set `KB_EXPECTED_DATABASE` to your production database name. `data_collection_pipeline`, `postgres` and the template databases are always refused.

## 1. Requirements
Windows 10/11 x64, Python 3.12, PostgreSQL 17 with the **pgvector** extension, about 20 GB free disk to start, internet access to `index.commoncrawl.org` and `data.commoncrawl.org`. An NVIDIA GPU is optional (about 70 chunks/s on CUDA versus 3-8 chunks/s on CPU).

## 2. Python environment
```powershell
cd C:\kb\kb-pipeline                      # the unzipped release
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e .
# GPU build (CUDA 12.8); on a machine without a GPU the same pins run on CPU:
.\.venv\Scripts\python.exe -m pip install -r requirements-embeddings.txt --extra-index-url https://download.pytorch.org/whl/cu128
.\.venv\Scripts\python.exe -m app.processing.embedding.cli download     # one-time, online: pinned MPNet model (1.1 GB)
```
Copy `.env.example` to `.env` and edit it (delete the `:CHANGE_ME` placeholders from the two database URLs). Passwords never go in `.env` or in URLs: put them in `%APPDATA%\postgresql\pgpass.conf` (`localhost:5432:<database>:kb_pipeline_app:<password>` and the same for `kb_pipeline_migrator`).

## 3. PostgreSQL 17 + pgvector
Install PostgreSQL 17 and pgvector for Windows. Add the two pipeline roles' passwords to `%APPDATA%\postgresql\pgpass.conf` (set them once with `\password kb_pipeline_app` and `\password kb_pipeline_migrator` in `psql` as `postgres`; the roles are created by step 4 below).

## 4. Database initialization (one command, from an empty database)
```powershell
.\scripts\production\init_database.ps1 -Database kb_prod        # prompts once for the postgres password (not stored)
```
Then put `KB_EXPECTED_DATABASE=kb_prod` and the two database URLs into `.env`. The script is idempotent and uses only repository SQL and CLIs, in this order:

| # | Step | Source |
|---|---|---|
| 1 | create the database (UTF8) and `CREATE EXTENSION vector` | `createdb`, `psql` |
| 2 | create `public.dataset_sources`, `datasets`, `metadata`, `kb` in the validated V2 layout (`metadata.crawled_at`, `kb.source_id` included), then verify the exact layout | `scripts\db\bootstrap_public_tables.sql` |
| 3 | roles, schema `kbp`, grants | `scripts\db\provision_kbp_roles.sql -v target_db=<db>` |
| 4 | `kbp` schema to revision `kbp_001e` | `alembic -x expect_database=<db> upgrade head` |
| 4b | make the frozen MPNet model register as **id 2** (the runtime pins `MODEL_ID = 2`; a clean database would give id 1): consumes one identity value, or removes a previously registered unused id 1 and realigns the empty identity | `scripts\db\ensure_embedding_model_id.sql` |
| 5 | frozen registrations: Silver methods, chunking version, dedup method, MPNet model (downloads the model once) | `silver.cli`, `chunking.cli`, `chunk_dedup.cli`, `embedding.cli` |
| 6 | Gold bridge rows, Gold V2 grants, bridge map | `gold_bridge.cli admin-bootstrap`, `scripts\db\admin_gold_v2_patch.sql`, `map-bootstrap` |
| 7 | model 2 to `active_gold` | `gold.cli promote-model` |

Options: `-CopyPgPassFrom <other_db>` duplicates that database's two `pgpass.conf` role lines for the new database name (useful for a disposable test database); `-SkipModelDownload` if the model is already cached. Do not run `gold_bridge.cli metadata-patch` on a database built this way (the tables are created with every column already). References: [docs/migrations_kbp.md](docs/migrations_kbp.md), [docs/database_privileges_v1.md](docs/database_privileges_v1.md), [docs/gold_bridge_v1_test.md](docs/gold_bridge_v1_test.md). Read-only verifiers: `scripts\dberify_*.sql`.

## 5. Operator workflow (`scripts\production\`)
They only call the existing CLIs and load `.env`.
```powershell
.\scripts\production\validate_predeploy.ps1     # offline: files, secrets hygiene, migration safety, unit tests
.\scripts\production\check_setup.ps1            # venv, packages, .env, disk, GPU, database target/role/revision/model
.\scripts\production\run_cc.ps1 -Crawl CC-MAIN-2024-33 -DryRun   # print the stage plan
.\scripts\production\run_cc.ps1 -Crawl CC-MAIN-2024-33           # first run (safe limits below)
.\scripts\production\status.ps1
```
Equivalent direct commands (use your `KB_EXPECTED_DATABASE` value in place of `kb_pipeline_test`):
```powershell
.\.venv\Scripts\python.exe -m app.pipeline.cli run --expect-database kb_pipeline_test --sources cc --cc-profile reference --cc-crawl CC-MAIN-2024-33 --cc-partitions 2 --cc-blocks 1 --cc-iterations 4
.\.venv\Scripts\python.exe -m app.pipeline.cli run --expect-database kb_pipeline_test --sources cc --skip-acquisition   # repeat until exit 0
.\.venv\Scripts\python.exe -m app.pipeline.cli status --expect-database kb_pipeline_test
```
Stages: `cc_plan_* -> cc_acquire_silver -> chunking -> chunk_dedup -> embedding_enqueue -> embedding_work -> gold_publish`. Exit codes: `0` ok, `1` a stage failed, `2` refused, `130` interrupted. **Each pass embeds and decides at most about 10,000 chunks**, so a run with more chunks needs several `--skip-acquisition` passes; `run_cc.ps1` does this. Gold publishes only when every eligible chunk is embedded (`chunk eligibility is incomplete` before that is expected, not an error).

## 6. Stop, restart, resume
Press **Ctrl+C once**: workers finish the current batch and the run is recorded `aborted` (exit 130). Never kill the processes. To resume, run **the same command again**: jobs continue from their checkpoint, expired leases are recovered, and only missing work is done; reruns create no duplicates. Inspect with `status.ps1`; list or cancel waiting jobs with `app.pipeline.cli jobs --expect-database kb_pipeline_test --status retry_pending [--cancel --execute]`; dead letters with `dead-letters`. Rate-limited requests (HTTP 429/503) are deferred automatically and honor Retry-After.

## 7. Logs and data
| What | Where |
|---|---|
| Per-stage logs | `logs\pipeline\<run_id>\<stage>.log` |
| Raw evidence (Bronze) | `data\bronze\commoncrawl\` (about 18 KB per capture; re-acquirable) |
| Near-dup indexes (rebuildable) | `data\silver\near_index.sqlite3`, `data\chunk_dedup\` |
| Model cache | Hugging Face cache (`HF_HOME`), about 1.1 GB |
| Results | PostgreSQL: schema `kbp` (provenance, chunks, vectors), `public.kb`, `public.metadata` |

Gold costs about 20 KB per chunk (text, vector in `kbp` and again in `public.kb`).

## 8. Safe initial Common Crawl limits and scaling
Start with **one crawl, `reference` profile, 2 partitions x 1 block** (about 1,000 captures and 4,000-5,000 Gold chunks per block; roughly 15 minutes on a GPU). Check quality (counts, `source_id` integrity, dates, domains) before growing.

Scale in this order: more `-Partitions` / `-Blocks` (each block is about 3,000 index lines); then `-Profile both` (adds a strictly filtered open-crawl sample, stride `--cc-open-stride`); then more crawls. Keep `KB_CC_FETCH_CONCURRENCY` at 4-8 (Common Crawl politeness) and set a contact address in `KB_CC_USER_AGENT`. Extra worker processes need `KB_DB_POOL_SIZE` connections each, within the `kb_pipeline_app` connection limit (20 by default). Embedding is deliberately one text per forward pass (bit-reproducible): about 100M chunks would take about 2 weeks on one GPU, so scale with GPUs or workers. Edit the curated publishers in `app/sources/commoncrawl/reference_domains_v1.json` or `KB_CC_REFERENCE_DOMAINS`; any change makes new jobs and never rewrites old data.

## 9. Backup and restore
```powershell
$pg = 'C:\Program Files\PostgreSQL\17\bin'
& "$pg\pg_dump.exe" -h localhost -U postgres -Fc -f backup\kb_pipeline_test_$(Get-Date -f yyyyMMdd).dump kb_pipeline_test
# restore into an empty database that has the roles and the vector extension:
& "$pg\pg_restore.exe" -h localhost -U postgres -d kb_pipeline_test --clean --if-exists backup\<file>.dump
```
Back up before every migration, patch or reset. Bronze, Silver and dedup indexes are rebuildable and are not part of the backup. A content-only reset (keeps schema and registrations) exists only for the local test database: `scripts\db\admin_reset_kb_pipeline_test.sql` refuses every other database name by design (confirmation flag required; stop all workers first).

## 10. Troubleshooting
| Symptom | Cause / fix |
|---|---|
| exit 2 `--expect-database ... differs from the configured expected database` | pass the same name as `KB_EXPECTED_DATABASE` (default `kb_pipeline_test`) |
| `GitHub ... is disabled in this release` | intended: Common Crawl only (see top) |
| `database revision is ..., expected 'kbp_001e'` | run step 2 (`alembic upgrade head`) |
| `permission denied for table metadata` | step 5 not applied (needs `SELECT (crawled_at)` for the runtime role) |
| `chunk eligibility is incomplete` | expected until dedup and embedding finish; run the next `--skip-acquisition` pass |
| `no password supplied` | missing or wrong `pgpass.conf` entry |
| `import sqlalchemy` fails (`_cy.pyd` blocked) | Windows Application Control; the app falls back to pure Python (`KB_SQLALCHEMY_PURE_PYTHON=1` forces it). Do not bypass the policy. |
| embedding cannot find the model | run `embedding.cli download` once (online) |
| `Bronze disk guard` / `disk_space` | free space or lower `KB_CC_MIN_FREE_BYTES` |
| a stage keeps failing | read `logs\pipeline\<run_id>\<stage>.log`, then `status.ps1` and `dead-letters` |

Run the offline tests with `scripts\production\validate_predeploy.ps1`. The opt-in PostgreSQL tests (`KB_RUN_PG_INTEGRATION=1`) write inert prefixed rows and must not be run on production data.

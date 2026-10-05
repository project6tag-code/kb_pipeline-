# Local Runbook — one Windows machine, `kb_pipeline_test`

Scope: run the validated source → Gold pipeline (Common Crawl + public GitHub → Bronze → Silver → dedup → chunking → MPNet → Gold) on one Windows PC. V1 is locked to the test database `kb_pipeline_test`; every command refuses any other database, including `data_collection_pipeline`.

## 1. Prerequisites
- Windows 10/11, Python 3.12 with the repo `.venv` (`pip install -e .` plus `requirements-embeddings.txt`).
- Local PostgreSQL 16+ with the `vector` extension. NVIDIA GPU optional (`--device cpu` works).
- ~10 GB free disk (Bronze guard `KB_*_MIN_FREE_BYTES`), internet for Common Crawl / GitHub.
- The pinned MPNet model in the local Hugging Face cache (once: `python -m app.processing.embedding.cli download`). Embedding runs fully offline.

## 2–3. PostgreSQL and environment
Database `kb_pipeline_test` at revision `kbp_001d`, roles `kb_pipeline_app` (runtime) and `kb_pipeline_migrator`, model 2 `active_gold` (already provisioned). Passwords live in `%APPDATA%\postgresql\pgpass.conf`, never in URLs.

```powershell
$env:KB_DATABASE_URL           = 'postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test'
$env:KB_MIGRATION_DATABASE_URL = 'postgresql+psycopg://kb_pipeline_migrator@localhost:5432/kb_pipeline_test'  # admin tasks only
$env:KB_GH_TOKEN = '...'   # optional; unauthenticated GitHub allows only 60 core requests/hour
```
Source bounds are options of `run` (below); the underlying `KB_CC_*` / `KB_GH_*` settings are described in `docs/commoncrawl_acquisition_v1.md` and `docs/github_acquisition_v1.md`.

## 4. Database initialisation (once, already done here)
`alembic upgrade head` as the migrator (revision `kbp_001d`), then the one-time registrations: `silver.cli register-methods`, `chunking.cli register-version`, `chunk_dedup.cli register-method`, `embedding.cli register-model`, `gold_bridge.cli` bootstrap, `gold.cli promote-model` (each with `--expect-database kb_pipeline_test`). See `docs/migrations_kbp.md`, `docs/gold_bridge_v1_test.md`, `docs/gold_publication_v1_schema.md`.

## 5. Start the pipeline (one command)
```powershell
.\.venv\Scripts\python.exe -m app.pipeline.cli run --expect-database kb_pipeline_test --sources cc,github
```
Stages (each is its own supported CLI, run as a child process, logs in `logs\pipeline\<run_id>\<stage>.log`):
`cc_plan → cc_acquire_silver → gh_plan → gh_acquire_silver → chunking → chunk_dedup → embedding_enqueue → embedding_work → gold_publish`.
Acquisition stages always include Silver (extraction, filters, document dedup, chunking). Exit codes: `0` ok, `1` a stage failed, `2` refused, `130` interrupted. The JSON summary prints `run_id`, per-stage results, row deltas, totals and runtime health.

## 6. Bounded pilot example
```powershell
.\.venv\Scripts\python.exe -m app.pipeline.cli run --expect-database kb_pipeline_test --sources cc,github `
  --cc-crawl CC-MAIN-2013-48 --cc-partitions 1 --cc-blocks 1 --cc-max-record-bytes 3000 --cc-iterations 1 `
  --gh-windows 1 --gh-min-stars 100 --gh-max-files 4 --gh-quota-reserve 5 --gh-iterations 5 --embed-limit 5000
```
A Common Crawl index block is ~3000 lines (an unbounded block can mean ~2,800 fetched pages); `--cc-max-record-bytes` shrinks a pilot. `--dry-run` validates the target and prints the stage plan without running anything. `--skip-acquisition` processes already-acquired data only; `--skip-gold` stops before publication.

## 7. Resume after a crash / stop
Run the **same command again**. Jobs resume from their checkpoint; expired leases are recovered; every later stage only handles what is missing. Nothing is deleted or duplicated.
Stop safely with **Ctrl+C** once: workers finish the current batch, the run is recorded `aborted` (exit 130). Do not kill the processes.

## 8. Rerun safely
Re-running an identical command is idempotent: observations, documents, chunks, embeddings and Gold rows are keyed by deterministic ids/hashes; reruns add zero duplicates (only run-history rows grow). For observations that were acquired but never reached Silver:
```powershell
.\.venv\Scripts\python.exe -m app.processing.silver.cli reprocess --expect-database kb_pipeline_test            # safe: only unprocessed observations
.\.venv\Scripts\python.exe -m app.processing.silver.cli reprocess --expect-database kb_pipeline_test --force-reprocess --limit 500   # re-evaluate and verify determinism; no duplicates
```
Scope options: `--source cc|github|all`, `--observation-id`, `--crawl-id`, `--repository-id`, `--batch-size`, `--limit`. It reads Bronze only (no network).

## 9–10. Inspect status, retries, dead letters
```powershell
python -m app.pipeline.cli status       --expect-database kb_pipeline_test    # counts, jobs by status, running/stale leases, queue
python -m app.pipeline.cli jobs         --expect-database kb_pipeline_test --status retry_pending
python -m app.pipeline.cli jobs         --expect-database kb_pipeline_test --status retry_pending --cancel            # dry run
python -m app.pipeline.cli jobs         --expect-database kb_pipeline_test --status retry_pending --cancel --execute  # cancel (history kept)
python -m app.pipeline.cli dead-letters --expect-database kb_pipeline_test
```
Rate-limited jobs (`quota_reserve`, 429/503) are *deferred*: they consume no attempt, honour Retry-After, and become claimable again after the reset; just rerun `run`. Other transient errors retry with bounded exponential backoff and jitter, then dead-letter. Cancelled jobs can be requeued later; nothing is deleted. Dead letters are content rejections (undecodable, unknown charset, secrets quarantine, missing Bronze for test fixtures) and stay as audit records.

## 11. Verify PostgreSQL
```sql
SELECT (SELECT count(*) FROM kbp.gold_publications) ledger, (SELECT count(*) FROM public.kb) kb, (SELECT count(id) FROM public.metadata) metadata;
SELECT count(*) FROM kbp.pipeline_jobs WHERE status = 'running';            -- expect 0
SELECT count(*) FROM kbp.embedding_queue;                                    -- expect 0
SELECT embedding_model_id, status FROM kbp.embedding_models;                 -- 2 | active_gold
```
Deeper audits: `python -m app.processing.gold.cli status --expect-database kb_pipeline_test`, `python -m app.processing.chunk_dedup.cli audit --expect-database kb_pipeline_test`, `python -m app.processing.embedding.cli verify --expect-database kb_pipeline_test --sample 50`.

## 12. Windows Application Control and SQLAlchemy
This PC's Application Control policy refuses to load SQLAlchemy's compiled `*_cy.pyd` extensions, so a plain `import sqlalchemy` fails in the `.venv`. `app/_compat.py` (imported by `app/__init__.py`) detects this and loads those few modules from their shipped pure-Python source instead. It never loads the blocked binaries, changes no policy and modifies no package; where the binaries load normally it does nothing (`KB_SQLALCHEMY_PURE_PYTHON=0` disables, `=1` forces). The acquisition, Silver, chunking, dedup and embedding stages use SQLAlchemy; the Gold publisher, the Gold bridge checks and the `app.pipeline.cli` status/jobs/dead-letters/run orchestration use psycopg directly. Do not disable or work around the policy any other way.

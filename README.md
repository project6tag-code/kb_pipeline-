# KB Pipeline: Knowledge Transfer (KT) Guide

A production-grade pipeline that builds an **English factual knowledge base** from two sources only, **Common Crawl** and **public GitHub**. It takes raw captures through Bronze, Silver, chunks, embeddings and Gold, and the Gold rows land in the existing `public.kb` / `public.metadata` tables in PostgreSQL.

This README is the starting point for anyone taking over the project. It explains what the system does, how it is built, how to run and operate it, where the detailed specs live and what is still open. The deep design documents in [docs/](docs/) are the authority. This file is the map to them.

---

## 1. TL;DR

| | |
|---|---|
| Language / runtime | Python 3.12, PostgreSQL 16+ (17 in the audit) with the `vector` (pgvector) extension |
| Core deps | SQLAlchemy 2, psycopg 3, Alembic; embeddings add torch + sentence-transformers ([requirements-embeddings.txt](requirements-embeddings.txt)) |
| Entry point | `python -m app.pipeline.cli run --expect-database kb_pipeline_test` |
| Target DB (V1) | **`kb_pipeline_test` only.** Every command refuses any other database, including production `data_collection_pipeline`. |
| Schema head | `kbp_001d` |
| Embedding model | `paraphrase-multilingual-mpnet-base-v2` (768-d, pinned revision), model id **2**, status `active_gold` |
| State | All V1 stages are implemented and validated end to end on `kb_pipeline_test` (last commit `ad77c0b Complete local E2E operator workflow V1`). |
| Tests | 641 offline unit tests pass at HEAD in about 9 s; the PostgreSQL and network integration tests are opt-in. |

---

## 2. The pipeline at a glance

```
            ┌──────────────── Common Crawl ────────────────┐   ┌──────────────── public GitHub ───────────────┐
            │ collinfo → cluster.idx → CDX block (Range GET)│   │ search windows (by creation date) → repo     │
            │ → WARC record (Range GET, never whole files)  │   │ → commit → tree → git blob (hash-verified)   │
            └──────────────────────┬────────────────────────┘   └──────────────────────┬───────────────────────┘
                                   v                                                   v
 BRONZE   immutable raw evidence on disk (data/bronze/...) + kbp.source_observations (+ cc_captures / github_file_observations)
                                   │
 SILVER   22 gates: integrity → MIME → decoding → HTML/MD/code extraction → canonicalize → English → PII → secrets
          → spam → quality → factual suitability → exact doc dedup → near-dup doc dedup → subject domain
          → kbp.document_registry (status = accepted)
                                   │
 CHUNK    deterministic chunker (~256 est. tokens, max 400) → kbp.chunk_registry (global, exact-dedup) + chunk_occurrences
                                   │
 C-DEDUP  MinHash/LSH candidates → exact Jaccard → fact-preservation guard → NEAR_DUPLICATE / RELATED / UNIQUE
          → only representatives are eligible for embedding
                                   │
 EMBED    MPNet, one text per forward pass (bit-reproducible) → kbp.chunk_embeddings + chunk_embedding_vectors ("shadow")
                                   │
 GOLD     one transaction per chunk: public.metadata → public.kb → kbp.gold_publications (insert-only, verify on conflict)
```

Bronze and Silver run **inside the acquisition jobs**: the acquisition worker calls the Silver processor and the inline chunker in the same transaction as the job checkpoint. Chunking backfill, chunk dedup, embedding and Gold then run as separate stage commands.

---

## 3. Core design principles (read these first)

These rules explain most of the code. Breaking one is almost always a bug.

1. **Deterministic identity everywhere.** Documents, chunks, source observations and bridge rows have UUIDv5 ids under one fixed namespace (`KB_NAMESPACE = dd5af6b8-…`). Re-running anything produces the same ids, so reruns create **zero duplicates**. See [app/utils/ids.py](app/utils/ids.py) and [docs/identity_extension_v1.md](docs/identity_extension_v1.md).
   - Observation id = **where** content was seen (WARC byte range, or repo + commit + path + blob).
   - Document and chunk id = **what** the content is (hash of the canonical text). The two kinds are never mixed.
2. **Everything is idempotent and resumable.** Jobs checkpoint, leases expire and are recovered, and inserts use `ON CONFLICT DO NOTHING`. After a crash, **run the same command again.**
3. **Nothing is overwritten or deleted.** Conflicts are *verified*, never replaced. A mismatch is quarantined as a dead letter for human review. The runtime role has no `DELETE` on most tables, and Gold rows are never deleted because `kb_id_fkey` is `ON DELETE CASCADE`.
4. **New infrastructure lives in schema `kbp`; `public` stays compatible.** The pipeline only *inserts* into `public.kb`, `public.metadata` and (one-time bootstrap) `public.datasets`. Legacy tables and the legacy `public.alembic_version` are never touched.
5. **Least privilege, with three identities:**
   - `kb_pipeline_app` (runtime DML): `KB_DATABASE_URL`
   - `kb_pipeline_migrator` (owns `kbp`; Alembic and one-time registrations): `KB_MIGRATION_DATABASE_URL`
   - `postgres` / admin (only for the one-time `public` bootstrap and metadata patch): `KB_ADMIN_DATABASE_URL`

   Commands check which role they are connected as and refuse to run under the wrong one.
6. **Versioned, frozen configuration.** Every policy (filters, chunker, dedup method, embedding model) has a version label derived from its parameters. Changing a parameter means **a new version row**, never an in-place edit.
7. **Quality over volume.** A questionable document is rejected or held, never silently promoted.
8. **Guard rails on the target.** `--expect-database` is mandatory and must equal `kb_pipeline_test`. The schema revision and role are verified before any work starts.

---

## 4. Repository map

```
app/
  _compat.py              SQLAlchemy pure-Python fallback for Windows Application Control (see §10)
  config/settings.py      DB / worker settings from env (KB_DATABASE_URL, pool, lease)
  db/                     connection pool, repositories, migration config and safety checker
  queue/                  job queue: states, deterministic job identities, retry/backoff, claim/lease/fence
  workers/                worker loop, handler registry, acquisition/processing/chunk/embedding workers
  monitoring/             read-only health and metrics
  sources/
    commoncrawl/          CC discovery, CDX index, HTTP (allow-listed), WARC parsing, Bronze store, handler, CLI
    github/               GitHub client (ETag, rate limits), discovery, snapshot acquisition, Bronze, CLI
  processing/
    extraction/ normalization/ language/ privacy/ quality/ factuality/ classification/ dedup/
                          the individual Silver gates
    silver/               Bronze → Silver orchestration (prepare/persist), reprocess, CLI
    chunking/             tokenizer estimate, structure-aware chunker, backfill, CLI
    chunk_dedup/          MinHash/LSH index, fact guard, eligibility, audit, CLI
    embedding/            pinned MPNet model, embedder, queue worker, validator, CLI
    gold_bridge/          public.datasets bridge rows, admin metadata patch, CLI
    gold/                 Gold Publisher V1 (psycopg-native), CLI
  pipeline/cli.py         ONE operator entry point: run / status / jobs / dead-letters
  utils/                  hashing, ids (identity spec), files, time

migrations/versions/      kbp_001a (baseline, 25 tables) → 001b (chunk dedup) → 001c (shadow vectors) → 001d (Gold ledger)
scripts/db/               provisioning + read-only verification SQL for each migration, permission probes
scripts/benchmark_chunk_candidate_indexes.py
tests/unit/               offline tests (fixtures, mocked HTTP, in-memory stores)
tests/integration/        opt-in PostgreSQL / network tests and pilot scripts
docs/                     LOCKED specs, per-stage design docs, runbook, audits (CSV)
artifacts/db/             schema baseline dump and pre-migration snapshot evidence
data/                     bronze/, silver/ (near-dup sqlite index), quarantine/ — git-ignored content
logs/pipeline/<run_id>/   per-stage logs of each `run`
config/pipeline.yaml, docker-compose.yml, docker/   empty placeholders (not used)
backup/                   local DB dump, untracked; do not commit
```

---

## 5. Data model (schema `kbp`)

Full spec: [docs/database_schema_v1.md](docs/database_schema_v1.md) (LOCKED, ~2,100 lines). The key tables are grouped by purpose:

| Group | Tables |
|---|---|
| Orchestration | `pipeline_runs`, `pipeline_jobs`, `processing_attempts`, `dead_letters`, `source_rate_limits` |
| Provenance (Bronze) | `source_observations` (supertype), `cc_crawls`, `cc_warc_files`, `cc_captures`, `github_repositories`, `github_commits`, `github_file_observations` |
| Silver | `observation_extractions`, `document_registry`, `dedup_methods`, `document_dedup_decisions`, `document_relationships` |
| Chunks | `chunking_versions`, `chunk_registry` (authoritative chunk text), `chunk_occurrences`, `chunk_dedup_decisions`, `chunk_relationships` (001B) |
| Embeddings | `embedding_models`, `embedding_queue` (transient), `chunk_embeddings`, `chunk_embedding_vectors` (001C) |
| Gold | `gold_publications` (001D ledger), `source_dataset_map` (bridge), `legacy_kb_reconciliation` |

**Gold contract (unchanged legacy tables):**
- `public.kb(id, chunk, embedding vector(768))`
- `public.metadata(id, source_id → public.datasets.id, source_url, source_title, source_type, domain, published_at, updated_at)`

For new rows, `kb.id = metadata.id = chunk_id`, and `metadata.domain` is the *subject* domain, not the web host.

---

## 6. Stage-by-stage

| Stage | Package | What it does | Spec |
|---|---|---|---|
| Runtime engine | `app/queue`, `app/workers` | Persistent job queue with `SKIP LOCKED` claims, leases, fencing tokens, retry with full-jitter backoff, rate-limit deferral (does not consume an attempt), dead letters, graceful shutdown | [runtime_engine_v1.md](docs/runtime_engine_v1.md) |
| Common Crawl acquisition | `app/sources/commoncrawl` | Index-first and byte-range only. Plans `cc_index_partition` jobs, fetches individual WARC records concurrently, validates them and writes Bronze atomically | [commoncrawl_acquisition_v1.md](docs/commoncrawl_acquisition_v1.md) |
| GitHub acquisition | `app/sources/github` | Discovers repos by creation-date windows (`gh_repository_discovery`), snapshots each HEAD commit (`gh_repository_snapshot`), verifies blob hashes, uses ETag/304 to save quota | [github_acquisition_v1.md](docs/github_acquisition_v1.md) |
| Silver | `app/processing/silver` + gate packages | 22 gates (table in the spec), `prepare` outside the transaction, `persist` in the same transaction as the checkpoint | [silver_preprocessing_v1.md](docs/silver_preprocessing_v1.md) |
| Chunking | `app/processing/chunking` | Version `kbp-chunker-c1-78f25e8bf5`: target 256 / max 400 est. tokens, no overlap, exact code-point spans; inline for new docs plus a backfill command | [chunking_v1.md](docs/chunking_v1.md) |
| Chunk near-dedup | `app/processing/chunk_dedup` | Method `oph-w3-b10r4-j70-v1`: NEAR if Jaccard ≥ 0.70 **and** fact signatures match, RELATED if ≥ 0.30. The side index under `data/chunk_dedup/` can be rebuilt from PostgreSQL | [chunk_near_dedup_v1.md](docs/chunk_near_dedup_v1.md), [candidate index](docs/chunk_near_dedup_candidate_index_v1.md) |
| Embeddings | `app/processing/embedding` | Offline MPNet, one text per forward pass for bit-reproducibility. Per item, one transaction plus a hash proof computed by PostgreSQL | [embeddings_v1.md](docs/embeddings_v1.md), [migration 001C](docs/migration_001c_shadow_embedding_vectors.md) |
| Gold bridge | `app/processing/gold_bridge` | Two `public.datasets` rows with status `KB_REFERENCE` plus `kbp.source_dataset_map`, written in two stages (admin, then migrator) | [gold_bridge_v1_test.md](docs/gold_bridge_v1_test.md) |
| Gold publisher | `app/processing/gold` | Promotes model 2 to `active_gold`, then publishes `metadata → kb → gold_publications` per transaction and verifies on conflict | [gold_publication_v1_schema.md](docs/gold_publication_v1_schema.md), schema §13 |

---

## 7. Setup (Windows, one machine)

**Prerequisites:** Python 3.12, PostgreSQL 16+ with `vector`, about 10 GB of free disk, and internet access for Common Crawl and GitHub. An NVIDIA GPU is optional (`--device cpu` works).

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe -m pip install -r requirements-embeddings.txt --extra-index-url https://download.pytorch.org/whl/cu128
.\.venv\Scripts\python.exe -m app.processing.embedding.cli download     # one-time model fetch; embedding is offline after this
```

**Environment.** Copy the names from [.env.example](.env.example). Keep passwords in `%APPDATA%\postgresql\pgpass.conf`, **never** in URLs or committed files.

```powershell
$env:KB_DATABASE_URL           = 'postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test'
$env:KB_MIGRATION_DATABASE_URL = 'postgresql+psycopg://kb_pipeline_migrator@localhost:5432/kb_pipeline_test'  # admin tasks only
$env:KB_GH_TOKEN               = '...'   # optional; unauthenticated GitHub allows only 60 requests/hour
```

**One-time database initialisation** (already done on the current `kb_pipeline_test`):

1. As `postgres`: run [scripts/db/provision_kbp_roles.sql](scripts/db/provision_kbp_roles.sql) to create the roles and schema `kbp`.
2. As the migrator: `alembic upgrade head`, which brings the schema to `kbp_001d`. Verify with `scripts/db/verify_migration_001*.sql`.
3. Registrations, each with `--expect-database kb_pipeline_test` and run as the migrator:
   `silver.cli register-methods` → `chunking.cli register-version` → `chunk_dedup.cli register-method` → `embedding.cli register-model`
4. Gold bridge: `gold_bridge.cli admin-bootstrap` (admin), `metadata-patch` (admin), `map-bootstrap` (migrator).
5. `gold.cli promote-model`, which sets model 2 to `active_gold`.

Details: [docs/migrations_kbp.md](docs/migrations_kbp.md), [docs/database_privileges_v1.md](docs/database_privileges_v1.md).

---

## 8. Running and operating

Full runbook: [docs/local_runbook.md](docs/local_runbook.md).

```powershell
# full run (both sources)
.\.venv\Scripts\python.exe -m app.pipeline.cli run --expect-database kb_pipeline_test --sources cc,github

# small bounded pilot
.\.venv\Scripts\python.exe -m app.pipeline.cli run --expect-database kb_pipeline_test --sources cc,github `
  --cc-crawl CC-MAIN-2013-48 --cc-partitions 1 --cc-blocks 1 --cc-max-record-bytes 3000 --cc-iterations 1 `
  --gh-windows 1 --gh-min-stars 100 --gh-max-files 4 --gh-quota-reserve 5 --gh-iterations 5 --embed-limit 5000
```

- **Stages**, each a child process with its own log in `logs/pipeline/<run_id>/`:
  `cc_plan → cc_acquire_silver → gh_plan → gh_acquire_silver → chunking → chunk_dedup → embedding_enqueue → embedding_work → gold_publish`
- **Useful flags:** `--dry-run` prints the plan only, `--skip-acquisition` processes existing data only, `--skip-gold` stops before publishing, `--reprocess` re-runs Silver over Bronze, and `--device auto|cpu|cuda` picks the embedding device.
- **Exit codes:** `0` ok, `1` a stage failed, `2` refused, `130` interrupted.
- **Stopping:** press Ctrl+C **once**. Workers finish the current batch and the run is recorded as `aborted`. Don't kill the processes.
- **Resuming:** run the same command again.

**Day-to-day inspection:**

```powershell
python -m app.pipeline.cli status       --expect-database kb_pipeline_test     # counts, jobs, leases, queue
python -m app.pipeline.cli jobs         --expect-database kb_pipeline_test --status retry_pending [--cancel [--execute]]
python -m app.pipeline.cli dead-letters --expect-database kb_pipeline_test
python -m app.processing.gold.cli status        --expect-database kb_pipeline_test
python -m app.processing.chunk_dedup.cli audit  --expect-database kb_pipeline_test
python -m app.processing.embedding.cli verify   --expect-database kb_pipeline_test --sample 50
python -m app.processing.silver.cli reprocess   --expect-database kb_pipeline_test   # Bronze-only, no network
```

**Healthy steady state:** 0 `running` jobs, empty `kbp.embedding_queue`, and model 2 = `active_gold`. Also `gold_publications` should match the `public.kb` and `public.metadata` rows written by the pipeline.

**Retries:** rate-limited jobs (GitHub quota reserve, HTTP 429/503) are *deferred*. They don't consume an attempt and become claimable after the reset. Other transient errors back off exponentially and then go to the dead-letter table. Dead letters are audit records: they are resolved or requeued, never deleted.

---

## 9. Testing

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests/unit              # offline, about 9 s, 641 tests
```

The integration tests are opt-in and run against a real database:

```powershell
$env:KB_RUN_PG_INTEGRATION = '1'
$env:KB_INTEGRATION_DATABASE_URL = 'postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test'
.\.venv\Scripts\python.exe -m unittest discover -s tests/integration -v
```

- The integration tests use a random `it-<hex>/` prefix. The runtime role has no DELETE, so their committed rows stay behind as inert, prefixed rows.
- The `*_network_smoke` tests make real HTTP calls.
- `tests/integration/*_pilot.py` are the scripts used for the validation pilots.
- Some unit tests pin the SQL, the migrations and the CHECK vocabularies against the locked specs. If you change one, a test is meant to fail. Update the spec deliberately rather than the test.

---

## 10. Gotchas and environment quirks

- **Windows Application Control blocks SQLAlchemy's compiled `*_cy.pyd`** on the current PC. [app/_compat.py](app/_compat.py) falls back to the pure-Python modules automatically. `KB_SQLALCHEMY_PURE_PYTHON=0/1` turns the fallback off or forces it on. Do not try to bypass the policy. The Gold publisher and `app.pipeline.cli` use psycopg directly for this reason.
- **Embedding bits depend on the device class.** CUDA and CPU vectors differ slightly (cosine ≈ 0.9999999). Recompute verification must use the device that wrote the vectors, and each run records its device in `pipeline_runs.notes`.
- **The 128-token model limit truncates text.** Chunks target about 256 estimated tokens, but MPNet reads only the first 128 subword tokens.
- **Throughput:** about 80 chunks/s per GPU with per-text encoding. 100M chunks would take about 2 weeks on one GPU. This is a deliberate trade for reproducibility.
- **Unauthenticated GitHub** allows only 60 requests/hour. Set `KB_GH_TOKEN`. The token is never logged; it is stored only as a `sha256:` fingerprint.
- **One CC index block is about 3,000 CDX lines**, which can mean about 2,800 page fetches. Use `--cc-max-record-bytes` and `--cc-blocks 1` for pilots.
- **Doc status headers are point-in-time.** Some older docs (for example `runtime_engine_v1.md` saying "GitHub: not started", or `handoff_chunk_near_dedup_v1.md`) predate later work. For current status, trust git history and [docs/local_runbook.md](docs/local_runbook.md).
- **Branches:** work is on `embeddings-v1` (which contains every stage through Gold and the E2E workflow). The repo also has `master`, `chunk-near-dedup-v1` and `silver-preprocessing-wip`. Nothing has been merged to `master` yet.

---

## 11. Current state and in-flight work

**Completed and locked** (in commit order): DB foundation (001A) → Runtime Engine → CC acquisition → GitHub acquisition → Bronze → Silver → document dedup → Chunking → Migration 001B → chunk near-dedup → Migration 001C → MPNet embeddings → Gold bridge → Gold publication schema (001D + metadata patch) → Gold Publisher → local E2E operator workflow.

**Uncommitted at the time of writing** (`app/sources/github/{config,discovery,files}.py`):
- adds test/example/demo/sample directories to the GitHub directory exclusions;
- rejects repositories whose description or topics contain test-like keywords;
- rejects files named `README.*` and `index.*`.

⚠️ These changes break **10 unit tests** (`test_gh_acquisition`, `test_gh_core`), which expect `README.md` to be accepted. At HEAD all 641 tests pass. They also change the GitHub *policy*, so they should come with a new policy version label, an update to [docs/github_acquisition_v1.md](docs/github_acquisition_v1.md) and updated tests before being committed. The `description` check also does a substring match, so for example "contest" matches "test".

---

## 12. Open items and future work (V2 candidates)

- **Production rollout to `data_collection_pipeline`.** V1 is locked to `kb_pipeline_test`. Production needs a legacy compatibility review (the meaning of `public.datasets.status`, O-1), legacy KB reconciliation (schema §15) and a fresh bridge bootstrap.
- **Embeddings V2.** Evaluate retrieval models (BGE, E5, Arctic), a longer context or sliding window, and possibly batched encoding with a tolerance-based verification. It would be a new `embedding_models` row; V1 rows are never rewritten.
- **Legacy vectors.** The 383 legacy `public.kb` vectors are not assumed comparable with the new ones (L18).
- **ANN index.** None exists yet; one was deliberately left out of V1 (L14).
- **Gold drift audit.** A periodic check of `gold_publications` against the public tables (schema §13.3).
- **Scale-out.** Run more worker processes (each one uses `KB_DB_POOL_SIZE` connections) or more GPUs.
- Empty placeholders for containerisation and YAML config: `docker-compose.yml`, `config/pipeline.yaml`.

---

## 13. Document index

| Read when… | Document |
|---|---|
| Starting out / operating | [local_runbook.md](docs/local_runbook.md) |
| Understanding the data model and every locked decision | [database_schema_v1.md](docs/database_schema_v1.md) |
| Touching roles, grants or connections | [database_privileges_v1.md](docs/database_privileges_v1.md) |
| Writing a migration | [migrations_kbp.md](docs/migrations_kbp.md) |
| Touching ids or hashing | [identity_extension_v1.md](docs/identity_extension_v1.md) |
| Touching the job queue or workers | [runtime_engine_v1.md](docs/runtime_engine_v1.md) |
| Touching a stage | the per-stage docs in §6 |
| Asking why chunk dedup is designed this way | [handoff_chunk_near_dedup_v1.md](docs/handoff_chunk_near_dedup_v1.md), [chunk_near_dedup_candidate_index_v1.md](docs/chunk_near_dedup_candidate_index_v1.md) |
| Checking audited facts about the legacy DB | [docs/audits/](docs/audits/) (CSV, read-only evidence) |

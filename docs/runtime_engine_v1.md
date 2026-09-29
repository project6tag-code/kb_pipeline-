# Runtime Engine V1

| | |
|---|---|
| Status | Implemented; unit-tested offline. The PostgreSQL integration tests are opt-in and have **not** been run. |
| Uses | kbp Schema V1 (LOCKED) exactly: `pipeline_runs`, `pipeline_jobs`, `processing_attempts`, `dead_letters`, `source_rate_limits`, and `kbp.alembic_version` (read). No schema change, no migration, no `public` access. |
| Role | `kb_pipeline_app` only (`KB_DATABASE_URL`); refused otherwise. |
| Not included | Source acquisition (Common Crawl: `docs/commoncrawl_acquisition_v1.md`, registered as the `cc_index_partition` handler; GitHub: not started), Gold publication, embedding. |

## 1. Modules

| Module | Responsibility |
|---|---|
| `app/config/settings.py` | `DatabaseSettings` (URL from `KB_DATABASE_URL`, never printed; refuses the migration URL; bounded pool), `WorkerSettings` (lease, poll interval, batch size) |
| `app/db/connection.py` | Bounded SQLAlchemy pool (`max_overflow=0`, pre-ping), session `TimeZone=UTC`, `application_name`; `Database.transaction()`; `health_check()` / `verify_runtime_identity()` (role `kb_pipeline_app`, not superuser, `kbp_001a`, UTC) |
| `app/queue/states.py` | The schema's CHECK-list vocabularies (test-bound to Migration 001A) and the §17.4 transition table |
| `app/queue/identity.py` | Deterministic `source_identifier` builders (schema §16.2) |
| `app/queue/retry.py` | Retry controller: backoff, full jitter, Retry-After, reset timestamps, classification |
| `app/queue/job_queue.py` | Enqueue, atomic claim, fenced checkpoint/heartbeat/complete/fail/release, lease recovery, operator cancel/requeue |
| `app/db/repositories.py` | Pipeline runs, dead letters, source rate limits (connection-scoped) |
| `app/workers/registry.py` | Handler registry and `JobContext` (the plug-in point for future handlers) |
| `app/workers/runtime.py` | Worker loop, graceful shutdown, recovery sweeper |
| `app/monitoring/health.py` | Read-only health and queue summary |

Repository functions take a connection, so one transaction can cover a job
transition, its attempt row and its dead letter. `JobQueue` wraps each
operation in its own short transaction.

## 2. Job state transitions (`pipeline_jobs.status`, schema §17.4)

| From | To | Trigger | Effects |
|---|---|---|---|
| — | `pending` | enqueue | `stage = DISCOVERED` (or given), `attempt_count = 0`, `next_attempt_at = available_at or now()` |
| `pending`, `retry_pending` | `running` | claim (`next_attempt_at <= now()`, `attempt_count < max_attempts`) | new `lease_token`, `worker_id`, `locked_at`, `lease_expires_at`, `attempt_count + 1`, `started_at` if NULL; new `processing_attempts` row |
| `running` | `succeeded` | handler `Completed` | `completed_at`; lease cleared; attempt `succeeded` (+ metrics) |
| `running` | `retry_pending` | retryable failure, attempts left | backoff `next_attempt_at`; `last_error_*`; attempt `failed` (or `lease_expired`) |
| `running` | `retry_pending` | `rate_limited` | attempt **not consumed** (`attempt_count - 1`), `deferral_count + 1`; attempt `deferred_rate_limit` |
| `running` | `retry_pending` | handler `Yielded` / shutdown | attempt **not consumed**; checkpoint kept; attempt `cancelled` |
| `running` | `dead_letter` | attempts exhausted, or `permanent_source` / `data_invalid` | `completed_at`; `dead_letters` row (`item_type = 'job'`, `disposition = 'dead_letter'`) |
| `running` | `quarantined` | `policy_quarantine` | `completed_at`; `dead_letters` row (`disposition = 'quarantined'`) |
| `running` (lease expired) | `retry_pending` / `dead_letter` | sweeper | attempt `lease_expired` (**consumed**) |
| `pending`, `retry_pending` | `cancelled` | operator `cancel` | `completed_at` |
| any terminal | `pending` | operator `requeue` | `completed_at = NULL`, `max_attempts = attempt_count + n`; open job dead letters resolved `requeued` |

There is no `failed` job status in Schema V1: a failure is a
`processing_attempts.outcome = 'failed'`.

## 3. Retry formula (schema §17.1)

```
ceiling = min(max_delay, base_delay * 2 ** (attempt_count - 1))
delay   = uniform(0, ceiling)                                  # full jitter
Retry-After (seconds):     delay = max(delay, retry_after)
reset timestamp (absolute): not_before = reset + uniform(0, rate_limit_jitter)
next_attempt_at = GREATEST(now() + delay, not_before)          # database clock
```

| Error class | Decision |
|---|---|
| `transient`, `resource_exhausted`, `internal_bug`, `lease_expired` | RETRY while `attempt_count < max_attempts`, else DEAD_LETTER |
| `rate_limited` | DEFER (attempt not consumed): `retry_at` → reset + jitter; else Retry-After + jitter; else backoff |
| `permanent_source`, `data_invalid` | DEAD_LETTER |
| `policy_quarantine` | QUARANTINE |

Unclassified exceptions are `internal_bug`. Defaults: `max_attempts = 5`,
`base = 30 s`, `cap = 3600 s`, rate-limit jitter 5 s; set per job type at
registration (O-20).

## 4. Idempotency

- **Logical job identity** = `UNIQUE (job_type, source_identifier)`; enqueue is
  `INSERT … ON CONFLICT DO NOTHING` and returns the existing job
  (`created = False`). Identifiers come from the §16.2 builders, so the same
  work unit always has the same key. Re-running an ingest therefore never
  duplicates jobs, and completed jobs (retained, §16.1) are not re-run unless
  an operator requeues them.
- **Claim exclusivity**: one `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED)`
  statement.
- **Fencing**: every later write is `WHERE job_id AND lease_token AND status =
  'running'`; zero rows → `LeaseLostError`, and the worker drops its result.
- **Attempts**: `attempt_no = max + 1` under the claim's row lock (it counts every
  claim, including deferrals), protected by `UNIQUE (job_id, attempt_no)`.
- **Dead letters**: UPSERT on `(item_type, item_key, stage)` increments
  `occurrence_count`; a repeat re-opens a resolved entry.
- **Handlers** must be idempotent after their last checkpoint (§16.1): a crash
  replays the items after it.

## 5. Crash and lease recovery (schema §17.3)

`recover_expired_leases()` (or `Worker(recover_every=N)`) locks `running`
jobs with `lease_expires_at < now()` using `SKIP LOCKED`:

- it closes the open attempt as `lease_expired`; the attempt stays consumed;
- the job goes to `retry_pending` with backoff, or to `dead_letter` plus a
  `dead_letters` row when `attempt_count >= max_attempts`;
- the lease token is cleared, so the crashed or stalled worker's later
  checkpoint or completion fails (`LeaseLostError`): no double processing.

Resumption uses the persisted `stage` and `checkpoint`. Handlers keep their
lease alive with `ctx.checkpoint_progress()` / `ctx.heartbeat()`.

## 6. Worker loop and shutdown

`claim (batch ≤ claim_batch_size) → handler(ctx) → Completed | Yielded | failure → complete | release | retry/defer/dead-letter/quarantine`.

- `request_stop()` or SIGINT/SIGTERM (`install_signal_handlers()`) stops claiming.
- The in-flight handler sees `ctx.stop_requested` and may checkpoint and
  return `Yielded`; claimed jobs not yet started are released unconsumed.
- `KeyboardInterrupt` releases the current job and re-raises. A killed
  process is covered by lease recovery.

## 7. Source rate limits

`record_rate_limit()` UPSERTs the latest `limit_total`, `remaining`,
`reset_at`, `retry_after_until = now() + Retry-After` and `last_http_status`,
keeping previous values for anything unknown. `rate_limit_blocked_until()`
implements §17.1: blocked while `remaining = 0 AND reset_at > now()` or
`retry_after_until > now()`. `credential_fingerprint()` stores a non-reversible
`sha256:` prefix, or `''` when unauthenticated, and refuses raw tokens. No HTTP
acquisition exists yet.

## 8. Tests

- **Offline** (`python -m unittest discover -s tests/unit`):
  - `test_runtime_states.py`: vocabularies equal the migration's CHECK lists;
  - `test_runtime_retry.py`: bounds, jitter, Retry-After, reset, classification;
  - `test_runtime_identity.py`: identities, `JobSpec`, settings, bounded engine, health report;
  - `test_runtime_sql.py`: every runtime SQL statement against the grants and
    schema, plus the lease invariants;
  - `test_runtime_worker.py`: lifecycle, retry, dead letter, deferral,
    checkpoints, crash recovery, lease loss, priority, shutdown, registry.
- **PostgreSQL, opt-in** (`tests/integration/test_runtime_postgres.py`): set
  `KB_RUN_PG_INTEGRATION=1` and `KB_INTEGRATION_DATABASE_URL` (as
  `kb_pipeline_app`, preferably on the restored copy), then run
  `python -m unittest discover -s tests/integration -v`.
  - The tests are scoped to a random `it-<hex>/` prefix and never touch `public`.
  - Tests that need no second connection roll back.
  - Committed test jobs end terminal. The runtime role has no DELETE, so they
    remain as inert prefixed rows.

## 9. Constraints imposed by the schema and privileges (by design)

| Item | Consequence |
|---|---|
| `pipeline_runs` is updatable only in `status`, `finished_at`, `notes`; it has no progress or error columns | run progress and run error details are written to `notes` |
| no DELETE on `pipeline_jobs`, `processing_attempts`, `dead_letters`, `source_rate_limits`, `pipeline_runs` | completed rows are retained (§16.1); integration-test rows cannot be removed by the runtime role |
| `updated_at`, `first_seen_at`, `last_seen_at`, `started_at` have no defaults | the runtime always supplies them (`now()`) |
| the schema defines no attempt semantics for a cooperative release or shutdown | decided here: outcome `cancelled`, attempt not consumed (like a deferral, but without `deferral_count`) |
| `deferral_count` is unbounded in the schema | rate-limit deferrals are not capped |

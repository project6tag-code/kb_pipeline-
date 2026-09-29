# Common Crawl Acquisition V1

| | |
|---|---|
| Status | Implemented; tested offline (mocked HTTP, loopback HTTP server, in-memory store). **No real Common Crawl request and no PostgreSQL connection have been made.** The opt-in PostgreSQL and network smoke tests have not been run. |
| Uses | kbp Schema V1 / Migration 001A unchanged: `cc_crawls`, `cc_warc_files`, `source_observations`, `cc_captures` (INSERT only), `pipeline_jobs`, `processing_attempts`, `dead_letters`, `source_rate_limits` through Runtime Engine V1. No `public` access, no new migration, no new job type. |
| Identity | Identity Extension V1 (LOCKED), unchanged: `observation_id = cc_observation_id(crawl_id, warc_filename, warc_offset, warc_length)`. |
| Role | `kb_pipeline_app` (runtime). |
| Not included | GitHub acquisition, extraction/Silver, Gold publication, embeddings. |

## 1. Architecture

Index-first acquisition with byte-range fetches (schema §7.1). WARC files are never downloaded whole.

```
collinfo.json ──> crawl window ──> cc_crawls (INSERT … DO NOTHING)
                         │
cluster.idx (streamed) ──> aligned partitions ──> pipeline_jobs (cc_index_partition, ON CONFLICT DO NOTHING)
                                                        │  Runtime Engine: claim (SKIP LOCKED), lease, retry, recovery
                                                        v
   CDX block (Range GET, 1 gzip member) ──> lines ──> parse/validate ──> filter ──> observation_id (LOCKED)
        │                                                                                   │
        │   already captured + Bronze complete ──> skip        Bronze complete (crash) ──> reuse, no network
        v                                                                                   v
   bounded pool (N threads, 2N window, in order) ──> WARC Range GET ──> validate record ──> Bronze (atomic)
                                                                                            │
   one transaction per batch: cc_warc_files + source_observations + cc_captures + dead_letters + checkpoint
```

| Module | Responsibility |
|---|---|
| `app/sources/commoncrawl/config.py` | `CCSettings` from `KB_CC_*` variables (bounded); `FilterConfig` with its deterministic `version` |
| `app/sources/commoncrawl/http.py` | Endpoint allow-list (SSRF guard), `StdlibTransport` connection pool, `HttpClient` (`get_bytes`, `get_range`, `iter_lines`), status classification, `Retry-After` |
| `app/sources/commoncrawl/compression.py` | Strict single-gzip-member decoding with an output limit |
| `app/sources/commoncrawl/index.py` | CDX line parsing and validation, candidate filter |
| `app/sources/commoncrawl/warc.py` | WARC/HTTP record validation, payload digest check, literal `<title>` / `rel=canonical` |
| `app/sources/commoncrawl/bronze.py` | Bronze evidence store (deterministic paths, atomic no-clobber writes, verification) |
| `app/sources/commoncrawl/repository.py` | SQL (`*_SQL` constants) and `PostgresCaptureStore` |
| `app/sources/commoncrawl/discovery.py` | Crawl discovery, window selection, `cluster.idx` partition planning, `CrawlPlanner` |
| `app/sources/commoncrawl/acquisition.py` | `CCPartitionHandler` (the `cc_index_partition` handler), `CC_PARTITION_POLICY` |
| `app/sources/commoncrawl/connector.py` | Wiring (production endpoints only) |
| `app/sources/commoncrawl/cli.py` | Operator commands |
| `app/workers/acquisition_worker.py` | Worker process: identity check, `pipeline_runs` row, worker loop, signals |

Concurrency model (one PC): each worker process runs one partition at a time. It has a fixed pool of
`KB_CC_FETCH_CONCURRENCY` threads, at most `2 × concurrency` records in flight, and at most `concurrency`
HTTP connections per host. Database writes happen only on the main thread, in short transactions. For
more throughput, run more worker processes (each needs `KB_DB_POOL_SIZE` connections within the
`kb_pipeline_app` connection limit).

## 2. Configuration

All optional. Defaults are safe for one PC. Values are range-checked (`SettingsError`).

| Variable | Default | Meaning |
|---|---|---|
| `KB_CC_CRAWL_WINDOW` | `1` | Plan the newest N crawls listed in `collinfo.json` (1..50) |
| `KB_CC_CRAWLS` | — | Explicit comma-separated crawl ids; each must be listed by the index server |
| `KB_CC_BLOCKS_PER_PARTITION` | `10` | CDX blocks (~3000 index lines each) per job (1..200) |
| `KB_CC_PARTITION_STRIDE` | `1` | Plan every Nth partition (deterministic sampling for pilots) |
| `KB_CC_MAX_PARTITIONS_PER_CRAWL` | `0` (no limit) | Stop planning after N partitions |
| `KB_CC_MIME_TYPES` | `text/html,application/xhtml+xml` | Allowed MIME (index `mime-detected`, else `mime`) |
| `KB_CC_LANGUAGES` | `eng` | Keep when the index lists one of these, **or lists none**; empty disables |
| `KB_CC_REJECT_TRUNCATED` | `true` | Skip records Common Crawl marked truncated |
| `KB_CC_MAX_RECORD_BYTES` | `5242880` | Maximum compressed record length (index filter) |
| `KB_CC_MAX_DECOMPRESSED_RECORD_BYTES` | `33554432` | Decompression limit per record |
| `KB_CC_BRONZE_ROOT` | `data/bronze/commoncrawl` | Bronze root |
| `KB_CC_MIN_FREE_BYTES` | `10737418240` | Disk-space guard |
| `KB_CC_FETCH_CONCURRENCY` | `4` | Parallel range requests per worker (1..32); also the per-host pool size |
| `KB_CC_BATCH_SIZE` | `50` | Index lines per committed transaction |
| `KB_CC_CONNECT_TIMEOUT_SECONDS` / `KB_CC_READ_TIMEOUT_SECONDS` / `KB_CC_REQUEST_DEADLINE_SECONDS` | `10` / `30` / `120` | Per-request limits |
| `KB_CC_ITEM_TRANSIENT_LIMIT` | `3` | Unit retries caused by one record before it is dead-lettered |
| `KB_CC_USER_AGENT` | `kb-pipeline-cc-acquisition/1.0 (…)` | HTTP User-Agent |
| `KB_WORKER_LEASE_SECONDS`, `KB_WORKER_POLL_SECONDS`, `KB_DB_POOL_SIZE` | Runtime Engine | see `docs/runtime_engine_v1.md` |

The **filter version**, for example `f1.a9403642b97c`, is `f1.` followed by the first 12 hex characters
of the SHA-256 of the canonical filter parameters (MIME types, languages, truncation, maximum record
size, status 200).
- It is part of every partition job identity.
- Changing the filter creates new jobs; observations stay idempotent.
- The parameters are also stored in each job's checkpoint, so any worker applies exactly the filter
  the job was planned with, whatever its own environment says.

The endpoint allow-list is **not** configurable.

## 3. Job types and state transitions

Only the existing schema job type `cc_index_partition` (source `common_crawl`) is used.
- **Crawl discovery and planning** are an idempotent operator command (`plan`), not a job.
- **Job identity** (schema §16.2):
  `<crawl_id>/cc-index/collections/<crawl_id>/indexes/cdx-NNNNN.gz#filter=<filter_version>#range=<a>-<b>`
- **Partitions** `a..b` are aligned block numbers inside the shard: partition `k` covers blocks
  `k·B … k·B+B−1`, in `cluster.idx` order.

Stages: `DISCOVERED` (planned) → `ACQUISITION_PENDING` (in progress, set at the first checkpoint) → `ACQUIRED` (completed).

| From | To | Cause |
|---|---|---|
| `pending` / `retry_pending` | `running` | claim (Runtime Engine) |
| `running` | `succeeded` | all blocks processed (`stage = ACQUIRED`) |
| `running` | `retry_pending` (attempt consumed) | transient network/5xx/timeout/short or mismatched range, disk guard, internal error |
| `running` | `retry_pending` (**not** consumed, `deferral_count + 1`) | HTTP 429/503, or `source_rate_limits` says `data.commoncrawl.org` is blocked |
| `running` | `retry_pending` (**not** consumed, outcome `cancelled`) | graceful shutdown (`Yielded`) |
| `running` | `dead_letter` (+ `dead_letters` row, `item_type = 'job'`) | attempts exhausted; permanent failure of the partition itself (CDX shard 404/403, invalid job identity or checkpoint) |
| `running` (lease expired) | `retry_pending` / `dead_letter` | crash; sweeper (`recover_every`) |

Item-level failures never fail the unit. They become `dead_letters` rows with `item_type = 'observation'`:

| Item | `item_key` | `stage` | `error_class` / `error_code` |
|---|---|---|---|
| malformed index row | `cdx:<cdx key>@<block offset>#line=<n>` | `DISCOVERED` | `data_invalid` / `index_<reason>` |
| corrupt index block | `cdx:<cdx key>@<block offset>` | `DISCOVERED` | `data_invalid` / `index_block_<reason>` |
| record HTTP 4xx/3xx, range beyond EOF | `<observation_id>` | `ACQUISITION_PENDING` | `permanent_source` / `http_404`, `range_beyond_eof`, … |
| invalid/mismatching WARC record, oversized, digest mismatch | `<observation_id>` | `ACQUISITION_PENDING` | `data_invalid` / `warc_<reason>` |
| Bronze conflict or corrupt manifest | `<observation_id>` | `ACQUISITION_PENDING` | `data_invalid` / `bronze_conflict`, `bronze_corrupt` |
| record causing `KB_CC_ITEM_TRANSIENT_LIMIT` unit retries | `<observation_id>` | `ACQUISITION_PENDING` | `transient` / `item_retry_limit` |

`payload_ref` holds the canonical location JSON (`cc_observation_payload`) or the index position, so every
dead letter is reproducible. At most `max_dead_letters_per_attempt` (1000) rows are written per attempt;
the rest are counted (`dead_letters_suppressed`).

## 4. Checkpoints

The checkpoint lives in `pipeline_jobs.checkpoint`:

```json
{"v": 1,
 "cdx_key": "cc-index/collections/CC-MAIN-2024-33/indexes/cdx-00042.gz",
 "blocks": [[0, 187421], [187421, 190002]],
 "filter": {"spec": "f1", "status": 200, "mime_types": ["application/xhtml+xml", "text/html"],
            "languages": ["eng"], "reject_truncated": true, "max_record_bytes": 5242880},
 "block": 1, "line": 150,
 "item_failures": {"1:150": 1},
 "totals": {"inserted": 4210, "rejected_mime": 12011, "fetched": 4230}}
```

- `blocks` holds the exact byte ranges from `cluster.idx`, written at planning time. The handler validates
  the count against the range, and checks that the blocks are contiguous and that the filter version matches.
- `{block, line}` is the resume position: blocks and lines already processed.
- The position is committed **in the same transaction** as the observations and dead letters of that
  batch, so after any crash the job resumes exactly after the last committed batch.
- The position advances every `KB_CC_BATCH_SIZE` lines and after every block, and each commit renews the
  lease. During long batches a heartbeat renews it every `lease_seconds / 3`.
- `totals` counts committed work cumulatively across attempts.
- `processing_attempts.metrics` holds the counters of the successful attempt.

## 5. Retry and rate-limit behaviour

HTTP never retries internally: each failure is classified (`FetchError`), and the Runtime Engine policy
`CC_PARTITION_POLICY` decides what happens (8 attempts, 60 s base, 1 h cap, full jitter, 30 s rate-limit jitter).

| Condition | Class | Effect |
|---|---|---|
| timeout (connect/read), connection reset/refused, deadline exceeded | `transient` | unit retry with exponential backoff and full jitter |
| HTTP 5xx (except 503), 408 | `transient` | unit retry; `Retry-After` honoured as a minimum |
| HTTP 429, 503 ("slow down") | `rate_limited` | deferral: the attempt is not consumed; `next_attempt_at ≥ now + Retry-After + jitter`; `source_rate_limits(common_crawl, data.commoncrawl.org).retry_after_until` updated so **every** worker waits |
| range ignored (HTTP 200 to a Range request: body never read), wrong `Content-Range`, short or over-long body | `transient` | unit retry |
| HTTP 3xx (redirects are never followed), 4xx, range beyond object end | `permanent_source` | item dead letter; for the CDX shard itself, the unit dead-letters |
| invalid record, oversize, decompression limit | `data_invalid` | item dead letter (never retried) |
| disk guard, Bronze I/O error | `resource_exhausted` | unit retry with backoff |

- **Resuming after a unit failure.** Before a failure is raised, the processed prefix and the checkpoint
  are committed, so the retry starts at the failing record.
- **Poisoned records.** A record that causes `KB_CC_ITEM_TRANSIENT_LIMIT` transient unit failures is
  dead-lettered, so one bad record cannot exhaust the partition's attempts.
- **Planner failures.** The planner (`plan`) does not retry. A failure is reported, and re-running is safe.

## 6. Bronze storage

```
<KB_CC_BRONZE_ROOT>/<crawl_id>/<oid[0:2]>/<oid[2:4]>/<observation_id>.warc.gz   exact bytes [offset, offset+length) of warc_filename
<KB_CC_BRONZE_ROOT>/<crawl_id>/<oid[0:2]>/<oid[2:4]>/<observation_id>.json      manifest = completion marker
```

- **Paths** are built only from the validated crawl id and the UUID. Containment in the root is checked.
- **The `.warc.gz` file** is the original compressed WARC record (one gzip member), byte-exact. It can be
  re-parsed offline and re-verified against Common Crawl with the same range request.
- **Write protocol:**
  1. write a temporary file, then `fsync`;
  2. publish it with a hard link (`os.link`, which fails if the target already exists), then remove the
     temporary name;
  3. write the record first and the manifest last.
- **An object is complete only when** its manifest exists and the record matches the manifest's
  `record_sha256` and `record_bytes`.
- **Existing evidence is never replaced by different bytes:**
  - same bytes: no-op;
  - different bytes: `BronzeConflict`, the item is dead-lettered and nothing changes;
  - damaged or missing record under a valid manifest: restored only from bytes whose hash equals the manifest;
  - record without manifest (crash between the two writes): the manifest is added only if the bytes are equal.
- **Abandoned temporary files** (`.<name>.tmp-<hex>`) are removed by `sweep-temp`.

Manifest (`format = kbp-bronze-cc-v1`, canonical JSON):
- `observation_id`, `identity` (the canonical Identity Extension payload);
- `source_type = common_crawl`;
- `crawl_id`, `warc_filename`, `warc_offset`, `warc_length`;
- `source_url`, `retrieved_at`;
- `retrieval` (`url`, `range`);
- `index`: `cdx_key`, `block_offset`, `line`, `urlkey`, `timestamp`, `filter_version`, and the verbatim
  index `fields` (`url`, `mime`, `mime-detected`, `status`, `digest`, `length`, `offset`, `filename`,
  `charset`, `languages`, `truncated`, `redirect`);
- `warc`: `version`, `record_id`, `date`, `payload_digest`, `payload_digest_verified`, `payload_bytes`, and
  selected headers;
- `http`: `status`, and selected headers such as `content-type` and `last-modified`;
- `html_metadata`: `version = ccmeta-v1`, `title`, `canonical_url`;
- `record_sha256`, `record_bytes`.

## 7. Provenance

| Requirement | Stored in |
|---|---|
| source type | `source_observations.source_channel = 'common_crawl'`; manifest `source_type` |
| crawl id | `cc_warc_files.crawl_id` → `cc_crawls`; manifest |
| original target URL | `source_observations.source_url` = `WARC-Target-URI` (verified equal to the index URL); `url_host` |
| WARC filename / offset / length | `cc_warc_files.warc_filename`, `cc_captures.warc_record_offset`, `warc_record_length`; manifest |
| HTTP status / content type | `cc_captures.http_status`, `http_content_type` (as recorded), `detected_mime` (index) |
| capture time | `cc_captures.warc_date` (`WARC-Date`) |
| retrieval time | `source_observations.acquired_at` = manifest `retrieved_at` (the time of the byte-range fetch) |
| WARC record id, payload digest | `cc_captures.warc_record_id`, `payload_digest` (`WARC-Payload-Digest`, else the index digest) |
| redirect / canonical | `cc_captures.redirect_location` (`Location` header), `canonical_url` (literal `<link rel=canonical>`) |
| index metadata | Bronze manifest `index.fields` |
| integrity | manifest `record_sha256`; payload SHA-1 re-verified against `WARC-Payload-Digest` and the index digest |

**Never invented:**
- **`source_title`** is the literal first HTML `<title>` (entities decoded, whitespace collapsed). It is
  only taken when the charset is declared or UTF-8 decodes strictly; otherwise it is NULL.
- **`source_published_at` and any update date** are always NULL in V1 (schema O-9 is open).
  `Last-Modified` is preserved in the manifest only.
- `source_observations` and `cc_captures` are insert-only for the runtime role, so these values are
  decided at acquisition time.

## 8. Idempotency guarantees

| Level | Guarantee |
|---|---|
| Crawl | `cc_crawls` PK, `DO NOTHING`. A crawl is registered only after `collinfo.json` lists it. |
| Partition job | `UNIQUE (job_type, source_identifier)`, `DO NOTHING`. Re-planning never duplicates. Planning is deterministic (immutable `cluster.idx`, aligned blocks, ordinal sampling). |
| Observation | `observation_id` from Identity Extension V1 only. `source_observations`/`cc_captures` `ON CONFLICT (observation_id) DO NOTHING`. The natural key `UNIQUE (warc_file_id, warc_record_offset)` is left to raise. |
| WARC file | `cc_warc_files (warc_filename)` `DO NOTHING`, then SELECT. Ids are never cached across transactions. |
| Fetch | Observations already captured with complete Bronze evidence are not fetched again. Complete local evidence from a crashed attempt is reused without network. Duplicate index lines in one batch are fetched once. |
| Bronze | No clobber; identical bytes → no-op; different bytes → conflict (never replaced). |
| Dead letters | `(item_type, item_key, stage)` UPSERT: repeats increment `occurrence_count`. |

Re-running a completed partition (after `requeue` and a checkpoint reset) inserts 0 rows, fetches 0 WARC
records and rewrites 0 Bronze files. Only the index blocks are read again.

## 9. Failure recovery

| Event | Result |
|---|---|
| Process killed mid-batch | The lease expires and the sweeper sets `retry_pending` (attempt consumed). The rerun resumes at the last committed `{block, line}`. Records stored in Bronze by the dead attempt are reused, not re-fetched. |
| Worker shutdown (SIGINT/SIGTERM) | Claiming stops. In-flight fetches finish, the processed prefix is committed, and the job is released (`cancelled`, not consumed). |
| Lease lost (another worker recovered the job) | Every write is fenced by the lease token. The late worker's transaction is rolled back and its result discarded. |
| DB write fails after Bronze write | The batch rolls back, and Bronze evidence is kept. The retry reuses the Bronze bytes and inserts the rows. |
| Bronze file damaged or deleted later | `verify-bronze` reports it. A rerun of the partition restores it from a fresh byte-exact fetch whose hash must match the manifest. The DB is unchanged. |
| Disk nearly full | `resource_exhausted` before any write; retried with backoff. |
| Requeue of a dead-lettered partition | `requeue` resumes from the checkpoint. To re-scan a completed partition, the checkpoint `block`/`line` must also be reset to 0. |

## 10. Security

- **Only two origins:** `https://data.commoncrawl.org` and `https://index.commoncrawl.org` on port 443.
  - Requests with user-info, a query, a fragment, other hosts or ports, `http`, IP literals, `.`, `..` or
    empty segments, percent-encoding, backslashes or whitespace are refused before any socket is opened.
  - URLs are built only from validated relative keys.
  - Redirects are never followed.
  - TLS certificates are verified.
- **Every index field is validated:**
  - WARC filename regex under `crawl-data/<same crawl>/segments/…/(warc|crawldiagnostics|robotstxt)/…warc.gz`;
  - integer ranges, with `length` within PostgreSQL `integer`;
  - URL scheme http/https, a host, and no control characters;
  - digest format.
- **Size limits:**
  - compressed record ≤ `KB_CC_MAX_RECORD_BYTES`;
  - decompressed record ≤ `KB_CC_MAX_DECOMPRESSED_RECORD_BYTES`;
  - index block ≤ 16 MiB compressed and 128 MiB decompressed;
  - index line ≤ 64 KiB;
  - WARC/HTTP header blocks ≤ 64 KiB;
  - HTML metadata scan ≤ 256 KiB;
  - `collinfo.json` ≤ 4 MiB;
  - `cluster.idx` streamed, ≤ 2 GiB, lines ≤ 16 KiB.
- **A range must contain exactly one gzip member**: no trailing bytes and no truncation.
- **Bronze paths** are derived from validated identifiers only; path traversal is impossible.
- **No secret** is used or logged. Database URLs are never printed.

## 11. Operational commands (Windows PowerShell, repository root)

```powershell
# offline, no network
.\.venv\Scripts\python.exe -m unittest discover -s tests/unit
.\.venv\Scripts\python.exe -m unittest discover -s tests/integration -v   # loopback e2e runs; DB/network tests skip
.\.venv\Scripts\python.exe -m app.db.migration_safety

# network (index.commoncrawl.org only)
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli discover

# database + network (KB_DATABASE_URL = kb_pipeline_app, password from pgpass)
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli plan                       # configured window
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli plan --crawl CC-MAIN-2024-33
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli work --max-iterations 1    # one bounded claim
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli work                       # until Ctrl+C (graceful)
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli status

# offline maintenance
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli verify-bronze --crawl CC-MAIN-2024-33
.\.venv\Scripts\python.exe -m app.sources.commoncrawl.cli sweep-temp --older-than 3600
```

For a bounded pilot, set these before `plan`:
`$env:KB_CC_MAX_PARTITIONS_PER_CRAWL = '2'; $env:KB_CC_BLOCKS_PER_PARTITION = '1'`.

## 12. Tests

- **Offline unit tests** (`tests/unit`, no network, no DB):

  | File | Covers |
  |---|---|
  | `test_cc_index.py` | CDX parsing, hostile fields, filters, filter version, frozen observation id vector |
  | `test_cc_warc.py` | gzip members, truncation, trailing data, decompression bombs; WARC/HTTP validation; digests; title and canonical rules |
  | `test_cc_http.py` | SSRF allow-list; status classes; `Retry-After`; exact, short, wrong or ignored ranges; timeouts; deadline; streaming limits. Against a loopback server: keep-alive reuse, read timeout, no redirects, pool bound |
  | `test_cc_bronze.py` | Layout, no-clobber idempotency, conflict, repair, crash states, corrupt manifests, traversal, disk guard, temp sweep, concurrent writers |
  | `test_cc_discovery.py` | `collinfo.json`, crawl window, aligned planning, sampling, streaming stop, malformed `cluster.idx`, idempotent re-plan, configuration bounds |
  | `test_cc_acquisition.py` | Handler through the real Runtime Engine worker (FakeQueue) |
  | `test_cc_sql.py` | Every `*_SQL` constant against Migration 001A columns, grants and schema §20 conflict targets |

  `test_cc_acquisition.py` covers:
  - end-to-end provenance and Bronze;
  - rejected candidates;
  - rerun idempotency and duplicate discovery;
  - Bronze repair;
  - 429 with `Retry-After`, and the shared rate-limit gate;
  - 5xx resume at the failing record;
  - timeouts and bad ranges;
  - permanent 4xx;
  - poisoned records;
  - index block failures;
  - malformed rows and invalid records;
  - oversize records;
  - corrupt blocks;
  - tampered checkpoints;
  - crash with lease recovery (no duplicates, Bronze reuse);
  - graceful shutdown;
  - disk guard;
  - bounded concurrency and backpressure.
- **Integration tests** (`tests/integration`):

  | File | When it runs |
  |---|---|
  | `test_cc_offline_e2e.py` | By default. Planner, worker and Bronze over a loopback HTTP server, with rerun |
  | `test_cc_postgres.py` | Opt-in: `KB_RUN_PG_INTEGRATION=1` plus `KB_INTEGRATION_DATABASE_URL`. Refuses any database except `kb_pipeline_test`. No network |
  | `test_cc_network_smoke.py` | Opt-in: `KB_RUN_CC_NETWORK_SMOKE=1`. Real Common Crawl: 1 collinfo, 1 streamed `cluster.idx` line, 1 CDX block, ≤ 3 WARC ranges, Bronze in a temp dir, no DB |

## 13. Known limitations and open items

- **Language filter.** It uses the index `languages` hint. Records without a hint are kept, and the
  actual language is decided downstream.
- **Title and canonical.** Only V1 literal extraction (`ccmeta-v1`) is implemented. Publication dates are
  never derived; schema O-9 is open.
- **Small files.** One file pair per record means very many small files at scale. A pack format is future work.
- **Reprocessing.** Reprocessing a completed partition requires resetting its checkpoint (section 9).
- **Tuning.** Worker counts, batch sizes, lease duration and retry base/cap are schema O-20
  (**benchmark**). The defaults are conservative.

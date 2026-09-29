# GitHub Acquisition V1

| | |
|---|---|
| Status | Implemented. Tested offline: mocked API, a loopback HTTP end-to-end test, and an in-memory store. **No real GitHub request and no database connection have been made.** The opt-in network smoke test has not been run. |
| Uses | kbp Schema V1 / Migration 001A unchanged: `github_repositories`, `github_commits`, `github_file_observations`, `source_observations`, and through Runtime Engine V1 `pipeline_jobs`, `processing_attempts`, `dead_letters`, `source_rate_limits`. Existing job types only: `gh_repository_discovery`, `gh_repository_snapshot`. No `public` access. |
| Identity | Identity Extension V1 (LOCKED), unchanged: `github_observation_id(repository_id, commit_sha, path, blob_sha)`. SHA-1 and SHA-256 object formats are both supported. |
| Not included | Extraction/filtering (downstream), Gold publication, Common Crawl changes. |

## 1. Architecture

```
plan (no network) ──> gh_repository_discovery jobs, one per creation-date window and page
     │  worker: GitHub search (qualifiers only) ──> local qualification ──> UPSERT github_repositories (by numeric id)
     │          HEAD of the default branch (If-None-Match on api_etag; 304 = unchanged, costs no quota)
     └────────> gh_repository_snapshot "<repository_id>@<commit_sha>", only when HEAD != last_snapshot_commit_sha
                     │  GET /repositories/{id} (still public?) ──> git commit (exact sha) ──> github_commits
                     │  recursive tree (bounded) ──> Bronze snapshot manifest (full listing + deletions)
                     │  qualify files on metadata ──> skip (path, blob) already observed ──> bounded pool:
                     │  git blob (raw, size-bounded) ──> verify git blob hash ──> UTF-8 text check ──> Bronze
                     └> one transaction per batch: source_observations + github_file_observations + checkpoint
                        (the last batch also advances last_snapshot_commit_sha with a compare-and-set)
```

| Module (`app/sources/github/`) | Responsibility |
|---|---|
| `config.py` | `GitHubSettings.from_env()`: versioned repository and file policies, resources, optional token (never rendered) |
| `client.py` | `api.github.com` allow-list, bounded GET, ETag/304, token only in `Authorization`, `redact()` |
| `rate_limit.py` | `x-ratelimit-*` parsing, primary/secondary classification, quota-reserve guard |
| `discovery.py` | Windows, job identities, repository qualification, `plan_discovery`, `RepositoryDiscoveryHandler` |
| `acquisition.py` | `RepositorySnapshotHandler` |
| `files.py` | Tree parsing, file qualification, git blob hash, text check |
| `bronze.py` | Bronze layout and write-once manifests |
| `repository.py` | `*_SQL` constants, `PostgresGitHubStore` |
| `common.py` | Shared gate, rate recording, error mapping, bounded ordered pool |
| `connector.py`, `cli.py` | Wiring and operator commands |

The worker entry point is `app/workers/acquisition_worker.py:run_github_worker`.

## 2. Automated discovery

- **Windows.** The public repository space is partitioned by creation date. The windows run from
  `KB_GH_CREATED_FROM` to today in steps of `KB_GH_WINDOW_DAYS`, newest first. They are deterministic,
  and no repository or topic is hard-coded.
- **The query** consists only of policy qualifiers:
  `is:public fork:false stars:>=N size:<=K archived:false created:A..B`.
- **Job identity:** `ghs1.<policy version>.<epoch>.<YYYYMMDD>-<YYYYMMDD>#page=<n>` (schema §16.2).
- **Result cap.** Page 1 handles the search API's 1000-result limit. A larger window is split into two
  half windows. Otherwise, pages 2..n are enqueued.
- **Single days over the cap.** A single day with more than 1000 qualifying repositories keeps its first
  1000, and the metric `window_saturated` records it.
- **Identity is the numeric `repository_id`.** Owner and name are mutable metadata: a rename or transfer
  updates the same row. Snapshots re-read the repository by id (`/repositories/{id}`), so they never
  follow a redirect.
- **Incremental operation.** The epoch (`KB_GH_DISCOVERY_EPOCH`, default: ISO year-week) is part of the
  job id.
  - Running `plan` in a new epoch re-walks every window. Unchanged repositories cost one conditional
    HEAD request, and a 304 costs no quota.
  - Re-planning within the same epoch is a no-op.

## 3. Repository qualification

Qualification is configurable, and the settings form the policy version `r1…`.
- A repository is **rejected** when it is not public (`private` is not `false`, or visibility is not
  `public`), disabled, a fork, archived, above `KB_GH_MAX_REPO_SIZE_KB`, below `KB_GH_MIN_STARS`, or
  outside the optional `KB_GH_LANGUAGES` / `KB_GH_LICENSES` lists, or when its default branch is not a
  safe ref.
- **Stored in `github_repositories`:** `repository_id`, `node_id`, `owner_login`, `repo_name`, `html_url`,
  `default_branch`, `visibility`, `license_spdx_id`, `is_fork`, `is_archived`, `api_etag`,
  `last_snapshot_commit_sha`, `first_seen_at`, `metadata_refreshed_at`.
- **Metadata the schema has no column for** is kept verbatim in the Bronze snapshot manifest: size,
  primary language, stars, topics, `created_at` / `updated_at` / `pushed_at`.
- Missing values stay missing; nothing is inferred.

## 4. Commit / version strategy

- **Snapshots** cover the default-branch HEAD (schema O-8 default). The commit is fetched by exact sha and
  checked, and `github_commits` gets the tree sha, `ref_name`, `committed_at` and
  `visibility_at_snapshot='public'` (a CHECK constraint).
- **Snapshot mode.** It is `incremental` when a previous snapshot exists (`previous_snapshot_sha`), and
  `full` otherwise.
- **Unchanged repositories.** A 304, or a HEAD equal to `last_snapshot_commit_sha`, means no snapshot job.
- **Changed commits** get a new job `<id>@<new sha>`. Files whose `(path, blob_sha)` has already been
  observed for this repository are not re-observed or downloaded; their earlier observation remains
  their provenance (schema §8.1).
- **Deleted files** are recorded in the new snapshot manifest (`changes.deleted`, plus added/modified
  counts). Earlier observations and Bronze are never removed.
- **Advancing `last_snapshot_commit_sha`.** The compare-and-set (`WHERE last_snapshot_commit_sha IS NOT
  DISTINCT FROM <previous>`) runs in the final batch transaction, so an older snapshot finishing late
  can never move it backwards.

## 5. File qualification

Qualification uses tree metadata only, before any download. The settings form the policy version `p1…`.

| Rule | Default |
|---|---|
| Entry type | regular files `100644`/`100755` only. Symlinks (`120000`) and submodules (`160000`) are skipped and never followed |
| Path | Identity Extension V1 rules (no `..`, `.`, empty, leading/trailing `/`, NUL, surrogates), no control characters. Paths are never used on the filesystem |
| Excluded directories | `node_modules`, `vendor`, `third_party`, `dist`, `build`, `target`, `.git`, `.github`, `__pycache__`, `site-packages`, … (`KB_GH_EXCLUDED_DIRS`) |
| Credential artifacts | `.env*`, `*.pem`, `*.key`, `id_rsa*`, `.npmrc`, `.netrc`, `credentials*`, `secrets*`, `*.tfstate`, … |
| Generated files | lock files, `*.min.js/css`, `*.map`, generated protobuf |
| Extensions | Documentation (`.md`, `.rst`, `.txt`, `.adoc`, …), common source code, structured text (`.json`, `.yaml`, `.toml`, `.csv`, …), plus `README`/`LICENSE`/`CHANGELOG`/`Makefile`/… (`KB_GH_FILE_EXTENSIONS`) |
| Size | 1 byte ≤ size ≤ `KB_GH_MAX_FILE_BYTES` (1 MiB) |
| Count | `KB_GH_MAX_FILES_PER_SNAPSHOT` (2000), documentation first, then by path (deterministic) |

After download, content must hash to its git blob id and be NUL-free UTF-8.
- **Hash mismatch:** item dead letter `blob_hash_mismatch`. The content is never stored.
- **Binary or non-UTF-8 content:** skipped, and cached as `rejected/<blob>.json` so it is never downloaded again.

## 6. Bronze storage

```
<KB_GH_BRONZE_ROOT>/blobs/<b[0:2]>/<b[2:4]>/<blob_sha>                          content (verified against the git blob id)
<KB_GH_BRONZE_ROOT>/rejected/<b[0:2]>/<blob_sha>.json                          binary / non-UTF-8 negative cache
<KB_GH_BRONZE_ROOT>/repos/<repository_id>/<commit_sha>/snapshot.json           repository + commit + full blob listing + changes
<KB_GH_BRONZE_ROOT>/repos/<repository_id>/<commit_sha>/files/<observation_id>.json   file provenance manifest
```

- **Write protocol.** Files are written to a temp file, fsynced, then published with a hard link, which
  never overwrites (`app/utils/files.py`).
- **Blobs are content-addressed and self-verifying.** Identical content is stored once. Content that does
  not match its id is refused before any write. A damaged blob file is detected on read and restored only
  from verified bytes.
- **Manifests are write-once.** The first retrieval's provenance is kept, and a repeat is a no-op.
- **File manifest fields:**
  - identity: `source_type=github`, `observation_id`, `identity` (canonical Identity Extension payload);
  - repository: `repository_id`, `owner_login`, `repo_name`, `html_url` (at observation time);
  - file: `commit_sha`, `blob_sha`, `path`, `mode`, `size`, `content_sha256`;
  - URLs: `source_url`, `api_url`;
  - `retrieved_at` and `file_policy_version`.
- **Snapshot manifest:** the repository object subset, commit dates and parents, `snapshot_mode`,
  `changes`, the tree listing, and `retrieved_at`. Commit author identities are deliberately not stored.

## 7. Provenance

| Requirement | Stored in |
|---|---|
| source type | `source_observations.source_channel='github'`; manifests |
| numeric repository id, owner/name, URL | `github_repositories` (last observed); file manifest (at observation time) |
| commit / tree | `github_commits.commit_sha`, `tree_sha`, `ref_name`, `committed_at` (commit metadata, **not** a publication date) |
| path / blob / size | `github_file_observations.file_path` (verbatim), `blob_sha`, `file_size_bytes` |
| commit-pinned URL | `source_observations.source_url = https://github.com/<owner>/<repo>/blob/<commit_sha>/<percent-encoded path>`, `url_host = github.com` |
| retrieval time | `source_observations.acquired_at` = manifest `retrieved_at` |
| integrity | git blob hash (verified), `content_sha256` |

`source_title` and `source_published_at` are always NULL; nothing is invented.

## 8. Rate limiting

- **Headers.** `x-ratelimit-limit`, `-remaining`, `-reset` and `-resource` are tracked per resource
  (`core`, `search`).
- **Primary limit:** a 403 or 429 with `remaining=0`. The job is deferred until `reset`.
- **Secondary limit:** a 429, or a 403 with `Retry-After` or a secondary-limit message. The job is
  deferred for `Retry-After` (default 60 s).
- **Deferrals do not consume attempts** (`deferral_count + 1`).
- **Shared state.** Every batch records the latest state in `source_rate_limits(github, <resource>,
  <credential fingerprint>)`.
  - The fingerprint is `''` when unauthenticated, otherwise a hash of a hash of the token (or of
    `KB_GH_TOKEN_IDENTITY`).
  - Each job first checks that state and defers, without any request, while the resource is blocked.
- **Quota reserve.** The client refuses a request once `remaining ≤ KB_GH_QUOTA_RESERVE` before `reset`,
  so there is no busy-loop and the quota is never burned to zero.
- **No internal retries.** The HTTP layer never retries: 5xx, timeouts and short reads are `transient`,
  and the Runtime Engine backs off and resumes from the checkpoint.
- **Poisoned files.** A file causing 3 unit retries is dead-lettered (`item_retry_limit`).

## 9. Idempotency

- **Same repository + commit + blob + path** gives the same `observation_id`.
  - `ON CONFLICT (observation_id) DO NOTHING`.
  - The natural key `UNIQUE (repository_id, commit_sha, file_path)` is left to raise.
  - The blob is stored once in Bronze, and the manifest is write-once.
  - No refetch: an already-observed `(path, blob)` is skipped before download. A crash-surviving blob is
    reused from Bronze. A rejected blob is cached.
- **Jobs.** `UNIQUE (job_type, source_identifier)`: re-planning and re-discovery never duplicate.
  Snapshots are keyed `<id>@<sha>`, so an unchanged repository never gets a second snapshot.
- **Repositories.** The UPSERT on `repository_id` only updates when metadata actually changed.
- **Rerunning an unchanged state** yields 0 new rows, 0 blob downloads and 0 Bronze writes. Tests prove this.

## 10. Checkpoint and recovery

| Job | Checkpoint | Resume |
|---|---|---|
| discovery page | `{v, policy, done: [repository ids]}` | the page is re-queried; processed ids are skipped |
| snapshot | `{v, owner, name, ref, previous, next, item_failures}` | commit and tree are re-read (immutable) and the candidate list rebuilt deterministically; processing resumes at `next` |

Checkpoints are committed in the same transaction as the rows of their batch.
- **Crash:** lease recovery, then resume. Blobs written before the crash are reused.
- **Graceful shutdown:** the job is returned between batches (`cancelled`, attempt not consumed).
- **Lease loss:** fenced writes roll back.

## 11. Authentication

- `KB_GH_TOKEN` is optional. Unauthenticated access (60 requests/hour) is enough for the smoke test.
- The token is read only from the environment, sent only as `Authorization: Bearer` to `api.github.com`,
  and hidden from every `repr`, exception and log message (`redact()` is available for any text).
- Never put a token in a file in the repository.

## 12. Configuration

See the `app/sources/github/config.py` docstring for the full list. The main variables:

| Variable | Default |
|---|---|
| `KB_GH_TOKEN`, `KB_GH_TOKEN_IDENTITY` | unset |
| `KB_GH_CREATED_FROM`, `KB_GH_WINDOW_DAYS`, `KB_GH_MAX_WINDOWS`, `KB_GH_DISCOVERY_EPOCH` | `2010-01-01`, `30`, `0` (all), `auto` |
| `KB_GH_MIN_STARS`, `KB_GH_MAX_REPO_SIZE_KB`, `KB_GH_ALLOW_FORKS`, `KB_GH_ALLOW_ARCHIVED`, `KB_GH_LANGUAGES`, `KB_GH_LICENSES` | `50`, `200000`, `false`, `false`, any, any |
| `KB_GH_FILE_EXTENSIONS`, `KB_GH_EXCLUDED_DIRS`, `KB_GH_MAX_FILE_BYTES`, `KB_GH_MAX_FILES_PER_SNAPSHOT`, `KB_GH_MAX_TREE_ENTRIES` | see §5, `1048576`, `2000`, `100000` |
| `KB_GH_BRONZE_ROOT`, `KB_GH_MIN_FREE_BYTES` | `data/bronze/github`, 10 GiB |
| `KB_GH_FETCH_CONCURRENCY` (also the connection count), `KB_GH_BATCH_SIZE`, `KB_GH_QUOTA_RESERVE` | `2`, `50`, `50` |
| `KB_GH_CONNECT_TIMEOUT_SECONDS`, `KB_GH_READ_TIMEOUT_SECONDS`, `KB_GH_REQUEST_DEADLINE_SECONDS` | `10`, `30`, `120` |

## 13. Operational commands (PowerShell, repository root)

```powershell
.\.venv\Scripts\python.exe -m app.sources.github.cli plan --max-windows 1   # DB only: enqueue the newest window
.\.venv\Scripts\python.exe -m app.sources.github.cli work --max-iterations 2 # network + DB + Bronze, bounded
.\.venv\Scripts\python.exe -m app.sources.github.cli status                 # read-only counts
```

For a bounded pilot, set these first: `$env:KB_GH_MAX_WINDOWS='1'; $env:KB_GH_MIN_STARS='1000';
$env:KB_GH_MAX_FILES_PER_SNAPSHOT='20'`.

## 14. Tests and the smoke-test procedure

- **Offline unit tests:**

  | File | Covers |
  |---|---|
  | `tests/unit/test_gh_core.py` | Identity vectors (SHA-1 and SHA-256) and rename independence, blob hash and integrity, file and repository qualification, binary detection, tree bounds, endpoint allow-list, rate classification and reserve, token redaction, windows |
  | `tests/unit/test_gh_acquisition.py` | Through the Runtime Engine worker: end-to-end provenance, rerun idempotency, incremental commit with deletions, rename, primary and secondary limits, 5xx and timeout resume, 404 and integrity dead letters, binary cache, crash recovery, no-longer-public, truncated tree, window split, bounded concurrency, token not leaked |
  | `tests/unit/test_gh_sql.py` | Every `*_SQL` constant against Migration 001A columns, grants (UPDATE only on granted columns) and conflict targets |

- **Offline integration test:** `tests/integration/test_gh_offline_e2e.py`. A loopback HTTP server runs
  discovery, snapshot and rerun with 304.
- **Real-network smoke test** (opt-in; not run). It uses at most 9 GitHub API requests, no database, and
  Bronze in a temp dir:

```powershell
$env:KB_RUN_GH_NETWORK_SMOKE = '1'
.\.venv\Scripts\python.exe -m unittest tests.integration.test_gh_network_smoke -v
Remove-Item Env:KB_RUN_GH_NETWORK_SMOKE
```

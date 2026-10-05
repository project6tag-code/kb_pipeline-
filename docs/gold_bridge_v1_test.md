# DCA_v2 V1 Gold Source Bridge — Development/Test Contract (kb_pipeline_test)

Status: **implemented and live-validated on `kb_pipeline_test` (2026-10-01)**. Gold publication has not started. The implementation is commit `997671f`; that commit message predates the Stage B validation, which passed afterwards (§9).

Code: `app/processing/gold_bridge/` (`spec.py`, `bootstrap.py`, `cli.py`).

## 1. Scope and project decision

- This is the **DCA_v2 V1 test/development bridge contract** for `kb_pipeline_test` only.
- By explicit project decision, the **actual** `kb_pipeline_test` public schema is authoritative: the 5-column `public.datasets` and `public.dataset_sources` stubs. The historical legacy Gold schema is not reconstructed.
- **The old Schema V1 §14.3 column set does not apply here**: description, dataset_url, created_at and `uq_datasets_source_name` do not exist in this database.
- **`data_collection_pipeline` is untouched.** Production rollout needs a separate legacy compatibility review, and none of these rows or values propagate automatically.

## 2. `KB_REFERENCE`

`public.datasets.status = 'KB_REFERENCE'` means:

> This dataset row exists only as a stable provenance / Gold bridge reference for the DCA_v2 KB pipeline. It is not an acquisition workflow state.

- It is a **DCA_v2 V1 control-plane value**, authorized initially for `kb_pipeline_test` only.
- **Legacy values are deliberately not reused.** `ACCEPTED`, `DISCOVERED` and `REJECTED` (legacy audit A2) are workflow states of the legacy collector on third-party datasets. The legacy consumer of `ACCEPTED` is unknown (Schema V1 O-1, privileges §11), and a bridge row must never look like collectable work.
- **No schema change is needed.** The column is `varchar(50) NOT NULL` with no CHECK, enum or default.

## 3. Rows (frozen in `spec.py`)

**`public.dataset_sources`:**

| id | name | source_type | authentication_required | status |
|---|---|---|---|---|
| `3a709ec5-0944-521c-b811-26dda33b186d` | `common_crawl` | `document` | `false` | `ACTIVE` |
| `bd32eea2-9888-5c0f-b9b9-dcad3e9caf90` | `github` | `dataset_repository` | `true` | `ACTIVE` |

- **name, source_type:** project decision; identical to the audited legacy rows of the same names.
- **authentication_required, status:** taken from those audited legacy rows (A2). All eight audited parents are `ACTIVE`.
  - GitHub's `true` describes the legacy `github` source registry entry. The KB pipeline's own GitHub client treats the token as optional.
- **id:** UUIDv5(`KB_NAMESPACE`, `dataset-source:v1:<name>`). It is deterministic and local to this test bridge. Per Identity Extension V1, consumers look parents up by `name`, never by id.

**`public.datasets`** (ids frozen by Identity Extension V1):

| id | source_id | dataset_name | status | metadata |
|---|---|---|---|---|
| `a7d8d2e1-8400-5916-9440-b492e8e692ce` | common_crawl parent | `Common Crawl` | `KB_REFERENCE` | `{"purpose": "dca_v2_gold_bridge", "source_channel": "common_crawl"}` |
| `b487f559-5f8e-56ba-9746-28658b9d09d1` | github parent | `Public GitHub` | `KB_REFERENCE` | `{"purpose": "dca_v2_gold_bridge", "source_channel": "github"}` |

There are no dates, URLs or descriptions: nothing is presented as a source fact.

**`kbp.source_dataset_map`:**

| source_channel | dataset_source_name | bridge_identity_name | dataset_id |
|---|---|---|---|
| `common_crawl` | `common_crawl` | `dataset-bridge:v1:common_crawl` | `a7d8d2e1-…` |
| `github` | `github` | `dataset-bridge:v1:github` | `b487f559-…` |

Any other channel is refused, by `spec.channel()` and by the table's own CHECK.

## 4. Two-stage privilege model

- **Stage A, admin** (`admin-bootstrap`, `KB_ADMIN_DATABASE_URL`). A superuser, or the owner (`postgres`) of the public tables, writes the two parent rows and the two dataset rows. It refuses the pipeline roles.
- **Stage B, migrator** (`map-bootstrap`, `KB_MIGRATION_DATABASE_URL`). `kb_pipeline_migrator` owns `kbp`. It verifies the Stage A rows (SELECT only), then writes `kbp.source_dataset_map`.
- **Why the admin role.** The public tables are owned by `postgres`. The migrator has SELECT only, and the runtime role cannot read them. Granting a permanent INSERT on a public table to a pipeline role would widen the attack surface for a one-time act. **No privilege or owner is changed.** The bootstrap is checked to issue no GRANT/REVOKE/ALTER/UPDATE/DELETE, and a live fingerprint of owners, ACLs, columns and constraints must be identical before and after.
- **Credentials.** The admin credentials are the operator's own (their pgpass or `PGPASSWORD`). The pipeline never stores or reads them.

## 5. Guards, idempotency and conflicts

Before any write, both stages verify:

- `current_database()` equals `--expect-database`, which must be `kb_pipeline_test` (`data_collection_pipeline` is always refused);
- the kbp revision is the current head, `kbp_001d` (the bridge rows were live-validated at `kbp_001c` and are unchanged by 001D);
- the expected role is connected;
- the public schema matches **exactly**: columns, types, NOT NULL, no defaults or identity, the four constraints, no triggers.

**Insert-or-verify only.** Each row is looked up by deterministic id **and** natural key:

- **Missing:** `INSERT … ON CONFLICT DO NOTHING`, then re-read and verify.
- **Identical:** reused.
- **Anything else: `BridgeConflict`.** This covers the same id with different fields, the same name with another id, a dataset with the wrong parent, a dataset name already used by another id, and a map row pointing elsewhere. The transaction rolls back.

There is no UPDATE path. A second run changes nothing: every action is `reused`.

## 6. Provenance resolution (for later Gold)

```
chunk_occurrences.chunk_id → chunk_registry.representative_observation_id
  → source_observations.source_channel ('common_crawl' | 'github')
  → kbp.source_dataset_map.dataset_id → public.datasets.id (a7d8d2e1… | b487f559…)
```

`cli resolve` proves this on real pilot chunks, read-only as the runtime role, and `bootstrap.resolve_dataset_id` is the lookup Gold will use. Until the map rows exist, it refuses with "Gold publication is blocked".

## 7. Operator runbook (kb_pipeline_test only)

```
python -m app.processing.gold_bridge.cli plan
set KB_MIGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_migrator@localhost:5432/kb_pipeline_test
python -m app.processing.gold_bridge.cli status --expect-database kb_pipeline_test           # BEFORE
set KB_ADMIN_DATABASE_URL=postgresql+psycopg://postgres@localhost:5432/kb_pipeline_test      # operator credentials
python -m app.processing.gold_bridge.cli admin-bootstrap --expect-database kb_pipeline_test  # Stage A (x2 for rerun)
python -m app.processing.gold_bridge.cli map-bootstrap --expect-database kb_pipeline_test    # Stage B (x2 for rerun)
python -m app.processing.gold_bridge.cli status --expect-database kb_pipeline_test           # AFTER (same fingerprint)
set KB_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
python -m app.processing.gold_bridge.cli resolve --expect-database kb_pipeline_test
```

## 8. Limitations

- **Test/development only.** Production needs the legacy compatibility review: the meaning of `KB_REFERENCE` to legacy consumers, the real legacy parents, and the legacy column set.
- **Parent ids are local to this bridge.** Portable lookups must use `dataset_sources.name`.
- **Stage A depends on the operator's admin credentials.**
- **`status` output.** Redirected `status` output (for example `bridge_before.json`) is local audit output. It is git-ignored (`/bridge_*.json`) and never committed.

## 9. Live validation record (kb_pipeline_test, 2026-10-01)

| Step | Result |
|---|---|
| Stage A run 1 (operator, admin) | `dataset_sources` 0→2, `datasets` 0→2, map 0; all four rows `created` |
| Stage A run 2 | all four `reused`; counts unchanged |
| Stage A rows, re-read as migrator | exactly the §3 contract; no conflict rows |
| Stage B run 1 (`kb_pipeline_migrator`) | map 0→2; `common_crawl` → `a7d8d2e1…`, `github` → `b487f559…`, both `created` |
| Stage B run 2 | both `reused`; counts unchanged (2 / 2 / 2) |
| provenance (`resolve`, runtime role) | CC chunk `0b124dba…` → document `050c3df7…` → observation `26ca9144…` → `common_crawl` → `a7d8d2e1…`; GitHub chunk `02e14bf2…` → document `00113948…` → observation `e7fae245…` → `github` → `b487f559…`; both equal the frozen ids |
| privilege/schema fingerprint | `a5d032c9…` before Stage A, after Stage A and after Stage B: **identical** (owners, ACLs, columns, constraints of `public.datasets`, `dataset_sources`, `kb`, `metadata`, `kbp.source_dataset_map`) |
| other state | Stage B changed only `kbp.source_dataset_map` (0→2). Chunks, occurrences, documents, observations, dedup decisions and relationships, the embedding model, results, vector hashes, queue, dead letters and the Stage A public rows all have identical content digests |
| PostgreSQL test | guards and schema contract OK; Stage B rerun, resolution and unchanged fingerprint OK; the admin-stage test skips without `KB_ADMIN_DATABASE_URL` (Stage A was validated by the operator runs above) |

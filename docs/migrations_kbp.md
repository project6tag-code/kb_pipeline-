# kbp Migrations — Migration 001A and the Live Test Runbook

| | |
|---|---|
| Status | **Prepared for review (Live Database Phase 1A). NOT executed against PostgreSQL.** No role, schema, grant or migration has been applied. |
| Implements | `docs/database_schema_v1.md` (LOCKED) §5, §6, §21.1–21.2, §22–23; `docs/database_privileges_v1.md` (LOCKED) §3–§6, §8–§10, §13 |
| Revision | `kbp_001a` (base of the isolated `kbp` lineage, branch label `kbp`) |

---

## 1. Files

| File | Purpose | Writes? |
|---|---|---|
| `alembic.ini` | Alembic config for the `kbp` lineage. It contains **no database URL**. | — |
| `migrations/env.py` | Environment: version table `kbp.alembic_version`, autogenerate refused, `kbp`-only filters, online preflight | — |
| `migrations/script.py.mako` | Template for future revisions (raw SQL via `op.execute`) | — |
| `migrations/versions/kbp_001a_schema_v1.py` | **Migration 001A**: the 25 `kbp` tables, the approved FK, 8 indexes, runtime grants | DDL in `kbp` |
| `app/db/migration_config.py` | Isolation constants, include filters, URL handling, read-only preflight checks | — |
| `app/db/migration_safety.py` | Static safety checker (CLI: `python -m app.db.migration_safety`) | — |
| `scripts/db/pre_migration_snapshot.sql` | Evidence snapshot of the legacy objects; prints the baseline lines | **read only** |
| `scripts/db/provision_kbp_roles.sql` | One-time administrative provisioning of the two roles and schema `kbp` | **yes** (administrator, manually) |
| `scripts/db/verify_kbp_provisioning.sql` | Verifies roles, attributes, ownership, public privileges, default privileges | **read only** |
| `scripts/db/verify_migration_001a.sql` | Verifies the database after Migration 001A, plus legacy immutability | **read only** |
| `scripts/db/verify_downgrade_001a.sql` | Verifies the database after `alembic downgrade base`, plus legacy immutability | **read only** |
| `scripts/db/permission_probes_app.sql` | Runtime permission tests, run as `kb_pipeline_app` (privileges §13 step 9) | no (probes touch no row; rolled back) |
| `scripts/db/lib/*.sql` | Shared parts: legacy fingerprint, baseline comparison, final verdict | **read only** |
| `tests/unit/test_migration_001a.py` | Migration vs locked specs (tables, columns, defaults, controlled lists, keys, FKs, indexes, grants, downgrade) | — |
| `tests/unit/test_migration_safety.py` | Checker rejects forbidden operations and allows the approved FK | — |
| `tests/unit/test_alembic_env.py` | Version table, filters, env behaviour, preflight, secret hygiene | — |
| `tests/unit/test_db_scripts.py` | The SQL scripts: read-only, no secrets, expected sets derived from the migration, provisioning grant set | — |
| `.gitignore`, `.env.example` | Secret safety: `.env` files are ignored; the example contains placeholders only | — |

## 2. Isolation design

- **Version table:** `kbp.alembic_version`, via `version_table="alembic_version"`
  and `version_table_schema="kbp"`.
  - `public.alembic_version` (legacy head `i4j5k6l7m8n9`) is never read,
    stamped or modified.
  - `kbp_001a` has `down_revision = None`: this lineage does not chain onto the
    legacy one.
- **Autogenerate is refused** by `env.py`, and `target_metadata = None`.
  - Every migration is hand-written raw SQL.
  - As defense in depth, `include_name` and `include_object` admit only schema
    `kbp`. Even a misconfigured autogenerate could never see, and so never
    DROP or ALTER, `public` objects.
- **Schema provisioning:** Alembic expects `kbp` to exist and never creates
  it. Two layers fail clearly if it is missing:
  1. `env.py` preflight (online; read-only catalog queries):
     - `kbp` exists;
     - the session is connected as `kb_pipeline_migrator` and is not a
       superuser;
     - `kb_pipeline_migrator` owns `kbp`;
     - `kb_pipeline_app` exists with no elevated attributes;
     - `public.datasets` exists;
     - the session has `REFERENCES (id)` on it.
  2. Migration 001A starts with a read-only `DO` guard that re-checks the
     schema, its owner and `kb_pipeline_app`. The guard also protects
     offline-rendered (`--sql`) scripts.
- **Transactions:** `transaction_per_migration=True`. PostgreSQL DDL is
  transactional, so 001A is all-or-nothing. It sets `SET LOCAL lock_timeout =
  '5s'` and `statement_timeout = '60s'` (schema §23).
- **Connection variables** (privileges §10, standardized in Phase 1A):
  `KB_MIGRATION_DATABASE_URL` (`kb_pipeline_migrator`, migrations and
  bootstrap only) and `KB_DATABASE_URL` (`kb_pipeline_app`, the application).
  The environment refuses to migrate if the two are equal. `env.py` does not
  read `.env` files; the variable must be in the process environment.

## 3. What Migration 001A does and does not do

It **does**:

- create the 25 Schema V1 tables with the exact columns, types, nullability,
  defaults, primary keys, UNIQUE constraints, CHECKs and FKs of schema §5 and
  §6;
- create the cross-schema FK `kbp.source_dataset_map.dataset_id →
  public.datasets(id)`, `ON DELETE NO ACTION`, first `NOT VALID` and then
  `VALIDATE` (schema §23);
- create the 8 indexes: the partial unique `active_gold` index (§5.16/§21.1)
  and the 7 initial-operation indexes (§21.2);
- grant `kb_pipeline_app` exactly the privileges of the privilege matrix
  (privileges §5.1):
  - `SELECT` on all 25 tables and on `kbp.alembic_version`;
  - `INSERT` on 21 tables;
  - column-level `UPDATE` on 5 tables;
  - table-level `UPDATE` on 4 tables;
  - `DELETE` only on `embedding_queue`.

It does **not**:

- create, alter or drop any `public` object (`kb`, `metadata`, `datasets`,
  `dataset_sources`, `alembic_version`, …);
- create schemas, roles or extensions, or set default privileges (these are
  administrative provisioning, `scripts/db/provision_kbp_roles.sql`);
- **insert the Gold bridge rows.** `kbp.source_dataset_map` stays **empty**.
  The frozen bridge ids (`a7d8d2e1-…`, `b487f559-…`) do not appear in the
  migration. The bridge bootstrap is **deferred** until the semantics of
  `datasets.status = 'ACCEPTED'` are confirmed (privileges §11);
- create any pgvector object or ANN index. `public.kb` stays `vector(768)`,
  untouched, and no embedding model is chosen.

**Downgrade:**

- revokes the `kbp.alembic_version` grant;
- drops the 25 tables in reverse dependency order, with `RESTRICT`. This also
  removes their indexes, identity sequences, grants, the FK to
  `public.datasets` and that FK's internal RI triggers on `public.datasets`;
  `public.datasets` rows are not touched;
- never touches `public` data;
- keeps schema `kbp` (administratively provisioned) and `kbp.alembic_version`
  (Alembic-managed; empty after `downgrade base`).

All data in `kbp` is lost on downgrade, so take `pg_dump --schema=kbp` first.

### 3.1 Review of the transcription interpretations (Phase 1A)

Each interpretation made while transcribing the locked schema was compared
directly with the literal text of `docs/database_schema_v1.md`.

| # | Interpretation | Locked text | Classification | Result |
|---|---|---|---|---|
| 1a | `DEFAULT now()` on columns named `created_at` (8 tables) | §5 conventions: "`created_at`-style columns are `NOT NULL DEFAULT now()` unless stated" | **EXACTLY SPECIFIED** | kept |
| 1b | `DEFAULT now()` also on `registered_at` (cc_crawls, cc_warc_files), `first_seen_at` (github_repositories, dead_letters), `enqueued_at` (embedding_queue), `updated_at` (pipeline_jobs, source_rate_limits), `first_reconciled_at` (legacy_kb_reconciliation) | The table rows state only `NOT NULL`; no default is stated for these columns. Treating them as "`created_at`-style" was an inference. | **LOGICAL BUT NOT SPECIFIED** | **removed.** They stay `NOT NULL` without a default: the application supplies the value, and an omission fails loudly. Adding a default later is an additive, reviewed migration. |
| 2 | The 8-value error-class CHECK on `pipeline_jobs.last_error_class`, `embedding_queue.last_error_class`, `processing_attempts.error_class`, `dead_letters.error_class` | §5.18 "Same CHECK list as section 17.2"; §5.19 "CHECK list in section 17.2"; the §17.2 heading "Error classes (`last_error_class`, `error_class` CHECK list)" names both column names, and `error_class` exists only in `processing_attempts` and `dead_letters` | **EXACTLY SPECIFIED** | kept |
| 3 | The 18-value stage CHECK on `pipeline_jobs.stage` and `dead_letters.stage` | §5.19 "CHECK in the state list of section 16.3"; §16.3 heading "Stages (`pipeline_jobs.stage`, `dead_letters.stage`)" and "The allowed stage values are: …" | **EXACTLY SPECIFIED** | kept |
| 4 | Environment verifies, never creates, schema `kbp` | Schema §22 said `CREATE SCHEMA IF NOT EXISTS`; privileges §3.2 (LOCKED) refines it | recorded refinement (privileges §3.2) | kept |

`tests/unit/test_migration_001a.py` now derives the expected defaults and
both controlled lists from the schema document itself
(`DefaultFidelityTests`, `ControlledListTests`).

### 3.2 Final static comparison (schema doc vs migration)

| Item | Result |
|---|---|
| Tables | exactly 25 `kbp` application tables (plus Alembic's `kbp.alembic_version`) |
| Columns, order, types, nullability, identity | 252 of 252 match §5 (test-derived from the doc) |
| Defaults | 17 match the literal §5 text (8 `created_at` → `now()`, 9 stated in the Rationale column); no others |
| Primary keys, UNIQUE | 25 PKs, 13 UNIQUE constraints, match §6.1; plus the partial unique index (§5.16) |
| CHECK constraints | 88, each compared with its §5 row; the §16.3/§17.2 lists are test-derived from the doc |
| Foreign keys | 33 inside `kbp` + 1 approved into `public.datasets`, match §6.2; all `ON DELETE NO ACTION` (default `ON UPDATE NO ACTION`), not deferrable |
| Indexes | the §21.1 partial unique index + the 7 §21.2 indexes; nothing from §21.3/§21.4; no ANN index |
| Runtime grants | 52 table-level + 43 column-level privileges = privileges §5.1 exactly |
| Downgrade | reverse dependency order (test-verified against every FK) |
| `public` | the only statement mentioning `public` is the approved FK; no DDL or DML on any `public` object |

## 4. Prerequisites (before any live run)

1. PostgreSQL 17 with pgvector 0.8.1 already installed (A1). Migration 001A
   does not use pgvector.
2. A full, restorable backup of `data_collection_pipeline` (runbook step 1).
3. `psql`, `pg_dump`, `pg_restore` of PostgreSQL 17 on `PATH`.
4. `pip install -e .` (installs `alembic`, `SQLAlchemy`, `psycopg[binary]`).
   Alembic is **not** installed in the current environment; the offline
   tests use a stub.
5. An evidence directory **outside the repository, without spaces in the
   path**, for example `E:\kbp_evidence\`. Everything saved there contains
   counts, digests and catalog definitions only — no credentials and no row
   content. Keep the backup files (which do contain data) in a protected
   location.

## 5. Review commands (offline; no database)

```
python -m unittest discover -s tests/unit        # all unit tests
python -m app.db.migration_safety                 # safety checker over migrations/versions
alembic upgrade kbp_001a --sql                    # render SQL only (needs alembic installed; no credentials)
alembic downgrade kbp_001a:base --sql             # render downgrade SQL
```

## 6. Live test runbook (Live Database Phase 1A) — NOT YET EXECUTED

**Where to run it.** First end to end on a **restored copy** (the scratch
database created in step 1, e.g. `data_collection_pipeline_restorecheck`),
then on `data_collection_pipeline`. For the copy, pass
`-v target_db=<copy>` to the provisioning script and use the copy's name in
every `-d` and URL. Roles are cluster-wide: on the second run the
provisioning script finds them, verifies their attributes and does not alter
them.

**Conventions.**

- Commands are for **Command Prompt (`cmd.exe`)**. PowerShell 5.1's `>` writes
  UTF-16 and wraps native stderr; if using PowerShell, prefix each command with
  `cmd /c "…"`.
- `%EV%` is the evidence directory: `set EV=E:\kbp_evidence`. Paths passed to
  psql with `-v` use forward slashes: `E:/kbp_evidence/...`.
- Every psql call uses `-X -v ON_ERROR_STOP=1`. Passwords are entered at the
  prompt or read from `pgpass`; they are never on the command line.
- **Stop rule:** if any step fails, or a verification does not end with
  `VERIFICATION PASSED`, stop. Keep all evidence. Do not retry, edit or "fix"
  anything in the database; report the output.

---

**STEP 1 — Backup and recoverability.** As `postgres`:

```
pg_dump -h localhost -U postgres -d data_collection_pipeline -Fc -f %EV%\01_backup_pre_001a.dump
pg_restore --list %EV%\01_backup_pre_001a.dump > %EV%\01_backup_toc.txt
createdb -h localhost -U postgres data_collection_pipeline_restorecheck
pg_restore -h localhost -U postgres -d data_collection_pipeline_restorecheck --exit-on-error %EV%\01_backup_pre_001a.dump
```

Recoverability is confirmed when the restore completes without error and
`pre_migration_snapshot.sql` run against the copy shows the same row counts
and data digests as against the original (step 2). The restored copy is the
dry-run target.

**STEP 2 — Pre-migration snapshot.** As `postgres`:

```
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -f scripts\db\pre_migration_snapshot.sql > %EV%\02_snapshot_pre.txt 2>&1
```

Expect: `kbp_exists = f` and both pipeline roles absent (first run).

**STEP 3 — Provision roles and schema `kbp` (administrator, manual).**
Review `scripts/db/provision_kbp_roles.sql` first. As `postgres`:

```
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -v app_connection_limit=20 -f scripts\db\provision_kbp_roles.sql > %EV%\03_provision.txt 2>&1
```

Then set the passwords **interactively** (no echo; only a SCRAM verifier
reaches the server; nothing is logged or saved):

```
psql -X -h localhost -U postgres -d data_collection_pipeline
postgres=# \password kb_pipeline_migrator
postgres=# \password kb_pipeline_app
postgres=# SHOW hba_file;
postgres=# \q
```

Add, **above** any broader rule in that `pg_hba.conf`:

```
host  data_collection_pipeline  kb_pipeline_migrator  127.0.0.1/32  scram-sha-256
host  data_collection_pipeline  kb_pipeline_migrator  ::1/128       scram-sha-256
host  data_collection_pipeline  kb_pipeline_app       127.0.0.1/32  scram-sha-256
host  data_collection_pipeline  kb_pipeline_app       ::1/128       scram-sha-256
```

then `SELECT pg_reload_conf();` as `postgres`. For the dry run on the copy,
add the same four lines with the copy's database name.

**STEP 4 — Verify provisioning, and record the baseline.** As `postgres`:

```
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -v app_connection_limit=20 -f scripts\db\verify_kbp_provisioning.sql > %EV%\04_verify_provisioning.txt 2>&1
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -f scripts\db\pre_migration_snapshot.sql > %EV%\04_snapshot_post_provisioning.txt 2>&1
```

- `04_verify_provisioning.txt` must end with `VERIFICATION PASSED`.
- Compare `04_snapshot_post_provisioning.txt` with `02_snapshot_pre.txt`: all
  counts, data digests, `structure_digest` and `public_inventory_digest` are
  identical. **Only `acl_digest` differs**, and the privilege listing differs
  exactly by the provisioning grants (the pipeline roles' entries on
  `datasets`, `dataset_sources`, `metadata`, `kb`).
- Create the baseline file from the post-provisioning snapshot (PowerShell
  shown; `ascii` avoids a byte-order mark that psql would reject):

  ```
  Select-String -Path E:\kbp_evidence\04_snapshot_post_provisioning.txt -Pattern '^\\set baseline_' | ForEach-Object { $_.Line } | Set-Content -Encoding ascii E:\kbp_evidence\baseline_001a.psql
  ```

  It must contain 12 lines.

**STEP 5 — Configure `KB_MIGRATION_DATABASE_URL` securely.**

- Put the migrator password in the libpq password file, **outside the
  repository**: `%APPDATA%\postgresql\pgpass.conf`, one line
  `localhost:5432:data_collection_pipeline:kb_pipeline_migrator:<password>`,
  typed in an editor (not echoed in a shell). Readable only by your Windows
  account.
- In the shell that will run Alembic, set a URL **without** a password:

  ```
  set KB_MIGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_migrator@localhost:5432/data_collection_pipeline
  set KB_DATABASE_URL=
  ```

- Confirm the login (read-only):
  `psql -X -h localhost -U kb_pipeline_migrator -d data_collection_pipeline -c "SELECT current_user"`.
- Never put the URL with a password into `alembic.ini`, a committed file, or a
  command line.

**STEP 6 — Safety checker (and unit tests).**

```
python -m app.db.migration_safety > %EV%\06_safety.txt 2>&1
python -m unittest discover -s tests/unit > %EV%\06_unit_tests.txt 2>&1
```

Expect `OK   ...kbp_001a_schema_v1.py` and all tests `OK`.

**STEP 7 — Render the SQL offline and review it** (no database connection):

```
alembic upgrade kbp_001a --sql > %EV%\07_upgrade_001a.sql
alembic downgrade kbp_001a:base --sql > %EV%\07_downgrade_001a.sql
```

Review: `CREATE TABLE kbp.alembic_version`; the preflight `DO` block; exactly
25 `CREATE TABLE kbp.`; the only `public.` reference is
`REFERENCES public.datasets (id)` in the `NOT VALID` FK; 8 `CREATE … INDEX … ON
kbp.`; `GRANT`s only to `kb_pipeline_app`; `INSERT INTO kbp.alembic_version …
'kbp_001a'`; no `INSERT` into any other table. The downgrade contains only the
revoke, 25 `DROP TABLE kbp.… RESTRICT` and the `DELETE FROM kbp.alembic_version`.

**STEP 8 — Upgrade.** In the shell from step 5:

```
alembic upgrade kbp_001a > %EV%\08_upgrade.txt 2>&1
```

Expect `Running upgrade  -> kbp_001a`. On any error the transaction is rolled
back; stop, and run `verify_downgrade_001a.sql` to confirm nothing remains.

**STEP 9 — Verify the migration.** As `postgres`:

```
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -v baseline=E:/kbp_evidence/baseline_001a.psql -f scripts\db\verify_migration_001a.sql > %EV%\09_verify_upgrade.txt 2>&1
```

Must end with `VERIFICATION PASSED` (checks M01–M22 and L-checks).

**STEP 10 — Compare the legacy objects with the snapshot.** The L-checks in
step 9 already compare all 12 legacy metrics with the baseline (a missing
baseline is a failure). For the record, also run:

```
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -f scripts\db\pre_migration_snapshot.sql > %EV%\10_snapshot_post_upgrade.txt 2>&1
fc %EV%\04_snapshot_post_provisioning.txt %EV%\10_snapshot_post_upgrade.txt > %EV%\10_diff.txt
```

The **only** expected differences:

- the snapshot time and section 3 (`kbp` now holds relations);
- section 6: one new row, `source_dataset_map_dataset_id_fkey`
  (`kbp.source_dataset_map` → `public.datasets`);
- section 7: two new internal RI triggers on `public.datasets` belonging to
  that FK.

Sections 9 and 10 (fingerprint and baseline lines) must be identical.

**STEP 11 — Downgrade.**

```
alembic downgrade base > %EV%\11_downgrade.txt 2>&1
```

Expect `Running downgrade kbp_001a -> `. (In a real environment with data in
`kbp`, take `pg_dump --schema=kbp` first; here `kbp` is empty.)

**STEP 12 — Verify the downgrade.** As `postgres`:

```
psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -v baseline=E:/kbp_evidence/baseline_001a.psql -f scripts\db\verify_downgrade_001a.sql > %EV%\12_verify_downgrade.txt 2>&1
alembic current > %EV%\12_alembic_current.txt 2>&1
```

- must end with `VERIFICATION PASSED`: the 25 application tables are gone
  (D03, D04), schema `kbp` is retained (D01), the FK and its RI triggers are
  gone (D06, D07), the legacy objects are unchanged (L-checks);
- migration history: `kbp.alembic_version` still exists and is **empty**
  (D02), `alembic current` prints no revision, and `public.alembic_version` is
  unchanged (L-check `public_alembic_version`).

**STEP 13 — Upgrade again.**

```
alembic upgrade kbp_001a > %EV%\13_upgrade_again.txt 2>&1
```

**STEP 14 — Verify again.** Repeat step 9 into `%EV%\14_verify_upgrade_again.txt`.
Must end with `VERIFICATION PASSED`: the re-created state is exactly the
expected state again.

**STEP 15 — Idempotency: the same upgrade is a no-op.**

```
alembic upgrade kbp_001a > %EV%\15_upgrade_noop.txt 2>&1
alembic current > %EV%\15_alembic_current.txt 2>&1
```

- `15_upgrade_noop.txt` contains **no** `Running upgrade` line;
- `alembic current` prints `kbp_001a (head)`;
- repeat step 9 into `%EV%\15_verify_noop.txt`: `VERIFICATION PASSED`.

**STEP 16 — Complete tests.**

1. Runtime permission tests, **as `kb_pipeline_app`** (password from
   `pgpass` or the prompt):

   ```
   psql -X -v ON_ERROR_STOP=1 -h localhost -U kb_pipeline_app -d data_collection_pipeline -f scripts\db\permission_probes_app.sql > %EV%\16_permission_probes.txt 2>&1
   ```

   Must end with `PERMISSION PROBES PASSED` (A01–A20 denied, B01–B10 allowed;
   every probe touches no row and the transaction is rolled back).
2. Provisioning drift check with the schema populated:

   ```
   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline -v app_connection_limit=20 -v expect_empty_kbp=false -f scripts\db\verify_kbp_provisioning.sql > %EV%\16_verify_provisioning_final.txt 2>&1
   ```

3. `python -m unittest discover -s tests/unit` and
   `python -m app.db.migration_safety` again → `%EV%\16_unit_tests.txt`.
4. Afterwards, clear the variable (`set KB_MIGRATION_DATABASE_URL=`). After
   the dry run on the copy, drop the copy
   (`dropdb -h localhost -U postgres data_collection_pipeline_restorecheck`).

The live test passes only if every step passed. The state left behind is
Migration 001A applied, `kbp.source_dataset_map` empty, and the legacy objects
unchanged.

## 7. Deferred: Gold bridge bootstrap (BLOCKED)

The bootstrap of the two `public.datasets` bridge rows (schema §14.3,
privileges §7) is a **separate, later, approved bootstrap operation** run as
`kb_pipeline_migrator`. It is blocked until the legacy application's handling
of `datasets.status = 'ACCEPTED'` is confirmed. Until then:

- `kbp.source_dataset_map` is empty, and the verification scripts fail if it
  is not (M13) or if a bridge row exists in `public.datasets` (L-bridge);
- the frozen ids `a7d8d2e1-8400-5916-9440-b492e8e692ce` and
  `b487f559-5f8e-56ba-9746-28658b9d09d1` are never inserted;
- `kb_pipeline_migrator` is provisioned **without** `INSERT` on
  `public.datasets` (see §8); the grant is part of the approved bootstrap step;
- Gold publication is blocked by design (schema §13.1 step 1).

Everything else in `kbp` is usable.

## 8. Provisioning decisions and findings (Phase 1A)

| # | Item | Decision |
|---|---|---|
| F1 | **Default privilege on functions.** Privileges §9 shows `ALTER DEFAULT PRIVILEGES FOR ROLE kb_pipeline_migrator IN SCHEMA kbp REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`. PostgreSQL applies per-schema default privileges only *additively* to the global defaults, so that statement has no effect; the built-in `EXECUTE` for `PUBLIC` would remain. | The provisioning script uses the effective form **without** `IN SCHEMA` (the migrator only creates objects in `kbp`, so the scope is the same). Intent of §9 unchanged. The literal example in the LOCKED privileges document still shows the ineffective form; amending that text needs the owner's approval. No function exists in Schema V1, so there is no impact today. |
| F2 | **`INSERT` on `public.datasets` for the migrator** (privileges §3.2) exists only for the blocked bridge bootstrap. | **Deferred** to the approved bootstrap step, so the bridge rows cannot be written by accident. `verify_kbp_provisioning.sql` (P15, P18) requires it to be absent until then. All other §3.2 and §6 privileges are provisioned. |
| F3 | Runtime role tuning values (O-20): `CONNECTION LIMIT`, `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`. | psql parameters with initial bounded defaults 20, `300s`, `5s`, `300s`; validated by the guard. |
| F4 | The provisioning guard aborts (and changes nothing) if `PUBLIC` has `CREATE` on the database or on schema `public`, or any privilege on the four protected tables, because every role would inherit it. Neither fact is in the audits. | Revoking such a `PUBLIC` privilege is a separate administrator decision; the script never does it. |
| F5 | `PUBLIC`'s default `TEMPORARY` privilege on the database is inherited by both roles. | Reported (not changed): revoking it would affect every role of the legacy system. |
| F6 | `pg_hba.conf` and the schema of the `vector` type are not in the audits. | Reported by `verify_kbp_provisioning.sql` for review at step 4. |

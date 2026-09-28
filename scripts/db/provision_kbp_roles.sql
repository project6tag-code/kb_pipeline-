-- =============================================================================
-- provision_kbp_roles.sql - ONE-TIME ADMINISTRATIVE PROVISIONING (WRITES)
-- =============================================================================
--
-- Implements docs/database_privileges_v1.md section 13, steps 2-3:
--   * roles kb_pipeline_migrator and kb_pipeline_app (sections 3.1, 4.1);
--   * schema kbp, owned by kb_pipeline_migrator (sections 3.2, 8);
--   * CONNECT, USAGE ON SCHEMA public, and the minimum public-table grants
--     (sections 3.2, 6);
--   * runtime role settings (section 4.1);
--   * as kb_pipeline_migrator: USAGE on kbp for the runtime role and the
--     default privileges (section 9).
--
-- THIS SCRIPT IS RUN MANUALLY BY THE PROJECT OWNER AS POSTGRESQL
-- ADMINISTRATOR (a superuser such as postgres), ONCE, AFTER REVIEW.
-- It is NOT run by the pipeline, by migrations or by any automation.
-- It was NOT executed when it was prepared.
--
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline
--        [-v app_connection_limit=20] -f scripts/db/provision_kbp_roles.sql
--
-- Passwords: NONE are set or stored here. Both roles are created with
-- PASSWORD NULL (they cannot log in yet). Afterwards set each password
-- interactively (docs/migrations_kbp.md, runbook step 3):
--   postgres=# \password kb_pipeline_migrator
--   postgres=# \password kb_pipeline_app
-- psql's \password prompts without echo and sends only a SCRAM verifier, so
-- the plaintext never reaches the server log, shell history or this file.
--
-- Safety properties:
--   * one transaction: any error rolls back everything (ON_ERROR_STOP);
--   * guard block first: refuses to run unless the session is a superuser on
--     the expected database, password_encryption is scram-sha-256, and the
--     legacy state matches the audits (no PUBLIC CREATE on the database or
--     schema public; no PUBLIC privileges on the four protected tables);
--   * existing roles are NEVER altered: if a role already exists (e.g. a
--     dry run on a restored copy in the same cluster), its attributes are
--     verified and the script aborts on any difference;
--   * every attribute is explicit: NOSUPERUSER NOCREATEDB NOCREATEROLE
--     NOREPLICATION NOBYPASSRLS; bounded CONNECTION LIMIT; no memberships;
--   * no GRANT ... TO PUBLIC, no ALL PRIVILEGES, no WITH GRANT OPTION, no
--     schema-wide (ON ALL TABLES IN SCHEMA) grant, no ownership change of any
--     public object, no UPDATE/DELETE/TRUNCATE on any public table;
--   * a final self-check re-verifies the result before COMMIT.
--
-- Deliberately NOT granted here: INSERT on public.datasets for
-- kb_pipeline_migrator (privileges section 3.2). It exists only for the Gold
-- bridge bootstrap, which is BLOCKED (privileges section 11). It is granted
-- in the separate, later, approved bootstrap step, so the bridge rows cannot
-- be written by accident before then.
--
-- Parameters (psql -v), all validated by the guard:
--   target_db                       expected database (default data_collection_pipeline)
--   app_connection_limit            kb_pipeline_app CONNECTION LIMIT (default 20;
--                                   = sum of worker pools + margin; tune per O-20)
--   app_statement_timeout           default 300s   (privileges section 4.1, O-20)
--   app_lock_timeout                default 5s
--   app_idle_in_transaction_timeout default 300s
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

\if :{?target_db}
\else
\set target_db data_collection_pipeline
\endif
\if :{?app_connection_limit}
\else
\set app_connection_limit 20
\endif
\if :{?app_statement_timeout}
\else
\set app_statement_timeout 300s
\endif
\if :{?app_lock_timeout}
\else
\set app_lock_timeout 5s
\endif
\if :{?app_idle_in_transaction_timeout}
\else
\set app_idle_in_transaction_timeout 300s
\endif

BEGIN;
SET LOCAL lock_timeout = '5s';

SELECT set_config('kbp_prov.target_db', :'target_db', true)                                             AS p1,
       set_config('kbp_prov.app_connection_limit', :'app_connection_limit', true)                       AS p2,
       set_config('kbp_prov.app_statement_timeout', :'app_statement_timeout', true)                     AS p3,
       set_config('kbp_prov.app_lock_timeout', :'app_lock_timeout', true)                               AS p4,
       set_config('kbp_prov.app_idle_in_transaction_timeout', :'app_idle_in_transaction_timeout', true) AS p5
\gset _ignored_

-- -----------------------------------------------------------------------------
-- 1. Guard (reads only; aborts the transaction on any unexpected state)
-- -----------------------------------------------------------------------------
DO $guard$
DECLARE
    r        record;
    rel      text;
    app_lim  text := current_setting('kbp_prov.app_connection_limit');
BEGIN
    IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) THEN
        RAISE EXCEPTION 'provisioning must be run by a PostgreSQL superuser (current_user = %)', current_user;
    END IF;
    IF current_database() <> current_setting('kbp_prov.target_db') THEN
        RAISE EXCEPTION 'connected to database %, expected % (pass -v target_db=... for a restored copy)',
            current_database(), current_setting('kbp_prov.target_db');
    END IF;
    IF current_setting('password_encryption') <> 'scram-sha-256' THEN
        RAISE EXCEPTION 'password_encryption is %, expected scram-sha-256 (passwords must be stored as SCRAM verifiers)',
            current_setting('password_encryption');
    END IF;

    -- Parameter validation (values are later interpolated into DDL).
    IF app_lim !~ '^[1-9][0-9]{0,2}$' THEN
        RAISE EXCEPTION 'app_connection_limit must be an integer 1..999, got %', app_lim;
    END IF;
    FOREACH rel IN ARRAY ARRAY['app_statement_timeout', 'app_lock_timeout', 'app_idle_in_transaction_timeout'] LOOP
        IF current_setting('kbp_prov.' || rel) !~ '^[1-9][0-9]*(ms|s|min)$' THEN
            RAISE EXCEPTION '% must be a positive duration such as 5s, 300s or 5min, got %',
                rel, current_setting('kbp_prov.' || rel);
        END IF;
    END LOOP;

    -- Legacy state must match the audits (A1/A2).
    FOREACH rel IN ARRAY ARRAY['public.kb', 'public.metadata', 'public.datasets', 'public.dataset_sources'] LOOP
        IF to_regclass(rel) IS NULL THEN
            RAISE EXCEPTION 'protected table % not found', rel;
        END IF;
        IF pg_catalog.pg_get_userbyid((SELECT relowner FROM pg_catalog.pg_class WHERE oid = to_regclass(rel))) <> 'postgres' THEN
            RAISE EXCEPTION 'owner of % is not postgres (audit A2); stop and review', rel;
        END IF;
        IF has_any_column_privilege('public', rel, 'SELECT') OR has_any_column_privilege('public', rel, 'INSERT')
           OR has_any_column_privilege('public', rel, 'UPDATE') OR has_table_privilege('public', rel, 'DELETE')
           OR has_table_privilege('public', rel, 'TRUNCATE') THEN
            RAISE EXCEPTION 'PUBLIC holds privileges on % (audit A2 lists none); every role would inherit them. Stop and review.', rel;
        END IF;
    END LOOP;
    IF has_database_privilege('public', current_database(), 'CREATE') THEN
        RAISE EXCEPTION 'PUBLIC has CREATE on database %; the pipeline roles would inherit schema creation. Revoking it is a separate administrator decision.',
            current_database();
    END IF;
    IF has_schema_privilege('public', 'public', 'CREATE') THEN
        RAISE EXCEPTION 'PUBLIC has CREATE on schema public; the pipeline roles would inherit DDL in public. Revoking it is a separate administrator decision.';
    END IF;

    -- Pre-existing roles are verified, never modified.
    FOR r IN
        SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,
               rolcanlogin, rolinherit, rolconnlimit
        FROM pg_catalog.pg_roles
        WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')
    LOOP
        IF r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
           OR NOT r.rolcanlogin OR NOT r.rolinherit THEN
            RAISE EXCEPTION 'role % already exists with unexpected attributes; it will not be altered. Stop and review.', r.rolname;
        END IF;
        IF r.rolconnlimit <> CASE r.rolname WHEN 'kb_pipeline_migrator' THEN 2 ELSE app_lim::integer END THEN
            RAISE EXCEPTION 'role % already exists with CONNECTION LIMIT %; expected %. Stop and review.',
                r.rolname, r.rolconnlimit, CASE r.rolname WHEN 'kb_pipeline_migrator' THEN '2' ELSE app_lim END;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members AS m
                   WHERE m.member = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = r.rolname)
                      OR m.roleid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = r.rolname)) THEN
            RAISE EXCEPTION 'role % already exists and has role memberships; stop and review.', r.rolname;
        END IF;
    END LOOP;

    -- A pre-existing schema kbp must already belong to the migrator.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') THEN
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_migrator') THEN
            RAISE EXCEPTION 'schema kbp exists but role kb_pipeline_migrator does not; stop and review.';
        END IF;
        IF (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'kbp')
           <> 'kb_pipeline_migrator' THEN
            RAISE EXCEPTION 'schema kbp exists and is not owned by kb_pipeline_migrator; stop and review.';
        END IF;
    END IF;
    RAISE NOTICE 'guard passed';
END
$guard$;

SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_migrator') AS create_migrator,
       NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_app')      AS create_app,
       current_database()                                                                    AS dbname
\gset prov_

-- -----------------------------------------------------------------------------
-- 2. Roles (privileges sections 3.1 and 4.1). No password: set it afterwards
--    with \password (see header).
-- -----------------------------------------------------------------------------
\if :prov_create_migrator
CREATE ROLE kb_pipeline_migrator WITH
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT
    CONNECTION LIMIT 2
    PASSWORD NULL;
\else
\echo 'kb_pipeline_migrator already exists: attributes verified by the guard; the role was NOT altered.'
\endif

\if :prov_create_app
CREATE ROLE kb_pipeline_app WITH
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT
    CONNECTION LIMIT :app_connection_limit
    PASSWORD NULL;
\else
\echo 'kb_pipeline_app already exists: attributes verified by the guard; the role was NOT altered.'
\endif

-- -----------------------------------------------------------------------------
-- 3. Schema kbp, owned by the migrator; nothing for PUBLIC (sections 3.2, 8).
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS kbp AUTHORIZATION kb_pipeline_migrator;
REVOKE ALL ON SCHEMA kbp FROM PUBLIC;

-- -----------------------------------------------------------------------------
-- 4. Database and schema public (section 13 step 2). USAGE only, never CREATE.
-- -----------------------------------------------------------------------------
GRANT CONNECT ON DATABASE :"prov_dbname" TO kb_pipeline_migrator, kb_pipeline_app;
GRANT USAGE ON SCHEMA public TO kb_pipeline_migrator, kb_pipeline_app;

-- -----------------------------------------------------------------------------
-- 5. Migrator on public tables (section 3.2). Needed by Migration 001A:
--    REFERENCES (id) for the approved FK kbp.source_dataset_map -> public.datasets.
--    SELECT on datasets / dataset_sources: read-only bridge verification.
--    No privilege at all on public.kb or public.metadata.
--    INSERT on public.datasets is DEFERRED (bridge bootstrap BLOCKED; see header).
-- -----------------------------------------------------------------------------
GRANT REFERENCES (id) ON TABLE public.datasets TO kb_pipeline_migrator;
GRANT SELECT ON TABLE public.datasets TO kb_pipeline_migrator;
GRANT SELECT ON TABLE public.dataset_sources TO kb_pipeline_migrator;

-- -----------------------------------------------------------------------------
-- 6. Runtime role on public tables (section 6). Exactly these four tables.
--    No UPDATE, DELETE or TRUNCATE on Gold. No INSERT on datasets.
-- -----------------------------------------------------------------------------
GRANT SELECT (id, name, source_type) ON TABLE public.dataset_sources TO kb_pipeline_app;
GRANT SELECT (id, source_id, dataset_name, metadata) ON TABLE public.datasets TO kb_pipeline_app;
GRANT SELECT (id, source_id) ON TABLE public.metadata TO kb_pipeline_app;
GRANT INSERT ON TABLE public.metadata TO kb_pipeline_app;
GRANT SELECT, INSERT ON TABLE public.kb TO kb_pipeline_app;

-- -----------------------------------------------------------------------------
-- 7. Runtime role settings, this database only (section 4.1; values per O-20).
-- -----------------------------------------------------------------------------
ALTER ROLE kb_pipeline_app IN DATABASE :"prov_dbname" SET search_path = kbp, public;
ALTER ROLE kb_pipeline_app IN DATABASE :"prov_dbname" SET statement_timeout = :'app_statement_timeout';
ALTER ROLE kb_pipeline_app IN DATABASE :"prov_dbname" SET lock_timeout = :'app_lock_timeout';
ALTER ROLE kb_pipeline_app IN DATABASE :"prov_dbname" SET idle_in_transaction_session_timeout = :'app_idle_in_transaction_timeout';

-- -----------------------------------------------------------------------------
-- 8. As kb_pipeline_migrator, the owner of kbp (section 13 step 3; section 9).
-- -----------------------------------------------------------------------------
SET LOCAL ROLE kb_pipeline_migrator;

GRANT USAGE ON SCHEMA kbp TO kb_pipeline_app;

-- Baseline: every future kbp table created by the migrator is readable (only)
-- by the runtime role. Write privileges are explicit per table in migrations.
ALTER DEFAULT PRIVILEGES FOR ROLE kb_pipeline_migrator IN SCHEMA kbp
    GRANT SELECT ON TABLES TO kb_pipeline_app;

-- Functions created by the migrator are not executable by PUBLIC.
-- NOTE: written WITHOUT "IN SCHEMA". PostgreSQL applies per-schema default
-- privileges only additively on top of the global defaults, so the
-- "IN SCHEMA kbp REVOKE EXECUTE ... FROM PUBLIC" form shown conceptually in
-- privileges section 9 would have no effect. The migrator creates objects
-- only in kbp, so the global form has exactly the intended scope.
ALTER DEFAULT PRIVILEGES FOR ROLE kb_pipeline_migrator
    REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

RESET ROLE;

-- -----------------------------------------------------------------------------
-- 9. Self-check before COMMIT (reads only). Any failure rolls back everything.
-- -----------------------------------------------------------------------------
DO $selfcheck$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT rolname FROM pg_catalog.pg_roles
        WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')
          AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls OR rolconnlimit < 1)
    LOOP
        RAISE EXCEPTION 'self-check: role % has an elevated attribute or unbounded connections', r.rolname;
    END LOOP;
    IF (SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')) <> 2 THEN
        RAISE EXCEPTION 'self-check: pipeline roles missing';
    END IF;
    IF (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'kbp')
       IS DISTINCT FROM 'kb_pipeline_migrator' THEN
        RAISE EXCEPTION 'self-check: schema kbp is not owned by kb_pipeline_migrator';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_shdepend
               WHERE deptype = 'o' AND refclassid = 'pg_catalog.pg_authid'::regclass
                 AND refobjid = 'kb_pipeline_app'::regrole::oid) THEN
        RAISE EXCEPTION 'self-check: kb_pipeline_app owns an object';
    END IF;
    IF has_schema_privilege('kb_pipeline_app', 'public', 'CREATE')
       OR has_schema_privilege('kb_pipeline_migrator', 'public', 'CREATE')
       OR has_schema_privilege('kb_pipeline_app', 'kbp', 'CREATE')
       OR has_database_privilege('kb_pipeline_app', current_database(), 'CREATE')
       OR has_database_privilege('kb_pipeline_migrator', current_database(), 'CREATE') THEN
        RAISE EXCEPTION 'self-check: a pipeline role can create schemas or objects outside its remit';
    END IF;
    IF has_table_privilege('kb_pipeline_app', 'public.kb', 'UPDATE')
       OR has_table_privilege('kb_pipeline_app', 'public.kb', 'DELETE')
       OR has_any_column_privilege('kb_pipeline_app', 'public.metadata', 'UPDATE')
       OR has_table_privilege('kb_pipeline_app', 'public.metadata', 'DELETE')
       OR has_table_privilege('kb_pipeline_app', 'public.kb', 'TRUNCATE')
       OR has_table_privilege('kb_pipeline_app', 'public.metadata', 'TRUNCATE')
       OR has_any_column_privilege('kb_pipeline_migrator', 'public.kb', 'SELECT')
       OR has_any_column_privilege('kb_pipeline_migrator', 'public.metadata', 'SELECT') THEN
        RAISE EXCEPTION 'self-check: forbidden privilege on Gold tables';
    END IF;
    IF has_any_column_privilege('kb_pipeline_app', 'public.datasets', 'INSERT')
       OR has_any_column_privilege('kb_pipeline_migrator', 'public.datasets', 'INSERT') THEN
        RAISE EXCEPTION 'self-check: INSERT on public.datasets must not be granted before the bridge bootstrap is approved';
    END IF;
    RAISE NOTICE 'self-check passed';
END
$selfcheck$;

COMMIT;

\echo ''
\echo 'Provisioning committed. NEXT (runbook step 3, continued; interactive, as the administrator):'
\echo '  1. \\password kb_pipeline_migrator'
\echo '  2. \\password kb_pipeline_app'
\echo '  3. add the pg_hba.conf scram-sha-256 entries, then SELECT pg_reload_conf();'
\echo '  4. run scripts/db/verify_kbp_provisioning.sql (runbook step 4)'

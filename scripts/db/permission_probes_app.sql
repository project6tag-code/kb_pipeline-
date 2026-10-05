-- =============================================================================
-- permission_probes_app.sql - runtime permission tests (privileges section 13 step 9)
-- =============================================================================
--
-- Run AS kb_pipeline_app (runbook step 16, docs/migrations_kbp.md section 6).
-- NOT executed during preparation.
--
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U kb_pipeline_app -d kb_pipeline_test
--        -f scripts/db/permission_probes_app.sql > <evidence>/16_permission_probes.txt 2>&1
--
-- Each probe attempts one operation and checks the outcome:
--   * DENIED probes must fail with insufficient_privilege (SQLSTATE 42501);
--   * ALLOWED probes must succeed.
--
-- Probes cannot change anything, even if a privilege were wrongly granted:
--   * every data statement is "... WHERE false" or "LIMIT 0" and touches no row;
--   * every probe runs in its own exception block (subtransaction);
--   * the whole script is one transaction ended with ROLLBACK (never COMMIT).
--
-- Exit status: non-zero if any probe has an unexpected outcome.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL client_min_messages = notice;

SELECT current_user AS connected_as,
       current_setting('search_path') AS search_path,
       current_setting('statement_timeout') AS statement_timeout,
       current_setting('lock_timeout') AS lock_timeout,
       current_setting('idle_in_transaction_session_timeout') AS idle_in_transaction_session_timeout;

DO $probes$
DECLARE
    p        record;
    failures integer := 0;
BEGIN
    IF current_user <> 'kb_pipeline_app' THEN
        RAISE EXCEPTION 'run these probes as kb_pipeline_app (current_user = %)', current_user;
    END IF;

    FOR p IN
        SELECT * FROM (VALUES
        -- Gold is insert-only for the runtime role (privileges sections 6 and 12).
        ('A01', 'denied',  $q$UPDATE public.kb SET chunk = chunk WHERE false$q$),
        ('A02', 'denied',  $q$DELETE FROM public.kb WHERE false$q$),
        ('A03', 'denied',  $q$UPDATE public.metadata SET domain = domain WHERE false$q$),
        ('A04', 'denied',  $q$DELETE FROM public.metadata WHERE false$q$),
        -- No writes to the bridge parents; no unlisted columns.
        ('A05', 'denied',  $q$INSERT INTO public.datasets (id, source_id, dataset_name, status, metadata)
                              SELECT NULL::uuid, NULL::uuid, NULL::varchar, NULL::varchar, NULL::jsonb WHERE false$q$),
        ('A06', 'denied',  $q$INSERT INTO public.dataset_sources (id, name, source_type, authentication_required, status)
                              SELECT NULL::uuid, NULL::varchar, NULL::varchar, NULL::boolean, NULL::varchar WHERE false$q$),
        ('A07', 'denied',  $q$SELECT status FROM public.datasets LIMIT 0$q$),
        -- Identity and content columns are immutable (privileges section 12, invariant 5).
        ('A09', 'denied',  $q$UPDATE kbp.chunk_registry SET chunk_text = chunk_text WHERE false$q$),
        ('A10', 'denied',  $q$UPDATE kbp.document_registry SET canonical_text = canonical_text WHERE false$q$),
        ('A11', 'denied',  $q$UPDATE kbp.document_registry SET content_hash = content_hash WHERE false$q$),
        ('A12', 'denied',  $q$UPDATE kbp.source_observations SET source_url = source_url WHERE false$q$),
        ('A13', 'denied',  $q$UPDATE kbp.github_repositories SET repository_id = repository_id WHERE false$q$),
        -- DELETE only on embedding_queue.
        ('A14', 'denied',  $q$DELETE FROM kbp.chunk_registry WHERE false$q$),
        ('A15', 'denied',  $q$DELETE FROM kbp.pipeline_jobs WHERE false$q$),
        -- The bridge map and reference tables are not writable at runtime.
        ('A16', 'denied',  $q$INSERT INTO kbp.source_dataset_map (source_channel, dataset_source_name, bridge_identity_name, dataset_id)
                              SELECT NULL::text, NULL::text, NULL::text, NULL::uuid WHERE false$q$),
        ('A17', 'denied',  $q$INSERT INTO kbp.embedding_models (model_name) SELECT NULL::text WHERE false$q$),
        -- No DDL anywhere.
        ('A18', 'denied',  $q$CREATE TABLE kbp.kbp_permission_probe (id integer)$q$),
        ('A19', 'denied',  $q$CREATE TABLE public.kbp_permission_probe (id integer)$q$),
        ('A20', 'denied',  $q$CREATE SCHEMA kbp_permission_probe$q$),
        -- What the runtime needs must work.
        ('B01', 'allowed', $q$SELECT version_num FROM kbp.alembic_version$q$),
        ('B02', 'allowed', $q$SELECT id, chunk, embedding FROM public.kb LIMIT 0$q$),
        ('B03', 'allowed', $q$SELECT id, source_id, source_url, source_title, source_type, domain, published_at, updated_at
                      FROM public.metadata LIMIT 0$q$),
        ('B04', 'allowed', $q$SELECT id, source_id, dataset_name, metadata FROM public.datasets LIMIT 0$q$),
        ('B05', 'allowed', $q$SELECT id, name, source_type FROM public.dataset_sources LIMIT 0$q$),
        ('B06', 'allowed', $q$SELECT * FROM kbp.source_dataset_map LIMIT 0$q$),
        ('B07', 'allowed', $q$DELETE FROM kbp.embedding_queue WHERE false$q$),
        ('B08', 'allowed', $q$UPDATE kbp.pipeline_runs SET status = status, finished_at = finished_at WHERE false$q$),
        ('B09', 'allowed', $q$UPDATE kbp.document_registry SET status = status WHERE false$q$),
        ('B10', 'allowed', $q$UPDATE kbp.pipeline_jobs SET status = status WHERE false$q$)
        ) AS v(id, expectation, statement)
        ORDER BY 1
    LOOP
        BEGIN
            EXECUTE p.statement;
            IF p.expectation = 'allowed' THEN
                RAISE NOTICE 'PASS %  allowed as expected: %', p.id, p.statement;
            ELSE
                failures := failures + 1;
                RAISE WARNING 'FAIL %  was ALLOWED but must be denied: %', p.id, p.statement;
            END IF;
        EXCEPTION
            WHEN insufficient_privilege THEN
                IF p.expectation = 'denied' THEN
                    RAISE NOTICE 'PASS %  denied as expected: %', p.id, p.statement;
                ELSE
                    failures := failures + 1;
                    RAISE WARNING 'FAIL %  was DENIED but must be allowed: % (%)', p.id, p.statement, SQLERRM;
                END IF;
            WHEN OTHERS THEN
                failures := failures + 1;
                RAISE WARNING 'FAIL %  unexpected error (%): % - %', p.id, SQLSTATE, p.statement, SQLERRM;
        END;
    END LOOP;

    IF failures > 0 THEN
        RAISE EXCEPTION 'PERMISSION PROBES FAILED: % unexpected outcome(s)', failures;
    END IF;
    IF NOT (
        has_column_privilege(current_user, 'public.metadata', 'id', 'SELECT')
        AND has_column_privilege(current_user, 'public.metadata', 'source_id', 'SELECT')
        AND has_column_privilege(current_user, 'public.metadata', 'source_url', 'SELECT')
        AND has_column_privilege(current_user, 'public.metadata', 'source_title', 'SELECT')
        AND has_column_privilege(current_user, 'public.metadata', 'source_type', 'SELECT')
        AND has_column_privilege(current_user, 'public.metadata', 'domain', 'SELECT')
        AND has_column_privilege(current_user, 'public.metadata', 'published_at', 'SELECT')
        AND has_column_privilege(current_user, 'public.metadata', 'updated_at', 'SELECT')
        AND has_table_privilege(current_user, 'public.metadata', 'INSERT')
        AND NOT has_table_privilege(current_user, 'public.metadata', 'UPDATE')
        AND NOT has_table_privilege(current_user, 'public.metadata', 'DELETE')
        AND NOT has_table_privilege(current_user, 'public.metadata', 'TRUNCATE')
    ) THEN
        RAISE EXCEPTION 'public.metadata privileges differ from SELECT(all 8 columns), INSERT only';
    END IF;
    RAISE NOTICE 'PERMISSION PROBES PASSED';
END
$probes$;

ROLLBACK;

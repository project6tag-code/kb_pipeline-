-- =============================================================================
-- verify_downgrade_001a.sql - READ ONLY verification after "alembic downgrade base"
-- =============================================================================
--
-- Verifies runbook step 12 (docs/migrations_kbp.md section 6):
--   * the 25 application tables of Migration 001A are gone;
--   * schema kbp is retained, still owned by kb_pipeline_migrator;
--   * kbp.alembic_version is retained and empty (migration history at base);
--   * the approved FK into public.datasets is gone;
--   * the legacy public objects are unchanged against the baseline.
-- NOT executed during preparation.
--
-- Run by: the PostgreSQL administrator (e.g. postgres). It only reads.
--
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline
--        -v baseline=<evidence>/baseline_001a.psql
--        -f scripts/db/verify_downgrade_001a.sql > <evidence>/12_verify_downgrade.txt 2>&1
--
-- Exit status: non-zero if any check FAILS. One READ ONLY transaction, rolled back.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';
SET LOCAL client_min_messages = notice;
SELECT set_config('kbp_verify.failures', '0', true) AS reset_failures \gset _ignored_

\echo '== INFORMATION (not pass/fail) =='
SELECT current_database() AS database, current_user AS checked_by, now() AS checked_at;

SELECT c.relname AS kbp_relation, c.relkind, pg_catalog.pg_get_userbyid(c.relowner) AS owner
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'kbp'
ORDER BY c.relname;

\echo '== CHECKS: state after downgrade to base =='
DO $checks$
DECLARE
    c        record;
    ok       boolean;
    detail   text;
    failures integer := 0;
BEGIN
    FOR c IN
        SELECT * FROM (VALUES
        ('D01', 'schema kbp is retained and owned by kb_pipeline_migrator',
         $q$SELECT coalesce(bool_and(pg_catalog.pg_get_userbyid(nspowner) = 'kb_pipeline_migrator'), false),
                   coalesce(string_agg('owner=' || pg_catalog.pg_get_userbyid(nspowner), ','), 'schema kbp is missing')
            FROM pg_catalog.pg_namespace WHERE nspname = 'kbp'$q$),

        ('D02', 'kbp.alembic_version is retained, owned by kb_pipeline_migrator, and empty (history at base)',
         $q$SELECT (SELECT count(*) FROM kbp.alembic_version) = 0
                   AND (SELECT relowner FROM pg_catalog.pg_class WHERE oid = 'kbp.alembic_version'::regclass)
                       = 'kb_pipeline_migrator'::regrole::oid,
                   (SELECT count(*) FROM kbp.alembic_version)::text || ' version row(s)'$q$),

        ('D03', 'none of the 25 application tables of Migration 001A remains',
         $q$SELECT count(*) = 0, coalesce(string_agg(c.relname, ', '), '')
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'kbp'
              AND c.relname = ANY (ARRAY[
                  'pipeline_runs', 'cc_crawls', 'cc_warc_files', 'github_repositories', 'github_commits',
                  'source_observations', 'cc_captures', 'github_file_observations', 'document_registry',
                  'observation_extractions', 'dedup_methods', 'document_dedup_decisions',
                  'document_relationships', 'chunking_versions', 'chunk_registry', 'chunk_occurrences',
                  'embedding_models', 'chunk_embeddings', 'embedding_queue', 'pipeline_jobs',
                  'processing_attempts', 'dead_letters', 'source_rate_limits', 'source_dataset_map',
                  'legacy_kb_reconciliation'])$q$),

        ('D04', 'the only relations left in kbp are alembic_version and its index',
         $q$SELECT count(*) = 0, coalesce(string_agg(c.relname || ' (' || c.relkind::text || ')', ', '), '')
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'kbp'
              AND c.relname <> 'alembic_version'
              AND NOT (c.relkind = 'i' AND c.oid IN (SELECT indexrelid FROM pg_catalog.pg_index
                                                     WHERE indrelid = 'kbp.alembic_version'::regclass))$q$),

        ('D05', 'no functions or procedures in kbp',
         $q$SELECT count(*) = 0, coalesce(string_agg(p.oid::regprocedure::text, ', '), '')
            FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'kbp'$q$),

        ('D06', 'no FK from outside public into kb/metadata/datasets/dataset_sources remains',
         $q$SELECT count(*) = 0, coalesce(string_agg(cn.nspname || '.' || cl.relname || '.' || co.conname, ', '), '')
            FROM pg_catalog.pg_constraint AS co
            JOIN pg_catalog.pg_class AS cl ON cl.oid = co.conrelid
            JOIN pg_catalog.pg_namespace AS cn ON cn.oid = cl.relnamespace
            JOIN pg_catalog.pg_class AS rc ON rc.oid = co.confrelid
            JOIN pg_catalog.pg_namespace AS rn ON rn.oid = rc.relnamespace
            WHERE co.contype = 'f' AND cn.nspname <> 'public'
              AND rn.nspname = 'public' AND rc.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources')$q$),

        ('D07', 'no trigger on the protected tables belongs to a constraint outside public',
         $q$SELECT count(*) = 0, coalesce(string_agg(c.relname || ':' || t.tgname, ', '), '')
            FROM pg_catalog.pg_trigger AS t
            JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_constraint AS co ON co.oid = t.tgconstraint
            JOIN pg_catalog.pg_class AS cc ON cc.oid = co.conrelid
            JOIN pg_catalog.pg_namespace AS cn ON cn.oid = cc.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')
              AND cn.nspname <> 'public'$q$),

        ('D08', 'kb_pipeline_app holds no privilege on any remaining kbp relation (the alembic_version grant was revoked)',
         $q$SELECT count(*) = 0, coalesce(string_agg(c.relname || ':' || x.privilege_type, ', '), '')
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL aclexplode(c.relacl) AS x
            WHERE n.nspname = 'kbp' AND x.grantee = 'kb_pipeline_app'::regrole::oid$q$),

        ('D09', 'schema kbp privileges are unchanged: kb_pipeline_app USAGE only; PUBLIC nothing',
         $q$SELECT has_schema_privilege('kb_pipeline_app', 'kbp', 'USAGE')
                   AND NOT has_schema_privilege('kb_pipeline_app', 'kbp', 'CREATE')
                   AND NOT has_schema_privilege('public', 'kbp', 'USAGE')
                   AND NOT has_schema_privilege('public', 'kbp', 'CREATE'),
                   (SELECT coalesce(array_to_string(nspacl, ','), '<default>')
                    FROM pg_catalog.pg_namespace WHERE nspname = 'kbp')$q$),

        ('D10', 'every remaining kbp relation and type is owned by kb_pipeline_migrator',
         $q$WITH bad AS (
                 SELECT 'relation ' || c.relname AS obj
                 FROM pg_catalog.pg_class AS c
                 JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'kbp' AND c.relowner <> 'kb_pipeline_migrator'::regrole::oid
                 UNION ALL
                 SELECT 'type ' || t.typname
                 FROM pg_catalog.pg_type AS t
                 JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
                 WHERE n.nspname = 'kbp' AND t.typowner <> 'kb_pipeline_migrator'::regrole::oid)
            SELECT count(*) = 0, coalesce(string_agg(obj, ', '), '') FROM bad$q$),

        ('D11', 'kb_pipeline_app owns no object in any database of the cluster',
         $q$SELECT count(*) = 0, count(*)::text || ' owned object(s)'
            FROM pg_catalog.pg_shdepend
            WHERE deptype = 'o'
              AND refclassid = 'pg_catalog.pg_authid'::regclass
              AND refobjid = 'kb_pipeline_app'::regrole::oid$q$)
        ) AS v(id, description, query)
        ORDER BY 1
    LOOP
        BEGIN
            EXECUTE c.query INTO ok, detail;
        EXCEPTION WHEN OTHERS THEN
            ok := false;
            detail := 'error: ' || SQLERRM;
        END;
        IF ok IS TRUE THEN
            RAISE NOTICE 'PASS %  %', c.id, c.description;
        ELSE
            failures := failures + 1;
            RAISE WARNING 'FAIL %  %  [%]', c.id, c.description, coalesce(detail, '');
        END IF;
    END LOOP;
    PERFORM set_config('kbp_verify.failures',
        (coalesce(nullif(current_setting('kbp_verify.failures', true), ''), '0')::integer + failures)::text,
        true);
END
$checks$;

\echo '== LEGACY OBJECTS (must equal the baseline) =='
\ir lib/legacy_fingerprint.sql
\ir lib/legacy_checks.sql

\ir lib/finish.sql

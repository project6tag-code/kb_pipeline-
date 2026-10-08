-- =============================================================================
-- verify_migration_001b.sql - READ ONLY verification after Migration 001B
-- =============================================================================
-- Run as a PostgreSQL administrator, but only against kb_pipeline_test:
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d kb_pipeline_test \
--        -f scripts/db/verify_migration_001b.sql
-- Catalog reads only; the transaction is READ ONLY and rolled back.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $checks$
DECLARE
    failures integer;
    expected_columns text[] := ARRAY[
        'chunk_dedup_decisions|chunk_id|uuid|NO',
        'chunk_dedup_decisions|dedup_method_id|smallint|NO',
        'chunk_dedup_decisions|classification|text|NO',
        'chunk_dedup_decisions|representative_chunk_id|uuid|YES',
        'chunk_dedup_decisions|similarity_score|real|YES',
        'chunk_dedup_decisions|decision_status|text|NO',
        'chunk_dedup_decisions|decided_at|timestamp with time zone|NO',
        'chunk_dedup_decisions|run_id|uuid|NO',
        'chunk_relationships|chunk_id|uuid|NO',
        'chunk_relationships|related_chunk_id|uuid|NO',
        'chunk_relationships|relationship_type|text|NO',
        'chunk_relationships|dedup_method_id|smallint|NO',
        'chunk_relationships|similarity_score|real|NO',
        'chunk_relationships|decision_status|text|NO',
        'chunk_relationships|created_at|timestamp with time zone|NO',
        'chunk_relationships|run_id|uuid|NO'
    ];
BEGIN
    IF current_database() <> 'kb_pipeline_test' THEN
        RAISE EXCEPTION 'refusing verification on database % (expected kb_pipeline_test)', current_database();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace
        WHERE nspname = 'kbp' AND pg_catalog.pg_get_userbyid(nspowner) = 'kb_pipeline_migrator'
    ) THEN
        RAISE EXCEPTION 'schema kbp is missing or not owned by kb_pipeline_migrator';
    END IF;
    -- 001B objects are verified at kbp_001b or any later revision that keeps them.
    IF (SELECT count(*) FROM kbp.alembic_version WHERE version_num IN ('kbp_001b', 'kbp_001c', 'kbp_001d', 'kbp_001e')) <> 1 THEN
        RAISE EXCEPTION 'kbp.alembic_version is not kbp_001b or a later revision that includes it';
    END IF;

    SELECT count(*) INTO failures
    FROM unnest(expected_columns) AS x(spec)
    CROSS JOIN LATERAL (
        SELECT split_part(x.spec, '|', 1) AS table_name,
               split_part(x.spec, '|', 2) AS column_name,
               split_part(x.spec, '|', 3) AS data_type,
               split_part(x.spec, '|', 4) AS is_nullable
    ) AS e
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.columns AS c
        WHERE c.table_schema = 'kbp' AND c.table_name = e.table_name
          AND c.column_name = e.column_name AND c.data_type = e.data_type
          AND c.is_nullable = e.is_nullable
    );
    IF failures <> 0 OR (
        SELECT count(*) FROM information_schema.columns
        WHERE table_schema = 'kbp' AND table_name IN ('chunk_dedup_decisions', 'chunk_relationships')
    ) <> cardinality(expected_columns) THEN
        RAISE EXCEPTION '001B column definitions differ from the reviewed schema';
    END IF;

    IF (SELECT count(*) FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'kbp' AND c.conname IN (
            'chunk_dedup_decisions_pkey', 'chunk_relationships_pkey'
        ) AND c.contype = 'p') <> 2 THEN
        RAISE EXCEPTION '001B primary keys are missing';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'kbp' AND c.conname IN (
            'chunk_dedup_decisions_chunk_fkey', 'chunk_dedup_decisions_method_fkey',
            'chunk_dedup_decisions_representative_fkey', 'chunk_dedup_decisions_run_fkey',
            'chunk_relationships_chunk_fkey', 'chunk_relationships_related_chunk_fkey',
            'chunk_relationships_method_fkey', 'chunk_relationships_run_fkey'
        ) AND c.contype = 'f' AND c.convalidated) <> 8 THEN
        RAISE EXCEPTION '001B foreign keys are missing or unvalidated';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'kbp' AND t.relname IN ('chunk_dedup_decisions', 'chunk_relationships')
          AND c.contype = 'c' AND c.convalidated) <> 10 THEN
        RAISE EXCEPTION '001B CHECK constraints are missing or unvalidated';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = 'kbp.chunk_relationships'::regclass
          AND c.conname = 'chunk_relationships_pair_order_check'
          AND c.contype = 'c' AND c.convalidated
          AND pg_catalog.pg_get_constraintdef(c.oid) LIKE '%chunk_id < related_chunk_id%'
    ) THEN
        RAISE EXCEPTION '001B deterministic relationship pair-order CHECK is missing';
    END IF;

    IF pg_catalog.pg_get_indexdef('kbp.chunk_dedup_decisions_representative_idx'::regclass)
           NOT LIKE '%(representative_chunk_id, dedup_method_id)%'
       OR pg_catalog.pg_get_indexdef('kbp.chunk_relationships_related_idx'::regclass)
           NOT LIKE '%(related_chunk_id, chunk_id, dedup_method_id)%' THEN
        RAISE EXCEPTION '001B lookup indexes are missing';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_class t
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'kbp' AND t.relname IN ('chunk_dedup_decisions', 'chunk_relationships')
          AND pg_catalog.pg_get_userbyid(t.relowner) = 'kb_pipeline_migrator') <> 2 THEN
        RAISE EXCEPTION '001B tables are not owned by kb_pipeline_migrator';
    END IF;

    IF NOT (
        has_table_privilege('kb_pipeline_app', 'kbp.chunk_dedup_decisions', 'SELECT')
        AND has_table_privilege('kb_pipeline_app', 'kbp.chunk_dedup_decisions', 'INSERT')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_dedup_decisions', 'UPDATE')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_dedup_decisions', 'DELETE')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_dedup_decisions', 'TRUNCATE')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_dedup_decisions', 'REFERENCES')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_dedup_decisions', 'TRIGGER')
        AND has_table_privilege('kb_pipeline_app', 'kbp.chunk_relationships', 'SELECT')
        AND has_table_privilege('kb_pipeline_app', 'kbp.chunk_relationships', 'INSERT')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_relationships', 'UPDATE')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_relationships', 'DELETE')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_relationships', 'TRUNCATE')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_relationships', 'REFERENCES')
        AND NOT has_table_privilege('kb_pipeline_app', 'kbp.chunk_relationships', 'TRIGGER')
        AND has_schema_privilege('kb_pipeline_app', 'kbp', 'USAGE')
        AND NOT has_schema_privilege('kb_pipeline_app', 'kbp', 'CREATE')
    ) THEN
        RAISE EXCEPTION '001B runtime privileges differ from SELECT/INSERT only';
    END IF;
    RAISE NOTICE 'MIGRATION 001B VERIFICATION PASSED';
END
$checks$;

SELECT current_database() AS database, current_user AS verified_by,
       (SELECT version_num FROM kbp.alembic_version) AS kbp_revision,
       (SELECT count(*) FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'kbp' AND t.relname IN ('chunk_dedup_decisions', 'chunk_relationships')) AS constraints;

ROLLBACK;
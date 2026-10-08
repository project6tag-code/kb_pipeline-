-- =============================================================================
-- verify_migration_001d.sql - READ ONLY validation of Migration 001D
-- =============================================================================
-- Run only on kb_pipeline_test (as kb_pipeline_migrator or a catalog reader):
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U kb_pipeline_migrator -d kb_pipeline_test \
--        -f scripts/db/verify_migration_001d.sql
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public;

DO $checks$
DECLARE
    tbl oid;
    columns text;
BEGIN
    IF current_database() <> 'kb_pipeline_test' THEN
        RAISE EXCEPTION 'refusing verification on database % (expected kb_pipeline_test)', current_database();
    END IF;
    IF (SELECT count(*) FROM kbp.alembic_version WHERE version_num IN ('kbp_001d', 'kbp_001e')) <> 1 THEN
        RAISE EXCEPTION 'kbp.alembic_version is not kbp_001d or kbp_001e';
    END IF;
    tbl := 'kbp.gold_publications'::regclass;
    IF (SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE oid = tbl)
       <> 'kb_pipeline_migrator' THEN
        RAISE EXCEPTION 'kbp.gold_publications is not owned by kb_pipeline_migrator';
    END IF;

    SELECT string_agg(a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) || ':' ||
                      a.attnotnull::text || ':' || (d.adbin IS NOT NULL)::text,
                      ',' ORDER BY a.attnum)
    INTO columns
    FROM pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE a.attrelid = tbl AND a.attnum > 0 AND NOT a.attisdropped;
    IF columns IS DISTINCT FROM
       'chunk_id:uuid:true:false,embedding_model_id:smallint:true:false,published_at:timestamp with time zone:true:false,run_id:uuid:true:false' THEN
        RAISE EXCEPTION '001D columns differ from the reviewed schema: %', columns;
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid = tbl) <> 3 THEN
        RAISE EXCEPTION '001D must contain exactly its PK and two FKs';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = tbl AND c.conname = 'gold_publications_pkey' AND c.contype = 'p'
          AND pg_catalog.pg_get_constraintdef(c.oid) = 'PRIMARY KEY (chunk_id, embedding_model_id)'
    ) THEN
        RAISE EXCEPTION '001D pair primary key is missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = tbl AND c.conname = 'gold_publications_embedding_fkey' AND c.contype = 'f'
          AND c.confrelid = 'kbp.chunk_embeddings'::regclass AND c.convalidated AND NOT c.condeferrable
          AND c.confupdtype = 'a' AND c.confdeltype = 'a'
          AND pg_catalog.pg_get_constraintdef(c.oid) =
              'FOREIGN KEY (chunk_id, embedding_model_id) REFERENCES kbp.chunk_embeddings(chunk_id, embedding_model_id)'
    ) THEN
        RAISE EXCEPTION '001D composite FK to chunk_embeddings is missing or differs';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        WHERE c.conrelid = tbl AND c.conname = 'gold_publications_run_fkey' AND c.contype = 'f'
          AND c.confrelid = 'kbp.pipeline_runs'::regclass AND c.convalidated AND NOT c.condeferrable
          AND c.confupdtype = 'a' AND c.confdeltype = 'a'
          AND pg_catalog.pg_get_constraintdef(c.oid) =
              'FOREIGN KEY (run_id) REFERENCES kbp.pipeline_runs(run_id)'
    ) THEN
        RAISE EXCEPTION '001D run FK is missing or differs';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_index WHERE indrelid = tbl) <> 1 THEN
        RAISE EXCEPTION '001D must have only the primary-key index';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = tbl AND NOT tgisinternal) THEN
        RAISE EXCEPTION '001D must not have user triggers';
    END IF;
    IF NOT (
        has_table_privilege('kb_pipeline_app', tbl, 'SELECT')
        AND has_table_privilege('kb_pipeline_app', tbl, 'INSERT')
        AND NOT has_table_privilege('kb_pipeline_app', tbl, 'UPDATE')
        AND NOT has_table_privilege('kb_pipeline_app', tbl, 'DELETE')
        AND NOT has_table_privilege('kb_pipeline_app', tbl, 'TRUNCATE')
        AND NOT has_table_privilege('kb_pipeline_app', tbl, 'REFERENCES')
        AND NOT has_table_privilege('kb_pipeline_app', tbl, 'TRIGGER')
        AND has_schema_privilege('kb_pipeline_app', 'kbp', 'USAGE')
        AND NOT has_schema_privilege('kb_pipeline_app', 'kbp', 'CREATE')
    ) THEN
        RAISE EXCEPTION '001D runtime privileges differ from SELECT/INSERT only';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c, LATERAL aclexplode(c.relacl) x
               WHERE c.oid = tbl AND (x.grantee = 0 OR x.is_grantable
                     OR pg_catalog.pg_get_userbyid(x.grantee) NOT IN ('kb_pipeline_migrator', 'kb_pipeline_app'))) THEN
        RAISE EXCEPTION '001D table has PUBLIC, third-party or grantable privileges';
    END IF;
    RAISE NOTICE 'MIGRATION 001D VERIFICATION PASSED';
END
$checks$;

SELECT current_database() AS database, current_user AS verified_by,
       (SELECT version_num FROM kbp.alembic_version) AS kbp_revision,
       (SELECT count(*) FROM kbp.gold_publications) AS publication_rows;

ROLLBACK;
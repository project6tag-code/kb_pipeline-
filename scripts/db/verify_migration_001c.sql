-- =============================================================================
-- verify_migration_001c.sql - READ ONLY verification after Migration 001C
-- =============================================================================
-- Run as kb_pipeline_migrator (or an administrator), only against kb_pipeline_test:
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U kb_pipeline_migrator -d kb_pipeline_test \
--        -f scripts/db/verify_migration_001c.sql
-- Catalog reads only; the transaction is READ ONLY and rolled back.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public;  -- deterministic format_type()/constraint text (read-only)

DO $checks$
DECLARE
    tbl oid;
    vector_type oid;
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
    IF (SELECT count(*) FROM kbp.alembic_version WHERE version_num = 'kbp_001c') <> 1 THEN
        RAISE EXCEPTION 'kbp.alembic_version is not exactly kbp_001c';
    END IF;

    -- pgvector: installed in public, and public.vector belongs to the extension
    SELECT t.oid INTO vector_type FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typname = 'vector' AND n.nspname = 'public';
    IF vector_type IS NULL OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
        JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
        WHERE d.classid = 'pg_catalog.pg_type'::regclass AND d.objid = vector_type AND d.deptype = 'e'
          AND e.extname = 'vector' AND n.nspname = 'public'
    ) THEN
        RAISE EXCEPTION 'type public.vector is missing or not owned by extension vector in schema public';
    END IF;

    tbl := 'kbp.chunk_embedding_vectors'::regclass;
    IF (SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE oid = tbl) <> 'kb_pipeline_migrator' THEN
        RAISE EXCEPTION 'kbp.chunk_embedding_vectors is not owned by kb_pipeline_migrator';
    END IF;

    -- exactly four columns, in order, all NOT NULL, embedding = public.vector(768)
    IF (SELECT string_agg(a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull::text,
                          ',' ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute a WHERE a.attrelid = tbl AND a.attnum > 0 AND NOT a.attisdropped)
       IS DISTINCT FROM
       'chunk_id:uuid:true,embedding_model_id:smallint:true,embedding:vector(768):true,vector_sha256:bytea:true' THEN
        RAISE EXCEPTION '001C column definitions differ from the reviewed schema';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                   WHERE attrelid = tbl AND attname = 'embedding' AND atttypid = vector_type AND atttypmod = 768) THEN
        RAISE EXCEPTION 'embedding is not public.vector with 768 dimensions';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = tbl AND conname = 'chunk_embedding_vectors_pkey' AND contype = 'p'
                     AND pg_catalog.pg_get_constraintdef(oid) = 'PRIMARY KEY (chunk_id, embedding_model_id)') THEN
        RAISE EXCEPTION '001C primary key is missing';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = tbl AND conname = 'chunk_embedding_vectors_result_fkey' AND contype = 'f'
                     AND confrelid = 'kbp.chunk_embeddings'::regclass AND convalidated AND NOT condeferrable
                     AND confupdtype = 'a' AND confdeltype = 'a'
                     AND pg_catalog.pg_get_constraintdef(oid) =
                         'FOREIGN KEY (chunk_id, embedding_model_id) REFERENCES kbp.chunk_embeddings(chunk_id, embedding_model_id)') THEN
        RAISE EXCEPTION '001C composite FK to kbp.chunk_embeddings is missing or not NO ACTION / validated';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = tbl AND conname = 'chunk_embedding_vectors_sha256_check' AND contype = 'c'
                     AND convalidated AND pg_catalog.pg_get_constraintdef(oid) LIKE '%octet_length(vector_sha256) = 32%') THEN
        RAISE EXCEPTION '001C vector_sha256 length CHECK is missing';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid = tbl) <> 3 THEN
        RAISE EXCEPTION '001C table has unexpected constraints';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_index WHERE indrelid = tbl) <> 1 THEN
        RAISE EXCEPTION '001C table must have only its primary-key index (no ANN / vector index)';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = tbl AND NOT tgisinternal) THEN
        RAISE EXCEPTION '001C table must have no user triggers';
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
        RAISE EXCEPTION '001C runtime privileges differ from SELECT/INSERT only';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c, LATERAL aclexplode(c.relacl) x
               WHERE c.oid = tbl AND (x.grantee = 0 OR x.is_grantable
                     OR pg_catalog.pg_get_userbyid(x.grantee) NOT IN ('kb_pipeline_migrator', 'kb_pipeline_app'))) THEN
        RAISE EXCEPTION '001C table has PUBLIC, third-party or grantable privileges';
    END IF;
    RAISE NOTICE 'MIGRATION 001C VERIFICATION PASSED';
END
$checks$;

SELECT current_database() AS database, current_user AS verified_by,
       (SELECT version_num FROM kbp.alembic_version) AS kbp_revision,
       (SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'vector') AS pgvector,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS embedding_type,
       (SELECT count(*) FROM kbp.chunk_embedding_vectors) AS vector_rows
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = 'kbp.chunk_embedding_vectors'::regclass AND a.attname = 'embedding';

ROLLBACK;

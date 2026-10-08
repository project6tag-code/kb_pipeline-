-- =============================================================================
-- admin_gold_v2_patch.sql - ONE-TIME admin patch for Gold V2 (kb_pipeline_test ONLY)
-- =============================================================================
-- What it does (docs/cc_fact_kb_v2.md §4), idempotent, in one transaction:
--   1. public.metadata gains ONE nullable column, crawled_at timestamptz (no default).
--   1b. public.kb gains ONE nullable column, source_id uuid (no default, no constraint):
--      the original source document/capture of the chunk (CC: its WARC-Record-ID), equal to
--      public.metadata.source_id. kb.id stays the unique chunk id; legacy rows keep NULL.
--      Existing columns, rows, constraints, owner and ACL are untouched.
--   2. kb_pipeline_app gets SELECT, INSERT on public.datasets, so the Gold publisher
--      can register one KB_SOURCE_DOCUMENT row per original source document
--      (public.metadata.source_id -> public.datasets.id). No UPDATE, no DELETE.
--
-- Run as the owner of the public tables (postgres), with credentials from pgpass:
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d kb_pipeline_test -f scripts/db/admin_gold_v2_patch.sql
-- Prerequisite: kbp is at kbp_001e (alembic upgrade head as kb_pipeline_migrator).
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public;

DO $guard$
BEGIN
    IF current_database() <> 'kb_pipeline_test' THEN
        RAISE EXCEPTION 'refusing Gold V2 patch on database % (only kb_pipeline_test)', current_database();
    END IF;
    IF (SELECT count(*) FROM kbp.alembic_version WHERE version_num = 'kbp_001e') <> 1 THEN
        RAISE EXCEPTION 'kbp must be at kbp_001e before the Gold V2 patch';
    END IF;
    IF current_user IN ('kb_pipeline_app', 'kb_pipeline_migrator') THEN
        RAISE EXCEPTION 'run the Gold V2 patch as the public table owner, not as a pipeline role';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
               WHERE attrelid = 'public.metadata'::regclass AND attname = 'crawled_at' AND NOT attisdropped
                 AND pg_catalog.format_type(atttypid, atttypmod) <> 'timestamp with time zone') THEN
        RAISE EXCEPTION 'public.metadata.crawled_at exists with an unexpected type';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
               WHERE attrelid = 'public.kb'::regclass AND attname = 'source_id' AND NOT attisdropped
                 AND pg_catalog.format_type(atttypid, atttypmod) <> 'uuid') THEN
        RAISE EXCEPTION 'public.kb.source_id exists with an unexpected type';
    END IF;
END
$guard$;

ALTER TABLE public.metadata ADD COLUMN IF NOT EXISTS crawled_at timestamptz;
ALTER TABLE public.kb ADD COLUMN IF NOT EXISTS source_id uuid;
GRANT SELECT, INSERT ON TABLE public.datasets TO kb_pipeline_app;
-- The runtime role holds column-level privileges on public.metadata (INSERT only on a new column), and the
-- publisher reads every inserted column back to verify it: crawled_at needs SELECT too.
GRANT SELECT (crawled_at) ON TABLE public.metadata TO kb_pipeline_app;

DO $verify$
BEGIN
    IF (SELECT string_agg(attname || ':' || pg_catalog.format_type(atttypid, atttypmod) || ':' || attnotnull::text,
                          ',' ORDER BY attnum)
        FROM pg_catalog.pg_attribute
        WHERE attrelid = 'public.metadata'::regclass AND attnum > 0 AND NOT attisdropped)
       <> 'id:uuid:true,source_id:uuid:true,domain:character varying(100):false,source_url:text:false,'
          'source_title:text:false,source_type:character varying(50):false,'
          'published_at:timestamp with time zone:false,updated_at:timestamp with time zone:false,'
          'crawled_at:timestamp with time zone:false' THEN
        RAISE EXCEPTION 'public.metadata does not have the Gold V2 column layout';
    END IF;
    IF (SELECT string_agg(attname || ':' || pg_catalog.format_type(atttypid, atttypmod) || ':' || attnotnull::text,
                          ',' ORDER BY attnum)
        FROM pg_catalog.pg_attribute
        WHERE attrelid = 'public.kb'::regclass AND attnum > 0 AND NOT attisdropped)
       <> 'id:uuid:true,chunk:text:true,embedding:vector(768):false,source_id:uuid:false' THEN
        RAISE EXCEPTION 'public.kb does not have the Gold V2 column layout';
    END IF;
    IF NOT pg_catalog.has_table_privilege('kb_pipeline_app', 'public.datasets', 'INSERT')
       OR pg_catalog.has_table_privilege('kb_pipeline_app', 'public.datasets', 'UPDATE')
       OR pg_catalog.has_table_privilege('kb_pipeline_app', 'public.datasets', 'DELETE') THEN
        RAISE EXCEPTION 'kb_pipeline_app privileges on public.datasets are not exactly SELECT, INSERT';
    END IF;
    IF NOT pg_catalog.has_column_privilege('kb_pipeline_app', 'public.metadata', 'crawled_at', 'SELECT')
       OR NOT pg_catalog.has_column_privilege('kb_pipeline_app', 'public.metadata', 'crawled_at', 'INSERT') THEN
        RAISE EXCEPTION 'kb_pipeline_app needs SELECT and INSERT on public.metadata.crawled_at';
    END IF;
END
$verify$;

COMMIT;

\echo 'Gold V2 patch: OK (metadata.crawled_at, kb.source_id present; kb_pipeline_app may SELECT, INSERT public.datasets)'

-- =============================================================================
-- admin_reset_kb_pipeline_test.sql - DESTRUCTIVE content reset of kb_pipeline_test ONLY
-- =============================================================================
-- Empties every pipeline CONTENT table so the Common Crawl fact KB can be rebuilt
-- from scratch under the V2 rules (docs/cc_fact_kb_v2.md §6). It keeps:
--   * the schema, roles, grants and kbp.alembic_version;
--   * the frozen registrations: kbp.dedup_methods, kbp.chunking_versions,
--     kbp.embedding_models (model 2 stays active_gold), kbp.source_dataset_map;
--   * public.dataset_sources and the two KB_REFERENCE bridge rows in public.datasets.
-- It deletes: public.kb, public.metadata, KB_SOURCE_DOCUMENT rows of public.datasets,
-- and all kbp observations, documents, chunks, embeddings, Gold ledger, jobs, runs,
-- attempts, dead letters and rate-limit state.
--
-- TAKE A BACKUP FIRST, then run as postgres with an explicit confirmation:
--   pg_dump -h localhost -U postgres -Fc -f backup\kb_pipeline_test_before_v2_reset.dump kb_pipeline_test
--   psql -X -v ON_ERROR_STOP=1 -v confirm=RESET_KB_PIPELINE_TEST -h localhost -U postgres -d kb_pipeline_test `
--        -f scripts/db/admin_reset_kb_pipeline_test.sql
-- Also delete the local derived state afterwards (docs/cc_fact_kb_v2.md §6): Bronze files,
-- data/silver/near_index.sqlite3*, data/chunk_dedup/*.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

SELECT :'confirm' = 'RESET_KB_PIPELINE_TEST' AS confirmed \gset
\if :confirmed
\else
    \echo 'refusing: pass -v confirm=RESET_KB_PIPELINE_TEST'
    \quit 3
\endif

BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '300s';
SET LOCAL search_path = pg_catalog, public;

DO $guard$
BEGIN
    IF current_database() <> 'kb_pipeline_test' THEN
        RAISE EXCEPTION 'refusing reset of database % (only kb_pipeline_test may be reset)', current_database();
    END IF;
    IF (SELECT count(*) FROM kbp.alembic_version WHERE version_num = 'kbp_001e') <> 1 THEN
        RAISE EXCEPTION 'kbp must be at kbp_001e (run alembic upgrade head first)';
    END IF;
    IF EXISTS (SELECT 1 FROM kbp.pipeline_jobs WHERE status = 'running') THEN
        RAISE EXCEPTION 'pipeline jobs are running: stop every worker before the reset';
    END IF;
END
$guard$;

TRUNCATE TABLE
    public.kb, public.metadata,
    kbp.gold_publications, kbp.chunk_embedding_vectors, kbp.chunk_embeddings, kbp.embedding_queue,
    kbp.chunk_relationships, kbp.chunk_dedup_decisions, kbp.chunk_occurrences, kbp.chunk_registry,
    kbp.document_relationships, kbp.document_dedup_decisions, kbp.observation_extractions, kbp.document_registry,
    kbp.source_dates, kbp.cc_captures, kbp.cc_warc_files, kbp.cc_crawls,
    kbp.github_file_observations, kbp.github_commits, kbp.github_repositories,
    kbp.source_observations, kbp.legacy_kb_reconciliation,
    kbp.dead_letters, kbp.processing_attempts, kbp.pipeline_jobs, kbp.source_rate_limits, kbp.pipeline_runs
    RESTART IDENTITY;

DELETE FROM public.datasets WHERE status = 'KB_SOURCE_DOCUMENT';

DO $verify$
BEGIN
    IF (SELECT count(*) FROM public.datasets WHERE status = 'KB_REFERENCE') <> 2 THEN
        RAISE EXCEPTION 'the two KB_REFERENCE bridge rows are not intact';
    END IF;
    IF (SELECT count(*) FROM kbp.source_dataset_map) <> 2
       OR NOT EXISTS (SELECT 1 FROM kbp.embedding_models WHERE embedding_model_id = 2) THEN
        RAISE EXCEPTION 'frozen registrations are not intact';
    END IF;
END
$verify$;

COMMIT;

\echo 'kb_pipeline_test content reset: OK (registrations and bridge rows kept)'

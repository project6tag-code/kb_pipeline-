-- =============================================================================
-- verify_migration_001a.sql - READ ONLY verification after Migration 001A
-- =============================================================================
--
-- Verifies the database against Migration 001A
-- (migrations/versions/kbp_001a_schema_v1.py), which implements the LOCKED
-- docs/database_schema_v1.md and docs/database_privileges_v1.md section 5.1,
-- and proves the legacy objects unchanged against the baseline. Runbook steps
-- 9, 10 and 14 (docs/migrations_kbp.md section 6). NOT executed during
-- preparation.
--
-- Run by: the PostgreSQL administrator (e.g. postgres), who can read both kbp
-- and the legacy tables. It only reads.
--
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline
--        -v baseline=<evidence>/baseline_001a.psql
--        -f scripts/db/verify_migration_001a.sql > <evidence>/09_verify_upgrade.txt 2>&1
--
-- The expected sets below are generated from the migration module; the unit
-- test tests/unit/test_db_scripts.py re-derives them and fails on any drift.
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
WHERE n.nspname = 'kbp' AND c.relkind IN ('r', 'p')
ORDER BY c.relname;

\echo '== CHECKS: Migration 001A structure, privileges and ownership =='
DO $checks$
DECLARE
    c        record;
    ok       boolean;
    detail   text;
    failures integer := 0;
BEGIN
    FOR c IN
        SELECT * FROM (VALUES
        ('M01', 'schema kbp exists and is owned by kb_pipeline_migrator',
         $q$SELECT coalesce(bool_and(pg_catalog.pg_get_userbyid(nspowner) = 'kb_pipeline_migrator'), false),
                   coalesce(string_agg('owner=' || pg_catalog.pg_get_userbyid(nspowner), ','), 'schema kbp is missing')
            FROM pg_catalog.pg_namespace WHERE nspname = 'kbp'$q$),

        ('M02', 'kbp.alembic_version exists (the isolated version table)',
         $q$SELECT to_regclass('kbp.alembic_version') IS NOT NULL,
                   coalesce(to_regclass('kbp.alembic_version')::text, 'missing')$q$),

        ('M03', 'kbp.alembic_version holds exactly one row: kbp_001a',
         $q$SELECT count(*) = 1 AND min(version_num) = 'kbp_001a',
                   coalesce(string_agg(version_num, ','), 'no rows')
            FROM kbp.alembic_version$q$),

        ('M04', 'kbp holds exactly the 25 application tables plus alembic_version, and only tables, indexes and identity sequences',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED tables (generated from the migration module)
                    ARRAY[
                        'pipeline_runs',
                        'cc_crawls',
                        'cc_warc_files',
                        'github_repositories',
                        'github_commits',
                        'source_observations',
                        'cc_captures',
                        'github_file_observations',
                        'document_registry',
                        'observation_extractions',
                        'dedup_methods',
                        'document_dedup_decisions',
                        'document_relationships',
                        'chunking_versions',
                        'chunk_registry',
                        'chunk_occurrences',
                        'embedding_models',
                        'chunk_embeddings',
                        'embedding_queue',
                        'pipeline_jobs',
                        'processing_attempts',
                        'dead_letters',
                        'source_rate_limits',
                        'source_dataset_map',
                        'legacy_kb_reconciliation'
                    ]::text[]
                    -- END EXPECTED tables
                 ) AS k),
                 actual AS (
                     SELECT c.relname AS k
                     FROM pg_catalog.pg_class AS c
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'kbp' AND c.relkind NOT IN ('i', 'S') AND c.relname <> 'alembic_version'
                     UNION ALL
                     SELECT 'unexpected relkind ' || c.relkind::text || ' ' || c.relname
                     FROM pg_catalog.pg_class AS c
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'kbp' AND c.relkind NOT IN ('r', 'i', 'S')),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'unexpected ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M05', 'columns: names, order, types, nullability, identity and defaults match (252 columns)',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED columns (generated from the migration module)
                    ARRAY[
                        'pipeline_runs|1|run_id|uuid|t|f|',
                        'pipeline_runs|2|run_kind|text|t|f|',
                        'pipeline_runs|3|status|text|t|f|',
                        'pipeline_runs|4|started_at|timestamp with time zone|t|f|',
                        'pipeline_runs|5|finished_at|timestamp with time zone|f|f|',
                        'pipeline_runs|6|code_version|text|f|f|',
                        'pipeline_runs|7|config_sha256|bytea|f|f|',
                        'pipeline_runs|8|identity_spec_version|text|t|f|',
                        'pipeline_runs|9|identity_extension_version|text|f|f|',
                        'pipeline_runs|10|host|text|f|f|',
                        'pipeline_runs|11|notes|text|f|f|',
                        'cc_crawls|1|crawl_id|text|t|f|',
                        'cc_crawls|2|index_kind|text|f|f|',
                        'cc_crawls|3|registered_at|timestamp with time zone|t|f|',
                        'cc_warc_files|1|warc_file_id|bigint|t|t|',
                        'cc_warc_files|2|crawl_id|text|t|f|',
                        'cc_warc_files|3|warc_filename|text|t|f|',
                        'cc_warc_files|4|registered_at|timestamp with time zone|t|f|',
                        'github_repositories|1|repository_id|bigint|t|f|',
                        'github_repositories|2|node_id|text|f|f|',
                        'github_repositories|3|owner_login|text|t|f|',
                        'github_repositories|4|repo_name|text|t|f|',
                        'github_repositories|5|html_url|text|t|f|',
                        'github_repositories|6|default_branch|text|f|f|',
                        'github_repositories|7|visibility|text|t|f|',
                        'github_repositories|8|license_spdx_id|text|f|f|',
                        'github_repositories|9|is_fork|boolean|f|f|',
                        'github_repositories|10|is_archived|boolean|f|f|',
                        'github_repositories|11|api_etag|text|f|f|',
                        'github_repositories|12|last_snapshot_commit_sha|text|f|f|',
                        'github_repositories|13|first_seen_at|timestamp with time zone|t|f|',
                        'github_repositories|14|metadata_refreshed_at|timestamp with time zone|f|f|',
                        'github_commits|1|repository_id|bigint|t|f|',
                        'github_commits|2|commit_sha|text|t|f|',
                        'github_commits|3|tree_sha|text|f|f|',
                        'github_commits|4|ref_name|text|f|f|',
                        'github_commits|5|committed_at|timestamp with time zone|f|f|',
                        'github_commits|6|visibility_at_snapshot|text|t|f|',
                        'github_commits|7|previous_snapshot_sha|text|f|f|',
                        'github_commits|8|snapshot_mode|text|t|f|',
                        'github_commits|9|resolved_at|timestamp with time zone|t|f|',
                        'source_observations|1|observation_id|uuid|t|f|',
                        'source_observations|2|source_channel|text|t|f|',
                        'source_observations|3|source_url|text|t|f|',
                        'source_observations|4|url_host|text|f|f|',
                        'source_observations|5|source_title|text|f|f|',
                        'source_observations|6|source_published_at|timestamp with time zone|f|f|',
                        'source_observations|7|acquired_at|timestamp with time zone|t|f|',
                        'source_observations|8|run_id|uuid|f|f|',
                        'cc_captures|1|observation_id|uuid|t|f|',
                        'cc_captures|2|warc_file_id|bigint|t|f|',
                        'cc_captures|3|warc_record_offset|bigint|t|f|',
                        'cc_captures|4|warc_record_length|integer|t|f|',
                        'cc_captures|5|warc_record_id|uuid|f|f|',
                        'cc_captures|6|warc_date|timestamp with time zone|f|f|',
                        'cc_captures|7|http_status|smallint|f|f|',
                        'cc_captures|8|http_content_type|text|f|f|',
                        'cc_captures|9|detected_mime|text|f|f|',
                        'cc_captures|10|redirect_location|text|f|f|',
                        'cc_captures|11|canonical_url|text|f|f|',
                        'cc_captures|12|payload_digest|text|f|f|',
                        'github_file_observations|1|observation_id|uuid|t|f|',
                        'github_file_observations|2|repository_id|bigint|t|f|',
                        'github_file_observations|3|commit_sha|text|t|f|',
                        'github_file_observations|4|file_path|text|t|f|',
                        'github_file_observations|5|blob_sha|text|t|f|',
                        'github_file_observations|6|file_size_bytes|bigint|f|f|',
                        'document_registry|1|document_id|uuid|t|f|',
                        'document_registry|2|content_hash|bytea|t|f|',
                        'document_registry|3|canonical_text|text|f|f|',
                        'document_registry|4|char_length|integer|t|f|',
                        'document_registry|5|language|text|f|f|',
                        'document_registry|6|language_confidence|real|f|f|',
                        'document_registry|7|quality_score|real|f|f|',
                        'document_registry|8|factuality_score|real|f|f|',
                        'document_registry|9|subject_domain|text|f|f|',
                        'document_registry|10|status|text|t|f|',
                        'document_registry|11|status_reason|text|f|f|',
                        'document_registry|12|policy_version|text|t|f|',
                        'document_registry|13|component_versions|jsonb|t|f|',
                        'document_registry|14|first_observation_id|uuid|t|f|',
                        'document_registry|15|created_at|timestamp with time zone|t|f|now()',
                        'document_registry|16|status_updated_at|timestamp with time zone|t|f|',
                        'observation_extractions|1|observation_id|uuid|t|f|',
                        'observation_extractions|2|extraction_version|text|t|f|',
                        'observation_extractions|3|outcome|text|t|f|',
                        'observation_extractions|4|document_id|uuid|f|f|',
                        'observation_extractions|5|document_was_new|boolean|f|f|',
                        'observation_extractions|6|extracted_at|timestamp with time zone|t|f|',
                        'dedup_methods|1|dedup_method_id|smallint|t|t|',
                        'dedup_methods|2|algorithm|text|t|f|',
                        'dedup_methods|3|algorithm_version|text|t|f|',
                        'dedup_methods|4|parameters|jsonb|t|f|',
                        'dedup_methods|5|created_at|timestamp with time zone|t|f|now()',
                        'document_dedup_decisions|1|document_id|uuid|t|f|',
                        'document_dedup_decisions|2|dedup_method_id|smallint|t|f|',
                        'document_dedup_decisions|3|classification|text|t|f|',
                        'document_dedup_decisions|4|canonical_document_id|uuid|f|f|',
                        'document_dedup_decisions|5|similarity_score|real|f|f|',
                        'document_dedup_decisions|6|decision_status|text|t|f|',
                        'document_dedup_decisions|7|decided_at|timestamp with time zone|t|f|',
                        'document_dedup_decisions|8|run_id|uuid|f|f|',
                        'document_relationships|1|document_id|uuid|t|f|',
                        'document_relationships|2|related_document_id|uuid|t|f|',
                        'document_relationships|3|relationship_type|text|t|f|',
                        'document_relationships|4|dedup_method_id|smallint|t|f|',
                        'document_relationships|5|similarity_score|real|f|f|',
                        'document_relationships|6|decision_status|text|t|f|',
                        'document_relationships|7|created_at|timestamp with time zone|t|f|now()',
                        'chunking_versions|1|chunking_version_id|smallint|t|t|',
                        'chunking_versions|2|name|text|t|f|',
                        'chunking_versions|3|tokenizer|text|f|f|',
                        'chunking_versions|4|parameters|jsonb|t|f|',
                        'chunking_versions|5|created_at|timestamp with time zone|t|f|now()',
                        'chunk_registry|1|chunk_id|uuid|t|f|',
                        'chunk_registry|2|chunk_hash|bytea|t|f|',
                        'chunk_registry|3|chunk_text|text|t|f|',
                        'chunk_registry|4|char_length|integer|t|f|',
                        'chunk_registry|5|token_count|integer|f|f|',
                        'chunk_registry|6|chunking_version_id|smallint|t|f|',
                        'chunk_registry|7|first_document_id|uuid|t|f|',
                        'chunk_registry|8|representative_observation_id|uuid|t|f|',
                        'chunk_registry|9|created_at|timestamp with time zone|t|f|now()',
                        'chunk_occurrences|1|document_id|uuid|t|f|',
                        'chunk_occurrences|2|chunking_version_id|smallint|t|f|',
                        'chunk_occurrences|3|ordinal|integer|t|f|',
                        'chunk_occurrences|4|chunk_id|uuid|t|f|',
                        'chunk_occurrences|5|char_start|integer|t|f|',
                        'chunk_occurrences|6|char_end|integer|t|f|',
                        'chunk_occurrences|7|section_path|text|f|f|',
                        'embedding_models|1|embedding_model_id|smallint|t|t|',
                        'embedding_models|2|model_name|text|t|f|',
                        'embedding_models|3|model_revision|text|t|f|',
                        'embedding_models|4|embedding_version|text|t|f|',
                        'embedding_models|5|embedding_dimension|integer|t|f|',
                        'embedding_models|6|vector_type|text|t|f|',
                        'embedding_models|7|distance_metric|text|t|f|',
                        'embedding_models|8|is_normalized|boolean|t|f|',
                        'embedding_models|9|max_input_tokens|integer|f|f|',
                        'embedding_models|10|config|jsonb|t|f|',
                        'embedding_models|11|status|text|t|f|',
                        'embedding_models|12|created_at|timestamp with time zone|t|f|now()',
                        'chunk_embeddings|1|chunk_id|uuid|t|f|',
                        'chunk_embeddings|2|embedding_model_id|smallint|t|f|',
                        'chunk_embeddings|3|status|text|t|f|',
                        'chunk_embeddings|4|attempt_count|smallint|t|f|',
                        'chunk_embeddings|5|embedded_at|timestamp with time zone|t|f|',
                        'chunk_embeddings|6|published_at|timestamp with time zone|f|f|',
                        'chunk_embeddings|7|run_id|uuid|f|f|',
                        'embedding_queue|1|chunk_id|uuid|t|f|',
                        'embedding_queue|2|embedding_model_id|smallint|t|f|',
                        'embedding_queue|3|status|text|t|f|',
                        'embedding_queue|4|priority|smallint|t|f|100',
                        'embedding_queue|5|attempt_count|smallint|t|f|0',
                        'embedding_queue|6|max_attempts|smallint|t|f|',
                        'embedding_queue|7|next_attempt_at|timestamp with time zone|t|f|',
                        'embedding_queue|8|locked_at|timestamp with time zone|f|f|',
                        'embedding_queue|9|lease_expires_at|timestamp with time zone|f|f|',
                        'embedding_queue|10|worker_id|text|f|f|',
                        'embedding_queue|11|lease_token|uuid|f|f|',
                        'embedding_queue|12|last_error_class|text|f|f|',
                        'embedding_queue|13|last_error_message|text|f|f|',
                        'embedding_queue|14|enqueued_at|timestamp with time zone|t|f|',
                        'pipeline_jobs|1|job_id|bigint|t|t|',
                        'pipeline_jobs|2|job_type|text|t|f|',
                        'pipeline_jobs|3|source|text|t|f|',
                        'pipeline_jobs|4|source_identifier|text|t|f|',
                        'pipeline_jobs|5|status|text|t|f|',
                        'pipeline_jobs|6|stage|text|t|f|',
                        'pipeline_jobs|7|checkpoint|jsonb|t|f|{}',
                        'pipeline_jobs|8|priority|smallint|t|f|100',
                        'pipeline_jobs|9|attempt_count|smallint|t|f|0',
                        'pipeline_jobs|10|max_attempts|smallint|t|f|',
                        'pipeline_jobs|11|deferral_count|integer|t|f|0',
                        'pipeline_jobs|12|next_attempt_at|timestamp with time zone|t|f|',
                        'pipeline_jobs|13|locked_at|timestamp with time zone|f|f|',
                        'pipeline_jobs|14|lease_expires_at|timestamp with time zone|f|f|',
                        'pipeline_jobs|15|worker_id|text|f|f|',
                        'pipeline_jobs|16|lease_token|uuid|f|f|',
                        'pipeline_jobs|17|last_error_class|text|f|f|',
                        'pipeline_jobs|18|last_error_code|text|f|f|',
                        'pipeline_jobs|19|last_error_message|text|f|f|',
                        'pipeline_jobs|20|run_id|uuid|f|f|',
                        'pipeline_jobs|21|created_at|timestamp with time zone|t|f|now()',
                        'pipeline_jobs|22|started_at|timestamp with time zone|f|f|',
                        'pipeline_jobs|23|completed_at|timestamp with time zone|f|f|',
                        'pipeline_jobs|24|updated_at|timestamp with time zone|t|f|',
                        'processing_attempts|1|attempt_id|bigint|t|t|',
                        'processing_attempts|2|job_id|bigint|t|f|',
                        'processing_attempts|3|attempt_no|integer|t|f|',
                        'processing_attempts|4|worker_id|text|t|f|',
                        'processing_attempts|5|lease_token|uuid|t|f|',
                        'processing_attempts|6|started_at|timestamp with time zone|t|f|',
                        'processing_attempts|7|finished_at|timestamp with time zone|f|f|',
                        'processing_attempts|8|outcome|text|f|f|',
                        'processing_attempts|9|error_class|text|f|f|',
                        'processing_attempts|10|error_code|text|f|f|',
                        'processing_attempts|11|error_message|text|f|f|',
                        'processing_attempts|12|retry_after_seconds|integer|f|f|',
                        'processing_attempts|13|scheduled_next_attempt_at|timestamp with time zone|f|f|',
                        'processing_attempts|14|metrics|jsonb|f|f|',
                        'dead_letters|1|dead_letter_id|bigint|t|t|',
                        'dead_letters|2|item_type|text|t|f|',
                        'dead_letters|3|item_key|text|t|f|',
                        'dead_letters|4|stage|text|t|f|',
                        'dead_letters|5|disposition|text|t|f|',
                        'dead_letters|6|job_id|bigint|f|f|',
                        'dead_letters|7|run_id|uuid|f|f|',
                        'dead_letters|8|error_class|text|t|f|',
                        'dead_letters|9|error_code|text|f|f|',
                        'dead_letters|10|error_message|text|f|f|',
                        'dead_letters|11|attempt_count|integer|t|f|',
                        'dead_letters|12|occurrence_count|integer|t|f|1',
                        'dead_letters|13|payload_ref|text|f|f|',
                        'dead_letters|14|first_seen_at|timestamp with time zone|t|f|',
                        'dead_letters|15|last_seen_at|timestamp with time zone|t|f|',
                        'dead_letters|16|resolved_at|timestamp with time zone|f|f|',
                        'dead_letters|17|resolution|text|f|f|',
                        'source_rate_limits|1|source_channel|text|t|f|',
                        'source_rate_limits|2|limit_scope|text|t|f|',
                        'source_rate_limits|3|credential_fingerprint|text|t|f|',
                        'source_rate_limits|4|limit_total|integer|f|f|',
                        'source_rate_limits|5|remaining|integer|f|f|',
                        'source_rate_limits|6|reset_at|timestamp with time zone|f|f|',
                        'source_rate_limits|7|retry_after_until|timestamp with time zone|f|f|',
                        'source_rate_limits|8|last_http_status|smallint|f|f|',
                        'source_rate_limits|9|updated_at|timestamp with time zone|t|f|',
                        'source_dataset_map|1|source_channel|text|t|f|',
                        'source_dataset_map|2|dataset_source_name|text|t|f|',
                        'source_dataset_map|3|bridge_identity_name|text|t|f|',
                        'source_dataset_map|4|dataset_id|uuid|t|f|',
                        'source_dataset_map|5|created_at|timestamp with time zone|t|f|now()',
                        'legacy_kb_reconciliation|1|legacy_kb_id|uuid|t|f|',
                        'legacy_kb_reconciliation|2|legacy_source_id|uuid|f|f|',
                        'legacy_kb_reconciliation|3|raw_chunk_sha256|bytea|t|f|',
                        'legacy_kb_reconciliation|4|canonical_chunk_hash|bytea|f|f|',
                        'legacy_kb_reconciliation|5|canonical_chunk_id|uuid|f|f|',
                        'legacy_kb_reconciliation|6|canonical_differs|boolean|f|f|',
                        'legacy_kb_reconciliation|7|reconciliation_status|text|t|f|',
                        'legacy_kb_reconciliation|8|collision_chunk_id|uuid|f|f|',
                        'legacy_kb_reconciliation|9|collision_legacy_kb_id|uuid|f|f|',
                        'legacy_kb_reconciliation|10|decision|text|t|f|undecided',
                        'legacy_kb_reconciliation|11|decision_reason|text|f|f|',
                        'legacy_kb_reconciliation|12|review_state|text|t|f|not_required',
                        'legacy_kb_reconciliation|13|reviewed_by|text|f|f|',
                        'legacy_kb_reconciliation|14|reviewed_at|timestamp with time zone|f|f|',
                        'legacy_kb_reconciliation|15|error_message|text|f|f|',
                        'legacy_kb_reconciliation|16|reconciliation_version|text|t|f|',
                        'legacy_kb_reconciliation|17|run_id|uuid|f|f|',
                        'legacy_kb_reconciliation|18|first_reconciled_at|timestamp with time zone|t|f|',
                        'legacy_kb_reconciliation|19|reconciled_at|timestamp with time zone|t|f|',
                        'legacy_kb_reconciliation|20|decided_at|timestamp with time zone|f|f|'
                    ]::text[]
                    -- END EXPECTED columns
                 ) AS k),
                 actual AS (
                     SELECT c.relname || '|' || a.attnum::text || '|' || a.attname || '|'
                            || pg_catalog.format_type(a.atttypid, a.atttypmod) || '|'
                            || CASE WHEN a.attnotnull THEN 't' ELSE 'f' END || '|'
                            || CASE WHEN a.attidentity = 'a' THEN 't' ELSE 'f' END || '|'
                            || coalesce(btrim(regexp_replace(pg_catalog.pg_get_expr(d.adbin, d.adrelid),
                                                             '::[a-z_ ]+$', ''), ''''), '') AS k
                     FROM pg_catalog.pg_class AS c
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                     LEFT JOIN pg_catalog.pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
                     WHERE n.nspname = 'kbp' AND c.relkind = 'r' AND c.relname <> 'alembic_version'),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'unexpected ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M06', 'primary keys and UNIQUE constraints match (schema section 6.1)',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED keys (generated from the migration module)
                    ARRAY[
                        'pipeline_runs|p|run_id',
                        'cc_crawls|p|crawl_id',
                        'cc_warc_files|p|warc_file_id',
                        'cc_warc_files|u|warc_filename',
                        'github_repositories|p|repository_id',
                        'github_commits|p|repository_id,commit_sha',
                        'source_observations|p|observation_id',
                        'cc_captures|p|observation_id',
                        'cc_captures|u|warc_file_id,warc_record_offset',
                        'github_file_observations|p|observation_id',
                        'github_file_observations|u|repository_id,commit_sha,file_path',
                        'document_registry|p|document_id',
                        'document_registry|u|content_hash',
                        'observation_extractions|p|observation_id,extraction_version',
                        'dedup_methods|p|dedup_method_id',
                        'dedup_methods|u|algorithm,algorithm_version',
                        'document_dedup_decisions|p|document_id,dedup_method_id',
                        'document_relationships|p|document_id,related_document_id,relationship_type,dedup_method_id',
                        'chunking_versions|p|chunking_version_id',
                        'chunking_versions|u|name',
                        'chunk_registry|p|chunk_id',
                        'chunk_registry|u|chunk_hash',
                        'chunk_occurrences|p|document_id,chunking_version_id,ordinal',
                        'embedding_models|p|embedding_model_id',
                        'embedding_models|u|model_name,model_revision,embedding_version',
                        'chunk_embeddings|p|chunk_id,embedding_model_id',
                        'embedding_queue|p|chunk_id,embedding_model_id',
                        'pipeline_jobs|p|job_id',
                        'pipeline_jobs|u|job_type,source_identifier',
                        'processing_attempts|p|attempt_id',
                        'processing_attempts|u|job_id,attempt_no',
                        'dead_letters|p|dead_letter_id',
                        'dead_letters|u|item_type,item_key,stage',
                        'source_rate_limits|p|source_channel,limit_scope,credential_fingerprint',
                        'source_dataset_map|p|source_channel',
                        'source_dataset_map|u|bridge_identity_name',
                        'source_dataset_map|u|dataset_id',
                        'legacy_kb_reconciliation|p|legacy_kb_id',
                        'alembic_version|p|version_num'
                    ]::text[]
                    -- END EXPECTED keys
                 ) AS k),
                 actual AS (
                     SELECT cl.relname || '|' || co.contype::text || '|'
                            || (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
                                FROM unnest(co.conkey) WITH ORDINALITY AS k(attnum, ord)
                                JOIN pg_catalog.pg_attribute AS a ON a.attrelid = co.conrelid AND a.attnum = k.attnum) AS k
                     FROM pg_catalog.pg_constraint AS co
                     JOIN pg_catalog.pg_class AS cl ON cl.oid = co.conrelid
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = cl.relnamespace
                     WHERE n.nspname = 'kbp' AND co.contype IN ('p', 'u')),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'unexpected ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M07', 'foreign keys match: columns, targets, ON UPDATE/ON DELETE NO ACTION, not deferrable, validated (34, incl. the approved FK to public.datasets)',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED fks (generated from the migration module)
                    ARRAY[
                        'cc_warc_files|crawl_id|kbp.cc_crawls|crawl_id|a|a|false|true',
                        'github_commits|repository_id|kbp.github_repositories|repository_id|a|a|false|true',
                        'source_observations|run_id|kbp.pipeline_runs|run_id|a|a|false|true',
                        'cc_captures|observation_id|kbp.source_observations|observation_id|a|a|false|true',
                        'cc_captures|warc_file_id|kbp.cc_warc_files|warc_file_id|a|a|false|true',
                        'github_file_observations|observation_id|kbp.source_observations|observation_id|a|a|false|true',
                        'github_file_observations|repository_id,commit_sha|kbp.github_commits|repository_id,commit_sha|a|a|false|true',
                        'document_registry|first_observation_id|kbp.source_observations|observation_id|a|a|false|true',
                        'observation_extractions|document_id|kbp.document_registry|document_id|a|a|false|true',
                        'observation_extractions|observation_id|kbp.source_observations|observation_id|a|a|false|true',
                        'document_dedup_decisions|canonical_document_id|kbp.document_registry|document_id|a|a|false|true',
                        'document_dedup_decisions|dedup_method_id|kbp.dedup_methods|dedup_method_id|a|a|false|true',
                        'document_dedup_decisions|document_id|kbp.document_registry|document_id|a|a|false|true',
                        'document_dedup_decisions|run_id|kbp.pipeline_runs|run_id|a|a|false|true',
                        'document_relationships|dedup_method_id|kbp.dedup_methods|dedup_method_id|a|a|false|true',
                        'document_relationships|document_id|kbp.document_registry|document_id|a|a|false|true',
                        'document_relationships|related_document_id|kbp.document_registry|document_id|a|a|false|true',
                        'chunk_registry|chunking_version_id|kbp.chunking_versions|chunking_version_id|a|a|false|true',
                        'chunk_registry|first_document_id|kbp.document_registry|document_id|a|a|false|true',
                        'chunk_registry|representative_observation_id|kbp.source_observations|observation_id|a|a|false|true',
                        'chunk_occurrences|chunk_id|kbp.chunk_registry|chunk_id|a|a|false|true',
                        'chunk_occurrences|chunking_version_id|kbp.chunking_versions|chunking_version_id|a|a|false|true',
                        'chunk_occurrences|document_id|kbp.document_registry|document_id|a|a|false|true',
                        'chunk_embeddings|chunk_id|kbp.chunk_registry|chunk_id|a|a|false|true',
                        'chunk_embeddings|embedding_model_id|kbp.embedding_models|embedding_model_id|a|a|false|true',
                        'chunk_embeddings|run_id|kbp.pipeline_runs|run_id|a|a|false|true',
                        'embedding_queue|chunk_id|kbp.chunk_registry|chunk_id|a|a|false|true',
                        'embedding_queue|embedding_model_id|kbp.embedding_models|embedding_model_id|a|a|false|true',
                        'pipeline_jobs|run_id|kbp.pipeline_runs|run_id|a|a|false|true',
                        'processing_attempts|job_id|kbp.pipeline_jobs|job_id|a|a|false|true',
                        'dead_letters|job_id|kbp.pipeline_jobs|job_id|a|a|false|true',
                        'dead_letters|run_id|kbp.pipeline_runs|run_id|a|a|false|true',
                        'legacy_kb_reconciliation|run_id|kbp.pipeline_runs|run_id|a|a|false|true',
                        'source_dataset_map|dataset_id|public.datasets|id|a|a|false|true'
                    ]::text[]
                    -- END EXPECTED fks
                 ) AS k),
                 actual AS (
                     SELECT cl.relname || '|'
                            || (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
                                FROM unnest(co.conkey) WITH ORDINALITY AS k(attnum, ord)
                                JOIN pg_catalog.pg_attribute AS a ON a.attrelid = co.conrelid AND a.attnum = k.attnum)
                            || '|' || rn.nspname || '.' || rc.relname || '|'
                            || (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
                                FROM unnest(co.confkey) WITH ORDINALITY AS k(attnum, ord)
                                JOIN pg_catalog.pg_attribute AS a ON a.attrelid = co.confrelid AND a.attnum = k.attnum)
                            || '|' || co.confupdtype::text || '|' || co.confdeltype::text
                            || '|' || co.condeferrable::text || '|' || co.convalidated::text AS k
                     FROM pg_catalog.pg_constraint AS co
                     JOIN pg_catalog.pg_class AS cl ON cl.oid = co.conrelid
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = cl.relnamespace
                     JOIN pg_catalog.pg_class AS rc ON rc.oid = co.confrelid
                     JOIN pg_catalog.pg_namespace AS rn ON rn.oid = rc.relnamespace
                     WHERE n.nspname = 'kbp' AND co.contype = 'f'),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'unexpected ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M08', 'CHECK constraints: count per table matches (88 in total)',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED check_counts (generated from the migration module)
                    ARRAY[
                        'pipeline_runs|5',
                        'cc_crawls|2',
                        'cc_warc_files|1',
                        'github_repositories|1',
                        'github_commits|4',
                        'source_observations|1',
                        'cc_captures|3',
                        'github_file_observations|2',
                        'document_registry|8',
                        'observation_extractions|3',
                        'document_dedup_decisions|5',
                        'document_relationships|4',
                        'chunk_registry|3',
                        'chunk_occurrences|2',
                        'embedding_models|5',
                        'chunk_embeddings|3',
                        'embedding_queue|4',
                        'pipeline_jobs|8',
                        'processing_attempts|3',
                        'dead_letters|6',
                        'source_rate_limits|2',
                        'source_dataset_map|3',
                        'legacy_kb_reconciliation|10'
                    ]::text[]
                    -- END EXPECTED check_counts
                 ) AS k),
                 actual AS (
                     SELECT cl.relname || '|' || count(*)::text AS k
                     FROM pg_catalog.pg_constraint AS co
                     JOIN pg_catalog.pg_class AS cl ON cl.oid = co.conrelid
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = cl.relnamespace
                     WHERE n.nspname = 'kbp' AND co.contype = 'c'
                     GROUP BY cl.relname),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'unexpected ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M09', 'every kbp constraint is validated',
         $q$SELECT count(*) = 0, coalesce(string_agg(co.conname, ', '), '')
            FROM pg_catalog.pg_constraint AS co
            JOIN pg_catalog.pg_namespace AS n ON n.oid = co.connamespace
            WHERE n.nspname = 'kbp' AND NOT co.convalidated$q$),

        ('M10', 'indexes that back no constraint are exactly the 8 of schema section 21.1/21.2 (table, name, unique, partial, columns)',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED indexes (generated from the migration module)
                    ARRAY[
                        'embedding_models|embedding_models_single_active_gold_idx|t|t|status',
                        'pipeline_jobs|pipeline_jobs_claim_idx|f|t|job_type,priority,next_attempt_at',
                        'pipeline_jobs|pipeline_jobs_lease_expiry_idx|f|t|lease_expires_at',
                        'embedding_queue|embedding_queue_claim_idx|f|t|embedding_model_id,priority,next_attempt_at',
                        'embedding_queue|embedding_queue_lease_expiry_idx|f|t|lease_expires_at',
                        'observation_extractions|observation_extractions_document_idx|f|t|document_id',
                        'dead_letters|dead_letters_open_idx|f|t|last_seen_at',
                        'legacy_kb_reconciliation|legacy_kb_reconciliation_canonical_hash_idx|f|f|canonical_chunk_hash'
                    ]::text[]
                    -- END EXPECTED indexes
                 ) AS k),
                 actual AS (
                     SELECT t.relname || '|' || i.relname || '|'
                            || CASE WHEN x.indisunique THEN 't' ELSE 'f' END || '|'
                            || CASE WHEN x.indpred IS NOT NULL THEN 't' ELSE 'f' END || '|'
                            || (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
                                FROM unnest(x.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                                JOIN pg_catalog.pg_attribute AS a ON a.attrelid = x.indrelid AND a.attnum = k.attnum) AS k
                     FROM pg_catalog.pg_index AS x
                     JOIN pg_catalog.pg_class AS i ON i.oid = x.indexrelid
                     JOIN pg_catalog.pg_class AS t ON t.oid = x.indrelid
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = t.relnamespace
                     WHERE n.nspname = 'kbp'
                       AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS co
                                       WHERE co.conindid = x.indexrelid AND co.conrelid = x.indrelid
                                         AND co.contype IN ('p', 'u', 'x'))),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'unexpected ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M11', 'every kbp index is valid, ready and live',
         $q$SELECT count(*) > 0 AND bool_and(x.indisvalid AND x.indisready AND x.indislive),
                   count(*)::text || ' index(es); invalid: '
                   || coalesce(string_agg(i.relname, ', ') FILTER (WHERE NOT (x.indisvalid AND x.indisready AND x.indislive)), 'none')
            FROM pg_catalog.pg_index AS x
            JOIN pg_catalog.pg_class AS i ON i.oid = x.indexrelid
            JOIN pg_catalog.pg_namespace AS n ON n.oid = i.relnamespace
            WHERE n.nspname = 'kbp'$q$),

        ('M12', 'every kbp sequence is an identity sequence (7 identity columns)',
         $q$SELECT count(*) = 7 AND count(*) FILTER (WHERE d.objid IS NULL) = 0,
                   count(*)::text || ' sequence(s); not identity-owned: '
                   || coalesce(string_agg(s.relname, ', ') FILTER (WHERE d.objid IS NULL), 'none')
            FROM pg_catalog.pg_class AS s
            JOIN pg_catalog.pg_namespace AS n ON n.oid = s.relnamespace
            LEFT JOIN pg_catalog.pg_depend AS d
                   ON d.classid = 'pg_catalog.pg_class'::regclass AND d.objid = s.oid AND d.deptype = 'i'
            WHERE n.nspname = 'kbp' AND s.relkind = 'S'$q$),

        ('M13', 'kbp.source_dataset_map is empty (bridge bootstrap NOT performed)',
         $q$SELECT count(*) = 0, count(*)::text || ' row(s)' FROM kbp.source_dataset_map$q$),

        ('M14', 'the only FK from outside public into kb/metadata/datasets/dataset_sources is the approved one',
         $q$WITH x AS (
                 SELECT cn.nspname || '.' || cl.relname || '|' || co.conname || '|' || rn.nspname || '.' || rc.relname AS k
                 FROM pg_catalog.pg_constraint AS co
                 JOIN pg_catalog.pg_class AS cl ON cl.oid = co.conrelid
                 JOIN pg_catalog.pg_namespace AS cn ON cn.oid = cl.relnamespace
                 JOIN pg_catalog.pg_class AS rc ON rc.oid = co.confrelid
                 JOIN pg_catalog.pg_namespace AS rn ON rn.oid = rc.relnamespace
                 WHERE co.contype = 'f' AND cn.nspname <> 'public'
                   AND rn.nspname = 'public' AND rc.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources'))
            SELECT coalesce((SELECT array_agg(k) FROM x)
                            = ARRAY['kbp.source_dataset_map|source_dataset_map_dataset_id_fkey|public.datasets'], false),
                   coalesce((SELECT string_agg(k, '; ') FROM x), 'none')$q$),

        ('M15', 'constraint triggers on the protected tables that belong to non-public constraints are only those of the approved FK',
         $q$SELECT count(*) = 0, coalesce(string_agg(c.relname || ':' || t.tgname || ' (' || co.conname || ')', ', '), '')
            FROM pg_catalog.pg_trigger AS t
            JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_constraint AS co ON co.oid = t.tgconstraint
            JOIN pg_catalog.pg_class AS cc ON cc.oid = co.conrelid
            JOIN pg_catalog.pg_namespace AS cn ON cn.oid = cc.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')
              AND cn.nspname <> 'public'
              AND NOT (cn.nspname = 'kbp' AND co.conname = 'source_dataset_map_dataset_id_fkey')$q$),

        ('M16', 'runtime table-level privileges of kb_pipeline_app equal privileges section 5.1 exactly',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED table_grants (generated from the migration module)
                    ARRAY[
                        'pipeline_runs|SELECT',
                        'pipeline_runs|INSERT',
                        'cc_crawls|SELECT',
                        'cc_crawls|INSERT',
                        'cc_warc_files|SELECT',
                        'cc_warc_files|INSERT',
                        'github_repositories|SELECT',
                        'github_repositories|INSERT',
                        'github_commits|SELECT',
                        'github_commits|INSERT',
                        'source_observations|SELECT',
                        'source_observations|INSERT',
                        'cc_captures|SELECT',
                        'cc_captures|INSERT',
                        'github_file_observations|SELECT',
                        'github_file_observations|INSERT',
                        'document_registry|SELECT',
                        'document_registry|INSERT',
                        'observation_extractions|SELECT',
                        'observation_extractions|INSERT',
                        'dedup_methods|SELECT',
                        'document_dedup_decisions|SELECT',
                        'document_dedup_decisions|INSERT',
                        'document_relationships|SELECT',
                        'document_relationships|INSERT',
                        'chunking_versions|SELECT',
                        'chunk_registry|SELECT',
                        'chunk_registry|INSERT',
                        'chunk_occurrences|SELECT',
                        'chunk_occurrences|INSERT',
                        'embedding_models|SELECT',
                        'chunk_embeddings|SELECT',
                        'chunk_embeddings|INSERT',
                        'embedding_queue|SELECT',
                        'embedding_queue|INSERT',
                        'embedding_queue|UPDATE',
                        'embedding_queue|DELETE',
                        'pipeline_jobs|SELECT',
                        'pipeline_jobs|INSERT',
                        'pipeline_jobs|UPDATE',
                        'processing_attempts|SELECT',
                        'processing_attempts|INSERT',
                        'dead_letters|SELECT',
                        'dead_letters|INSERT',
                        'source_rate_limits|SELECT',
                        'source_rate_limits|INSERT',
                        'source_rate_limits|UPDATE',
                        'source_dataset_map|SELECT',
                        'legacy_kb_reconciliation|SELECT',
                        'legacy_kb_reconciliation|INSERT',
                        'legacy_kb_reconciliation|UPDATE',
                        'alembic_version|SELECT'
                    ]::text[]
                    -- END EXPECTED table_grants
                 ) AS k),
                 actual AS (
                     SELECT c.relname || '|' || x.privilege_type AS k
                     FROM pg_catalog.pg_class AS c
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     CROSS JOIN LATERAL aclexplode(c.relacl) AS x
                     WHERE n.nspname = 'kbp' AND x.grantee = 'kb_pipeline_app'::regrole::oid),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'FORBIDDEN ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M17', 'runtime column-level privileges of kb_pipeline_app equal privileges section 5.1 exactly',
         $q$WITH expected AS (SELECT unnest(
                    -- BEGIN EXPECTED column_grants (generated from the migration module)
                    ARRAY[
                        'pipeline_runs|status|UPDATE',
                        'pipeline_runs|finished_at|UPDATE',
                        'pipeline_runs|notes|UPDATE',
                        'github_repositories|node_id|UPDATE',
                        'github_repositories|owner_login|UPDATE',
                        'github_repositories|repo_name|UPDATE',
                        'github_repositories|html_url|UPDATE',
                        'github_repositories|default_branch|UPDATE',
                        'github_repositories|visibility|UPDATE',
                        'github_repositories|license_spdx_id|UPDATE',
                        'github_repositories|is_fork|UPDATE',
                        'github_repositories|is_archived|UPDATE',
                        'github_repositories|api_etag|UPDATE',
                        'github_repositories|last_snapshot_commit_sha|UPDATE',
                        'github_repositories|metadata_refreshed_at|UPDATE',
                        'document_registry|status|UPDATE',
                        'document_registry|status_reason|UPDATE',
                        'document_registry|policy_version|UPDATE',
                        'document_registry|component_versions|UPDATE',
                        'document_registry|language|UPDATE',
                        'document_registry|language_confidence|UPDATE',
                        'document_registry|quality_score|UPDATE',
                        'document_registry|factuality_score|UPDATE',
                        'document_registry|subject_domain|UPDATE',
                        'document_registry|status_updated_at|UPDATE',
                        'processing_attempts|finished_at|UPDATE',
                        'processing_attempts|outcome|UPDATE',
                        'processing_attempts|error_class|UPDATE',
                        'processing_attempts|error_code|UPDATE',
                        'processing_attempts|error_message|UPDATE',
                        'processing_attempts|retry_after_seconds|UPDATE',
                        'processing_attempts|scheduled_next_attempt_at|UPDATE',
                        'processing_attempts|metrics|UPDATE',
                        'dead_letters|occurrence_count|UPDATE',
                        'dead_letters|last_seen_at|UPDATE',
                        'dead_letters|attempt_count|UPDATE',
                        'dead_letters|error_class|UPDATE',
                        'dead_letters|error_code|UPDATE',
                        'dead_letters|error_message|UPDATE',
                        'dead_letters|payload_ref|UPDATE',
                        'dead_letters|disposition|UPDATE',
                        'dead_letters|resolved_at|UPDATE',
                        'dead_letters|resolution|UPDATE'
                    ]::text[]
                    -- END EXPECTED column_grants
                 ) AS k),
                 actual AS (
                     SELECT c.relname || '|' || a.attname || '|' || x.privilege_type AS k
                     FROM pg_catalog.pg_class AS c
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                     CROSS JOIN LATERAL aclexplode(a.attacl) AS x
                     WHERE n.nspname = 'kbp' AND x.grantee = 'kb_pipeline_app'::regrole::oid),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'FORBIDDEN ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('M18', 'no PUBLIC or third-party grants and no grant options on kbp relations or columns',
         $q$WITH entries AS (
                 SELECT c.relname AS obj, c.relkind, x.grantee, x.is_grantable
                 FROM pg_catalog.pg_class AS c
                 JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                 CROSS JOIN LATERAL aclexplode(c.relacl) AS x
                 WHERE n.nspname = 'kbp'
                 UNION ALL
                 SELECT c.relname || '.' || a.attname, c.relkind, x.grantee, x.is_grantable
                 FROM pg_catalog.pg_class AS c
                 JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                 JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid AND a.attnum > 0
                 CROSS JOIN LATERAL aclexplode(a.attacl) AS x
                 WHERE n.nspname = 'kbp')
            SELECT count(*) = 0,
                   coalesce(string_agg(obj || ' -> ' || CASE WHEN grantee = 0 THEN 'PUBLIC' ELSE grantee::regrole::text END, ', '), '')
            FROM entries
            WHERE grantee NOT IN ('kb_pipeline_migrator'::regrole::oid, 'kb_pipeline_app'::regrole::oid)
               OR (grantee = 'kb_pipeline_app'::regrole::oid AND (is_grantable OR relkind <> 'r'))$q$),

        ('M19', 'schema kbp: kb_pipeline_app has USAGE only; PUBLIC has nothing',
         $q$SELECT has_schema_privilege('kb_pipeline_app', 'kbp', 'USAGE')
                   AND NOT has_schema_privilege('kb_pipeline_app', 'kbp', 'CREATE')
                   AND NOT has_schema_privilege('public', 'kbp', 'USAGE')
                   AND NOT has_schema_privilege('public', 'kbp', 'CREATE'),
                   (SELECT coalesce(array_to_string(nspacl, ','), '<default>')
                    FROM pg_catalog.pg_namespace WHERE nspname = 'kbp')$q$),

        ('M20', 'every kbp relation and type is owned by kb_pipeline_migrator',
         $q$WITH bad AS (
                 SELECT 'relation ' || c.relname || ' owner=' || pg_catalog.pg_get_userbyid(c.relowner) AS obj
                 FROM pg_catalog.pg_class AS c
                 JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'kbp' AND c.relowner <> 'kb_pipeline_migrator'::regrole::oid
                 UNION ALL
                 SELECT 'type ' || t.typname || ' owner=' || pg_catalog.pg_get_userbyid(t.typowner)
                 FROM pg_catalog.pg_type AS t
                 JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
                 WHERE n.nspname = 'kbp' AND t.typowner <> 'kb_pipeline_migrator'::regrole::oid)
            SELECT count(*) = 0, coalesce(string_agg(obj, ', '), '') FROM bad$q$),

        ('M21', 'no functions or procedures in kbp (Schema V1 defines none)',
         $q$SELECT count(*) = 0, coalesce(string_agg(p.oid::regprocedure::text, ', '), '')
            FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'kbp'$q$),

        ('M22', 'kb_pipeline_app owns no object in any database of the cluster',
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

\echo '== LEGACY OBJECTS (runbook step 10: must equal the baseline) =='
\ir lib/legacy_fingerprint.sql
\ir lib/legacy_checks.sql

\ir lib/finish.sql

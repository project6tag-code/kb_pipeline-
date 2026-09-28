-- =============================================================================
-- pre_migration_snapshot.sql - READ ONLY evidence snapshot of the legacy DB
-- =============================================================================
--
-- Purpose: record enough catalog and data evidence to prove afterwards that
-- Migration 001A (and its downgrade) left the legacy objects unchanged.
--
-- Run by: the PostgreSQL administrator (e.g. postgres). The pipeline roles
-- cannot read public.kb / public.metadata in full. Runbook steps 2 and 4
-- (docs/migrations_kbp.md section 6). NOT executed during preparation.
--
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline
--        -f scripts/db/pre_migration_snapshot.sql > <evidence>/02_snapshot_pre.txt 2>&1
--
-- Safety:
--   * one READ ONLY, REPEATABLE READ transaction, ended with ROLLBACK: the
--     server rejects any write, and all queries see one consistent snapshot;
--   * lock_timeout keeps it from queueing behind DDL; it takes only
--     ACCESS SHARE locks;
--   * no chunk text, embedding, metadata value or dataset row content is
--     printed: only counts and md5 digests (lib/legacy_fingerprint.sql);
--   * no passwords or password hashes are read.
--
-- The last section prints "\set baseline_<metric> '<value>'" lines. Save them
-- to a baseline file (runbook step 4) for verify_migration_001a.sql and
-- verify_downgrade_001a.sql.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

\echo '== 1. SESSION AND SERVER =='
SELECT now()                                   AS snapshot_time,
       version()                               AS postgresql_version,
       current_setting('server_version_num')   AS server_version_num,
       current_database()                      AS current_database,
       current_user                            AS current_user,
       session_user                            AS session_user,
       current_setting('server_encoding')      AS server_encoding,
       current_setting('transaction_read_only') AS transaction_read_only,
       current_setting('password_encryption')  AS password_encryption;

\echo '== 2. INSTALLED EXTENSIONS =='
SELECT e.extname, e.extversion, n.nspname AS extension_schema
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n ON n.oid = e.extnamespace
ORDER BY e.extname;

\echo '== 3. SCHEMA kbp (expected ABSENT before provisioning; EMPTY before migration) =='
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') AS kbp_exists,
       (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace
         WHERE nspname = 'kbp') AS kbp_owner,
       (SELECT array_to_string(nspacl, ',') FROM pg_catalog.pg_namespace
         WHERE nspname = 'kbp') AS kbp_acl,
       (SELECT count(*) FROM pg_catalog.pg_class AS c
          JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = 'kbp') AS kbp_relation_count;

\echo '== 4. PIPELINE ROLES (expected ABSENT before provisioning) =='
SELECT w.rolname,
       r.rolname IS NOT NULL AS role_exists,
       r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolreplication,
       r.rolbypassrls, r.rolcanlogin, r.rolinherit, r.rolconnlimit
FROM unnest(ARRAY['kb_pipeline_migrator', 'kb_pipeline_app']) AS w(rolname)
LEFT JOIN pg_catalog.pg_roles AS r ON r.rolname = w.rolname
ORDER BY w.rolname;

\echo '== 5. OWNERSHIP OF THE PROTECTED PUBLIC OBJECTS =='
SELECT n.nspname || '.' || c.relname AS relation,
       c.relkind,
       pg_catalog.pg_get_userbyid(c.relowner) AS owner
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')
ORDER BY relation;

\echo '== 6. FOREIGN KEYS TOUCHING THE PROTECTED OBJECTS (defined on them, or referencing them) =='
SELECT co.conname,
       cn.nspname || '.' || cl.relname  AS referencing_table,
       rn.nspname || '.' || rc.relname  AS referenced_table,
       pg_catalog.pg_get_constraintdef(co.oid) AS definition,
       co.confupdtype AS on_update, co.confdeltype AS on_delete,
       co.condeferrable, co.condeferred, co.convalidated
FROM pg_catalog.pg_constraint AS co
JOIN pg_catalog.pg_class AS cl ON cl.oid = co.conrelid
JOIN pg_catalog.pg_namespace AS cn ON cn.oid = cl.relnamespace
JOIN pg_catalog.pg_class AS rc ON rc.oid = co.confrelid
JOIN pg_catalog.pg_namespace AS rn ON rn.oid = rc.relnamespace
WHERE co.contype = 'f'
  AND ((cn.nspname = 'public' AND cl.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources'))
    OR (rn.nspname = 'public' AND rc.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources')))
ORDER BY referencing_table, co.conname;

\echo '== 7. TRIGGERS ON THE PROTECTED OBJECTS (including internal RI triggers) =='
SELECT c.relname AS table_name, t.tgname, t.tgisinternal, t.tgenabled,
       pg_catalog.pg_get_triggerdef(t.oid) AS definition
FROM pg_catalog.pg_trigger AS t
JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')
ORDER BY c.relname, t.tgname;

\echo '== 8. DATABASE AND SCHEMA public PRIVILEGES =='
SELECT d.datname,
       pg_catalog.pg_get_userbyid(d.datdba) AS database_owner,
       array_to_string(d.datacl, ',') AS database_acl,
       (SELECT array_to_string(nspacl, ',') FROM pg_catalog.pg_namespace
         WHERE nspname = 'public') AS public_schema_acl,
       has_database_privilege('public', d.datname, 'CREATE') AS public_pseudo_role_can_create_schemas,
       has_schema_privilege('public', 'public', 'CREATE') AS public_pseudo_role_can_create_in_public
FROM pg_catalog.pg_database AS d
WHERE d.datname = current_database();

\echo '== 9. LEGACY FINGERPRINT: counts, content digests, structure, privileges =='
\ir lib/legacy_fingerprint.sql

\echo
\echo '== 10. BASELINE LINES: copy these lines (only) into the baseline file =='
\pset tuples_only on
\pset format unaligned
SELECT format('\set baseline_%s %s', v.metric, quote_literal(v.value))
FROM (VALUES
    (1,  'kb_rows',                     :'fp_kb_rows'),
    (2,  'metadata_rows',               :'fp_metadata_rows'),
    (3,  'datasets_rows',               :'fp_datasets_rows'),
    (4,  'dataset_sources_rows',        :'fp_dataset_sources_rows'),
    (5,  'public_alembic_version',      :'fp_public_alembic_version'),
    (6,  'kb_data_digest',              :'fp_kb_data_digest'),
    (7,  'metadata_data_digest',        :'fp_metadata_data_digest'),
    (8,  'datasets_data_digest',        :'fp_datasets_data_digest'),
    (9,  'dataset_sources_data_digest', :'fp_dataset_sources_data_digest'),
    (10, 'structure_digest',            :'fp_structure_digest'),
    (11, 'acl_digest',                  :'fp_acl_digest'),
    (12, 'public_inventory_digest',     :'fp_public_inventory_digest')
) AS v(ord, metric, value)
ORDER BY v.ord;
\pset format aligned
\pset tuples_only off

ROLLBACK;
\echo '== snapshot complete (read-only transaction rolled back; nothing was changed) =='

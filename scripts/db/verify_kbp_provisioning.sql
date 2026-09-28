-- =============================================================================
-- verify_kbp_provisioning.sql - READ ONLY check of roles and schema kbp
-- =============================================================================
--
-- Verifies the result of provision_kbp_roles.sql against
-- docs/database_privileges_v1.md (section 3, section 4, section 6, section 8, section 9, section 12). Runbook step 4
-- (docs/migrations_kbp.md section 6). NOT executed during preparation.
--
-- Run by: the PostgreSQL administrator (e.g. postgres). Superuser rights are
-- needed only to READ pg_authid (password type check; the hash itself is never
-- printed) and pg_hba_file_rules.
--
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d data_collection_pipeline
--        [-v app_connection_limit=20] [-v expect_empty_kbp=true]
--        -f scripts/db/verify_kbp_provisioning.sql > <evidence>/04_verify_provisioning.txt 2>&1
--
-- Parameters (psql -v):
--   app_connection_limit  the value used at provisioning (default 20)
--   expect_empty_kbp      true before Migration 001A (default), false afterwards
--
-- Exit status: non-zero if any check FAILS. One READ ONLY transaction, rolled back.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

\if :{?app_connection_limit}
\else
\set app_connection_limit 20
\endif
\if :{?expect_empty_kbp}
\else
\set expect_empty_kbp true
\endif

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '5min';
SET LOCAL client_min_messages = notice;

SELECT set_config('kbp_verify.app_connection_limit', :'app_connection_limit', true) AS p1,
       set_config('kbp_verify.expect_empty_kbp', :'expect_empty_kbp', true)         AS p2,
       set_config('kbp_verify.failures', '0', true)                                 AS p3
\gset _ignored_

\echo '== INFORMATION (not pass/fail) =='
SELECT current_database() AS database, current_user AS checked_by, now() AS checked_at;

SELECT r.rolname, r.rolcanlogin, r.rolinherit, r.rolsuper, r.rolcreatedb, r.rolcreaterole,
       r.rolreplication, r.rolbypassrls, r.rolconnlimit, r.rolvaliduntil
FROM pg_catalog.pg_roles AS r
WHERE r.rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')
ORDER BY r.rolname;

SELECT n.nspname, pg_catalog.pg_get_userbyid(n.nspowner) AS owner, array_to_string(n.nspacl, ',') AS acl
FROM pg_catalog.pg_namespace AS n
WHERE n.nspname IN ('kbp', 'public')
ORDER BY n.nspname;

\echo 'Schema of the pgvector type (privileges spec section 4.1: must be on the runtime search_path or qualified):'
SELECT e.extname, e.extversion, n.nspname AS extension_schema
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n ON n.oid = e.extnamespace
WHERE e.extname = 'vector';

\echo 'Inherited from PUBLIC (informational; revoking would affect every role):'
SELECT has_database_privilege('public', current_database(), 'TEMPORARY') AS public_can_create_temp_tables,
       has_schema_privilege('public', 'public', 'USAGE')                  AS public_has_usage_on_public;

\echo 'pg_hba.conf rules that can match the pipeline roles (first matching rule wins):'
SELECT h.line_number, h.type, h.database, h.user_name, h.address, h.netmask, h.auth_method, h.error
FROM pg_catalog.pg_hba_file_rules AS h
WHERE h.user_name && ARRAY['kb_pipeline_migrator', 'kb_pipeline_app', 'all']::text[]
ORDER BY h.line_number;

\echo '== CHECKS =='
DO $checks$
DECLARE
    c        record;
    ok       boolean;
    detail   text;
    failures integer := 0;
BEGIN
    FOR c IN
        SELECT * FROM (VALUES
        ('P01', 'both pipeline roles exist',
         $q$SELECT count(*) = 2, coalesce(string_agg(rolname, ', '), 'none')
            FROM pg_catalog.pg_roles
            WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')$q$),

        ('P02', 'neither role is SUPERUSER, CREATEDB, CREATEROLE, REPLICATION or BYPASSRLS; both are LOGIN INHERIT',
         $q$SELECT count(*) = 2 AND bool_and(rolcanlogin AND rolinherit
                   AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)),
                   string_agg(format('%s super=%s createdb=%s createrole=%s replication=%s bypassrls=%s login=%s inherit=%s',
                              rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,
                              rolcanlogin, rolinherit), '; ')
            FROM pg_catalog.pg_roles
            WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')$q$),

        ('P03', 'kb_pipeline_migrator CONNECTION LIMIT is 2 (privileges section 3.1)',
         $q$SELECT rolconnlimit = 2, rolconnlimit::text
            FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_migrator'$q$),

        ('P04', 'kb_pipeline_app CONNECTION LIMIT is bounded and equals the provisioned value',
         $q$SELECT rolconnlimit > 0
                   AND rolconnlimit = current_setting('kbp_verify.app_connection_limit')::integer,
                   rolconnlimit::text || ' (expected ' || current_setting('kbp_verify.app_connection_limit') || ')'
            FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_app'$q$),

        ('P05', 'no role memberships in either direction (no inherited privileges, no escalation path)',
         $q$SELECT count(*) = 0,
                   coalesce(string_agg(pg_catalog.pg_get_userbyid(m.member) || ' member of '
                            || pg_catalog.pg_get_userbyid(m.roleid), '; '), '')
            FROM pg_catalog.pg_auth_members AS m
            WHERE m.member IN (SELECT oid FROM pg_catalog.pg_roles
                               WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app'))
               OR m.roleid IN (SELECT oid FROM pg_catalog.pg_roles
                               WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app'))$q$),

        ('P06', 'both roles have a SCRAM-SHA-256 password (value not shown)',
         $q$SELECT count(*) = 2 AND bool_and(coalesce(rolpassword LIKE 'SCRAM-SHA-256$%', false)),
                   string_agg(rolname || '=' || CASE WHEN rolpassword IS NULL THEN 'NO PASSWORD SET'
                                                     WHEN rolpassword LIKE 'SCRAM-SHA-256$%' THEN 'scram-sha-256'
                                                     ELSE 'NOT SCRAM' END, ', ')
            FROM pg_catalog.pg_authid
            WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')$q$),

        ('P07', 'server password_encryption is scram-sha-256',
         $q$SELECT current_setting('password_encryption') = 'scram-sha-256',
                   current_setting('password_encryption')$q$),

        ('P08', 'schema kbp exists and is owned by kb_pipeline_migrator',
         $q$SELECT coalesce(bool_and(pg_catalog.pg_get_userbyid(nspowner) = 'kb_pipeline_migrator'), false),
                   coalesce(string_agg('owner=' || pg_catalog.pg_get_userbyid(nspowner), ','), 'schema kbp is missing')
            FROM pg_catalog.pg_namespace WHERE nspname = 'kbp'$q$),

        ('P09', 'kb_pipeline_app owns no object in any database of the cluster',
         $q$SELECT count(*) = 0, count(*)::text || ' owned object(s)'
            FROM pg_catalog.pg_shdepend
            WHERE deptype = 'o'
              AND refclassid = 'pg_catalog.pg_authid'::regclass
              AND refobjid = 'kb_pipeline_app'::regrole::oid$q$),

        ('P10', 'kb_pipeline_migrator owns nothing in this database outside schema kbp',
         $q$WITH m AS (SELECT 'kb_pipeline_migrator'::regrole::oid AS oid),
                 k AS (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'kbp'),
                 owned AS (
                     SELECT 'schema ' || n.nspname AS obj FROM pg_catalog.pg_namespace AS n, m
                     WHERE n.nspowner = m.oid AND n.nspname <> 'kbp'
                     UNION ALL
                     SELECT 'relation ' || c.oid::regclass::text FROM pg_catalog.pg_class AS c, m
                     WHERE c.relowner = m.oid AND c.relnamespace IS DISTINCT FROM (SELECT oid FROM k)
                     UNION ALL
                     SELECT 'function ' || p.oid::regprocedure::text FROM pg_catalog.pg_proc AS p, m
                     WHERE p.proowner = m.oid AND p.pronamespace IS DISTINCT FROM (SELECT oid FROM k)
                     UNION ALL
                     SELECT 'type ' || t.oid::regtype::text FROM pg_catalog.pg_type AS t, m
                     WHERE t.typowner = m.oid AND t.typnamespace IS DISTINCT FROM (SELECT oid FROM k)
                     UNION ALL
                     SELECT 'database ' || d.datname FROM pg_catalog.pg_database AS d, m
                     WHERE d.datdba = m.oid)
            SELECT count(*) = 0, coalesce(string_agg(obj, ', '), '') FROM owned$q$),

        ('P11', 'schema kbp: kb_pipeline_app has USAGE but not CREATE; PUBLIC has nothing',
         $q$SELECT has_schema_privilege('kb_pipeline_app', 'kbp', 'USAGE')
                   AND NOT has_schema_privilege('kb_pipeline_app', 'kbp', 'CREATE')
                   AND NOT has_schema_privilege('public', 'kbp', 'USAGE')
                   AND NOT has_schema_privilege('public', 'kbp', 'CREATE'),
                   (SELECT coalesce(array_to_string(nspacl, ','), '<default>')
                    FROM pg_catalog.pg_namespace WHERE nspname = 'kbp')$q$),

        ('P12', 'schema kbp holds no relations yet (skipped with -v expect_empty_kbp=false)',
         $q$SELECT lower(current_setting('kbp_verify.expect_empty_kbp')) IN ('false', 'off', '0', 'no', 'f', 'n')
                   OR count(*) = 0,
                   count(*)::text || ' relation(s) in kbp'
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'kbp'$q$),

        ('P13', 'schema public: both roles have USAGE, neither has CREATE',
         $q$SELECT bool_and(has_schema_privilege(r, 'public', 'USAGE')
                            AND NOT has_schema_privilege(r, 'public', 'CREATE')),
                   string_agg(format('%s usage=%s create=%s', r,
                              has_schema_privilege(r, 'public', 'USAGE'),
                              has_schema_privilege(r, 'public', 'CREATE')), '; ')
            FROM unnest(ARRAY['kb_pipeline_migrator', 'kb_pipeline_app']) AS r$q$),

        ('P14', 'database: both roles can CONNECT, neither can CREATE schemas',
         $q$SELECT bool_and(has_database_privilege(r, current_database(), 'CONNECT')
                            AND NOT has_database_privilege(r, current_database(), 'CREATE')),
                   string_agg(format('%s connect=%s create=%s', r,
                              has_database_privilege(r, current_database(), 'CONNECT'),
                              has_database_privilege(r, current_database(), 'CREATE')), '; ')
            FROM unnest(ARRAY['kb_pipeline_migrator', 'kb_pipeline_app']) AS r$q$),

        ('P15', 'effective privileges on every relation in schema public equal the matrix exactly (privileges section 3.2, section 6)',
         $q$WITH roles(r) AS (VALUES ('kb_pipeline_migrator'), ('kb_pipeline_app')),
                 rels AS (
                     SELECT c.oid AS relid, c.relname, c.relkind
                     FROM pg_catalog.pg_class AS c
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')),
                 expected(k) AS (VALUES
                     ('kb_pipeline_app|kb|SELECT'),
                     ('kb_pipeline_app|kb|INSERT'),
                     ('kb_pipeline_app|metadata|SELECT'),
                     ('kb_pipeline_app|metadata|INSERT'),
                     ('kb_pipeline_app|datasets|SELECT'),
                     ('kb_pipeline_app|dataset_sources|SELECT'),
                     ('kb_pipeline_migrator|datasets|SELECT'),
                     ('kb_pipeline_migrator|datasets|REFERENCES'),
                     ('kb_pipeline_migrator|dataset_sources|SELECT')),
                 actual AS (
                     SELECT r || '|' || relname || '|' || p AS k
                     FROM roles, rels, unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'REFERENCES']) AS p
                     WHERE relkind <> 'S' AND has_any_column_privilege(r, relid, p)
                     UNION ALL
                     SELECT r || '|' || relname || '|' || p
                     FROM roles, rels, unnest(ARRAY['DELETE', 'TRUNCATE', 'TRIGGER']) AS p
                     WHERE relkind <> 'S' AND has_table_privilege(r, relid, p)
                     UNION ALL
                     SELECT r || '|' || relname || '|SEQUENCE ' || p
                     FROM roles, rels, unnest(ARRAY['USAGE', 'SELECT', 'UPDATE']) AS p
                     WHERE relkind = 'S' AND has_sequence_privilege(r, relid, p)),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'FORBIDDEN ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('P16', 'column-level privileges on kb, metadata, datasets, dataset_sources equal the matrix exactly',
         $q$WITH roles(r) AS (VALUES ('kb_pipeline_migrator'), ('kb_pipeline_app')),
                 tcols AS (
                     SELECT c.oid AS relid, c.relname, a.attname
                     FROM pg_catalog.pg_class AS c
                     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                     WHERE n.nspname = 'public' AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources')),
                 spec(rname, tbl, cols, priv) AS (VALUES
                     ('kb_pipeline_app', 'kb', '*', 'SELECT'),
                     ('kb_pipeline_app', 'kb', '*', 'INSERT'),
                     ('kb_pipeline_app', 'metadata', '*', 'INSERT'),
                     ('kb_pipeline_app', 'metadata', 'id,source_id', 'SELECT'),
                     ('kb_pipeline_app', 'datasets', 'id,source_id,dataset_name,metadata', 'SELECT'),
                     ('kb_pipeline_app', 'dataset_sources', 'id,name,source_type', 'SELECT'),
                     ('kb_pipeline_migrator', 'datasets', '*', 'SELECT'),
                     ('kb_pipeline_migrator', 'datasets', 'id', 'REFERENCES'),
                     ('kb_pipeline_migrator', 'dataset_sources', '*', 'SELECT')),
                 expected AS (
                     SELECT s.rname || '|' || t.relname || '.' || t.attname || '|' || s.priv AS k
                     FROM spec AS s
                     JOIN tcols AS t ON t.relname = s.tbl
                      AND (s.cols = '*' OR t.attname = ANY (string_to_array(s.cols, ',')))),
                 actual AS (
                     SELECT r || '|' || relname || '.' || attname || '|' || p AS k
                     FROM roles, tcols, unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'REFERENCES']) AS p
                     WHERE has_column_privilege(r, relid, attname, p)),
                 diff AS (
                     SELECT 'missing ' || k AS d FROM (SELECT k FROM expected EXCEPT SELECT k FROM actual) AS x
                     UNION ALL
                     SELECT 'FORBIDDEN ' || k FROM (SELECT k FROM actual EXCEPT SELECT k FROM expected) AS y)
            SELECT count(*) = 0, left(coalesce(string_agg(d, '; ' ORDER BY d), ''), 2000) FROM diff$q$),

        ('P17', 'Gold write protection: no UPDATE, DELETE or TRUNCATE on public.kb / public.metadata for either role',
         $q$SELECT NOT bool_or(has_table_privilege(r, t, p) OR has_any_column_privilege(r, t, 'UPDATE')),
                   coalesce(string_agg(r || ' ' || p || ' ' || t, '; ')
                            FILTER (WHERE has_table_privilege(r, t, p)), '')
            FROM unnest(ARRAY['kb_pipeline_migrator', 'kb_pipeline_app']) AS r,
                 unnest(ARRAY['public.kb', 'public.metadata']) AS t,
                 unnest(ARRAY['UPDATE', 'DELETE', 'TRUNCATE']) AS p$q$),

        ('P18', 'bridge bootstrap not enabled: neither role can INSERT into public.datasets or public.dataset_sources',
         $q$SELECT NOT bool_or(has_any_column_privilege(r, t, 'INSERT')),
                   coalesce(string_agg(r || ' INSERT ' || t, '; ')
                            FILTER (WHERE has_any_column_privilege(r, t, 'INSERT')), '')
            FROM unnest(ARRAY['kb_pipeline_migrator', 'kb_pipeline_app']) AS r,
                 unnest(ARRAY['public.datasets', 'public.dataset_sources']) AS t$q$),

        ('P19', 'neither role holds a grant option on any public relation',
         $q$SELECT count(*) = 0, coalesce(string_agg(c.relname || ' ' || x.privilege_type, ', '), '')
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = 'public'
            CROSS JOIN LATERAL aclexplode(c.relacl) AS x
            WHERE x.is_grantable
              AND x.grantee IN ('kb_pipeline_migrator'::regrole::oid, 'kb_pipeline_app'::regrole::oid)$q$),

        ('P20', 'default privileges: new kbp tables of kb_pipeline_migrator grant SELECT (only) to kb_pipeline_app',
         $q$WITH d AS (
                 SELECT x.grantee, x.privilege_type, x.is_grantable
                 FROM pg_catalog.pg_default_acl AS da
                 CROSS JOIN LATERAL aclexplode(da.defaclacl) AS x
                 WHERE da.defaclrole = 'kb_pipeline_migrator'::regrole::oid
                   AND da.defaclnamespace = 'kbp'::regnamespace::oid
                   AND da.defaclobjtype = 'r')
            SELECT coalesce((SELECT array_agg(DISTINCT privilege_type) FROM d
                             WHERE grantee = 'kb_pipeline_app'::regrole::oid) = ARRAY['SELECT'], false)
                   AND NOT EXISTS (SELECT 1 FROM d WHERE is_grantable
                                   OR grantee NOT IN ('kb_pipeline_app'::regrole::oid,
                                                      'kb_pipeline_migrator'::regrole::oid)),
                   coalesce((SELECT string_agg(grantee::regrole::text || ':' || privilege_type, ', ') FROM d), 'no entry')$q$),

        ('P21', 'default privileges: functions created by kb_pipeline_migrator are not executable by PUBLIC',
         $q$SELECT EXISTS (
                     SELECT 1 FROM pg_catalog.pg_default_acl AS da
                     WHERE da.defaclrole = 'kb_pipeline_migrator'::regrole::oid
                       AND da.defaclnamespace = 0
                       AND da.defaclobjtype = 'f'
                       AND NOT EXISTS (SELECT 1 FROM aclexplode(da.defaclacl) AS x WHERE x.grantee = 0)),
                   coalesce((SELECT array_to_string(da.defaclacl, ',') FROM pg_catalog.pg_default_acl AS da
                             WHERE da.defaclrole = 'kb_pipeline_migrator'::regrole::oid
                               AND da.defaclnamespace = 0 AND da.defaclobjtype = 'f'), 'no entry')$q$),

        ('P22', 'no other default privileges exist for either pipeline role',
         $q$SELECT count(*) = 0,
                   coalesce(string_agg(da.defaclrole::regrole::text || ' ' || da.defaclobjtype::text || ' in '
                            || coalesce(nullif(da.defaclnamespace, 0)::regnamespace::text, '<all schemas>'), '; '), '')
            FROM pg_catalog.pg_default_acl AS da
            WHERE da.defaclrole IN ('kb_pipeline_migrator'::regrole::oid, 'kb_pipeline_app'::regrole::oid)
              AND NOT (da.defaclrole = 'kb_pipeline_migrator'::regrole::oid
                       AND ((da.defaclnamespace = 'kbp'::regnamespace::oid AND da.defaclobjtype = 'r')
                         OR (da.defaclnamespace = 0 AND da.defaclobjtype = 'f')))$q$),

        ('P23', 'kb_pipeline_app settings in this database: search_path = kbp, public and bounded timeouts',
         $q$WITH s AS (
                 SELECT unnest(setconfig) AS kv FROM pg_catalog.pg_db_role_setting
                 WHERE setrole = 'kb_pipeline_app'::regrole::oid
                   AND setdatabase = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()))
            SELECT EXISTS (SELECT 1 FROM s WHERE kv ~ '^search_path=kbp, ?public$')
                   AND EXISTS (SELECT 1 FROM s WHERE kv ~ '^lock_timeout=[1-9]')
                   AND EXISTS (SELECT 1 FROM s WHERE kv ~ '^statement_timeout=[1-9]')
                   AND EXISTS (SELECT 1 FROM s WHERE kv ~ '^idle_in_transaction_session_timeout=[1-9]'),
                   coalesce((SELECT string_agg(kv, '; ' ORDER BY kv) FROM s), 'no settings')$q$)
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

\ir lib/finish.sql

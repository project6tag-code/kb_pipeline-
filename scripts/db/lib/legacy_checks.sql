-- Legacy comparison checks. READ ONLY.
--
-- Included (\ir) by verify_migration_001a.sql and verify_downgrade_001a.sql
-- after lib/legacy_fingerprint.sql (which sets fp_<metric>).
--
-- Baseline: the caller passes -v baseline=<path> to a file of psql
-- "\set baseline_<metric> '<value>'" lines, produced by
-- pre_migration_snapshot.sql (runbook steps 2 and 4). A missing baseline is a
-- FAILURE, not a skip: legacy immutability must be proven, not assumed.
--
-- The baseline should come from the POST-provisioning snapshot (runbook step
-- 4); then every metric, acl_digest included, must match exactly. If the
-- baseline predates provisioning, acl_digest cannot match, because provisioning
-- adds the approved grants. Only in that case, and only when the baseline
-- acl_digest provably is the all-default state (every protected relation
-- "<default>", no column ACLs), the ACL check instead requires:
--   * every ACL entry of any other role to be exactly the owner's built-in default;
--   * the pipeline roles' entries to be EXACTLY the approved provisioning
--     grants (docs/database_privileges_v1.md sections 3.2 and 6; INSERT on
--     public.datasets withheld), with no grant option.
-- Any other ACL difference fails.
--
-- Results are RAISE NOTICE (PASS) / RAISE WARNING (FAIL) lines; failures are
-- accumulated in the transaction-local setting kbp_verify.failures, which
-- lib/finish.sql turns into a non-zero psql exit status.

\if :{?baseline}
\echo 'Loading legacy baseline from' :baseline
\i :baseline
\else
\echo 'NO BASELINE SUPPLIED (-v baseline=<file>): every legacy comparison will FAIL.'
\endif

\if :{?baseline_kb_rows}
\else
\set baseline_kb_rows ''
\set baseline_metadata_rows ''
\set baseline_datasets_rows ''
\set baseline_dataset_sources_rows ''
\set baseline_public_alembic_version ''
\set baseline_kb_data_digest ''
\set baseline_metadata_data_digest ''
\set baseline_datasets_data_digest ''
\set baseline_dataset_sources_data_digest ''
\set baseline_structure_digest ''
\set baseline_acl_digest ''
\set baseline_public_inventory_digest ''
\endif

-- Hand the psql variables to the DO block through transaction-local settings
-- (psql does not interpolate variables inside dollar-quoted bodies).
SELECT
    set_config('kbp_verify.b_kb_rows',                     :'baseline_kb_rows', true)                     AS b01,
    set_config('kbp_verify.c_kb_rows',                     :'fp_kb_rows', true)                           AS c01,
    set_config('kbp_verify.b_metadata_rows',               :'baseline_metadata_rows', true)               AS b02,
    set_config('kbp_verify.c_metadata_rows',               :'fp_metadata_rows', true)                     AS c02,
    set_config('kbp_verify.b_datasets_rows',               :'baseline_datasets_rows', true)               AS b03,
    set_config('kbp_verify.c_datasets_rows',               :'fp_datasets_rows', true)                     AS c03,
    set_config('kbp_verify.b_dataset_sources_rows',        :'baseline_dataset_sources_rows', true)        AS b04,
    set_config('kbp_verify.c_dataset_sources_rows',        :'fp_dataset_sources_rows', true)              AS c04,
    set_config('kbp_verify.b_public_alembic_version',      :'baseline_public_alembic_version', true)      AS b05,
    set_config('kbp_verify.c_public_alembic_version',      :'fp_public_alembic_version', true)            AS c05,
    set_config('kbp_verify.b_kb_data_digest',              :'baseline_kb_data_digest', true)              AS b06,
    set_config('kbp_verify.c_kb_data_digest',              :'fp_kb_data_digest', true)                    AS c06,
    set_config('kbp_verify.b_metadata_data_digest',        :'baseline_metadata_data_digest', true)        AS b07,
    set_config('kbp_verify.c_metadata_data_digest',        :'fp_metadata_data_digest', true)              AS c07,
    set_config('kbp_verify.b_datasets_data_digest',        :'baseline_datasets_data_digest', true)        AS b08,
    set_config('kbp_verify.c_datasets_data_digest',        :'fp_datasets_data_digest', true)              AS c08,
    set_config('kbp_verify.b_dataset_sources_data_digest', :'baseline_dataset_sources_data_digest', true) AS b09,
    set_config('kbp_verify.c_dataset_sources_data_digest', :'fp_dataset_sources_data_digest', true)       AS c09,
    set_config('kbp_verify.b_structure_digest',            :'baseline_structure_digest', true)            AS b10,
    set_config('kbp_verify.c_structure_digest',            :'fp_structure_digest', true)                  AS c10,
    set_config('kbp_verify.b_acl_digest',                  :'baseline_acl_digest', true)                  AS b11,
    set_config('kbp_verify.c_acl_digest',                  :'fp_acl_digest', true)                        AS c11,
    set_config('kbp_verify.b_public_inventory_digest',     :'baseline_public_inventory_digest', true)     AS b12,
    set_config('kbp_verify.c_public_inventory_digest',     :'fp_public_inventory_digest', true)           AS c12
\gset _legacy_ignored_

\echo '== LEGACY COMPARISON AGAINST BASELINE =='
DO $legacy$
DECLARE
    metric      text;
    baseline    text;
    observed    text;
    failures    integer := 0;
    all_default text;
    acl_delta   text;
BEGIN
    -- acl_digest of the pre-provisioning state in which every protected
    -- relation has a default ACL and no column ACL exists. Same item text and
    -- ordering (relname, collation "C") as lib/legacy_fingerprint.sql.
    all_default := (
        SELECT md5(string_agg(item, E'\n' ORDER BY item))
        FROM (SELECT 'public.' || c.relname || ' acl <default>' AS item
              FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
              WHERE n.nspname = 'public'
                AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')) AS s);

    -- Differences between the current ACLs of the protected objects and
    -- "owner defaults + exactly the approved provisioning grants"; NULL if none.
    acl_delta := (
        WITH rels AS (
            SELECT c.oid, 'public.' || c.relname AS rel, c.relowner, c.relacl
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')),
        pipeline AS (
            SELECT oid FROM pg_catalog.pg_roles
            WHERE rolname IN ('kb_pipeline_migrator', 'kb_pipeline_app')),
        other_entries AS (
            SELECT r.rel, x.grantor, x.grantee, x.privilege_type, x.is_grantable
            FROM rels AS r
            CROSS JOIN LATERAL aclexplode(coalesce(r.relacl, acldefault('r', r.relowner))) AS x
            WHERE x.grantee NOT IN (SELECT oid FROM pipeline)),
        owner_default AS (
            SELECT r.rel, x.grantor, x.grantee, x.privilege_type, x.is_grantable
            FROM rels AS r
            CROSS JOIN LATERAL aclexplode(acldefault('r', r.relowner)) AS x),
        pipeline_entries AS (
            SELECT r.rel || '||' || pg_catalog.pg_get_userbyid(x.grantee) || '|' || x.privilege_type
                   || '|' || x.is_grantable::text AS k
            FROM rels AS r
            CROSS JOIN LATERAL aclexplode(r.relacl) AS x
            WHERE x.grantee IN (SELECT oid FROM pipeline)
            UNION ALL
            SELECT r.rel || '|' || a.attname || '|' || pg_catalog.pg_get_userbyid(x.grantee) || '|'
                   || x.privilege_type || '|' || x.is_grantable::text
            FROM rels AS r
            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
            CROSS JOIN LATERAL aclexplode(a.attacl) AS x
            WHERE x.grantee IN (SELECT oid FROM pipeline)),
        other_column_entries AS (
            SELECT r.rel || '.' || a.attname || ' -> ' || pg_catalog.pg_get_userbyid(x.grantee) AS obj
            FROM rels AS r
            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
            CROSS JOIN LATERAL aclexplode(a.attacl) AS x
            WHERE x.grantee NOT IN (SELECT oid FROM pipeline)),
        -- BEGIN APPROVED PROVISIONING ACL (scripts/db/provision_kbp_roles.sql)
        approved(k) AS (VALUES
            ('public.datasets||kb_pipeline_migrator|SELECT|false'),
            ('public.datasets|id|kb_pipeline_migrator|REFERENCES|false'),
            ('public.dataset_sources||kb_pipeline_migrator|SELECT|false'),
            ('public.datasets|id|kb_pipeline_app|SELECT|false'),
            ('public.datasets|source_id|kb_pipeline_app|SELECT|false'),
            ('public.datasets|dataset_name|kb_pipeline_app|SELECT|false'),
            ('public.datasets|metadata|kb_pipeline_app|SELECT|false'),
            ('public.dataset_sources|id|kb_pipeline_app|SELECT|false'),
            ('public.dataset_sources|name|kb_pipeline_app|SELECT|false'),
            ('public.dataset_sources|source_type|kb_pipeline_app|SELECT|false'),
            ('public.metadata|id|kb_pipeline_app|SELECT|false'),
            ('public.metadata|source_id|kb_pipeline_app|SELECT|false'),
            ('public.metadata|source_url|kb_pipeline_app|SELECT|false'),
            ('public.metadata|source_title|kb_pipeline_app|SELECT|false'),
            ('public.metadata|source_type|kb_pipeline_app|SELECT|false'),
            ('public.metadata|domain|kb_pipeline_app|SELECT|false'),
            ('public.metadata|published_at|kb_pipeline_app|SELECT|false'),
            ('public.metadata|updated_at|kb_pipeline_app|SELECT|false'),
            ('public.metadata||kb_pipeline_app|INSERT|false'),
            ('public.kb||kb_pipeline_app|SELECT|false'),
            ('public.kb||kb_pipeline_app|INSERT|false')),
        -- END APPROVED PROVISIONING ACL
        problems AS (
            SELECT 'non-pipeline ACL of ' || d.rel || ' differs from the owner default' AS p
            FROM ((SELECT * FROM other_entries EXCEPT SELECT * FROM owner_default)
                  UNION ALL
                  (SELECT * FROM owner_default EXCEPT SELECT * FROM other_entries)) AS d
            UNION ALL
            SELECT 'non-pipeline column ACL entry ' || obj FROM other_column_entries
            UNION ALL
            SELECT 'unapproved ACL entry ' || k FROM (SELECT k FROM pipeline_entries EXCEPT SELECT k FROM approved) AS u
            UNION ALL
            SELECT 'missing approved ACL entry ' || k FROM (SELECT k FROM approved EXCEPT SELECT k FROM pipeline_entries) AS m)
        SELECT string_agg(DISTINCT p, '; ') FROM problems);

    FOREACH metric IN ARRAY ARRAY[
        'kb_rows', 'metadata_rows', 'datasets_rows', 'dataset_sources_rows',
        'public_alembic_version', 'kb_data_digest', 'metadata_data_digest',
        'datasets_data_digest', 'dataset_sources_data_digest',
        'structure_digest', 'acl_digest', 'public_inventory_digest']
    LOOP
        baseline := current_setting('kbp_verify.b_' || metric);
        observed := current_setting('kbp_verify.c_' || metric);
        IF baseline = '' THEN
            failures := failures + 1;
            RAISE WARNING 'FAIL L-%: no baseline value supplied (current: %)', metric, observed;
        ELSIF baseline = observed THEN
            RAISE NOTICE 'PASS L-%: unchanged (%)', metric, observed;
        ELSIF metric = 'acl_digest' AND baseline = all_default AND acl_delta IS NULL THEN
            RAISE NOTICE 'PASS L-acl_digest: the baseline predates provisioning (all-default ACLs, %); the only ACL change is exactly the approved provisioning grants (now: %)',
                baseline, observed;
        ELSIF metric = 'acl_digest' AND baseline = all_default THEN
            failures := failures + 1;
            RAISE WARNING 'FAIL L-acl_digest: the baseline predates provisioning, and the ACL change is NOT exactly the approved provisioning grants: %',
                acl_delta;
        ELSE
            failures := failures + 1;
            RAISE WARNING 'FAIL L-%: CHANGED (baseline: %, now: %)', metric, baseline, observed;
        END IF;
    END LOOP;

    -- The frozen bridge rows must not exist: the bridge bootstrap is BLOCKED.
    IF EXISTS (
        SELECT 1 FROM public.datasets
        WHERE id IN ('a7d8d2e1-8400-5916-9440-b492e8e692ce'::uuid,
                     'b487f559-5f8e-56ba-9746-28658b9d09d1'::uuid)
           OR dataset_name IN ('kb_pipeline_common_crawl', 'kb_pipeline_public_github')
    ) THEN
        failures := failures + 1;
        RAISE WARNING 'FAIL L-bridge: a Gold bridge row exists in public.datasets (bootstrap is BLOCKED)';
    ELSE
        RAISE NOTICE 'PASS L-bridge: no Gold bridge rows in public.datasets';
    END IF;

    PERFORM set_config('kbp_verify.failures',
        (coalesce(nullif(current_setting('kbp_verify.failures', true), ''), '0')::integer + failures)::text,
        true);
END
$legacy$;

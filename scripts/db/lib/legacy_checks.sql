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
    metric   text;
    baseline text;
    observed text;
    failures integer := 0;
BEGIN
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

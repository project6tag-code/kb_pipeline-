-- =============================================================================
-- verify_gold_metadata.sql - READ ONLY public.metadata compatibility check
-- =============================================================================
-- Admin/catalog reader only. Run only against kb_pipeline_test:
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d kb_pipeline_test \
--        -f scripts/db/verify_gold_metadata.sql
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public;

DO $checks$
DECLARE
    columns text;
    metadata_oid oid := 'public.metadata'::regclass;
BEGIN
    IF current_database() <> 'kb_pipeline_test' THEN
        RAISE EXCEPTION 'refusing verification on database % (expected kb_pipeline_test)', current_database();
    END IF;
    IF (SELECT count(*) FROM kbp.alembic_version WHERE version_num = 'kbp_001d') <> 1 THEN
        RAISE EXCEPTION 'kbp revision must be kbp_001d before metadata validation';
    END IF;
    SELECT string_agg(a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) || ':' ||
                      a.attnotnull::text || ':' || (d.adbin IS NOT NULL)::text,
                      ',' ORDER BY a.attnum)
    INTO columns
    FROM pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE a.attrelid = metadata_oid AND a.attnum > 0 AND NOT a.attisdropped;
    IF columns IS DISTINCT FROM
       'id:uuid:true:false,source_id:uuid:true:false,domain:character varying(100):false:false,source_url:text:false:false,source_title:text:false:false,source_type:character varying(50):false:false,published_at:timestamp with time zone:false:false,updated_at:timestamp with time zone:false:false' THEN
        RAISE EXCEPTION 'public.metadata does not match the additive final contract: %', columns;
    END IF;
    IF pg_catalog.pg_get_userbyid((SELECT relowner FROM pg_catalog.pg_class WHERE oid = metadata_oid)) <> 'postgres' THEN
        RAISE EXCEPTION 'public.metadata owner changed';
    END IF;
    IF (SELECT relacl::text FROM pg_catalog.pg_class WHERE oid = metadata_oid)
       IS DISTINCT FROM '{postgres=arwdDxtm/postgres,kb_pipeline_app=a/postgres}' THEN
        RAISE EXCEPTION 'public.metadata grants differ from the pre-patch fingerprint';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid = metadata_oid) <> 2
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                      WHERE conrelid = metadata_oid AND conname = 'metadata_pkey' AND contype = 'p'
                        AND pg_catalog.pg_get_constraintdef(oid) = 'PRIMARY KEY (id)')
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                      WHERE conrelid = metadata_oid AND conname = 'metadata_source_id_fkey' AND contype = 'f'
                        AND confrelid = 'public.datasets'::regclass AND convalidated AND NOT condeferrable
                        AND confupdtype = 'a' AND confdeltype = 'a') THEN
        RAISE EXCEPTION 'public.metadata PK/FK contract changed';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = metadata_oid AND NOT tgisinternal) THEN
        RAISE EXCEPTION 'public.metadata has user triggers';
    END IF;
    IF (SELECT count(*) FROM public.kb) <> 0 OR (SELECT count(id) FROM public.metadata) <> 0 THEN
        RAISE EXCEPTION 'Gold tables are not empty; publication is out of scope for this schema task';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c, LATERAL aclexplode(c.relacl) x
               WHERE c.oid = metadata_oid AND (x.grantee = 0 OR x.is_grantable
                     OR pg_catalog.pg_get_userbyid(x.grantee) NOT IN ('postgres', 'kb_pipeline_app'))) THEN
        RAISE EXCEPTION 'public.metadata has PUBLIC, third-party or grantable privileges';
    END IF;
    RAISE NOTICE 'GOLD METADATA VERIFICATION PASSED';
END
$checks$;

SELECT current_database() AS database, current_user AS verified_by,
       (SELECT count(*) FROM public.kb) AS public_kb_rows,
       (SELECT count(id) FROM public.metadata) AS public_metadata_rows;

ROLLBACK;
-- =============================================================================
-- bootstrap_public_tables.sql - create the four Gold-contract tables in an EMPTY database (clean-machine setup)
-- =============================================================================
-- The pipeline publishes into public.kb / public.metadata and registers each source document in public.datasets
-- (parent: public.dataset_sources). They are not created by any Alembic migration (migrations only touch schema
-- kbp). This script creates them in the validated final V2 layout of the working database, byte for byte: the same
-- column order, types, nullability, constraint and index names, the nonexistent defaults, and the non-deferrable
-- kb -> metadata ON DELETE CASCADE foreign key. Nothing is invented: it is the catalog of kb_pipeline_test.
--
--   public.kb       id, chunk, embedding vector(768), source_id        (source_id = original source document)
--   public.metadata id, source_id, domain, source_url, source_title, source_type, published_at, updated_at, crawled_at
--
-- Run ONCE as the database owner / superuser (postgres), BEFORE provision_kbp_roles.sql (whose guard requires these
-- tables, owned by postgres and without PUBLIC privileges, and whose grants name the metadata columns):
--   psql -X -v ON_ERROR_STOP=1 -h localhost -U postgres -d <database> -f scripts/db/bootstrap_public_tables.sql
-- Prerequisite: CREATE EXTENSION vector; in that database (public schema).
--
-- Idempotent: existing tables are left untouched and verified against the exact layout; any difference aborts.
-- No GRANT is issued here (privileges are the job of provision_kbp_roles.sql and admin_gold_v2_patch.sql).
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off

BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '120s';
SET LOCAL search_path = pg_catalog, public;

DO $guard$
BEGIN
    IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) THEN
        RAISE EXCEPTION 'run the public-table bootstrap as a PostgreSQL superuser (current_user = %)', current_user;
    END IF;
    IF current_user IN ('kb_pipeline_app', 'kb_pipeline_migrator') THEN
        RAISE EXCEPTION 'the public-table bootstrap never runs as a pipeline role';
    END IF;
    IF current_database() IN ('data_collection_pipeline', 'postgres', 'template0', 'template1') THEN
        RAISE EXCEPTION 'refusing to bootstrap protected database %', current_database();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
                   WHERE e.extname = 'vector' AND n.nspname = 'public') THEN
        RAISE EXCEPTION 'extension vector is not installed in schema public (CREATE EXTENSION vector;)';
    END IF;
END
$guard$;

-- BEGIN-DDL
CREATE TABLE IF NOT EXISTS public.dataset_sources (
    id uuid NOT NULL,
    name character varying(100) NOT NULL,
    source_type character varying(50) NOT NULL,
    authentication_required boolean NOT NULL,
    status character varying(20) NOT NULL,
    CONSTRAINT dataset_sources_pkey PRIMARY KEY (id),
    CONSTRAINT uq_dataset_sources_name UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS public.datasets (
    id uuid NOT NULL,
    source_id uuid NOT NULL,
    dataset_name character varying(255) NOT NULL,
    status character varying(50) NOT NULL,
    metadata jsonb NOT NULL,
    CONSTRAINT datasets_pkey PRIMARY KEY (id),
    CONSTRAINT datasets_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.dataset_sources (id)
);

CREATE TABLE IF NOT EXISTS public.metadata (
    id uuid NOT NULL,
    source_id uuid NOT NULL,
    domain character varying(100),
    source_url text,
    source_title text,
    source_type character varying(50),
    published_at timestamp with time zone,
    updated_at timestamp with time zone,
    crawled_at timestamp with time zone,
    CONSTRAINT metadata_pkey PRIMARY KEY (id),
    CONSTRAINT metadata_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.datasets (id)
);

CREATE TABLE IF NOT EXISTS public.kb (
    id uuid NOT NULL,
    chunk text NOT NULL,
    embedding public.vector(768),
    source_id uuid,
    CONSTRAINT kb_pkey PRIMARY KEY (id),
    CONSTRAINT kb_id_fkey FOREIGN KEY (id) REFERENCES public.metadata (id) ON DELETE CASCADE
);
-- END-DDL

DO $verify$
DECLARE
    expected_columns constant jsonb := jsonb_build_object(
        'dataset_sources', 'id:uuid:true,name:character varying(100):true,source_type:character varying(50):true,authentication_required:boolean:true,status:character varying(20):true',
        'datasets', 'id:uuid:true,source_id:uuid:true,dataset_name:character varying(255):true,status:character varying(50):true,metadata:jsonb:true',
        'metadata', 'id:uuid:true,source_id:uuid:true,domain:character varying(100):false,source_url:text:false,source_title:text:false,source_type:character varying(50):false,published_at:timestamp with time zone:false,updated_at:timestamp with time zone:false,crawled_at:timestamp with time zone:false',
        'kb', 'id:uuid:true,chunk:text:true,embedding:vector(768):false,source_id:uuid:false');
    expected_constraints constant jsonb := jsonb_build_object(
        'dataset_sources', 'dataset_sources_pkey:PRIMARY KEY (id)|uq_dataset_sources_name:UNIQUE (name)',
        'datasets', 'datasets_pkey:PRIMARY KEY (id)|datasets_source_id_fkey:FOREIGN KEY (source_id) REFERENCES dataset_sources(id)',
        'metadata', 'metadata_pkey:PRIMARY KEY (id)|metadata_source_id_fkey:FOREIGN KEY (source_id) REFERENCES datasets(id)',
        'kb', 'kb_id_fkey:FOREIGN KEY (id) REFERENCES metadata(id) ON DELETE CASCADE|kb_pkey:PRIMARY KEY (id)');
    t text;
    actual text;
BEGIN
    FOR t IN SELECT jsonb_object_keys(expected_columns) LOOP
        SELECT string_agg(a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull::text,
                          ',' ORDER BY a.attnum)
        INTO actual
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = ('public.' || t)::regclass AND a.attnum > 0 AND NOT a.attisdropped;
        IF actual IS DISTINCT FROM expected_columns ->> t THEN
            RAISE EXCEPTION 'public.% differs from the Gold V2 layout: %', t, actual;
        END IF;

        SELECT string_agg(c.conname || ':' || pg_catalog.pg_get_constraintdef(c.oid), '|' ORDER BY c.conname)
        INTO actual
        FROM pg_catalog.pg_constraint c WHERE c.conrelid = ('public.' || t)::regclass;
        IF actual IS DISTINCT FROM expected_constraints ->> t THEN
            RAISE EXCEPTION 'public.% constraints differ from the Gold V2 layout: %', t, actual;
        END IF;

        IF (SELECT count(*) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = ('public.' || t)::regclass) <> 0 THEN
            RAISE EXCEPTION 'public.% must have no column defaults', t;
        END IF;
        IF (SELECT count(*) FROM pg_catalog.pg_trigger g WHERE g.tgrelid = ('public.' || t)::regclass AND NOT g.tgisinternal) <> 0 THEN
            RAISE EXCEPTION 'public.% must have no triggers', t;
        END IF;
        IF (SELECT count(*) FROM pg_catalog.pg_index i WHERE i.indrelid = ('public.' || t)::regclass)
           <> (SELECT count(*) FROM pg_catalog.pg_constraint c WHERE c.conrelid = ('public.' || t)::regclass AND c.contype IN ('p', 'u')) THEN
            RAISE EXCEPTION 'public.% has indexes beyond its primary/unique keys (no ANN index in V1)', t;
        END IF;
        IF (SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE oid = ('public.' || t)::regclass)
           IN ('kb_pipeline_app', 'kb_pipeline_migrator') THEN
            RAISE EXCEPTION 'public.% must not be owned by a pipeline role', t;
        END IF;
    END LOOP;
END
$verify$;

COMMIT;

\echo 'public tables bootstrap: OK (dataset_sources, datasets, metadata, kb in the Gold V2 layout)'

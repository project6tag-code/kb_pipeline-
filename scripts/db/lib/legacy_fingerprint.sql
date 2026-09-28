-- Legacy fingerprint of the protected public objects. READ ONLY.
--
-- Included (\ir) by pre_migration_snapshot.sql, verify_migration_001a.sql and
-- verify_downgrade_001a.sql, so every run computes the digests with identical
-- SQL. Must be run inside the caller's READ ONLY transaction.
--
-- Protected objects (plus an inventory of all of schema public): public.kb, public.metadata, public.datasets,
-- public.dataset_sources, public.alembic_version.
--
-- Output: exact row counts and md5 digests only. No chunk text, embedding,
-- metadata value or dataset row content is printed: each row is reduced to
-- md5(row::text) and the per-table digest is md5 over those, ordered by id.
--
-- Sets psql variables fp_<metric> for the comparison in legacy_checks.sql.

-- 1. Data: exact counts and content digests.
SELECT
    (SELECT count(*) FROM public.kb)::text              AS kb_rows,
    (SELECT count(*) FROM public.metadata)::text        AS metadata_rows,
    (SELECT count(*) FROM public.datasets)::text        AS datasets_rows,
    (SELECT count(*) FROM public.dataset_sources)::text AS dataset_sources_rows,
    (SELECT coalesce(string_agg(v.version_num, ',' ORDER BY v.version_num), '<none>')
       FROM public.alembic_version AS v)                AS public_alembic_version,
    (SELECT coalesce(md5(string_agg(md5(r::text), '' ORDER BY r.id)), '<empty>')
       FROM public.kb AS r)                             AS kb_data_digest,
    (SELECT coalesce(md5(string_agg(md5(r::text), '' ORDER BY r.id)), '<empty>')
       FROM public.metadata AS r)                       AS metadata_data_digest,
    (SELECT coalesce(md5(string_agg(md5(r::text), '' ORDER BY r.id)), '<empty>')
       FROM public.datasets AS r)                       AS datasets_data_digest,
    (SELECT coalesce(md5(string_agg(md5(r::text), '' ORDER BY r.id)), '<empty>')
       FROM public.dataset_sources AS r)                AS dataset_sources_data_digest
\gset fp_

-- 2. Structure: relation, columns, own constraints (incl. FK actions),
--    indexes, user triggers, owner. ACLs are fingerprinted separately (3.)
--    because provisioning legitimately adds grants. Internal (RI) triggers are
--    excluded: the approved kbp FK adds RI triggers on public.datasets, which
--    legacy_checks/verify scripts check explicitly instead.
WITH rels AS (
    SELECT c.oid, 'public.' || c.relname AS rel
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')
), items AS (
    SELECT r.rel || ' relation kind=' || c.relkind::text
           || ' owner=' || pg_catalog.pg_get_userbyid(c.relowner)
           || ' rls=' || c.relrowsecurity::text || '/' || c.relforcerowsecurity::text
           || ' options=' || coalesce(array_to_string(c.reloptions, ','), '') AS item
    FROM rels AS r JOIN pg_catalog.pg_class AS c ON c.oid = r.oid
    UNION ALL
    SELECT r.rel || ' column ' || lpad(a.attnum::text, 3, '0') || ' ' || a.attname
           || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod)
           || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
           || coalesce(' DEFAULT ' || pg_catalog.pg_get_expr(d.adbin, d.adrelid), '')
           || CASE WHEN a.attidentity <> '' THEN ' IDENTITY ' || a.attidentity::text ELSE '' END
           || CASE WHEN a.attgenerated <> '' THEN ' GENERATED ' || a.attgenerated::text ELSE '' END
    FROM rels AS r
    JOIN pg_catalog.pg_attribute AS a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_catalog.pg_attrdef AS d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    UNION ALL
    SELECT r.rel || ' constraint ' || co.conname || ' ' || pg_catalog.pg_get_constraintdef(co.oid)
           || ' deferrable=' || co.condeferrable::text || ' deferred=' || co.condeferred::text
           || ' validated=' || co.convalidated::text
    FROM rels AS r JOIN pg_catalog.pg_constraint AS co ON co.conrelid = r.oid
    UNION ALL
    SELECT r.rel || ' index ' || i.relname || ' ' || pg_catalog.pg_get_indexdef(x.indexrelid)
           || ' valid=' || x.indisvalid::text
    FROM rels AS r
    JOIN pg_catalog.pg_index AS x ON x.indrelid = r.oid
    JOIN pg_catalog.pg_class AS i ON i.oid = x.indexrelid
    UNION ALL
    SELECT r.rel || ' trigger ' || t.tgname || ' ' || pg_catalog.pg_get_triggerdef(t.oid)
           || ' enabled=' || t.tgenabled::text
    FROM rels AS r
    JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = r.oid AND NOT t.tgisinternal
    UNION ALL
    SELECT 'MISSING relation ' || w.rel
    FROM unnest(ARRAY['public.kb', 'public.metadata', 'public.datasets',
                      'public.dataset_sources', 'public.alembic_version']) AS w(rel)
    WHERE w.rel NOT IN (SELECT rel FROM rels)
)
SELECT md5(string_agg(item, E'\n' ORDER BY item)) AS structure_digest,
       string_agg(item, E'\n' ORDER BY item)      AS structure_listing
FROM items
\gset fp_

-- 3. Privileges (table and column ACLs) of the protected objects.
WITH rels AS (
    SELECT c.oid, 'public.' || c.relname AS rel, c.relacl
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname IN ('kb', 'metadata', 'datasets', 'dataset_sources', 'alembic_version')
), items AS (
    SELECT r.rel || ' acl ' || coalesce(array_to_string(r.relacl, ','), '<default>') AS item
    FROM rels AS r
    UNION ALL
    SELECT r.rel || '.' || a.attname || ' column-acl ' || array_to_string(a.attacl, ',')
    FROM rels AS r
    JOIN pg_catalog.pg_attribute AS a ON a.attrelid = r.oid AND a.attnum > 0
         AND NOT a.attisdropped AND a.attacl IS NOT NULL
)
SELECT md5(string_agg(item, E'\n' ORDER BY item)) AS acl_digest,
       string_agg(item, E'\n' ORDER BY item)      AS acl_listing
FROM items
\gset fp_

-- 4. Inventory of schema public: every relation and function (name, kind,
--    owner). Any object created in, or removed from, public changes it.
WITH items AS (
    SELECT 'relation ' || c.relname || ' kind=' || c.relkind::text
           || ' owner=' || pg_catalog.pg_get_userbyid(c.relowner) AS item
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
    UNION ALL
    SELECT 'function ' || p.oid::regprocedure::text || ' owner=' || pg_catalog.pg_get_userbyid(p.proowner)
    FROM pg_catalog.pg_proc AS p
    JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
)
SELECT md5(string_agg(item, E'\n' ORDER BY item)) AS public_inventory_digest,
       count(*)::text                             AS public_inventory_count
FROM items
\gset fp_

\echo
\echo '== LEGACY FINGERPRINT (counts and digests only; no row content) =='
SELECT v.metric, v.value
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
    (12, 'public_inventory_digest',     :'fp_public_inventory_digest'),
    (13, 'public_inventory_count',      :'fp_public_inventory_count')
) AS v(ord, metric, value)
ORDER BY v.ord;

\echo '== legacy structure listing (source of structure_digest) =='
SELECT unnest(string_to_array(:'fp_structure_listing', E'\n')) AS legacy_structure;

\echo '== legacy privilege listing (source of acl_digest) =='
SELECT unnest(string_to_array(:'fp_acl_listing', E'\n')) AS legacy_privileges;

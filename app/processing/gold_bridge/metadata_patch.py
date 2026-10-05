"""Admin-only additive compatibility patch for ``public.metadata``.

This patches only the DCA_v2 V1 test database. It never modifies rows,
constraints, grants, ownership, or any public relation other than adding the
five audited nullable provenance columns.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from app.processing.gold_bridge import spec

EXPECTED_REVISION = "kbp_001d"
REQUIRED_DATABASE = "kb_pipeline_test"
FORBIDDEN_DATABASES = frozenset({"data_collection_pipeline"})
PIPELINE_ROLES = frozenset({"kb_pipeline_app", "kb_pipeline_migrator"})

BASE_COLUMNS = (
    ("id", "uuid", True),
    ("source_id", "uuid", True),
    ("domain", "character varying(100)", False),
)
ADDITIVE_COLUMNS = (
    ("source_url", "text", False),
    ("source_title", "text", False),
    ("source_type", "character varying(50)", False),
    ("published_at", "timestamp with time zone", False),
    ("updated_at", "timestamp with time zone", False),
)
FINAL_COLUMNS = BASE_COLUMNS + ADDITIVE_COLUMNS

SESSION_GUARDS = (
    "SET LOCAL lock_timeout = '5s'",
    "SET LOCAL statement_timeout = '60s'",
    "SET LOCAL search_path = pg_catalog, public",
)

IDENTITY_SQL = """
SELECT current_database() AS database, current_user AS role,
       (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) AS superuser,
       (SELECT pg_catalog.pg_get_userbyid(c.relowner)
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'metadata' AND c.relkind = 'r') AS metadata_owner,
       EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = 'metadata' AND c.relkind = 'r') AS has_metadata,
       (SELECT version_num FROM kbp.alembic_version) AS revision
"""

COLUMNS_SQL = """
SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull,
       d.adbin IS NOT NULL AS has_default, a.attidentity, a.attgenerated
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = 'public.metadata'::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""

CONSTRAINTS_SQL = """
SELECT conname, contype, pg_catalog.pg_get_constraintdef(oid), convalidated, condeferrable,
       confupdtype, confdeltype
FROM pg_catalog.pg_constraint
WHERE conrelid = 'public.metadata'::regclass
ORDER BY conname
"""

TRIGGERS_SQL = """
SELECT count(*) FROM pg_catalog.pg_trigger
WHERE tgrelid = 'public.metadata'::regclass AND NOT tgisinternal
"""

ACL_SQL = """
SELECT pg_catalog.pg_get_userbyid(c.relowner), c.relacl::text
FROM pg_catalog.pg_class c WHERE c.oid = 'public.metadata'::regclass
"""

PUBLIC_INVENTORY_SQL = """
SELECT c.relname, c.relkind, pg_catalog.pg_get_userbyid(c.relowner), c.relacl::text,
       (SELECT string_agg(a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) || ':' ||
                          a.attnotnull::text || ':' || (d.adbin IS NOT NULL)::text,
                          ',' ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute a
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS columns,
       (SELECT string_agg(k.conname || ':' || pg_catalog.pg_get_constraintdef(k.oid), ';' ORDER BY k.conname)
        FROM pg_catalog.pg_constraint k WHERE k.conrelid = c.oid) AS constraints,
       (SELECT string_agg(i.indexrelid::regclass::text || ':' || pg_catalog.pg_get_indexdef(i.indexrelid),
                          ';' ORDER BY i.indexrelid::regclass::text)
        FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid) AS indexes,
       (SELECT count(*) FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) AS triggers
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
ORDER BY c.relname
"""

ROW_COUNTS_SQL = """
SELECT (SELECT count(*) FROM public.kb) AS kb,
       (SELECT count(*) FROM public.metadata) AS metadata,
       (SELECT count(*) FROM public.datasets) AS datasets,
       (SELECT count(*) FROM public.dataset_sources) AS dataset_sources
"""

METADATA_DATA_SQL = """
SELECT id::text, source_id::text, domain
FROM public.metadata
ORDER BY id
"""

ADD_COLUMN_SQL = {
    "source_url": "ALTER TABLE public.metadata ADD COLUMN source_url text",
    "source_title": "ALTER TABLE public.metadata ADD COLUMN source_title text",
    "source_type": "ALTER TABLE public.metadata ADD COLUMN source_type character varying(50)",
    "published_at": "ALTER TABLE public.metadata ADD COLUMN published_at timestamp with time zone",
    "updated_at": "ALTER TABLE public.metadata ADD COLUMN updated_at timestamp with time zone",
}

EXPECTED_CONSTRAINTS = {
    ("metadata_pkey", "p", "PRIMARY KEY (id)", True, False, None, None),
    ("metadata_source_id_fkey", "f", "FOREIGN KEY (source_id) REFERENCES datasets(id)",
     True, False, "a", "a"),
}


class MetadataPatchRefused(RuntimeError):
    """A target, role, or schema precondition failed before any ALTER."""


class MetadataPatchConflict(RuntimeError):
    """The current or resulting metadata schema differs from the exact contract."""


def _text(sql: str) -> Any:
    from sqlalchemy import text

    return text(sql)


def _sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _columns(conn: Any) -> tuple[tuple[str, str, bool], ...]:
    rows = conn.execute(_text(COLUMNS_SQL)).all()
    for name, _type, _not_null, has_default, identity, generated in rows:
        if has_default or identity or generated:
            raise MetadataPatchConflict(f"public.metadata.{name} has a default, identity, or generated expression")
    return tuple((str(name), str(data_type), bool(not_null)) for name, data_type, not_null, *_ in rows)


def _verify_constraints(conn: Any) -> tuple[tuple, ...]:
    actual = {tuple(row) for row in conn.execute(_text(CONSTRAINTS_SQL)).all()}
    if actual != EXPECTED_CONSTRAINTS:
        raise MetadataPatchConflict(f"public.metadata constraints differ from the audited baseline: {sorted(actual)}")
    return tuple(sorted(actual))


def _row_digest(conn: Any) -> tuple[int, str]:
    rows = [tuple(row) for row in conn.execute(_text(METADATA_DATA_SQL)).all()]
    return len(rows), _sha256_json(rows)


def _public_inventory(conn: Any) -> dict[str, tuple]:
    rows = [tuple(row) for row in conn.execute(_text(PUBLIC_INVENTORY_SQL)).all()]
    return {row[0]: row for row in rows}


def _metadata_columns_from_inventory(inventory: dict[str, tuple]) -> tuple[str, ...]:
    encoded = inventory["metadata"][4]
    return tuple(part.split(":", 1)[0] for part in (encoded or "").split(",") if part)


def verify_target(conn: Any, expect_database: str) -> dict[str, Any]:
    if expect_database in FORBIDDEN_DATABASES or expect_database != REQUIRED_DATABASE:
        raise MetadataPatchRefused(f"--expect-database {expect_database!r} is refused; only {REQUIRED_DATABASE!r} is allowed")
    row = dict(conn.execute(_text(IDENTITY_SQL)).mappings().one())
    if row["database"] in FORBIDDEN_DATABASES or row["database"] != expect_database:
        raise MetadataPatchRefused(f"connected to database {row['database']!r}, expected {expect_database!r}")
    if not row["has_metadata"]:
        raise MetadataPatchRefused("public.metadata does not exist")
    if row["revision"] != EXPECTED_REVISION:
        raise MetadataPatchRefused(f"kbp revision is {row['revision']!r}, expected {EXPECTED_REVISION!r}")
    if row["role"] in PIPELINE_ROLES:
        raise MetadataPatchRefused("metadata admin patch never runs as a pipeline role")
    if not row["superuser"] and row["role"] != row["metadata_owner"]:
        raise MetadataPatchRefused("metadata patch requires a superuser or public.metadata table owner")
    return {key: row[key] for key in ("database", "role", "superuser", "metadata_owner", "revision")}


def patch_metadata(conn: Any, *, expect_database: str) -> dict[str, Any]:
    """Apply the additive patch or report the already-patched schema; caller owns transaction."""
    from sqlalchemy import text

    identity = verify_target(conn, expect_database)
    for statement in SESSION_GUARDS:
        conn.execute(_text(statement))

    before_columns = _columns(conn)
    if before_columns not in (BASE_COLUMNS, FINAL_COLUMNS):
        raise MetadataPatchConflict(
            f"public.metadata columns differ from the exact base/final contract: {before_columns!r}"
        )
    constraints_before = _verify_constraints(conn)
    if conn.execute(_text(TRIGGERS_SQL)).scalar_one() != 0:
        raise MetadataPatchConflict("public.metadata has user triggers; refusing DDL")

    owner_acl_before = tuple(conn.execute(_text(ACL_SQL)).one())
    inventory_before = _public_inventory(conn)
    counts_before = dict(conn.execute(_text(ROW_COUNTS_SQL)).mappings().one())
    metadata_rows_before, metadata_digest_before = _row_digest(conn)

    if before_columns == BASE_COLUMNS:
        for column_name, _column_type, _nullable in ADDITIVE_COLUMNS:
            conn.execute(text(ADD_COLUMN_SQL[column_name]))
        action = "added"
    else:
        action = "reused"

    after_columns = _columns(conn)
    if after_columns != FINAL_COLUMNS:
        raise MetadataPatchConflict(f"patched public.metadata columns differ from the final contract: {after_columns!r}")
    constraints_after = _verify_constraints(conn)
    if constraints_after != constraints_before:
        raise MetadataPatchConflict("public.metadata constraints changed during the additive patch")
    if conn.execute(_text(TRIGGERS_SQL)).scalar_one() != 0:
        raise MetadataPatchConflict("public.metadata now has user triggers")

    owner_acl_after = tuple(conn.execute(_text(ACL_SQL)).one())
    inventory_after = _public_inventory(conn)
    counts_after = dict(conn.execute(_text(ROW_COUNTS_SQL)).mappings().one())
    metadata_rows_after, metadata_digest_after = _row_digest(conn)
    if owner_acl_after != owner_acl_before:
        raise MetadataPatchConflict("public.metadata owner or ACL changed")
    if counts_after != counts_before:
        raise MetadataPatchConflict("public row counts changed during metadata schema patch")
    if (metadata_rows_after, metadata_digest_after) != (metadata_rows_before, metadata_digest_before):
        raise MetadataPatchConflict("existing public.metadata id/source_id/domain data changed")

    expected_inventory = dict(inventory_before)
    expected_inventory["metadata"] = (
        *inventory_before["metadata"][:4],
        ",".join(f"{name}:{data_type}:{str(not_null).lower()}:false" for name, data_type, not_null in FINAL_COLUMNS),
        *inventory_before["metadata"][5:],
    )
    if inventory_after != expected_inventory:
        raise MetadataPatchConflict("public schema fingerprint differs beyond the five appended nullable columns")

    return {
        "stage": "admin_metadata_patch",
        **identity,
        "action": action,
        "columns_added": [name for name, _type, _null in ADDITIVE_COLUMNS] if action == "added" else [],
        "columns_reused": [name for name, _type, _null in ADDITIVE_COLUMNS] if action == "reused" else [],
        "metadata_columns_before": before_columns,
        "metadata_columns_after": after_columns,
        "constraints_unchanged": True,
        "owner_acl_unchanged": True,
        "public_row_counts_before": counts_before,
        "public_row_counts_after": counts_after,
        "metadata_rows": metadata_rows_after,
        "metadata_existing_values_sha256": metadata_digest_after,
        "public_inventory_sha256_before": _sha256_json(inventory_before),
        "public_inventory_sha256_after": _sha256_json(inventory_after),
    }
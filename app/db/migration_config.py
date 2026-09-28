"""Alembic isolation settings and preflight checks for the ``kbp`` schema.

This module is imported by ``migrations/env.py``. It is deliberately free of
Alembic imports so it can be unit-tested offline.

Isolation rules (docs/database_schema_v1.md §22, docs/database_privileges_v1.md):

* the version table is ``kbp.alembic_version``; ``public.alembic_version``
  (owned by the legacy migration history) is never used;
* autogenerate is disabled (migrations are reviewed raw SQL); the name/object
  filters additionally exclude every non-``kbp`` object;
* migrations run as ``kb_pipeline_migrator``, the owner of schema ``kbp``,
  which is provisioned administratively and never created by a migration.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Final

KBP_SCHEMA: Final[str] = "kbp"
VERSION_TABLE: Final[str] = "alembic_version"
VERSION_TABLE_SCHEMA: Final[str] = KBP_SCHEMA

MIGRATOR_ROLE: Final[str] = "kb_pipeline_migrator"
RUNTIME_ROLE: Final[str] = "kb_pipeline_app"

MIGRATION_URL_ENV: Final[str] = "KB_MIGRATION_DATABASE_URL"
RUNTIME_URL_ENV: Final[str] = "KB_DATABASE_URL"


class MigrationPreflightError(RuntimeError):
    """Raised when the database is not provisioned for kbp migrations."""


def alembic_configure_kwargs() -> dict[str, Any]:
    """Keyword arguments shared by offline and online ``context.configure``."""
    return {
        "target_metadata": None,
        "version_table": VERSION_TABLE,
        "version_table_schema": VERSION_TABLE_SCHEMA,
        "include_schemas": True,
        "include_name": include_name,
        "include_object": include_object,
        "transaction_per_migration": True,
    }


def include_name(name: str | None, type_: str, parent_names: Mapping[str, Any]) -> bool:
    """Autogenerate name filter: admit only the ``kbp`` schema and its objects."""
    if type_ == "schema":
        return name == KBP_SCHEMA
    schema = parent_names.get("schema_name")
    return schema == KBP_SCHEMA


def include_object(obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any) -> bool:
    """Autogenerate object filter: admit only objects that live in ``kbp``."""
    if type_ == "table":
        return getattr(obj, "schema", None) == KBP_SCHEMA
    table = getattr(obj, "table", None)
    if table is not None:
        return getattr(table, "schema", None) == KBP_SCHEMA
    return getattr(obj, "schema", None) == KBP_SCHEMA


def get_migration_url(environ: Mapping[str, str] | None = None) -> str:
    """Return the migration connection URL from the environment.

    The value is never logged. The runtime URL must not be reused for
    migrations (role separation).
    """
    env = os.environ if environ is None else environ
    url = env.get(MIGRATION_URL_ENV, "").strip()
    if not url:
        raise MigrationPreflightError(
            f"{MIGRATION_URL_ENV} is not set; configure it from your secrets store "
            "(see .env.example and docs/migrations_kbp.md)"
        )
    runtime_url = env.get(RUNTIME_URL_ENV, "").strip()
    if runtime_url and runtime_url == url:
        raise MigrationPreflightError(
            f"{MIGRATION_URL_ENV} must not equal {RUNTIME_URL_ENV}: migrations run as "
            f"{MIGRATOR_ROLE}, the application as {RUNTIME_ROLE}"
        )
    return url


def create_migration_engine(url: str) -> Any:
    """Create a non-pooled SQLAlchemy engine for a migration run."""
    from sqlalchemy import create_engine, pool

    return create_engine(url, poolclass=pool.NullPool)


# Each check: (description, SQL returning one row with one boolean, failure message).
PREFLIGHT_CHECKS: Final[tuple[tuple[str, str, str], ...]] = (
    (
        "schema kbp exists",
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'kbp')",
        "schema kbp does not exist. It must be provisioned by the administrator "
        "(CREATE SCHEMA kbp AUTHORIZATION kb_pipeline_migrator) before migrating; "
        "migrations never create it.",
    ),
    (
        "connected as kb_pipeline_migrator",
        "SELECT current_user = 'kb_pipeline_migrator'",
        "migrations must connect as kb_pipeline_migrator (docs/database_privileges_v1.md).",
    ),
    (
        "migration role is not a superuser",
        "SELECT NOT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user",
        "the migration role must not be a superuser.",
    ),
    (
        "kb_pipeline_migrator owns schema kbp",
        "SELECT pg_catalog.pg_get_userbyid(nspowner) = current_user "
        "FROM pg_catalog.pg_namespace WHERE nspname = 'kbp'",
        "schema kbp must be owned by kb_pipeline_migrator.",
    ),
    (
        "runtime role kb_pipeline_app exists with no elevated attributes",
        "SELECT COALESCE((SELECT NOT (rolsuper OR rolcreatedb OR rolcreaterole "
        "OR rolreplication OR rolbypassrls) FROM pg_catalog.pg_roles "
        "WHERE rolname = 'kb_pipeline_app'), false)",
        "role kb_pipeline_app must exist (provisioned administratively) and must not "
        "have SUPERUSER, CREATEDB, CREATEROLE, REPLICATION or BYPASSRLS.",
    ),
    (
        "public.datasets exists",
        "SELECT to_regclass('public.datasets') IS NOT NULL",
        "public.datasets must exist (target of the approved kbp.source_dataset_map FK).",
    ),
    (
        "REFERENCES privilege on public.datasets(id)",
        "SELECT has_column_privilege('public.datasets', 'id', 'REFERENCES')",
        "kb_pipeline_migrator needs REFERENCES (id) on public.datasets "
        "(docs/database_privileges_v1.md §3.2).",
    ),
)


def run_preflight(connection: Any) -> None:
    """Run read-only provisioning checks; raise on the first failure.

    Only catalog reads are performed; nothing is created or modified.

    The first ``execute`` autobegins a transaction (SQLAlchemy 2.0). It is
    always ended here, on success and on failure. If it stayed open, Alembic
    would treat it as an external transaction owned by the caller, skip its
    own BEGIN/COMMIT, and the migration would be rolled back when the
    connection closes.
    """
    from sqlalchemy import text

    try:
        for description, sql, message in PREFLIGHT_CHECKS:
            result = connection.execute(text(sql)).scalar()
            if result is not True:
                raise MigrationPreflightError(f"preflight failed ({description}): {message}")
    finally:
        # Read-only checks: nothing to keep, so end the transaction by rolling back.
        connection.rollback()

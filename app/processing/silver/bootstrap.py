"""Migrator-only bootstrap: register the Silver near-duplicate method in ``kbp.dedup_methods``.

The runtime role ``kb_pipeline_app`` has SELECT only on ``kbp.dedup_methods``
(docs/database_privileges_v1.md §5.1), like ``chunking_versions`` and
``embedding_models``: method registration is a controlled, idempotent
operator step run with KB_MIGRATION_DATABASE_URL (``kb_pipeline_migrator``).
It is NOT run automatically. Until it is run, Silver persistence stops with
``dedup_method_not_registered`` (retried; nothing is silently skipped).
"""

from __future__ import annotations

import json
from typing import Any

from app.db.connection import EXPECTED_KBP_REVISION
from app.processing.dedup import near

REGISTER_METHOD_SQL = """
INSERT INTO kbp.dedup_methods (algorithm, algorithm_version, parameters)
VALUES (:algorithm, :version, CAST(:parameters AS jsonb))
ON CONFLICT (algorithm, algorithm_version) DO NOTHING
"""

GET_METHOD_SQL = """
SELECT dedup_method_id, parameters FROM kbp.dedup_methods WHERE algorithm = :algorithm AND algorithm_version = :version
"""

# Read-only target checks, run before any write.
TARGET_SQL = """
SELECT current_database() AS database, current_user AS role,
       to_regclass('kbp.dedup_methods') IS NOT NULL AS has_table,
       (SELECT version_num FROM kbp.alembic_version) AS revision
"""
EXPECTED_ROLE = "kb_pipeline_migrator"
EXPECTED_REVISION = EXPECTED_KBP_REVISION  # one source of truth: the runtime's current kbp head


class BootstrapTargetError(RuntimeError):
    pass


def verify_target(conn: Any, expect_database: str) -> dict[str, Any]:
    """Refuse unless connected to exactly ``expect_database`` as the migrator at revision kbp_001c."""
    from sqlalchemy import text

    row = dict(conn.execute(text(TARGET_SQL)).mappings().one())
    if row["database"] != expect_database:
        raise BootstrapTargetError(f"connected to database {row['database']!r}, expected {expect_database!r}")
    if row["role"] != EXPECTED_ROLE:
        raise BootstrapTargetError(f"connected as {row['role']!r}; register-methods must run as {EXPECTED_ROLE}")
    if not row["has_table"] or row["revision"] != EXPECTED_REVISION:
        raise BootstrapTargetError(f"kbp schema is not at {EXPECTED_REVISION} (revision {row['revision']!r})")
    return row


def register_methods(conn: Any, *, expect_database: str) -> dict[str, Any]:
    """Idempotently register the near-duplicate method; verify frozen parameters. Returns the row."""
    from sqlalchemy import text

    verify_target(conn, expect_database)
    params = {"algorithm": near.ALGORITHM, "version": near.VERSION,
              "parameters": json.dumps(near.PARAMETERS, sort_keys=True)}
    conn.execute(text(REGISTER_METHOD_SQL), params)
    row = conn.execute(text(GET_METHOD_SQL), params).one()
    stored = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    if stored != near.PARAMETERS:
        raise RuntimeError(f"dedup method {near.ALGORITHM}:{near.VERSION} exists with different parameters")
    return {"database": expect_database, "dedup_method_id": int(row[0]), "algorithm": near.ALGORITHM,
            "algorithm_version": near.VERSION}

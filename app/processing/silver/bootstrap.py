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

from app.processing.dedup import near

REGISTER_METHOD_SQL = """
INSERT INTO kbp.dedup_methods (algorithm, algorithm_version, parameters)
VALUES (:algorithm, :version, CAST(:parameters AS jsonb))
ON CONFLICT (algorithm, algorithm_version) DO NOTHING
"""

GET_METHOD_SQL = """
SELECT dedup_method_id, parameters FROM kbp.dedup_methods WHERE algorithm = :algorithm AND algorithm_version = :version
"""


def register_methods(conn: Any) -> dict[str, Any]:
    """Idempotently register the near-duplicate method; verify frozen parameters. Returns the row."""
    from sqlalchemy import text

    params = {"algorithm": near.ALGORITHM, "version": near.VERSION,
              "parameters": json.dumps(near.PARAMETERS, sort_keys=True)}
    conn.execute(text(REGISTER_METHOD_SQL), params)
    row = conn.execute(text(GET_METHOD_SQL), params).one()
    stored = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    if stored != near.PARAMETERS:
        raise RuntimeError(f"dedup method {near.ALGORITHM}:{near.VERSION} exists with different parameters")
    return {"dedup_method_id": int(row[0]), "algorithm": near.ALGORITHM, "algorithm_version": near.VERSION}

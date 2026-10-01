"""Migrator-only bootstrap: register ``chunk_minhash_lsh`` / ``oph-w3-b10r4-j70-v1`` in ``kbp.dedup_methods``.

``kb_pipeline_app`` has SELECT only on ``kbp.dedup_methods``, so registration
is an idempotent operator step run with KB_MIGRATION_DATABASE_URL
(``kb_pipeline_migrator``). It is refused unless the connection is on exactly
``--expect-database``, as the migrator, at revision kbp_001c. An existing row
with different frozen parameters is refused, never overwritten: a changed
parameter must be a new version.
"""

from __future__ import annotations

import json
from typing import Any

from app.processing.chunk_dedup import method
from app.processing.silver.bootstrap import GET_METHOD_SQL, REGISTER_METHOD_SQL, verify_target


def register_method(conn: Any, *, expect_database: str) -> dict[str, Any]:
    from sqlalchemy import text

    verify_target(conn, expect_database)
    params = {"algorithm": method.ALGORITHM, "version": method.VERSION,
              "parameters": json.dumps(method.PARAMETERS, sort_keys=True)}
    conn.execute(text(REGISTER_METHOD_SQL), params)
    row = conn.execute(text(GET_METHOD_SQL), params).one()
    stored = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    if stored != method.PARAMETERS:
        raise RuntimeError(f"dedup method {method.ALGORITHM}:{method.VERSION} exists with different parameters")
    return {"database": expect_database, "dedup_method_id": int(row[0]), "algorithm": method.ALGORITHM,
            "algorithm_version": method.VERSION, "parameters_sha256": method.parameters_sha256()}

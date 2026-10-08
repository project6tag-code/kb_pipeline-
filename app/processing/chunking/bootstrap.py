"""Migrator-only bootstrap: register the frozen Chunking V1 configuration in ``kbp.chunking_versions``.

``kb_pipeline_app`` has SELECT only on ``kbp.chunking_versions``
(docs/database_privileges_v1.md §5.1). Registration is an idempotent operator
step run with KB_MIGRATION_DATABASE_URL (``kb_pipeline_migrator``), refused
unless the connection is on exactly ``--expect-database`` at revision kbp_001e.
The version ``name`` is derived from a hash of every output-affecting parameter,
so a changed configuration is a NEW version; an existing name with different
stored parameters is refused, never overwritten.
"""

from __future__ import annotations

import json
from typing import Any

from app.processing.chunking import tokenizer
from app.processing.chunking.chunker import ChunkingConfig
from app.processing.silver.bootstrap import BootstrapTargetError, verify_target

REGISTER_VERSION_SQL = """
INSERT INTO kbp.chunking_versions (name, tokenizer, parameters)
VALUES (:name, :tokenizer, CAST(:parameters AS jsonb))
ON CONFLICT (name) DO NOTHING
"""

GET_VERSION_SQL = """
SELECT chunking_version_id, tokenizer, parameters FROM kbp.chunking_versions WHERE name = :name
"""


def register_version(conn: Any, config: ChunkingConfig, *, expect_database: str) -> dict[str, Any]:
    from sqlalchemy import text

    verify_target(conn, expect_database)
    if conn.execute(text("SELECT to_regclass('kbp.chunking_versions') IS NOT NULL")).scalar() is not True:
        raise BootstrapTargetError("kbp.chunking_versions does not exist")
    params = {"name": config.name, "tokenizer": tokenizer.IDENTITY,
              "parameters": json.dumps(config.parameters(), sort_keys=True)}
    conn.execute(text(REGISTER_VERSION_SQL), params)
    row = conn.execute(text(GET_VERSION_SQL), params).one()
    stored = row[2] if isinstance(row[2], dict) else json.loads(row[2])
    if stored != config.parameters() or row[1] != tokenizer.IDENTITY:
        raise RuntimeError(f"chunking version {config.name} exists with different parameters")
    return {"database": expect_database, "chunking_version_id": int(row[0]), "name": config.name,
            "tokenizer": tokenizer.IDENTITY}

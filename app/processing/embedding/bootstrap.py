"""Migrator-only bootstrap: register the frozen Embeddings V1 model in ``kbp.embedding_models``.

``kb_pipeline_app`` has SELECT only on ``kbp.embedding_models``, so registration
is an idempotent operator step run with KB_MIGRATION_DATABASE_URL
(``kb_pipeline_migrator``). It is refused unless the connection is on exactly
``--expect-database``, as the migrator, at the current kbp revision
(``kbp_001d``).

The row is registered as ``candidate`` (schema §12.4 step 1): never
``active_gold``, and Gold is not started. An existing row with a different
frozen config is refused, never overwritten: a change needs a new
``EMBEDDING_VERSION``.
"""

from __future__ import annotations

import json
from typing import Any

from app.processing.embedding import model as spec
from app.processing.silver.bootstrap import verify_target

REGISTER_MODEL_SQL = """
INSERT INTO kbp.embedding_models (model_name, model_revision, embedding_version, embedding_dimension, vector_type,
                                  distance_metric, is_normalized, max_input_tokens, config, status)
VALUES (:model_name, :model_revision, :embedding_version, :embedding_dimension, :vector_type, :distance_metric,
        :is_normalized, :max_input_tokens, CAST(:config AS jsonb), :status)
ON CONFLICT (model_name, model_revision, embedding_version) DO NOTHING
"""

GET_REGISTERED_SQL = """
SELECT embedding_model_id, embedding_dimension, vector_type, distance_metric, is_normalized, max_input_tokens,
       config, status
FROM kbp.embedding_models
WHERE model_name = :model_name AND model_revision = :model_revision AND embedding_version = :embedding_version
"""


class RegistrationRefused(RuntimeError):
    pass


def register_model(conn: Any, *, expect_database: str) -> dict[str, Any]:
    from sqlalchemy import text

    if "PENDING" in (spec.MODEL_REVISION, spec.MODEL_WEIGHTS_SHA256) or "PENDING" in spec.PINNED_PACKAGES.values():
        raise RegistrationRefused("the V1 model spec is not frozen yet (revision, weights hash or packages pending)")
    verify_target(conn, expect_database)
    row = spec.registration_row()
    params = {**row, "config": json.dumps(row["config"], sort_keys=True)}
    conn.execute(text(REGISTER_MODEL_SQL), params)
    stored = conn.execute(text(GET_REGISTERED_SQL), params).mappings().one()
    comparable = {k: stored[k] for k in ("embedding_dimension", "vector_type", "distance_metric", "is_normalized",
                                         "max_input_tokens")}
    comparable["config"] = stored["config"] if isinstance(stored["config"], dict) else json.loads(stored["config"])
    if comparable != {k: row[k] for k in comparable}:
        raise RegistrationRefused(f"{spec.MODEL_NAME}@{spec.MODEL_REVISION} ({spec.EMBEDDING_VERSION}) exists with a "
                                  "different frozen config")
    return {"database": expect_database, "embedding_model_id": int(stored["embedding_model_id"]),
            "model_name": spec.MODEL_NAME, "model_revision": spec.MODEL_REVISION,
            "embedding_version": spec.EMBEDDING_VERSION, "status": stored["status"],
            "config_sha256": spec.config_sha256()}

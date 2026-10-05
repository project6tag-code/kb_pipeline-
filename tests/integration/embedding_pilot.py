"""Embeddings V1 PostgreSQL pilot against ``kb_pipeline_test`` ONLY (shadow vectors; no Gold).

    set KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m tests.integration.embedding_pilot run        # counts before -> enqueue -> worker -> audit
    python -m tests.integration.embedding_pilot state      # read-only audit

Prerequisites (once):
    python -m app.processing.embedding.cli download                                   (online, model cache)
    python -m app.processing.embedding.cli register-model --expect-database kb_pipeline_test   (migrator)

The pilot embeds every currently eligible representative chunk. Suppressed
near-duplicates are never embedded; they resolve to their representative. A
rerun adds nothing. It refuses to run unless connected as kb_pipeline_app to
exactly kb_pipeline_test at the current kbp revision (kbp_001d).
"""

from __future__ import annotations

import json
import sys
from typing import Any

from tests.integration.silver_pilot import open_database


def counts(db) -> dict[str, Any]:
    from sqlalchemy import text

    with db.transaction() as conn:
        one = lambda sql: conn.execute(text(sql)).scalar_one()  # noqa: E731
        return {
            "embedding_models": one("SELECT count(*) FROM kbp.embedding_models"),
            "embedding_queue": one("SELECT count(*) FROM kbp.embedding_queue"),
            "chunk_embeddings": one("SELECT count(*) FROM kbp.chunk_embeddings"),
            "chunk_embedding_vectors": one("SELECT count(*) FROM kbp.chunk_embedding_vectors"),
            "vector_hashes_sha256": one("SELECT encode(sha256(string_agg(chunk_id::text || ':' || "
                                        "encode(vector_sha256, 'hex'), ',' ORDER BY chunk_id)::bytea), 'hex') "
                                        "FROM kbp.chunk_embedding_vectors"),
            "chunk_registry": one("SELECT count(*) FROM kbp.chunk_registry"),
            "chunk_occurrences": one("SELECT count(*) FROM kbp.chunk_occurrences"),
            "chunk_dedup_decisions": one("SELECT count(*) FROM kbp.chunk_dedup_decisions"),
            "representative_map_sha256": one(
                "SELECT encode(sha256(coalesce(string_agg(chunk_id::text || '>' || representative_chunk_id::text, ',' "
                "ORDER BY chunk_id), '')::bytea), 'hex') FROM kbp.chunk_dedup_decisions "
                "WHERE classification = 'NEAR_DUPLICATE'"),
            "documents": one("SELECT count(*) FROM kbp.document_registry"),
            "embedding_dead_letters": one("SELECT count(*) FROM kbp.dead_letters WHERE item_type = 'chunk_embedding'"),
        }


def audit(db) -> dict[str, Any]:
    from sqlalchemy import text

    from app.processing.embedding.cli import status_report

    with db.transaction() as conn:
        report = status_report(conn)
        report["result_vector_pairs"] = conn.execute(text(
            "SELECT count(*) FROM kbp.chunk_embeddings e JOIN kbp.chunk_embedding_vectors v "
            "USING (chunk_id, embedding_model_id)")).scalar_one()
        report["vector_without_result"] = conn.execute(text(
            "SELECT count(*) FROM kbp.chunk_embedding_vectors v WHERE NOT EXISTS (SELECT 1 FROM kbp.chunk_embeddings e "
            "WHERE e.chunk_id = v.chunk_id AND e.embedding_model_id = v.embedding_model_id)")).scalar_one()
        report["non_shadow_results"] = conn.execute(text(
            "SELECT count(*) FROM kbp.chunk_embeddings WHERE status <> 'stored_shadow' OR published_at IS NOT NULL"
        )).scalar_one()
        report["model_rows"] = [dict(r) for r in conn.execute(text(
            "SELECT embedding_model_id, model_name, model_revision, embedding_version, embedding_dimension, status "
            "FROM kbp.embedding_models")).mappings()]
    problems = []
    if report["eligible_without_embedding"]:
        problems.append(f"{report['eligible_without_embedding']} eligible chunks have no embedding")
    for key in ("result_without_vector", "vector_without_result", "vector_hash_mismatch", "vector_wrong_dimension",
                "vector_norm_out_of_tolerance", "embedded_suppressed_near_duplicates", "non_shadow_results",
                "open_dead_letters"):
        if report[key]:
            problems.append(f"{key} = {report[key]}")
    if report["queue_by_status"]:
        problems.append(f"queue not empty: {report['queue_by_status']}")
    if report["chunk_embeddings"] != report["result_vector_pairs"] or report["vectors"] != report["chunk_embeddings"]:
        problems.append("chunk_embeddings and chunk_embedding_vectors are not 1:1")
    if any(not r["representative_has_vector"] or r["status"] != "suppressed" for r in report["suppressed_resolution"]):
        problems.append("a suppressed chunk does not resolve to an embedded representative")
    if len(report["model_rows"]) != 1:
        problems.append(f"expected exactly one model row, got {len(report['model_rows'])}")
    report["problems"] = problems
    return report


def run(db, *, device: str = "auto", encode_batch_size: int = 16, claim_batch: int = 32) -> dict[str, Any]:
    from app.processing.chunk_dedup import eligibility
    from app.processing.embedding.cli import _ids, run_worker
    from app.processing.embedding.repository import PostgresEmbeddingStore
    from app.processing.embedding.service import enqueue_eligible

    with db.transaction() as conn:
        model_id, method_id = _ids(conn)
        eligible = len(eligibility.eligible_chunks(conn, method_id, limit=10_000_000))
        suppressed = len(eligibility.suppressed_chunks(conn, method_id, limit=10_000_000))
    enqueue = enqueue_eligible(db.transaction, PostgresEmbeddingStore(), model_id=model_id,
                               dedup_method_id=method_id, limit=100_000, batch_size=500)
    worker = run_worker(db, limit=100_000, claim_batch=claim_batch, encode_batch_size=encode_batch_size,
                        device=device)
    return {"eligible_before": eligible, "suppressed_before": suppressed, "enqueue": vars(enqueue), "worker": worker}


def main(argv: list[str]) -> int:
    command = argv[0] if argv else "state"
    db = open_database()
    try:
        if command == "state":
            print(json.dumps({"counts": counts(db), "audit": audit(db)}, indent=2, default=str))
            return 0
        before = counts(db)
        result = run(db)
        after = counts(db)
        report = audit(db)
        print(json.dumps({"before": before, "run": result, "after": after, "audit": report}, indent=2, default=str))
        return 0 if not report["problems"] else 1
    finally:
        db.dispose()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

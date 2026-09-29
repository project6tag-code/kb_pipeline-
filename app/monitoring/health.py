"""Read-only runtime health and queue summary (role kb_pipeline_app)."""

from __future__ import annotations

from typing import Any

from app.db.connection import Database

QUEUE_SUMMARY_SQL = """
SELECT job_type, status, count(*) AS jobs, min(next_attempt_at) AS oldest_next_attempt_at
FROM kbp.pipeline_jobs
GROUP BY job_type, status
ORDER BY job_type, status
"""

EXPIRED_LEASES_COUNT_SQL = """
SELECT count(*) FROM kbp.pipeline_jobs WHERE status = 'running' AND lease_expires_at < now()
"""

OPEN_DEAD_LETTERS_COUNT_SQL = "SELECT count(*) FROM kbp.dead_letters WHERE resolved_at IS NULL"


def runtime_health(db: Database) -> dict[str, Any]:
    """Identity/revision check plus queue counters. Raises if the identity is wrong."""
    from sqlalchemy import text

    report = db.verify_runtime_identity()
    with db.engine.connect() as conn:
        summary = [dict(r) for r in conn.execute(text(QUEUE_SUMMARY_SQL)).mappings()]
        expired = conn.execute(text(EXPIRED_LEASES_COUNT_SQL)).scalar_one()
        open_dead_letters = conn.execute(text(OPEN_DEAD_LETTERS_COUNT_SQL)).scalar_one()
        conn.rollback()
    return {
        "role": report.role,
        "kbp_revision": report.kbp_revision,
        "server_time": report.server_time,
        "queue": summary,
        "expired_running_leases": int(expired),
        "open_dead_letters": int(open_dead_letters),
    }

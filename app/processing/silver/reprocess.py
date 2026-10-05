"""Operator Silver reprocess: run the existing Silver processor over already-acquired observations.

Bronze evidence is read, never fetched (no network). Nothing is deleted or overwritten.

* Default (safe): only observations with NO extraction under the current ``extraction_version`` are
  evaluated, i.e. acquired-but-never-processed observations. Re-running it is a no-op once they are done.
* ``force=True`` (``--force-reprocess``): every selected observation is re-evaluated from Bronze and handed to
  the processor's own rerun path, which verifies the stored outcome/document id. A mismatch is dead-lettered
  as ``silver_nondeterministic_extraction``; matching observations add no rows. Changing the extraction
  version (config/code) is what produces new extraction rows; ``--force-reprocess`` never duplicates one.

Selection is keyset-paged by ``observation_id`` in bounded batches, one transaction per batch.
"""

from __future__ import annotations

import socket
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from app.processing.silver.model import SourceRef
from app.processing.silver.processor import Prepared

MAX_BATCH = 500
SOURCES = ("cc", "github", "all")

CC_SELECT = """
SELECT o.observation_id, o.source_url, w.crawl_id, w.warc_filename, c.warc_record_offset, c.warc_record_length
FROM kbp.source_observations o
JOIN kbp.cc_captures c USING (observation_id)
JOIN kbp.cc_warc_files w USING (warc_file_id)
WHERE o.observation_id > CAST(:after AS uuid) {where}
ORDER BY o.observation_id LIMIT :limit
"""
GH_SELECT = """
SELECT o.observation_id, o.source_url, g.repository_id, g.commit_sha, g.file_path, g.blob_sha
FROM kbp.source_observations o
JOIN kbp.github_file_observations g USING (observation_id)
WHERE o.observation_id > CAST(:after AS uuid) {where}
ORDER BY o.observation_id LIMIT :limit
"""
NOT_EXTRACTED = ("AND NOT EXISTS (SELECT 1 FROM kbp.observation_extractions e "
                 "WHERE e.observation_id = o.observation_id AND e.extraction_version = :version)")
MIN_UUID = str(uuid.UUID(int=0))


class ReprocessError(RuntimeError):
    """Invalid reprocess request or target."""


@dataclass(frozen=True)
class Scope:
    source: str = "all"                      # cc | github | all
    force: bool = False
    observation_ids: tuple[uuid.UUID, ...] = ()
    crawl_id: str | None = None              # Common Crawl only
    repository_id: int | None = None         # GitHub only
    batch_size: int = 50
    limit: int | None = None                 # total observations to evaluate (None = all selected)

    def __post_init__(self) -> None:
        if self.source not in SOURCES:
            raise ReprocessError(f"source must be one of {SOURCES}")
        if not 1 <= self.batch_size <= MAX_BATCH:
            raise ReprocessError(f"batch size must be 1..{MAX_BATCH}")
        if self.limit is not None and self.limit < 1:
            raise ReprocessError("limit must be >= 1")
        if self.crawl_id is not None and self.source == "github":
            raise ReprocessError("--crawl-id only applies to Common Crawl")
        if self.repository_id is not None and self.source == "cc":
            raise ReprocessError("--repository-id only applies to GitHub")

    def channels(self) -> list[str]:
        out = []
        if self.source in ("cc", "all") and self.repository_id is None:
            out.append("cc")
        if self.source in ("github", "all") and self.crawl_id is None:
            out.append("github")
        return out


def build_query(channel: str, scope: Scope, version: str, after: str, limit: int) -> tuple[str, dict[str, Any]]:
    """Pure: the SELECT for one page and its bind parameters."""
    where: list[str] = []
    params: dict[str, Any] = {"after": after, "limit": limit}
    if not scope.force:
        where.append(NOT_EXTRACTED)
        params["version"] = version
    if scope.observation_ids:
        where.append("AND o.observation_id = ANY(CAST(:ids AS uuid[]))")
        params["ids"] = [str(i) for i in scope.observation_ids]
    if channel == "cc" and scope.crawl_id is not None:
        where.append("AND w.crawl_id = :crawl_id")
        params["crawl_id"] = scope.crawl_id
    if channel == "github" and scope.repository_id is not None:
        where.append("AND g.repository_id = :repository_id")
        params["repository_id"] = scope.repository_id
    template = CC_SELECT if channel == "cc" else GH_SELECT
    return template.format(where=" ".join(where)), params


def to_ref(channel: str, row: Sequence[Any]) -> SourceRef:
    if channel == "cc":
        return SourceRef(row[0], "common_crawl", row[1], crawl_id=row[2], warc_filename=row[3],
                         warc_offset=row[4], warc_length=row[5])
    return SourceRef(row[0], "github", row[1], repository_id=row[2], commit_sha=row[3], path=row[4], blob_sha=row[5])


def select_page(conn: Any, channel: str, scope: Scope, version: str, after: str, limit: int) -> list[SourceRef]:
    from sqlalchemy import text

    sql, params = build_query(channel, scope, version, after, limit)
    return [to_ref(channel, row) for row in conn.execute(text(sql), params)]


def verify_database(conn: Any, expect_database: str) -> str:
    from sqlalchemy import text

    actual = conn.execute(text("SELECT current_database()")).scalar_one()
    if actual != expect_database or actual == "data_collection_pipeline":
        raise ReprocessError(f"connected to {actual!r}; --expect-database is {expect_database!r}")
    return actual


def reprocess(db: Any, processor: Any, scope: Scope, *, should_stop: Callable[[], bool] = lambda: False,
              page_selector: Callable[..., list[SourceRef]] = select_page) -> dict[str, Any]:
    """Run the reprocess; returns an operator summary. ``db.transaction()`` yields a connection context."""
    totals: Counter = Counter()
    remaining = scope.limit
    for channel in scope.channels():
        after = MIN_UUID
        while not should_stop():
            size = scope.batch_size if remaining is None else min(scope.batch_size, remaining)
            if size <= 0:
                break
            with db.transaction() as conn:
                refs = page_selector(conn, channel, scope, processor.extraction_version, after, size)
            if not refs:
                break
            after = str(refs[-1].observation_id)
            evaluations = [processor.evaluator.evaluate(ref) for ref in refs]   # Bronze reads only
            with db.transaction() as conn:
                counts = processor.persist(conn, Prepared(evaluations=evaluations), job_id=None, attempt_count=1)
            totals.update(counts)
            totals[f"evaluated_{channel}"] += len(refs)
            totals["evaluated"] += len(refs)
            if remaining is not None:
                remaining -= len(refs)
    return {"extraction_version": processor.extraction_version, "force": scope.force,
            "source": scope.source, "stopped_early": bool(should_stop()), "counts": dict(sorted(totals.items()))}


def run_cli(expect_database: str, scope: Scope) -> dict[str, Any]:
    """Wire the real database, Bronze stores and processor; used by ``silver.cli reprocess``."""
    from app.config.settings import DatabaseSettings
    from app.db import repositories
    from app.db.connection import Database
    from app.processing.silver.config import SilverConfig
    from app.queue.states import RunKind, RunStatus
    from app.sources.commoncrawl.config import CCSettings
    from app.sources.commoncrawl.connector import build_bronze
    from app.sources.github.bronze import GitHubBronze
    from app.sources.github.config import GitHubSettings
    from app.workers.processing_worker import build_silver_processor

    db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:silver_reprocess"))
    try:
        db.verify_runtime_identity()
        with db.transaction() as conn:
            verify_database(conn, expect_database)
            run = repositories.create_run(conn, RunKind.REPROCESS, code_version="silver_reprocess_v1",
                                          host=socket.gethostname(),
                                          notes=f"silver reprocess source={scope.source} force={scope.force}")
        processor = build_silver_processor(SilverConfig.from_env(), cc_bronze=build_bronze(CCSettings.from_env()),
                                           gh_bronze=GitHubBronze(GitHubSettings.from_env().acquisition.bronze_root),
                                           run_id=run.run_id)
        status, summary = RunStatus.FAILED, {}
        try:
            summary = reprocess(db, processor, scope)
            status = RunStatus.SUCCEEDED
            return {"run_id": str(run.run_id), **summary}
        finally:
            with db.transaction() as conn:
                repositories.finish_run(conn, run.run_id, status, notes=str(summary.get("counts", ""))[:3900])
    finally:
        db.dispose()

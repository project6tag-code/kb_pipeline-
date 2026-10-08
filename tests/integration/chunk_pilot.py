"""Tiny, deterministic Chunking V1 pilot against PostgreSQL ``kb_pipeline_test`` ONLY.

    set KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m tests.integration.chunk_pilot run      # before counts -> inline pilot + backfill -> after counts
    python -m tests.integration.chunk_pilot counts   # read-only counts and invariant checks

Two execution paths, both on PostgreSQL:

* inline (schema §10.3): a CC page and a GitHub README that share one identical
  section run through the Runtime Engine worker, the real acquisition handlers
  and the Silver stage with the Chunking V1 writer attached, so each accepted
  document is chunked in the same transaction as its registration;
* backfill: ``run_backfill`` chunks accepted documents that have no occurrences
  for this chunking version (the Silver pilot documents).

Identities are synthetic, fixed and rerun-safe (rows are permanent): crawl
``CC-MAIN-1999-00`` WARC ``chunk-pilot-v1`` / CDX ``cdx-99001.gz``, repository
900000000002 ``chunk-pilot/glaciology``. Refuses anything but kb_pipeline_test.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from tests.integration.silver_pilot import PILOT_CRAWL, open_database

PILOT_WARC_LEAF = "chunk-pilot-v1.warc.gz"
PILOT_CDX = "cdx-99001.gz"
PILOT_REPO = 900_000_000_002
PILOT_ROOT = Path("data/silver/chunk-pilot")

SHARED = ("Glaciers form where the accumulation of snow exceeds its ablation over many years. The snow compacts "
          "into firn and then into dense glacial ice. A glacier is a persistent body of ice that moves under its "
          "own weight. Glacial ice covers about 10 percent of the land surface of the Earth today.")
WEB_ONLY = ("Ice sheets are glaciers that cover more than 50,000 square kilometres of land. The two modern ice "
            "sheets cover most of Greenland and Antarctica. The Antarctic ice sheet contains about 26.5 million "
            "cubic kilometres of ice, which is roughly 90 percent of the fresh water on the surface of the Earth.")
REPO_ONLY = ("Glacier mass balance is the difference between accumulation and ablation over one year. It is "
             "measured with stakes, snow pits and satellite altimetry. A negative balance means the glacier is "
             "losing mass, and most mountain glaciers have had a negative balance since the 1980s.")
SHARED_CHUNK = f"## Formation\n\n{SHARED}"


def build_sources():
    from tests.unit.cc_fixtures import FakeCommonCrawl, make_cdx_line, make_warc_record
    from tests.unit.gh_fixtures import FakeGitHub

    url = "https://www.chunk-pilot.example/glaciers"
    page = (f"<html><head><title>Glaciers</title></head><body><main><article><h2>Formation</h2><p>{SHARED}</p>"
            f"<h2>Ice sheets</h2><p>{WEB_ONLY}</p></article></main></body></html>").encode()
    site = FakeCommonCrawl()
    name, spans = site.add_warc([make_warc_record(url, page)], leaf=PILOT_WARC_LEAF, crawl=PILOT_CRAWL)
    site.add_index([[make_cdx_line(url, name, *spans[0], urlkey="example,chunk-pilot)/glaciers")]],
                   cdx_file=PILOT_CDX, crawl=PILOT_CRAWL)
    gh = FakeGitHub()
    head = gh.add_repo(PILOT_REPO, "chunk-pilot", "glaciology",
                       {"README.md": f"## Formation\n\n{SHARED}\n\n## Mass balance\n\n{REPO_ONLY}\n"})
    return site, gh, head


def run_inline(db, root: Path = PILOT_ROOT, *, sources=None, cdx: str = PILOT_CDX, repo: int = PILOT_REPO,
               name: str = "chunk-pilot", iterations: int = 2) -> dict[str, Any]:
    """Acquire + Silver + inline chunking of the pilot documents through the Runtime Engine.

    ``sources`` (site, gh, head), ``cdx``, ``repo`` and ``name`` let other pilots reuse this exact path.
    """
    from app.config.settings import WorkerSettings
    from app.processing.chunking.repository import PostgresChunkStore
    from app.processing.chunking.service import ChunkWriter
    from app.processing.dedup.near import NearIndex
    from app.processing.silver.config import SilverConfig
    from app.processing.silver.processor import SilverProcessor
    from app.processing.silver.repository import PostgresSilverStore
    from app.queue.identity import gh_repository_snapshot
    from app.queue.job_queue import JobQueue, JobSpec
    from app.queue.states import JobSource, JobType
    from app.sources.commoncrawl.acquisition import CC_PARTITION_POLICY, CCPartitionHandler
    from app.sources.commoncrawl.bronze import BronzeStore
    from app.sources.commoncrawl.config import AcquisitionConfig, FilterConfig, PlanningConfig
    from app.sources.commoncrawl.discovery import CrawlPlanner
    from app.sources.commoncrawl.http import HttpClient
    from app.sources.commoncrawl.repository import PostgresCaptureStore
    from app.sources.github.bronze import GitHubBronze
    from app.sources.github.client import GitHubClient
    from app.sources.github.common import GH_POLICY
    from app.sources.github.config import GitHubAcquisitionConfig, GitHubSettings
    from app.sources.github.connector import register_github_handlers
    from app.sources.github.repository import PostgresGitHubStore
    from app.workers.registry import HandlerRegistry
    from app.workers.runtime import Worker
    from tests.unit.cc_fixtures import FakeTransport

    site, gh, head = sources or build_sources()
    root.mkdir(parents=True, exist_ok=True)
    queue = JobQueue(db)
    config = SilverConfig(near_index_path=root / "near_index.sqlite3")
    gh_settings = GitHubSettings(acquisition=GitHubAcquisitionConfig(bronze_root=root / "bronze-gh"))
    cc_bronze, gh_bronze = BronzeStore(root / "bronze-cc"), GitHubBronze(gh_settings.acquisition.bronze_root)
    index = NearIndex(config.near_index_path)
    report: dict[str, Any] = {}
    try:
        processor = SilverProcessor(config, PostgresSilverStore(), index, cc_bronze=cc_bronze, gh_bronze=gh_bronze,
                                    chunker=ChunkWriter(PostgresChunkStore()))
        if site is not None:  # a GitHub-only source set skips Common Crawl
            cc_client = HttpClient(transport=FakeTransport(site))
            cc_prefix = f"{PILOT_CRAWL}/cc-index/collections/{PILOT_CRAWL}/indexes/{cdx}"
            report["cc_partitions_created"] = CrawlPlanner(
                cc_client, queue, PostgresCaptureStore(), planning=PlanningConfig(blocks_per_partition=10),
                filter_config=FilterConfig(), policy=CC_PARTITION_POLICY,
                max_block_bytes=16 * 1024 * 1024).plan([PILOT_CRAWL]).crawls[0].created
            cc_registry = HandlerRegistry()
            cc_registry.register(JobType.CC_INDEX_PARTITION, CCPartitionHandler(
                client=cc_client, store=PostgresCaptureStore(), bronze=cc_bronze,
                config=AcquisitionConfig(bronze_root=root / "bronze-cc"), silver=processor),
                policy=CC_PARTITION_POLICY)
            report["cc_worker"] = vars(Worker(queue, cc_registry, settings=WorkerSettings(
                lease_seconds=120, poll_interval_seconds=0, source_identifier_prefix=cc_prefix),
                worker_id=f"{name}-cc").run(max_iterations=iterations))
            cc_client.close()
        gh_client = GitHubClient(gh_settings.http, gh_settings.credentials, transport=gh)
        gh_registry = HandlerRegistry()
        register_github_handlers(gh_registry, gh_settings, client=gh_client, store=PostgresGitHubStore(),
                                 bronze=gh_bronze, silver=processor)
        report["gh_job_created"] = queue.enqueue(JobSpec.for_policy(
            GH_POLICY, job_type=JobType.GH_REPOSITORY_SNAPSHOT, source=JobSource.GITHUB,
            source_identifier=gh_repository_snapshot(repo, head),
            checkpoint={"v": 1, "previous": None, "ref": "main"})).created
        report["gh_worker"] = vars(Worker(queue, gh_registry, settings=WorkerSettings(
            lease_seconds=120, poll_interval_seconds=0, source_identifier_prefix=f"{repo}@"),
            worker_id=f"{name}-gh").run(max_iterations=iterations))
        gh_client.close()
        return report
    finally:
        index.close()


def run_backfill_pilot(db, *, limit: int = 20, batch_size: int = 2) -> dict[str, Any]:
    from app.processing.chunking.repository import PostgresChunkStore
    from app.processing.chunking.service import ChunkWriter, run_backfill

    report = run_backfill(db.transaction, ChunkWriter(PostgresChunkStore()), limit=limit, batch_size=batch_size)
    return {"documents": report.documents, "batches": report.batches, "retries": report.retries,
            "counts": dict(sorted(report.counts.items()))}


# --------------------------------------------------------------------------- #
# Read-only counts and invariant checks (whole test database, this chunking version)
# --------------------------------------------------------------------------- #

def chunk_state(db) -> dict[str, Any]:
    from sqlalchemy import text

    from app.processing.chunking import tokenizer
    from app.processing.chunking.chunker import ChunkingConfig, chunk_document
    from app.processing.chunking import plain
    from app.processing.chunking.service import silver_kind
    from app.processing.normalization.text import canonicalize_text
    from app.utils.ids import chunk_id_from_hash

    config = ChunkingConfig()
    with db.transaction() as conn:
        one = lambda sql, **p: conn.execute(text(sql), p).scalar_one()  # noqa: E731
        version = conn.execute(text("SELECT chunking_version_id, name FROM kbp.chunking_versions WHERE name = :n"),
                               {"n": config.name}).one_or_none()
        v = version[0] if version else -1
        state: dict[str, Any] = {
            "chunking_version": {"id": version[0], "name": version[1]} if version else None,
            "chunking_versions_with_this_name": one("SELECT count(*) FROM kbp.chunking_versions WHERE name = :n",
                                                    n=config.name),
            "chunk_registry": one("SELECT count(*) FROM kbp.chunk_registry"),
            "chunk_registry_duplicate_hashes": one("SELECT count(*) - count(DISTINCT chunk_hash) FROM kbp.chunk_registry"),
            "chunk_occurrences": one("SELECT count(*) FROM kbp.chunk_occurrences WHERE chunking_version_id = :v", v=v),
            "chunked_documents": one("SELECT count(DISTINCT document_id) FROM kbp.chunk_occurrences "
                                     "WHERE chunking_version_id = :v", v=v),
            "accepted_documents": one("SELECT count(*) FROM kbp.document_registry WHERE status = 'accepted'"),
            "accepted_not_chunked": one(
                "SELECT count(*) FROM kbp.document_registry d WHERE d.status = 'accepted' AND NOT EXISTS "
                "(SELECT 1 FROM kbp.chunk_occurrences o WHERE o.document_id = d.document_id "
                "AND o.chunking_version_id = :v)", v=v),
            "orphan_occurrences": one(
                "SELECT count(*) FROM kbp.chunk_occurrences o LEFT JOIN kbp.chunk_registry c USING (chunk_id) "
                "LEFT JOIN kbp.document_registry d ON d.document_id = o.document_id "
                "WHERE c.chunk_id IS NULL OR d.document_id IS NULL"),
            "chunks_without_occurrence": one(
                "SELECT count(*) FROM kbp.chunk_registry c WHERE NOT EXISTS "
                "(SELECT 1 FROM kbp.chunk_occurrences o WHERE o.chunk_id = c.chunk_id)"),
            "shared_chunks": one("SELECT count(*) FROM (SELECT chunk_id FROM kbp.chunk_occurrences "
                                 "WHERE chunking_version_id = :v GROUP BY chunk_id "
                                 "HAVING count(DISTINCT document_id) > 1) s", v=v),
            "chunking_dead_letters": one("SELECT count(*) FROM kbp.dead_letters WHERE item_type = 'document' "
                                         "AND stage = 'SILVER' AND error_code LIKE 'chunking_%'"),
        }
        tokens = conn.execute(text("SELECT min(token_count), round(avg(token_count)), max(token_count) "
                                   "FROM kbp.chunk_registry")).one()
        state["token_count_min_avg_max"] = [tokens[0], None if tokens[1] is None else int(tokens[1]), tokens[2]]
        rows = conn.execute(text(
            "SELECT o.document_id, o.ordinal, o.chunk_id, o.char_start, o.char_end, o.section_path, "
            "c.chunk_text, c.chunk_hash, c.token_count, d.canonical_text, d.status_reason "
            "FROM kbp.chunk_occurrences o JOIN kbp.chunk_registry c USING (chunk_id) "
            "JOIN kbp.document_registry d ON d.document_id = o.document_id "
            "WHERE o.chunking_version_id = :v ORDER BY o.document_id, o.ordinal"), {"v": v}).all()
    problems: list[str] = []
    by_document: dict[Any, list] = {}
    for r in rows:
        (doc_id, ordinal, chunk_id, start, end, path, chunk_text, chunk_hash, token_count, doc_text, reason) = r
        by_document.setdefault(doc_id, []).append(r)
        if canonicalize_text(plain.render(doc_text[start:end], silver_kind(reason))) != chunk_text:  # c2 invariant
            problems.append(f"offset {doc_id}/{ordinal}")
        if hashlib.sha256(chunk_text.encode("utf-8")).digest() != bytes(chunk_hash):
            problems.append(f"hash {chunk_id}")
        if chunk_id_from_hash(bytes(chunk_hash).hex()) != chunk_id:
            problems.append(f"id {chunk_id}")
        if token_count != tokenizer.count_tokens(chunk_text) or token_count > config.max_tokens:
            problems.append(f"tokens {chunk_id}")
    ordered = {}
    for doc_id, occ in by_document.items():
        recomputed = [(c.ordinal, c.char_start, c.char_end, c.chunk_text) for c in
                      chunk_document(occ[0][9], kind=silver_kind(occ[0][10]), config=config) if c.rejected is None]
        stored = [(r[1], r[3], r[4], r[6]) for r in occ]
        if recomputed != stored:
            problems.append(f"order {doc_id}")
        ordered[str(doc_id)] = [str(r[2]) for r in occ]
    state["invariant_problems"] = problems
    state["ordered_chunk_ids"] = ordered
    return state


def main(argv: list[str]) -> int:
    command = argv[0] if argv else "counts"
    db = open_database()
    try:
        before = chunk_state(db)
        if command != "run":
            print(json.dumps(before, indent=2, default=str))
            return 0
        inline = run_inline(db)
        backfill = run_backfill_pilot(db)
        after = chunk_state(db)
        print(json.dumps({"before": before, "inline": inline, "backfill": backfill, "after": after}, indent=2,
                         default=str))
        return 0
    finally:
        db.dispose()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

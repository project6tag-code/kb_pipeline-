"""Tiny, deterministic Bronze -> Silver pilot against PostgreSQL ``kb_pipeline_test`` ONLY.

    set KB_INTEGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    python -m tests.integration.silver_pilot run      # before counts -> pilot -> reprocess -> after counts (JSON)
    python -m tests.integration.silver_pilot counts   # pilot-scoped counts only (read-only)

The production path runs unchanged: planner / enqueue -> Runtime Engine worker
(``JobQueue`` on PostgreSQL) -> real acquisition handlers with the Silver
stage attached -> Postgres stores. Only HTTP is faked (in-memory Common Crawl
and GitHub transports): no network. Runtime writes are permanent
(``kb_pipeline_app`` cannot DELETE), so every identity is synthetic, fixed and
rerun-safe:

* crawl ``CC-MAIN-1999-00`` (week 00 does not exist), WARC ``silver-pilot-v1``,
  CDX ``cdx-99000.gz``, URLs under ``silver-pilot.example``;
* GitHub repository id 900000000001 ``silver-pilot/fixtures``;
* worker claims are restricted to these units (``source_identifier`` prefix).

Local Bronze and the near-duplicate index live in ``data/silver/pilot`` (git-ignored).
Refuses to run unless connected as kb_pipeline_app to exactly kb_pipeline_test at kbp_001a.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

REQUIRED_DATABASE = "kb_pipeline_test"
REQUIRED_ROLE = "kb_pipeline_app"
PILOT_CRAWL = "CC-MAIN-1999-00"
PILOT_WARC_LEAF = "silver-pilot-v1.warc.gz"
PILOT_CDX = "cdx-99000.gz"
PILOT_REPO = 900_000_000_001
PILOT_ROOT = Path("data/silver/pilot")

KEY_BODY = base64.b64encode(b"".join(hashlib.sha256(b"pilot" + bytes([i])).digest() for i in range(12))).decode()
PRIVATE_KEY = ("-----BEGIN RSA PRIVATE KEY-----\n" + "\n".join(KEY_BODY[i:i + 64] for i in range(0, 384, 64))
               + "\n-----END RSA PRIVATE KEY-----")
SSN = "123-45-6789"
FRENCH = ("La photosynthèse est le processus par lequel les plantes vertes convertissent l'énergie lumineuse en "
          "énergie chimique. Elle se déroule principalement dans les chloroplastes des cellules des feuilles, où "
          "la chlorophylle absorbe la lumière. Les réactions produisent de l'oxygène et des sucres pour la plante.")
SENSITIVE_FRAGMENTS = (KEY_BODY[:48], SSN)


class PilotRefused(RuntimeError):
    pass


# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #

def open_database(url: str | None = None):
    """Runtime connection to kb_pipeline_test, verified before any write."""
    from sqlalchemy import text

    from app.config.settings import DatabaseSettings
    from app.db.connection import Database
    from tests.unit.test_migration_001a import MODULE as MIGRATION_001A

    url = url or os.environ.get("KB_INTEGRATION_DATABASE_URL", "")
    if not url:
        raise PilotRefused("KB_INTEGRATION_DATABASE_URL is not set")
    db = Database.from_settings(DatabaseSettings(url=url, pool_size=2, application_name="kb_pipeline:silver_pilot"))
    try:
        with db.engine.connect() as conn:
            name, role = conn.execute(text("SELECT current_database(), current_user")).one()
            conn.rollback()
        if name != REQUIRED_DATABASE:
            raise PilotRefused(f"connected to {name!r}; the Silver pilot only runs on {REQUIRED_DATABASE!r}")
        if role != REQUIRED_ROLE:
            raise PilotRefused(f"connected as {role!r}; the Silver pilot runs as {REQUIRED_ROLE}")
        db.verify_runtime_identity()  # role, kbp revision kbp_001a, UTC
        with db.transaction() as conn:
            missing = [t for t in MIGRATION_001A.KBP_TABLES
                       if conn.execute(text("SELECT to_regclass(:t)"), {"t": f"kbp.{t}"}).scalar() is None]
            revision = conn.execute(text("SELECT version_num FROM kbp.alembic_version")).scalar_one()
        if missing or revision != "kbp_001a":
            raise PilotRefused(f"kbp schema incomplete (missing {missing}, revision {revision!r})")
    except BaseException:
        db.dispose()
        raise
    return db


# --------------------------------------------------------------------------- #
# Deterministic sources
# --------------------------------------------------------------------------- #

def _html(title: str, body: str) -> bytes:
    return f"<!doctype html><html><head><title>{title}</title></head><body><main>{body}</main></body></html>".encode()


def silver_text_of(html: bytes) -> str:
    """The Silver text the pipeline derives from ``html`` (for the cross-source duplicate)."""
    from app.processing.silver.config import SilverConfig
    from app.processing.silver.pipeline import SilverEvaluator
    from app.sources.commoncrawl.bronze import BronzeStore
    from tests.unit.silver_fixtures import store_cc

    with tempfile.TemporaryDirectory() as tmp:
        bronze = BronzeStore(tmp)
        return SilverEvaluator(SilverConfig(), cc_bronze=bronze).evaluate(
            store_cc(bronze, "https://tmp.silver-pilot.example/a", html)).canonical_text


def build_sources():
    from tests.unit.cc_fixtures import FakeCommonCrawl, make_cdx_line, make_warc_record
    from tests.unit.gh_fixtures import FakeGitHub
    from tests.unit.silver_fixtures import article_html
    from tests.unit.test_silver_dedup import glacier_doc
    from tests.unit.test_silver_processor import REFERENCE

    glaciers = glacier_doc()
    glaciers_copy = glacier_doc({11: 0})
    pages = {
        "https://www.silver-pilot.example/photosynthesis": article_html(),
        "https://mirror.silver-pilot.example/photosynthesis": article_html(),
        "https://www.silver-pilot.example/glaciers": _html("Glaciers", _paragraphs(glaciers)),
        "https://copy.silver-pilot.example/glaciers": _html("Glaciers", _paragraphs(glaciers_copy)),
        "https://www.silver-pilot.example/fr": _html("Photosynthèse", f"<h1>Photosynthèse</h1><p>{FRENCH}</p>"),
        "https://www.silver-pilot.example/deploy": article_html(
            "<h1>Deploy notes</h1><p>Binary search is an algorithm that finds a target value in a sorted array by "
            "halving the search interval at every step, so it runs in logarithmic time.</p>"
            f"<pre>{PRIVATE_KEY}</pre>"),
    }
    site = FakeCommonCrawl()
    name, spans = site.add_warc([make_warc_record(url, body) for url, body in pages.items()], leaf=PILOT_WARC_LEAF,
                                crawl=PILOT_CRAWL)
    lines = [make_cdx_line(url, name, offset, length, urlkey=f"example,silver-pilot)/{i:02d}")
             for i, (url, (offset, length)) in enumerate(zip(pages, spans))]
    site.add_index([lines], cdx_file=PILOT_CDX, crawl=PILOT_CRAWL)

    gh = FakeGitHub()
    head = gh.add_repo(PILOT_REPO, "silver-pilot", "fixtures", {
        "README.md": REFERENCE + "\n",
        "docs/photosynthesis.txt": silver_text_of(article_html()),   # cross-source exact duplicate
        "docs/fr.md": FRENCH + "\n",
        "docs/customer.md": REFERENCE + f"\n\nCustomer SSN: {SSN} is stored in this record.\n",
    })
    return site, name, gh, head


def _paragraphs(markdown: str) -> str:
    blocks = markdown.split("\n\n")
    return f"<h1>{blocks[0].lstrip('# ')}</h1>" + "".join(f"<p>{b}</p>" for b in blocks[1:])


# --------------------------------------------------------------------------- #
# Pilot
# --------------------------------------------------------------------------- #

def run_pilot(db, root: Path = PILOT_ROOT) -> dict[str, Any]:
    """Acquire + Silver both pilot units through the Runtime Engine, then a Silver reprocess of every observation."""
    from app.config.settings import WorkerSettings
    from app.processing.dedup.near import NearIndex
    from app.processing.silver.config import SilverConfig
    from app.processing.silver.processor import Prepared, SilverProcessor
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

    site, _warc, gh, head = build_sources()
    root.mkdir(parents=True, exist_ok=True)
    queue = JobQueue(db)
    silver_config = SilverConfig(near_index_path=root / "near_index.sqlite3")
    gh_settings = GitHubSettings(acquisition=GitHubAcquisitionConfig(bronze_root=root / "bronze-gh"))
    cc_bronze, gh_bronze = BronzeStore(root / "bronze-cc"), GitHubBronze(gh_settings.acquisition.bronze_root)
    index = NearIndex(silver_config.near_index_path)
    report: dict[str, Any] = {}
    try:
        processor = SilverProcessor(silver_config, PostgresSilverStore(), index, cc_bronze=cc_bronze,
                                    gh_bronze=gh_bronze)
        report["extraction_version"] = processor.extraction_version
        report["policy_version"] = processor.policy_version

        # Common Crawl unit: plan the pilot partition only, run it with the Silver stage.
        cc_client = HttpClient(transport=FakeTransport(site))
        cc_prefix = f"{PILOT_CRAWL}/cc-index/collections/{PILOT_CRAWL}/indexes/{PILOT_CDX}"
        planned = CrawlPlanner(cc_client, queue, PostgresCaptureStore(), planning=PlanningConfig(blocks_per_partition=10),
                               filter_config=FilterConfig(), policy=CC_PARTITION_POLICY,
                               max_block_bytes=16 * 1024 * 1024).plan([PILOT_CRAWL]).crawls[0]
        report["cc_partitions_created"] = planned.created
        cc_registry = HandlerRegistry()
        cc_registry.register(JobType.CC_INDEX_PARTITION, CCPartitionHandler(
            client=cc_client, store=PostgresCaptureStore(), bronze=cc_bronze,
            config=AcquisitionConfig(bronze_root=root / "bronze-cc", fetch_concurrency=2, batch_size=3),
            silver=processor), policy=CC_PARTITION_POLICY)
        cc_worker = Worker(queue, cc_registry, settings=WorkerSettings(
            lease_seconds=120, poll_interval_seconds=0, source_identifier_prefix=cc_prefix), worker_id="silver-pilot-cc")
        report["cc_worker"] = vars(cc_worker.run(max_iterations=2))

        # GitHub unit: one snapshot job for the pilot repository at its fixed commit.
        gh_client = GitHubClient(gh_settings.http, gh_settings.credentials, transport=gh)
        gh_registry = HandlerRegistry()
        register_github_handlers(gh_registry, gh_settings, client=gh_client, store=PostgresGitHubStore(),
                                 bronze=gh_bronze, silver=processor)
        spec = JobSpec.for_policy(GH_POLICY, job_type=JobType.GH_REPOSITORY_SNAPSHOT, source=JobSource.GITHUB,
                                  source_identifier=gh_repository_snapshot(PILOT_REPO, head),
                                  checkpoint={"v": 1, "previous": None, "ref": "main"})
        report["gh_job_created"] = queue.enqueue(spec).created
        gh_worker = Worker(queue, gh_registry, settings=WorkerSettings(
            lease_seconds=120, poll_interval_seconds=0, source_identifier_prefix=f"{PILOT_REPO}@"),
            worker_id="silver-pilot-gh")
        report["gh_worker"] = vars(gh_worker.run(max_iterations=2))
        cc_client.close()
        gh_client.close()

        # Silver reprocess of every pilot observation with fresh evaluations (bypassing the skip in
        # prepare), exactly the calls a unit makes, in one transaction: proves determinism + idempotency.
        refs = pilot_refs(db)
        evaluations = [processor.evaluator.evaluate(ref) for ref in refs]
        with db.transaction() as conn:
            report["reprocess"] = dict(processor.persist(conn, Prepared(evaluations=evaluations), job_id=None,
                                                         attempt_count=1))
        report["recomputed_document_ids"] = {str(ev.ref.observation_id): str(ev.document_id) if ev.document_id
                                             else None for ev in evaluations}
        return report
    finally:
        index.close()


def pilot_refs(db) -> list:
    from sqlalchemy import text

    from app.processing.silver.model import SourceRef

    with db.transaction() as conn:
        cc = conn.execute(text(
            "SELECT o.observation_id, o.source_url, w.crawl_id, w.warc_filename, c.warc_record_offset, "
            "c.warc_record_length FROM kbp.source_observations o JOIN kbp.cc_captures c USING (observation_id) "
            "JOIN kbp.cc_warc_files w USING (warc_file_id) WHERE w.warc_filename LIKE :f "
            "ORDER BY c.warc_record_offset"), {"f": f"%/{PILOT_WARC_LEAF}"}).all()
        gh = conn.execute(text(
            "SELECT o.observation_id, o.source_url, g.repository_id, g.commit_sha, g.file_path, g.blob_sha "
            "FROM kbp.source_observations o JOIN kbp.github_file_observations g USING (observation_id) "
            "WHERE g.repository_id = :r ORDER BY g.file_path"), {"r": PILOT_REPO}).all()
    return ([SourceRef(r[0], "common_crawl", r[1], crawl_id=r[2], warc_filename=r[3], warc_offset=r[4],
                       warc_length=r[5]) for r in cc]
            + [SourceRef(r[0], "github", r[1], repository_id=r[2], commit_sha=r[3], path=r[4], blob_sha=r[5])
               for r in gh])


# --------------------------------------------------------------------------- #
# Pilot-scoped validation queries (read-only)
# --------------------------------------------------------------------------- #

PILOT_OBSERVATIONS_SQL = """
SELECT o.observation_id, o.source_channel, o.source_url
FROM kbp.source_observations o
LEFT JOIN kbp.cc_captures c USING (observation_id)
LEFT JOIN kbp.cc_warc_files w USING (warc_file_id)
LEFT JOIN kbp.github_file_observations g USING (observation_id)
WHERE w.warc_filename LIKE :warc OR g.repository_id = :repo
"""


def pilot_state(db, extraction_version: str) -> dict[str, Any]:
    from sqlalchemy import text

    params = {"warc": f"%/{PILOT_WARC_LEAF}", "repo": PILOT_REPO, "v": extraction_version,
              "cc_prefix": f"{PILOT_CRAWL}/%/{PILOT_CDX}#%", "repo_prefix": f"{PILOT_REPO}@%"}
    scope = f"WITH pilot AS ({PILOT_OBSERVATIONS_SQL})"
    docs_scope = (scope + ", pilot_docs AS (SELECT DISTINCT e.document_id FROM kbp.observation_extractions e "
                  "JOIN pilot p USING (observation_id) WHERE e.document_id IS NOT NULL)")
    with db.transaction() as conn:
        def rows(sql: str) -> list[dict[str, Any]]:
            return [dict(r) for r in conn.execute(text(sql), params).mappings()]

        observations = rows(scope + " SELECT observation_id, source_channel, source_url FROM pilot "
                                    "ORDER BY source_channel, source_url")
        extractions = rows(scope + " SELECT p.source_channel, p.source_url, e.extraction_version, e.outcome, "
                                   "e.document_id, e.document_was_new FROM kbp.observation_extractions e "
                                   "JOIN pilot p USING (observation_id) ORDER BY p.source_channel, p.source_url, "
                                   "e.extraction_version")
        documents = rows(docs_scope + " SELECT d.document_id, encode(d.content_hash, 'hex') AS content_hash, "
                                      "d.status, d.language, d.language_confidence, d.quality_score, "
                                      "d.factuality_score, d.subject_domain, d.char_length, "
                                      "d.canonical_text IS NOT NULL AS has_text, "
                                      "d.canonical_text IS NULL OR sha256(convert_to(d.canonical_text, 'UTF8')) = "
                                      "d.content_hash AS hash_matches, d.policy_version, d.component_versions, "
                                      "split_part(d.status_reason, '|', 1) AS primary_reason, d.status_reason, "
                                      "d.first_observation_id "
                                      "FROM kbp.document_registry d JOIN pilot_docs USING (document_id) "
                                      "ORDER BY d.status, d.document_id")
        decisions = rows(docs_scope + " SELECT x.document_id, m.algorithm, m.algorithm_version, x.classification, "
                                      "x.canonical_document_id, x.similarity_score, x.decision_status "
                                      "FROM kbp.document_dedup_decisions x JOIN kbp.dedup_methods m "
                                      "USING (dedup_method_id) JOIN pilot_docs USING (document_id)")
        relationships = rows(docs_scope + " SELECT r.document_id, r.related_document_id, r.relationship_type, "
                                          "r.similarity_score, m.algorithm_version FROM kbp.document_relationships r "
                                          "JOIN kbp.dedup_methods m USING (dedup_method_id) "
                                          "JOIN pilot_docs USING (document_id)")
        dead_letters = rows(docs_scope + " SELECT item_type, stage, disposition, error_class, error_code, "
                                         "error_message, occurrence_count, resolved_at IS NULL AS open "
                                         "FROM kbp.dead_letters WHERE item_key IN (SELECT document_id::text FROM "
                                         "pilot_docs UNION SELECT observation_id::text FROM pilot)")
        jobs = rows("SELECT job_type, status, stage FROM kbp.pipeline_jobs WHERE source_identifier LIKE :cc_prefix "
                    "OR source_identifier LIKE :repo_prefix ORDER BY job_type")
    return {"observations": observations, "extractions": extractions, "documents": documents,
            "decisions": decisions, "relationships": relationships, "dead_letters": dead_letters, "jobs": jobs}


def summarize(state: dict[str, Any], extraction_version: str) -> dict[str, Any]:
    current = [e for e in state["extractions"] if e["extraction_version"] == extraction_version]
    by_doc: dict[str, list[str]] = {}
    for e in current:
        if e["document_id"] is not None:
            by_doc.setdefault(str(e["document_id"]), []).append(e["source_channel"] + ":" + e["source_url"])
    statuses: dict[str, int] = {}
    for d in state["documents"]:
        statuses[d["status"]] = statuses.get(d["status"], 0) + 1
    outcomes: dict[str, int] = {}
    for e in current:
        outcomes[e["outcome"]] = outcomes.get(e["outcome"], 0) + 1
    return {
        "source_observations": len(state["observations"]),
        "observation_extractions": len(current),
        "document_registry": len(state["documents"]),
        "document_status_counts": dict(sorted(statuses.items())),
        "extraction_outcome_counts": dict(sorted(outcomes.items())),
        "exact_duplicate_links": sum(1 for e in current if e["document_was_new"] is False),
        "documents_with_multiple_observations": {k: v for k, v in by_doc.items() if len(v) > 1},
        "dedup_decisions": len(state["decisions"]),
        "decision_classes": sorted(d["classification"] for d in state["decisions"]),
        "document_relationships": len(state["relationships"]),
        "dead_letters": len(state["dead_letters"]),
        "open_dead_letters": sum(1 for d in state["dead_letters"] if d["open"]),
        "dead_letter_occurrences": sum(d["occurrence_count"] for d in state["dead_letters"]),
        "jobs": [f"{j['job_type']}:{j['status']}:{j['stage']}" for j in state["jobs"]],
    }


def main(argv: list[str]) -> int:
    from app.processing.silver.config import SilverConfig

    command = argv[0] if argv else "counts"
    version = SilverConfig().extraction_version
    db = open_database()
    try:
        before = summarize(pilot_state(db, version), version)
        if command != "run":
            print(json.dumps(before, indent=2, default=str))
            return 0
        report = run_pilot(db)
        after = summarize(pilot_state(db, version), version)
        report.pop("recomputed_document_ids")
        print(json.dumps({"before": before, "pilot": report, "after": after}, indent=2, default=str))
        return 0
    finally:
        db.dispose()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

"""One operator entry point for the local source -> Gold run (kb_pipeline_test only).

    python -m app.pipeline.cli run --expect-database kb_pipeline_test [--sources cc,github] [bounds...]
        discovery -> acquisition -> Bronze -> Silver (extraction, filters, document dedup) -> chunking
        -> chunk dedup -> embedding eligibility -> MPNet embeddings -> Gold publication
    python -m app.pipeline.cli status --expect-database kb_pipeline_test      # read-only health and counts
    python -m app.pipeline.cli jobs --expect-database kb_pipeline_test [--status S] [--type T]
                                    [--cancel [--execute]]                    # list / cancel pending|retry_pending
    python -m app.pipeline.cli dead-letters --expect-database kb_pipeline_test [--all] [--limit N]

``run`` only sequences the existing stage commands (each is its own supported CLI) as child processes, so each
keeps its own transactions, checkpoints, retries and graceful shutdown. Every stage is idempotent: after a crash
or Ctrl+C simply run the same command again. Exit codes: 0 ok, 1 a stage failed, 2 refused/invalid, 130 interrupted.
Per-stage logs: logs/pipeline/<run_id>/<stage>.log. Database commands use KB_DATABASE_URL (kb_pipeline_app).
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REQUIRED_DATABASE = "kb_pipeline_test"
REQUIRED_REVISION = "kbp_001d"
RUNTIME_ROLE = "kb_pipeline_app"
MODEL_ID = 2
EXIT_OK, EXIT_STAGE_FAILED, EXIT_REFUSED, EXIT_INTERRUPTED = 0, 1, 2, 130
CODE_VERSION = "pipeline-run-v1"

COUNT_SQL = """
SELECT (SELECT count(*) FROM kbp.source_observations) AS observations,
       (SELECT count(*) FROM kbp.observation_extractions) AS extractions,
       (SELECT count(*) FROM kbp.document_registry) AS documents,
       (SELECT count(*) FROM kbp.document_registry WHERE status = 'accepted') AS accepted_documents,
       (SELECT count(*) FROM kbp.document_dedup_decisions) AS document_decisions,
       (SELECT count(*) FROM kbp.chunk_registry) AS chunks,
       (SELECT count(*) FROM kbp.chunk_occurrences) AS chunk_occurrences,
       (SELECT count(*) FROM kbp.chunk_dedup_decisions) AS chunk_decisions,
       (SELECT count(*) FROM kbp.chunk_embeddings) AS embeddings,
       (SELECT count(*) FROM kbp.chunk_embedding_vectors) AS vectors,
       (SELECT count(*) FROM kbp.gold_publications) AS gold_ledger,
       (SELECT count(*) FROM public.kb) AS gold_kb,
       (SELECT count(id) FROM public.metadata) AS gold_metadata,
       (SELECT count(*) FROM kbp.dead_letters WHERE resolved_at IS NULL) AS open_dead_letters
"""
HEALTH_SQL = """
SELECT (SELECT count(*) FROM kbp.pipeline_jobs WHERE status = 'running') AS running_jobs,
       (SELECT count(*) FROM kbp.pipeline_jobs WHERE lease_expires_at IS NOT NULL) AS leased_jobs,
       (SELECT count(*) FROM kbp.pipeline_jobs WHERE status = 'running' AND lease_expires_at < now()) AS stale_leases,
       (SELECT count(*) FROM kbp.pipeline_jobs WHERE status = 'retry_pending') AS retry_pending_jobs,
       (SELECT count(*) FROM kbp.embedding_queue) AS embedding_queue,
       (SELECT count(*) FROM kbp.pipeline_runs WHERE status = 'running') AS running_runs
"""
IDENTITY_SQL = """
SELECT current_database() AS database, current_user AS role,
       (SELECT version_num FROM kbp.alembic_version) AS revision,
       (SELECT status FROM kbp.embedding_models WHERE embedding_model_id = %s) AS model_status
"""


class Refused(RuntimeError):
    """Target, role, revision or model precondition failed; nothing was started."""


@dataclass(frozen=True)
class Stage:
    name: str
    module: str
    args: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Pure planning (unit-tested)
# --------------------------------------------------------------------------- #

def parse_sources(value: str) -> tuple[str, ...]:
    chosen = tuple(dict.fromkeys(part.strip() for part in value.split(",") if part.strip()))
    if not chosen or any(item not in ("cc", "github") for item in chosen):
        raise Refused("--sources must be a comma list of: cc, github")
    return chosen


def bounds_env(args: argparse.Namespace) -> dict[str, str]:
    """Bounded-workload options -> the existing acquisition settings (same env for plan and work)."""
    env: dict[str, str] = {}

    def put(name: str, value: Any) -> None:
        if value is not None:
            env[name] = str(value)

    put("KB_CC_CRAWLS", args.cc_crawl)
    put("KB_CC_MAX_PARTITIONS_PER_CRAWL", args.cc_partitions)
    put("KB_CC_BLOCKS_PER_PARTITION", args.cc_blocks)
    put("KB_CC_MIME_TYPES", args.cc_mime)
    put("KB_CC_MAX_RECORD_BYTES", args.cc_max_record_bytes)
    put("KB_GH_MAX_WINDOWS", args.gh_windows)
    put("KB_GH_MAX_FILES_PER_SNAPSHOT", args.gh_max_files)
    put("KB_GH_MIN_STARS", args.gh_min_stars)
    put("KB_GH_QUOTA_RESERVE", args.gh_quota_reserve)
    put("KB_GH_CREATED_FROM", args.gh_created_from)
    put("KB_GH_WINDOW_DAYS", args.gh_window_days)
    return env


def build_stages(args: argparse.Namespace) -> list[Stage]:
    db = args.expect_database
    sources = parse_sources(args.sources)
    env = bounds_env(args)
    stages: list[Stage] = []
    if not args.skip_acquisition:
        if "cc" in sources:
            stages.append(Stage("cc_plan", "app.sources.commoncrawl.cli", ("plan",), env))
            stages.append(Stage("cc_acquire_silver", "app.processing.silver.cli",
                                ("work", "--source", "cc", "--max-iterations", str(args.cc_iterations)), env))
        if "github" in sources:
            plan = ["plan"] + (["--max-windows", str(args.gh_windows)] if args.gh_windows is not None else [])
            stages.append(Stage("gh_plan", "app.sources.github.cli", tuple(plan), env))
            stages.append(Stage("gh_acquire_silver", "app.processing.silver.cli",
                                ("work", "--source", "github", "--max-iterations", str(args.gh_iterations)), env))
    if args.reprocess:
        stages.append(Stage("silver_reprocess", "app.processing.silver.cli",
                            ("reprocess", "--expect-database", db, "--source", "all"), env))
    stages.append(Stage("chunking", "app.processing.chunking.cli", ("backfill", "--expect-database", db), env))
    stages.append(Stage("chunk_dedup", "app.processing.chunk_dedup.cli", ("backfill", "--expect-database", db), env))
    stages.append(Stage("embedding_enqueue", "app.processing.embedding.cli",
                        ("enqueue", "--expect-database", db), env))
    work = ["work", "--expect-database", db, "--device", args.device]
    if args.embed_limit is not None:
        work += ["--limit", str(args.embed_limit)]
    stages.append(Stage("embedding_work", "app.processing.embedding.cli", tuple(work), env))
    if not args.skip_gold:
        stages.append(Stage("gold_publish", "app.processing.gold.cli", ("publish", "--expect-database", db), env))
    return stages


def check_target(identity: dict[str, Any], expect_database: str) -> None:
    if expect_database != REQUIRED_DATABASE:
        raise Refused(f"only --expect-database {REQUIRED_DATABASE} is permitted (got {expect_database!r})")
    if identity["database"] != REQUIRED_DATABASE:
        raise Refused(f"connected to database {identity['database']!r}; expected {REQUIRED_DATABASE!r}")
    if identity["role"] != RUNTIME_ROLE:
        raise Refused(f"connected as {identity['role']!r}; expected {RUNTIME_ROLE}")
    if identity["revision"] != REQUIRED_REVISION:
        raise Refused(f"database revision is {identity['revision']!r}; expected {REQUIRED_REVISION!r}")
    if identity["model_status"] != "active_gold":
        raise Refused(f"embedding model {MODEL_ID} status is {identity['model_status']!r}; expected 'active_gold'")


# --------------------------------------------------------------------------- #
# Database access (psycopg only)
# --------------------------------------------------------------------------- #

def connect():
    import psycopg
    from psycopg.rows import dict_row

    url = os.environ.get("KB_DATABASE_URL", "").strip()
    if not url:
        raise Refused("KB_DATABASE_URL is not set (role kb_pipeline_app; password via pgpass)")
    conninfo = "postgresql://" + url.split("://", 1)[1] if url.startswith("postgresql+psycopg://") else url
    return psycopg.connect(conninfo, autocommit=True, row_factory=dict_row, application_name="kb_pipeline:operator")


def preflight(conn: Any, expect_database: str) -> dict[str, Any]:
    identity = dict(conn.execute(IDENTITY_SQL, (MODEL_ID,)).fetchone())
    check_target(identity, expect_database)
    return identity


def snapshot(conn: Any) -> dict[str, int]:
    return {key: int(value) for key, value in conn.execute(COUNT_SQL).fetchone().items()}


def health(conn: Any) -> dict[str, int]:
    return {key: int(value) for key, value in conn.execute(HEALTH_SQL).fetchone().items()}


def open_run(conn: Any, notes: str) -> uuid.UUID:
    run_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO kbp.pipeline_runs (run_id, run_kind, status, started_at, code_version, identity_spec_version, "
        "identity_extension_version, host, notes) VALUES (%s, 'maintenance', 'running', now(), %s, 'v1', 'v1', %s, %s)",
        (run_id, CODE_VERSION, socket.gethostname(), notes[:4000]))
    return run_id


def close_run(conn: Any, run_id: uuid.UUID, status: str, notes: str) -> None:
    conn.execute("UPDATE kbp.pipeline_runs SET status = %s, finished_at = now(), notes = %s "
                 "WHERE run_id = %s AND status = 'running'", (status, notes[:4000], run_id))


# --------------------------------------------------------------------------- #
# run
# --------------------------------------------------------------------------- #

def run_stage(stage: Stage, log_dir: Path) -> dict[str, Any]:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{stage.name}.log"
    env = {**os.environ, **stage.env, "PYTHONUNBUFFERED": "1"}
    started = time.monotonic()
    with log_path.open("wb") as log:
        proc = subprocess.Popen([sys.executable, "-m", stage.module, *stage.args], stdout=log, stderr=subprocess.STDOUT,
                                env=env)
        interrupted = False
        try:
            code = proc.wait()
        except KeyboardInterrupt:
            interrupted = True
            try:  # the console already delivered Ctrl+C to the child; give it time to stop at a safe point
                code = proc.wait(timeout=180)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                proc.terminate()
                code = proc.wait()
    return {"stage": stage.name, "exit_code": code, "seconds": round(time.monotonic() - started, 1),
            "log": str(log_path), "interrupted": interrupted}


def cmd_run(args: argparse.Namespace) -> int:
    stages = build_stages(args)
    conn = connect()
    try:
        identity = preflight(conn, args.expect_database)
        before = snapshot(conn)
        if args.dry_run:
            print(json.dumps({"dry_run": True, "database": identity["database"],
                              "stages": [{"stage": s.name, "command": f"python -m {s.module} {' '.join(s.args)}",
                                          "env": s.env} for s in stages]}, indent=2))
            return EXIT_OK
        notes = json.dumps({"sources": args.sources, "stages": [s.name for s in stages]})
        run_id = open_run(conn, notes)
        log_dir = Path(args.log_dir) / str(run_id)
        results: list[dict[str, Any]] = []
        status, code = "succeeded", EXIT_OK
        try:
            for stage in stages:
                result = run_stage(stage, log_dir)
                results.append(result)
                if result["interrupted"]:
                    status, code = "aborted", EXIT_INTERRUPTED
                    break
                if result["exit_code"] != 0:
                    status, code = "failed", EXIT_STAGE_FAILED
                    break
        except KeyboardInterrupt:
            status, code = "aborted", EXIT_INTERRUPTED
        after = snapshot(conn)
        close_run(conn, run_id, status, json.dumps({"stages": [(r["stage"], r["exit_code"]) for r in results],
                                                     "delta": {k: after[k] - before[k] for k in after}}))
        state = health(conn)
        summary = {"run_id": str(run_id), "database": identity["database"], "status": status, "exit_code": code,
                   "stages": results, "delta": {k: after[k] - before[k] for k in after}, "totals": after,
                   "health": state, "logs": str(log_dir)}
        print(json.dumps(summary, indent=2))
        return code
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# status / jobs / dead letters
# --------------------------------------------------------------------------- #

def cmd_status(args: argparse.Namespace) -> int:
    with connect() as conn:
        identity = preflight(conn, args.expect_database)
        jobs = [dict(r) for r in conn.execute(
            "SELECT job_type, status, count(*) AS jobs FROM kbp.pipeline_jobs GROUP BY 1, 2 ORDER BY 1, 2")]
        runs = [dict(r) for r in conn.execute(
            "SELECT run_kind, status, count(*) AS runs FROM kbp.pipeline_runs GROUP BY 1, 2 ORDER BY 1, 2")]
        print(json.dumps({"identity": identity, "counts": snapshot(conn), "health": health(conn), "jobs": jobs,
                          "runs": runs}, indent=2, default=str))
    return EXIT_OK


def cmd_jobs(args: argparse.Namespace) -> int:
    with connect() as conn:
        preflight(conn, args.expect_database)
        clauses, params = [], []
        if args.status:
            clauses.append("status = %s")
            params.append(args.status)
        if args.type:
            clauses.append("job_type = %s")
            params.append(args.type)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = [dict(r) for r in conn.execute(
            f"SELECT job_id, job_type, status, attempt_count, max_attempts, deferral_count, next_attempt_at, "
            f"last_error_code, left(source_identifier, 80) AS source_identifier FROM kbp.pipeline_jobs {where} "
            f"ORDER BY job_id LIMIT %s", (*params, args.limit))]
    result: dict[str, Any] = {"jobs": rows}
    if args.cancel:
        cancellable = [r["job_id"] for r in rows if r["status"] in ("pending", "retry_pending")]
        result["cancel"] = {"candidates": cancellable, "executed": False}
        if args.execute and cancellable:
            from app.db.connection import Database
            from app.config.settings import DatabaseSettings
            from app.queue.job_queue import JobQueue

            db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:operator_cancel"))
            try:
                queue = JobQueue(db)
                for job_id in cancellable:
                    queue.cancel(job_id)
            finally:
                db.dispose()
            result["cancel"]["executed"] = True
    print(json.dumps(result, indent=2, default=str))
    return EXIT_OK


def cmd_dead_letters(args: argparse.Namespace) -> int:
    with connect() as conn:
        preflight(conn, args.expect_database)
        where = "" if args.all else "WHERE resolved_at IS NULL"
        summary = [dict(r) for r in conn.execute(
            f"SELECT item_type, disposition, error_code, count(*) AS entries, sum(occurrence_count) AS occurrences "
            f"FROM kbp.dead_letters {where} GROUP BY 1, 2, 3 ORDER BY 4 DESC")]
        rows = [dict(r) for r in conn.execute(
            f"SELECT dead_letter_id, item_type, left(item_key, 60) AS item_key, stage, disposition, error_code, "
            f"occurrence_count, last_seen_at, resolution FROM kbp.dead_letters {where} "
            f"ORDER BY dead_letter_id DESC LIMIT %s", (args.limit,))]
        print(json.dumps({"summary": summary, "latest": rows}, indent=2, default=str))
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.pipeline.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
        p.add_argument("--expect-database", required=True, help="must equal kb_pipeline_test")
        return p

    run = common(sub.add_parser("run", help="source -> Gold, bounded, rerunnable"))
    run.add_argument("--sources", default="cc,github", help="comma list: cc,github (default both)")
    run.add_argument("--cc-crawl", help="explicit crawl id, e.g. CC-MAIN-2013-20 (default: latest window)")
    run.add_argument("--cc-partitions", type=int, help="max index partitions per crawl")
    run.add_argument("--cc-blocks", type=int, default=1, help="CDX blocks (~3000 lines) per partition (default 1)")
    run.add_argument("--cc-max-record-bytes", type=int, help="max compressed WARC record size (smaller = compact pilot)")
    run.add_argument("--cc-mime", help="comma MIME list narrowing the candidate filter (default html+xhtml)")
    run.add_argument("--cc-iterations", type=int, default=2, help="max partition jobs to run (default 2)")
    run.add_argument("--gh-windows", type=int, help="max creation-date discovery windows (newest first)")
    run.add_argument("--gh-max-files", type=int, help="max files per repository snapshot")
    run.add_argument("--gh-min-stars", type=int)
    run.add_argument("--gh-quota-reserve", type=int, help="API requests left unspent (unauthenticated: 60/hour)")
    run.add_argument("--gh-created-from", help="YYYY-MM-DD start of discovery windows")
    run.add_argument("--gh-window-days", type=int)
    run.add_argument("--gh-iterations", type=int, default=8, help="max discovery+snapshot jobs to run (default 8)")
    run.add_argument("--embed-limit", type=int, help="max chunks to embed this run")
    run.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    run.add_argument("--skip-acquisition", action="store_true", help="process what is already acquired only")
    run.add_argument("--reprocess", action="store_true",
                     help="also run the safe Silver reprocess (observations that never reached Silver)")
    run.add_argument("--skip-gold", action="store_true")
    run.add_argument("--dry-run", action="store_true", help="validate the target and print the stage plan only")
    run.add_argument("--log-dir", default="logs/pipeline")
    run.set_defaults(func=cmd_run)

    common(sub.add_parser("status")).set_defaults(func=cmd_status)

    jobs = common(sub.add_parser("jobs"))
    jobs.add_argument("--status")
    jobs.add_argument("--type")
    jobs.add_argument("--limit", type=int, default=50)
    jobs.add_argument("--cancel", action="store_true", help="select pending/retry_pending jobs for cancellation")
    jobs.add_argument("--execute", action="store_true", help="actually cancel (default is a dry run)")
    jobs.set_defaults(func=cmd_jobs)

    dead = common(sub.add_parser("dead-letters"))
    dead.add_argument("--all", action="store_true", help="include resolved entries")
    dead.add_argument("--limit", type=int, default=25)
    dead.set_defaults(func=cmd_dead_letters)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except Refused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    raise SystemExit(main())

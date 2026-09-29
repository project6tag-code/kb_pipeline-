"""Operator commands for Common Crawl Acquisition V1.

    python -m app.sources.commoncrawl.cli discover            # list published crawls + the selected window (network: collinfo.json)
    python -m app.sources.commoncrawl.cli plan [--crawl ID]   # register crawls, enqueue partition jobs (network: cluster.idx; DB)
    python -m app.sources.commoncrawl.cli work [--max-iterations N]   # run an acquisition worker (network; DB; Bronze)
    python -m app.sources.commoncrawl.cli status              # partition job and capture counts (DB, read-only)
    python -m app.sources.commoncrawl.cli verify-bronze [--crawl ID] [--limit N]   # re-hash Bronze evidence (offline)
    python -m app.sources.commoncrawl.cli sweep-temp [--older-than SECONDS]        # remove abandoned temp files (offline)

Database commands use KB_DATABASE_URL (role kb_pipeline_app). The URL is never printed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.sources.commoncrawl.bronze import BronzeCorrupt, BronzeState
from app.sources.commoncrawl.config import CCSettings


def _db():
    from app.config.settings import DatabaseSettings
    from app.db.connection import Database

    db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:cc_cli"))
    db.verify_runtime_identity()
    return db


def cmd_discover(settings: CCSettings, _args: argparse.Namespace) -> int:
    from app.sources.commoncrawl.connector import build_client
    from app.sources.commoncrawl.discovery import parse_collinfo, select_crawls

    client = build_client(settings)
    try:
        available = parse_collinfo(client.get_bytes(client.endpoints.collinfo_url(), max_bytes=4 * 1024 * 1024))
    finally:
        client.close()
    print(json.dumps({"available": available, "selected": select_crawls(available, settings.planning),
                      "filter_version": settings.acquisition.filter.version}, indent=2))
    return 0


def cmd_plan(settings: CCSettings, args: argparse.Namespace) -> int:
    from app.db import repositories
    from app.queue.job_queue import JobQueue
    from app.queue.states import RunKind, RunStatus
    from app.sources.commoncrawl.connector import build_client, build_planner

    db, client = _db(), build_client(settings)
    try:
        with db.transaction() as conn:
            run = repositories.create_run(conn, RunKind.INGEST, code_version="cc_acquisition_v1",
                                          notes="common crawl partition planning")
        planner = build_planner(settings, client=client, queue=JobQueue(db), run_id=run.run_id,
                                progress=lambda msg: print(msg, file=sys.stderr))
        status, notes = RunStatus.FAILED, "planning failed"
        try:
            report = planner.plan(args.crawl or None)
            status = RunStatus.SUCCEEDED
            notes = json.dumps({"filter_version": report.filter_version,
                                "crawls": [vars(c) for c in report.crawls]})
            print(notes)
            return 0
        finally:
            with db.transaction() as conn:
                repositories.finish_run(conn, run.run_id, status, notes=notes)
    finally:
        client.close()
        db.dispose()


def cmd_work(_settings: CCSettings, args: argparse.Namespace) -> int:
    from app.workers.acquisition_worker import run_cc_worker

    stats = run_cc_worker(max_iterations=args.max_iterations, recover_every=args.recover_every)
    print(json.dumps(vars(stats)))
    return 0


def cmd_status(_settings: CCSettings, _args: argparse.Namespace) -> int:
    from app.sources.commoncrawl.repository import capture_counts, partition_summary

    db = _db()
    try:
        with db.transaction() as conn:
            result = {"partitions": partition_summary(conn), "captures": capture_counts(conn)}
        print(json.dumps(result, indent=2, default=str))
        return 0
    finally:
        db.dispose()


def cmd_verify_bronze(settings: CCSettings, args: argparse.Namespace) -> int:
    from app.sources.commoncrawl.connector import build_bronze

    bronze = build_bronze(settings)
    root = bronze.root / args.crawl if args.crawl else bronze.root
    counts = {state.value: 0 for state in BronzeState}
    counts["corrupt_manifest"] = 0
    problems = []
    for manifest_path in sorted(Path(root).rglob("*.json")) if Path(root).exists() else []:
        crawl_id = manifest_path.relative_to(bronze.root).parts[0]
        observation_id = manifest_path.name[:-len(".json")]
        try:
            state = bronze.inspect(crawl_id, observation_id).state
        except BronzeCorrupt as exc:
            counts["corrupt_manifest"] += 1
            problems.append(f"{manifest_path}: {exc}")
            continue
        counts[state.value] += 1
        if state is not BronzeState.COMPLETE:
            problems.append(f"{manifest_path}: {state.value}")
        if args.limit and sum(counts.values()) >= args.limit:
            break
    print(json.dumps({"counts": counts, "problems": problems[:100]}, indent=2))
    return 0 if not problems else 1


def cmd_sweep_temp(settings: CCSettings, args: argparse.Namespace) -> int:
    from app.sources.commoncrawl.connector import build_bronze

    print(json.dumps({"removed": build_bronze(settings).sweep_temp_files(older_than_seconds=args.older_than)}))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.sources.commoncrawl.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("discover")
    plan = sub.add_parser("plan")
    plan.add_argument("--crawl", action="append", help="crawl id (repeatable); default: the configured window")
    work = sub.add_parser("work")
    work.add_argument("--max-iterations", type=int, default=None)
    work.add_argument("--recover-every", type=int, default=10)
    sub.add_parser("status")
    verify = sub.add_parser("verify-bronze")
    verify.add_argument("--crawl")
    verify.add_argument("--limit", type=int, default=0)
    sweep = sub.add_parser("sweep-temp")
    sweep.add_argument("--older-than", type=float, default=3600.0)
    args = parser.parse_args(argv)
    settings = CCSettings.from_env()
    handlers = {"discover": cmd_discover, "plan": cmd_plan, "work": cmd_work, "status": cmd_status,
                "verify-bronze": cmd_verify_bronze, "sweep-temp": cmd_sweep_temp}
    return handlers[args.command](settings, args)


if __name__ == "__main__":
    raise SystemExit(main())

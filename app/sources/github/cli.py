"""Operator commands for GitHub Acquisition V1.

    python -m app.sources.github.cli plan [--max-windows N]   # enqueue discovery windows (DB only; no network)
    python -m app.sources.github.cli work [--max-iterations N] # run a worker: discovery + snapshots (network; DB; Bronze)
    python -m app.sources.github.cli status                   # job and row counts (DB, read-only)

Database commands use KB_DATABASE_URL (role kb_pipeline_app). KB_GH_TOKEN is
optional and never printed.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

from app.sources.github.config import GitHubSettings


def _db():
    from app.config.settings import DatabaseSettings
    from app.db.connection import Database

    db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:gh_cli"))
    db.verify_runtime_identity()
    return db


def cmd_plan(settings: GitHubSettings, args: argparse.Namespace) -> int:
    from app.queue.job_queue import JobQueue
    from app.sources.github.discovery import plan_discovery

    config = settings.discovery
    if args.max_windows is not None:
        config = dataclasses.replace(config, max_windows=args.max_windows)
    db = _db()
    try:
        report = plan_discovery(JobQueue(db), config)
        print(json.dumps(vars(report)))
        return 0
    finally:
        db.dispose()


def cmd_work(_settings: GitHubSettings, args: argparse.Namespace) -> int:
    from app.workers.acquisition_worker import run_github_worker

    print(json.dumps(vars(run_github_worker(max_iterations=args.max_iterations, recover_every=args.recover_every))))
    return 0


def cmd_status(_settings: GitHubSettings, _args: argparse.Namespace) -> int:
    from app.sources.github.repository import counts, job_summary

    db = _db()
    try:
        with db.transaction() as conn:
            print(json.dumps({"jobs": job_summary(conn), "rows": counts(conn)}, indent=2, default=str))
        return 0
    finally:
        db.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.sources.github.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan")
    plan.add_argument("--max-windows", type=int, default=None)
    work = sub.add_parser("work")
    work.add_argument("--max-iterations", type=int, default=None)
    work.add_argument("--recover-every", type=int, default=10)
    sub.add_parser("status")
    args = parser.parse_args(argv)
    settings = GitHubSettings.from_env()
    return {"plan": cmd_plan, "work": cmd_work, "status": cmd_status}[args.command](settings, args)


if __name__ == "__main__":
    sys.exit(main())

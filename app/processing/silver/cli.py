"""Operator commands for Bronze -> Silver Preprocessing V1.

    python -m app.processing.silver.cli versions                  # component versions and labels (no I/O)
    python -m app.processing.silver.cli register-methods --expect-database NAME
                                                                  # MIGRATOR ONLY: register the near-dup method
    python -m app.processing.silver.cli work --source cc|github   # acquisition worker with the Silver stage
    python -m app.processing.silver.cli status                    # Silver counts (DB, read-only)

``register-methods`` uses KB_MIGRATION_DATABASE_URL (kb_pipeline_migrator) and refuses to write
unless the connected database is exactly ``--expect-database`` at revision kbp_001a;
the other database commands use KB_DATABASE_URL (kb_pipeline_app).
"""

from __future__ import annotations

import argparse
import json
import sys

from app.processing.silver.config import SilverConfig


def cmd_versions(config: SilverConfig, _args: argparse.Namespace) -> int:
    print(json.dumps({"extraction_version": config.extraction_version, "policy_version": config.policy_version,
                      "components": config.component_versions()}, indent=2))
    return 0


def cmd_register_methods(_config: SilverConfig, args: argparse.Namespace) -> int:
    from app.db.migration_config import create_migration_engine, get_migration_url
    from app.processing.silver.bootstrap import register_methods

    engine = create_migration_engine(get_migration_url())
    try:
        with engine.begin() as conn:
            print(json.dumps(register_methods(conn, expect_database=args.expect_database)))
        return 0
    finally:
        engine.dispose()


def cmd_work(_config: SilverConfig, args: argparse.Namespace) -> int:
    from app.workers.acquisition_worker import run_cc_worker, run_github_worker

    run = run_cc_worker if args.source == "cc" else run_github_worker
    stats = run(max_iterations=args.max_iterations, recover_every=args.recover_every, silver=True)
    print(json.dumps(vars(stats)))
    return 0


def cmd_status(_config: SilverConfig, _args: argparse.Namespace) -> int:
    from app.config.settings import DatabaseSettings
    from app.db.connection import Database
    from app.processing.silver.repository import status_counts

    db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:silver_cli"))
    try:
        db.verify_runtime_identity()
        with db.transaction() as conn:
            print(json.dumps(status_counts(conn), indent=2, default=str))
        return 0
    finally:
        db.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.processing.silver.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("versions")
    register = sub.add_parser("register-methods")
    register.add_argument("--expect-database", required=True,
                          help="the exact database name the connection must be on (refuses otherwise)")
    work = sub.add_parser("work")
    work.add_argument("--source", choices=("cc", "github"), required=True)
    work.add_argument("--max-iterations", type=int, default=None)
    work.add_argument("--recover-every", type=int, default=10)
    sub.add_parser("status")
    args = parser.parse_args(argv)
    config = SilverConfig.from_env()
    handler = {"versions": cmd_versions, "register-methods": cmd_register_methods, "work": cmd_work,
               "status": cmd_status}[args.command]
    return handler(config, args)


if __name__ == "__main__":
    sys.exit(main())

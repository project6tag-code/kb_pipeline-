"""Operator commands for Chunking V1.

    python -m app.processing.chunking.cli version
        the frozen configuration: name, tokenizer, parameters (no I/O)
    python -m app.processing.chunking.cli register-version --expect-database NAME
        MIGRATOR ONLY: register it in kbp.chunking_versions (idempotent)
    python -m app.processing.chunking.cli backfill --expect-database NAME [--limit N] [--batch-size N]
        chunk accepted Silver documents not yet chunked for this version (resumable, rerunnable)
    python -m app.processing.chunking.cli status [--expect-database NAME]
        chunk counts per chunking version (read-only)

``register-version`` uses KB_MIGRATION_DATABASE_URL (kb_pipeline_migrator); the
others use KB_DATABASE_URL (kb_pipeline_app). Every database command refuses
to write unless the connected database is exactly ``--expect-database``.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading

from app.processing.chunking import tokenizer
from app.processing.chunking.chunker import ChunkingConfig


class WrongDatabase(RuntimeError):
    pass


def _runtime_db(expect_database: str):
    from sqlalchemy import text

    from app.config.settings import DatabaseSettings
    from app.db.connection import Database

    db = Database.from_settings(DatabaseSettings.from_env(application_name="kb_pipeline:chunking_cli"))
    try:
        with db.engine.connect() as conn:
            name = conn.execute(text("SELECT current_database()")).scalar_one()
            conn.rollback()
        if name != expect_database:
            raise WrongDatabase(f"connected to database {name!r}, expected {expect_database!r}")
        db.verify_runtime_identity()  # kb_pipeline_app, revision kbp_001d, UTC
    except BaseException:
        db.dispose()
        raise
    return db


def cmd_version(config: ChunkingConfig, _args: argparse.Namespace) -> int:
    print(json.dumps({"name": config.name, "tokenizer": tokenizer.IDENTITY, "parameters": config.parameters()},
                     indent=2))
    return 0


def cmd_register_version(config: ChunkingConfig, args: argparse.Namespace) -> int:
    from app.db.migration_config import create_migration_engine, get_migration_url
    from app.processing.chunking.bootstrap import register_version

    engine = create_migration_engine(get_migration_url())
    try:
        with engine.begin() as conn:
            print(json.dumps(register_version(conn, config, expect_database=args.expect_database)))
        return 0
    finally:
        engine.dispose()


def cmd_backfill(config: ChunkingConfig, args: argparse.Namespace) -> int:
    from app.processing.chunking.repository import PostgresChunkStore
    from app.processing.chunking.service import ChunkWriter, run_backfill

    db = _runtime_db(args.expect_database)
    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            signal.signal(sig, lambda *_: stop.set())  # finish the current batch, then stop
    try:
        report = run_backfill(db.transaction, ChunkWriter(PostgresChunkStore(), config), limit=args.limit,
                              batch_size=args.batch_size, stop_event=stop)
        print(json.dumps({"chunking_version": report.chunking_version, "documents": report.documents,
                          "batches": report.batches, "retries": report.retries, "stopped": report.stopped,
                          "counts": dict(sorted(report.counts.items()))}, indent=2))
        return 0
    finally:
        db.dispose()


def cmd_status(_config: ChunkingConfig, args: argparse.Namespace) -> int:
    from app.processing.chunking.repository import chunk_counts

    db = _runtime_db(args.expect_database)
    try:
        with db.transaction() as conn:
            print(json.dumps(chunk_counts(conn), indent=2, default=str))
        return 0
    finally:
        db.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.processing.chunking.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version")
    for name in ("register-version", "backfill", "status"):
        command = sub.add_parser(name)
        command.add_argument("--expect-database", required=True,
                             help="the exact database name the connection must be on (refuses otherwise)")
        if name == "backfill":
            command.add_argument("--limit", type=int, default=1000, help="maximum documents in this run")
            command.add_argument("--batch-size", type=int, default=25, help="documents per transaction")
    args = parser.parse_args(argv)
    handler = {"version": cmd_version, "register-version": cmd_register_version, "backfill": cmd_backfill,
               "status": cmd_status}[args.command]
    return handler(ChunkingConfig(), args)


if __name__ == "__main__":
    sys.exit(main())

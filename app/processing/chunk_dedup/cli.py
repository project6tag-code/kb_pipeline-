"""Operator commands for Global Chunk Near-Dedup V1 (``chunk_minhash_lsh`` / ``oph-w3-b10r4-j70-v1``).

    python -m app.processing.chunk_dedup.cli method
        the frozen method: algorithm, version, parameters (no I/O)
    python -m app.processing.chunk_dedup.cli register-method --expect-database NAME
        MIGRATOR ONLY: register it in kbp.dedup_methods (idempotent)
    python -m app.processing.chunk_dedup.cli backfill --expect-database NAME [--limit N] [--batch-size N]
        decide chunks that have no decision for this method (bounded, resumable, rerunnable; also the
        incremental path for newly chunked text)
    python -m app.processing.chunk_dedup.cli rebuild-index --expect-database NAME
        rebuild the disposable side index from PostgreSQL (atomic swap; decisions are never touched)
    python -m app.processing.chunk_dedup.cli status --expect-database NAME
        read-only counts, audit invariants, index completeness and bucket statistics
    python -m app.processing.chunk_dedup.cli audit --expect-database NAME [--replay]
        read-only invariant audit; --replay re-derives every decision from scratch (bounded)

``register-method`` uses KB_MIGRATION_DATABASE_URL (kb_pipeline_migrator); the
others use KB_DATABASE_URL (kb_pipeline_app). Every database command refuses
to run unless the connected database is exactly ``--expect-database``. The side
index lives at ``data/chunk_dedup/<database>/<algorithm>-<version>.sqlite3``
unless ``--index-path`` or KB_CHUNK_DEDUP_INDEX says otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from pathlib import Path

from app.processing.chunk_dedup import method

MIN_FREE_BYTES = 2 * 1024 ** 3


def _index_path(args: argparse.Namespace) -> Path:
    from app.processing.chunk_dedup.index import default_index_path

    explicit = args.index_path or os.environ.get("KB_CHUNK_DEDUP_INDEX")
    return Path(explicit) if explicit else default_index_path(args.expect_database)


def cmd_method(_args: argparse.Namespace) -> int:
    print(json.dumps({"algorithm": method.ALGORITHM, "version": method.VERSION,
                      "parameters_sha256": method.parameters_sha256(), "parameters": method.PARAMETERS}, indent=2))
    return 0


def cmd_register_method(args: argparse.Namespace) -> int:
    from app.db.migration_config import create_migration_engine, get_migration_url
    from app.processing.chunk_dedup.bootstrap import register_method

    engine = create_migration_engine(get_migration_url())
    try:
        with engine.begin() as conn:
            print(json.dumps(register_method(conn, expect_database=args.expect_database)))
        return 0
    finally:
        engine.dispose()


def open_deduplicator(db, database: str, index_path: Path, run_id, *, create_index: bool):
    """Resolve the method, open (or, for an empty method, create) the index and bind a deduplicator."""
    from app.processing.chunk_dedup.index import CandidateIndex, IndexIdentity
    from app.processing.chunk_dedup.repository import PostgresChunkDedupStore
    from app.processing.chunk_dedup.service import ChunkNearDeduplicator, resolve_method

    store = PostgresChunkDedupStore()
    with db.transaction() as conn:
        method_id = resolve_method(store, conn)
        representatives = store.representative_count(conn, method_id)
    identity = IndexIdentity(database, method_id)
    index = CandidateIndex(index_path, identity, create=create_index and representatives == 0)
    return ChunkNearDeduplicator(store, index, run_id=run_id, min_free_bytes=MIN_FREE_BYTES)


def backfill_run(db, *, database: str, index_path: Path, limit: int, batch_size: int,
                 stop_event: threading.Event | None = None) -> dict:
    """One provenance-tracked backfill: open a ``pipeline_runs`` row, decide, close the run with the metrics."""
    from app.db import repositories
    from app.processing.chunk_dedup.service import run_backfill, run_notes
    from app.queue.states import RunKind, RunStatus

    with db.transaction() as conn:
        run = repositories.create_run(conn, RunKind.REPROCESS, code_version=f"{method.ALGORITHM}:{method.VERSION}",
                                      config_sha256=bytes.fromhex(method.parameters_sha256()),
                                      notes="chunk near-dedup backfill")
    dedup = None
    status, report = RunStatus.FAILED, None
    try:
        dedup = open_deduplicator(db, database, index_path, run.run_id, create_index=True)
        report = run_backfill(db.transaction, dedup, limit=limit, batch_size=batch_size, stop_event=stop_event)
        status = RunStatus.ABORTED if report.stopped else RunStatus.SUCCEEDED
        return report.summary(dedup.metrics)
    finally:
        with db.transaction() as conn:
            metrics = dedup.metrics.summary() if dedup else {}
            notes = run_notes(report, dedup.metrics) if report and dedup else json.dumps({"index_metrics": metrics})
            repositories.finish_run(conn, run.run_id, status, notes=notes)
        if dedup is not None:
            dedup.index.close()


def cmd_backfill(args: argparse.Namespace) -> int:
    from app.processing.chunking.cli import _runtime_db

    db = _runtime_db(args.expect_database)
    stop = threading.Event()
    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            signal.signal(sig, lambda *_: stop.set())  # finish the current batch, then stop
    try:
        print(json.dumps(backfill_run(db, database=args.expect_database, index_path=_index_path(args),
                                      limit=args.limit, batch_size=args.batch_size, stop_event=stop),
                         indent=2, default=str))
        return 0
    finally:
        db.dispose()


def cmd_rebuild_index(args: argparse.Namespace) -> int:
    from app.processing.chunk_dedup.repository import PostgresChunkDedupStore
    from app.processing.chunk_dedup.service import rebuild_index
    from app.processing.chunking.cli import _runtime_db

    db = _runtime_db(args.expect_database)
    try:
        print(json.dumps(rebuild_index(db.transaction, PostgresChunkDedupStore(), _index_path(args),
                                       database=args.expect_database), indent=2))
        return 0
    finally:
        db.dispose()


def _audit(args: argparse.Namespace, *, replay_all: bool) -> dict:
    from app.processing.chunk_dedup import repository
    from app.processing.chunk_dedup.index import CandidateIndex, CandidateIndexError, IndexIdentity
    from app.processing.chunk_dedup.service import replay, resolve_method
    from app.processing.chunking.cli import _runtime_db

    db = _runtime_db(args.expect_database)
    try:
        store = repository.PostgresChunkDedupStore()
        with db.transaction() as conn:
            method_id = resolve_method(store, conn)
            result: dict = {"method": f"{method.ALGORITHM}:{method.VERSION}", "dedup_method_id": method_id,
                            "audit": repository.audit(conn, method_id)}
            if replay_all:
                result["replay"] = replay(repository.decisions_in_order(conn, method_id),
                                          repository.stored_relationships(conn, method_id))
        path = _index_path(args)
        try:
            index = CandidateIndex(path, IndexIdentity(args.expect_database, method_id))
            try:
                result["index"] = {"path": str(path), **index.bucket_stats(),
                                   "representatives": result["audit"]["decisions"] - result["audit"]["near_duplicate"]}
            finally:
                index.close()
        except CandidateIndexError as exc:
            result["index"] = {"path": str(path), "error": str(exc)}
        return result
    finally:
        db.dispose()


def cmd_status(args: argparse.Namespace) -> int:
    print(json.dumps(_audit(args, replay_all=False), indent=2, default=str))
    return 0


def cmd_audit(args: argparse.Namespace) -> int:
    result = _audit(args, replay_all=args.replay)
    print(json.dumps(result, indent=2, default=str))
    violations = sum(v for k, v in result["audit"].items() if k.endswith("_violations"))
    replay_bad = result.get("replay", {}).get("decision_mismatch_count", 0) + \
        result.get("replay", {}).get("relationship_mismatch_count", 0)
    return 1 if violations or replay_bad else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.processing.chunk_dedup.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("method")
    for name in ("register-method", "backfill", "rebuild-index", "status", "audit"):
        command = sub.add_parser(name)
        command.add_argument("--expect-database", required=True,
                             help="the exact database name the connection must be on (refuses otherwise)")
        if name != "register-method":
            command.add_argument("--index-path", help="side-index file (default data/chunk_dedup/<db>/...)")
        if name == "backfill":
            command.add_argument("--limit", type=int, default=10_000, help="maximum chunks in this run")
            command.add_argument("--batch-size", type=int, default=100, help="chunks per transaction")
        if name == "audit":
            command.add_argument("--replay", action="store_true", help="re-derive every decision (bounded)")
    args = parser.parse_args(argv)
    handler = {"method": cmd_method, "register-method": cmd_register_method, "backfill": cmd_backfill,
               "rebuild-index": cmd_rebuild_index, "status": cmd_status, "audit": cmd_audit}[args.command]
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())

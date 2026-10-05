"""Operator commands for the DCA_v2 V1 TEST Gold source bridge (kb_pipeline_test only; no Gold publication).

    python -m app.processing.gold_bridge.cli plan
        the frozen bridge rows (no I/O)
    python -m app.processing.gold_bridge.cli admin-bootstrap --expect-database kb_pipeline_test
        STAGE A, ADMIN ONLY (KB_ADMIN_DATABASE_URL: a superuser or the owner of the public tables; the
        operator's own credentials, e.g. via their pgpass/PGPASSWORD; never stored by the pipeline):
        create/reuse public.dataset_sources (common_crawl, github) and public.datasets (frozen bridge ids)
    python -m app.processing.gold_bridge.cli metadata-patch --expect-database kb_pipeline_test
        ADMIN ONLY (KB_ADMIN_DATABASE_URL): add the five audited nullable Gold provenance columns
    python -m app.processing.gold_bridge.cli map-bootstrap --expect-database kb_pipeline_test
        STAGE B, MIGRATOR (KB_MIGRATION_DATABASE_URL): verify Stage A, create/reuse kbp.source_dataset_map
    python -m app.processing.gold_bridge.cli status --expect-database kb_pipeline_test
        read-only (migrator): rows, counts, and a privilege/schema fingerprint of the bridge tables
    python -m app.processing.gold_bridge.cli resolve --expect-database kb_pipeline_test
        read-only (runtime): real chunk -> observation -> channel -> source_dataset_map -> dataset id

Every command refuses any database other than ``kb_pipeline_test`` (never
``data_collection_pipeline``). Both bootstrap stages run in one transaction
each and are rerunnable; a rerun changes nothing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import asdict

from app.processing.gold_bridge import spec

FINGERPRINT_SQL = """
SELECT c.oid::regclass::text AS relation, pg_catalog.pg_get_userbyid(c.relowner) AS owner, c.relacl::text AS acl,
       (SELECT string_agg(a.attname || ':' || pg_catalog.format_type(a.atttypid, a.atttypmod) || ':' || a.attnotnull,
                          ',' ORDER BY a.attnum) FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS columns,
       (SELECT string_agg(k.conname || '=' || pg_catalog.pg_get_constraintdef(k.oid), ';' ORDER BY k.conname)
        FROM pg_catalog.pg_constraint k WHERE k.conrelid = c.oid) AS constraints
FROM pg_catalog.pg_class c
WHERE c.oid IN ('public.datasets'::regclass, 'public.dataset_sources'::regclass, 'public.kb'::regclass,
                'public.metadata'::regclass, 'kbp.source_dataset_map'::regclass)
ORDER BY 1
"""

RESOLVE_SQL = """
SELECT DISTINCT ON (so.source_channel)
       o.chunk_id, o.document_id, e.observation_id, so.source_channel, m.bridge_identity_name, m.dataset_id
FROM kbp.chunk_occurrences o
JOIN kbp.chunk_registry r ON r.chunk_id = o.chunk_id
JOIN kbp.observation_extractions e ON e.document_id = o.document_id AND e.outcome = 'document'
JOIN kbp.source_observations so ON so.observation_id = r.representative_observation_id
                               AND so.observation_id = e.observation_id
LEFT JOIN kbp.source_dataset_map m ON m.source_channel = so.source_channel
ORDER BY so.source_channel, o.chunk_id
"""


def _engine(env_var: str):
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    url = os.environ.get(env_var, "").strip()
    if not url:
        raise SystemExit(f"{env_var} is not set (password-free URL; credentials come from the operator's pgpass)")
    return create_engine(url, poolclass=NullPool)


def _run_stage(env_var: str, func, expect_database: str) -> int:
    engine = _engine(env_var)
    try:
        with engine.begin() as conn:  # one transaction: commit on success, rollback on any refusal/conflict
            print(json.dumps(func(conn, expect_database=expect_database), indent=2, default=str))
        return 0
    finally:
        engine.dispose()


def fingerprint(conn) -> dict:
    from sqlalchemy import text

    rows = [dict(r) for r in conn.execute(text(FINGERPRINT_SQL)).mappings()]
    return {"sha256": hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest(), "relations": rows}


def cmd_plan(_args) -> int:
    print(json.dumps({name: {"dataset_sources": asdict(c.source), "datasets": asdict(c.dataset),
                             "source_dataset_map": asdict(c.mapping)} for name, c in spec.CHANNELS.items()},
                     indent=2, default=str))
    return 0


def cmd_admin(args) -> int:
    from app.processing.gold_bridge.bootstrap import bootstrap_public

    return _run_stage("KB_ADMIN_DATABASE_URL", bootstrap_public, args.expect_database)


def cmd_metadata_patch(args) -> int:
    from app.processing.gold_bridge.metadata_patch import patch_metadata

    return _run_stage("KB_ADMIN_DATABASE_URL", patch_metadata, args.expect_database)


def cmd_map(args) -> int:
    from app.processing.gold_bridge.bootstrap import bootstrap_map

    return _run_stage("KB_MIGRATION_DATABASE_URL", bootstrap_map, args.expect_database)


def cmd_status(args) -> int:
    from sqlalchemy import text

    from app.processing.gold_bridge import bootstrap

    engine = _engine("KB_MIGRATION_DATABASE_URL")
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("BEGIN READ ONLY")
            try:
                identity = bootstrap.verify_target(conn, args.expect_database, stage="migrator")
                bootstrap.verify_public_schema(conn)
                out = {**identity,
                       "counts": dict(conn.execute(text(bootstrap.COUNTS_SQL)).mappings().one()),
                       "dataset_sources": [list(map(str, r)) for r in conn.execute(text(
                           "SELECT id, name, source_type, authentication_required, status FROM public.dataset_sources "
                           "ORDER BY name"))],
                       "datasets": [list(map(str, r)) for r in conn.execute(text(
                           "SELECT id, source_id, dataset_name, status, metadata FROM public.datasets ORDER BY id"))],
                       "source_dataset_map": [list(map(str, r)) for r in conn.execute(text(
                           "SELECT source_channel, dataset_source_name, bridge_identity_name, dataset_id "
                           "FROM kbp.source_dataset_map ORDER BY source_channel"))],
                       "fingerprint": fingerprint(conn)}
            finally:
                conn.rollback()
        print(json.dumps(out, indent=2, default=str))
        return 0
    finally:
        engine.dispose()


def cmd_resolve(args) -> int:
    from sqlalchemy import text

    from app.processing.gold_bridge import bootstrap

    engine = _engine("KB_DATABASE_URL")
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("BEGIN READ ONLY")
            try:
                if args.expect_database not in spec.ALLOWED_DATABASES:
                    raise SystemExit(f"{args.expect_database!r} is not authorized for the V1 test bridge")
                database = conn.execute(text("SELECT current_database()")).scalar_one()
                if database != args.expect_database:
                    raise SystemExit(f"connected to {database!r}, expected {args.expect_database!r}")
                rows = [dict(r) for r in conn.execute(text(RESOLVE_SQL)).mappings()]
                for row in rows:
                    row["resolve_dataset_id"] = bootstrap.resolve_dataset_id(conn, row["source_channel"])
                    row["frozen_dataset_id"] = spec.FROZEN_DATASET_IDS[row["source_channel"]]
                    row["matches_frozen"] = row["resolve_dataset_id"] == row["frozen_dataset_id"]
            finally:
                conn.rollback()
        print(json.dumps(rows, indent=2, default=str))
        return 0 if rows and all(r["matches_frozen"] for r in rows) else 1
    finally:
        engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.processing.gold_bridge.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("plan")
    for name in ("admin-bootstrap", "metadata-patch", "map-bootstrap", "status", "resolve"):
        sub.add_parser(name).add_argument("--expect-database", required=True,
                                          help="must be exactly kb_pipeline_test (the only authorized target)")
    args = parser.parse_args(argv)
    return {"plan": cmd_plan, "admin-bootstrap": cmd_admin, "metadata-patch": cmd_metadata_patch,
            "map-bootstrap": cmd_map, "status": cmd_status, "resolve": cmd_resolve}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())

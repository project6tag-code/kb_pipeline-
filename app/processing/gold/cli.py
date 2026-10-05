"""Gold Publisher V1 operator/runtime commands (psycopg only; kb_pipeline_test only).

    python -m app.processing.gold.cli promote-model --expect-database kb_pipeline_test
        MIGRATOR ONLY: idempotently promote the exact MPNet V1 model 2.
    python -m app.processing.gold.cli publish --expect-database kb_pipeline_test [--limit N] [--batch-size N]
        RUNTIME ONLY: publish eligible representatives by copying their stored shadow vectors.
    python -m app.processing.gold.cli status --expect-database kb_pipeline_test
        RUNTIME ONLY: read-only publication/model/eligibility audit.

No model weights are loaded. No SQLAlchemy import is used by this package.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import psycopg
from psycopg.rows import dict_row

from app.processing.gold import service


def _connect(env_name: str, *, application_name: str):
    url = os.environ.get(env_name, "").strip()
    if not url:
        raise SystemExit(f"{env_name} is not set; use the existing password-free URL and operator pgpass")
    if url.startswith("postgresql+psycopg://"):
        conninfo = "postgresql://" + url.removeprefix("postgresql+psycopg://")
    elif url.startswith(("postgresql://", "postgres://")):
        conninfo = url
    else:
        raise SystemExit(f"{env_name} must use a PostgreSQL psycopg URL")
    return psycopg.connect(conninfo, autocommit=True, row_factory=dict_row, application_name=application_name)


def cmd_promote(args: argparse.Namespace) -> int:
    with _connect("KB_DATABASE_URL", application_name="kb_pipeline:gold_promote_precheck") as runtime:
        public = service.public_gold_counts(runtime, args.expect_database)
    if any(public.values()):
        raise SystemExit(f"candidate model promotion requires empty public Gold tables; found {public}")
    with _connect("KB_MIGRATION_DATABASE_URL", application_name="kb_pipeline:gold_promote") as conn:
        with conn.transaction():
            result = service.promote_model(conn, args.expect_database)
            result["public_gold_before"] = public
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    with _connect("KB_DATABASE_URL", application_name="kb_pipeline:gold_publisher") as conn:
        result = service.run_publisher(conn, expect_database=args.expect_database,
                                       limit=args.limit, batch_size=args.batch_size)
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    with _connect("KB_DATABASE_URL", application_name="kb_pipeline:gold_status") as conn:
        identity = service.verify_target(conn, args.expect_database, role=service.RUNTIME_ROLE)
        model = service.read_model(conn)
        service.validate_model_row(model, allowed_statuses={"candidate", "active_gold"})
        service.preflight_public_schema(conn)
        method_id = service.resolve_method_id(conn)
        state = service.audit_shadow_state(conn, method_id, allowed_statuses={"candidate", "active_gold"})
        state["bridge"] = service._validate_bridges(conn)
        state["model"] = {key: model[key] for key in (
            "embedding_model_id", "model_name", "model_revision", "embedding_version", "embedding_dimension", "status")}
        state["gold"] = service._audit_gold(conn, method_id)
    print(json.dumps({**identity, **state}, indent=2, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.processing.gold.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("promote-model", "publish", "status"):
        sub = commands.add_parser(name)
        sub.add_argument("--expect-database", required=True,
                         help="must equal kb_pipeline_test; all other databases are refused")
        if name == "publish":
            sub.add_argument("--limit", type=int, default=100_000)
            sub.add_argument("--batch-size", type=int, default=4)
    args = parser.parse_args(argv)
    return {"promote-model": cmd_promote, "publish": cmd_publish, "status": cmd_status}[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
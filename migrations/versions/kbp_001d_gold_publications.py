"""Gold publication ledger — Migration 001D.

Revision ID: kbp_001d
Revises: kbp_001c

Records successful publication of an exact chunk/model pair without changing
the immutable embedding result or vector rows.
"""

from __future__ import annotations

revision = "kbp_001d"
down_revision = "kbp_001c"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "kb_pipeline_app"
KBP_TABLES: tuple[str, ...] = ("gold_publications",)

SESSION_GUARDS: tuple[str, ...] = (
    "SET LOCAL lock_timeout = '5s'",
    "SET LOCAL statement_timeout = '60s'",
)

PREFLIGHT_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') THEN
        RAISE EXCEPTION 'kbp preflight: schema kbp is not provisioned';
    END IF;
    IF (SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') <> current_user THEN
        RAISE EXCEPTION 'kbp preflight: current role must own schema kbp';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'kb_pipeline_app') THEN
        RAISE EXCEPTION 'kbp preflight: runtime role kb_pipeline_app is not provisioned';
    END IF;
END
$$
"""

CREATE_TABLES: tuple[str, ...] = (
    """
CREATE TABLE kbp.gold_publications (
    chunk_id uuid NOT NULL,
    embedding_model_id smallint NOT NULL,
    published_at timestamptz NOT NULL,
    run_id uuid NOT NULL,
    CONSTRAINT gold_publications_pkey PRIMARY KEY (chunk_id, embedding_model_id),
    CONSTRAINT gold_publications_embedding_fkey
        FOREIGN KEY (chunk_id, embedding_model_id)
        REFERENCES kbp.chunk_embeddings (chunk_id, embedding_model_id),
    CONSTRAINT gold_publications_run_fkey
        FOREIGN KEY (run_id) REFERENCES kbp.pipeline_runs (run_id)
)
""",
)

RUNTIME_PRIVILEGES: dict[str, tuple[bool, bool, bool, bool]] = {
    "gold_publications": (True, True, False, False),  # SELECT, INSERT, UPDATE, DELETE
}

GRANTS: tuple[str, ...] = tuple(
    statement
    for table, (select, insert, update, delete) in RUNTIME_PRIVILEGES.items()
    for statement in (
        *((f"GRANT SELECT ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if select else ()),
        *((f"GRANT INSERT ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if insert else ()),
        *((f"GRANT UPDATE ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if update else ()),
        *((f"GRANT DELETE ON TABLE kbp.{table} TO {RUNTIME_ROLE}",) if delete else ()),
    )
)

UPGRADE_STATEMENTS: tuple[str, ...] = SESSION_GUARDS + (PREFLIGHT_SQL,) + CREATE_TABLES + GRANTS

DOWNGRADE_STATEMENTS: tuple[str, ...] = (
    SESSION_GUARDS
    + tuple(f"DROP TABLE kbp.{table} RESTRICT" for table in reversed(KBP_TABLES))
)


def upgrade() -> None:
    from alembic import op

    for statement in UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    from alembic import op

    for statement in DOWNGRADE_STATEMENTS:
        op.execute(statement)
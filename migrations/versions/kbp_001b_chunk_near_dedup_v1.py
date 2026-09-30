"""Chunk near-dedup persistence — Migration 001B.

Revision ID: kbp_001b
Revises: kbp_001a

Adds only the durable chunk decision and relationship tables. The runtime role
is insert-only; immutable decisions are verified by the application after
conflicts rather than updated in place.
"""

from __future__ import annotations

revision = "kbp_001b"
down_revision = "kbp_001a"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "kb_pipeline_app"

KBP_TABLES: tuple[str, ...] = (
    "chunk_dedup_decisions",
    "chunk_relationships",
)

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
CREATE TABLE kbp.chunk_dedup_decisions (
    chunk_id uuid NOT NULL,
    dedup_method_id smallint NOT NULL,
    classification text NOT NULL CHECK (classification IN ('NEAR_DUPLICATE', 'RELATED', 'UNIQUE')),
    representative_chunk_id uuid,
    similarity_score real CHECK (similarity_score IS NULL OR similarity_score BETWEEN 0 AND 1),
    decision_status text NOT NULL CHECK (decision_status IN ('proposed', 'confirmed', 'overridden')),
    decided_at timestamptz NOT NULL,
    run_id uuid NOT NULL,
    CONSTRAINT chunk_dedup_decisions_pkey PRIMARY KEY (chunk_id, dedup_method_id),
    CONSTRAINT chunk_dedup_decisions_chunk_fkey
        FOREIGN KEY (chunk_id) REFERENCES kbp.chunk_registry (chunk_id),
    CONSTRAINT chunk_dedup_decisions_method_fkey
        FOREIGN KEY (dedup_method_id) REFERENCES kbp.dedup_methods (dedup_method_id),
    CONSTRAINT chunk_dedup_decisions_representative_fkey
        FOREIGN KEY (representative_chunk_id) REFERENCES kbp.chunk_registry (chunk_id),
    CONSTRAINT chunk_dedup_decisions_run_fkey
        FOREIGN KEY (run_id) REFERENCES kbp.pipeline_runs (run_id),
    CONSTRAINT chunk_dedup_decisions_representative_check
        CHECK ((classification = 'NEAR_DUPLICATE') = (representative_chunk_id IS NOT NULL)),
    CONSTRAINT chunk_dedup_decisions_not_self_check
        CHECK (representative_chunk_id IS DISTINCT FROM chunk_id),
    CONSTRAINT chunk_dedup_decisions_score_check
        CHECK (classification = 'UNIQUE' OR similarity_score IS NOT NULL)
)
""",
    """
CREATE TABLE kbp.chunk_relationships (
    chunk_id uuid NOT NULL,
    related_chunk_id uuid NOT NULL,
    relationship_type text NOT NULL CHECK (relationship_type IN ('NEAR_DUPLICATE', 'RELATED')),
    dedup_method_id smallint NOT NULL,
    similarity_score real NOT NULL CHECK (similarity_score BETWEEN 0 AND 1),
    decision_status text NOT NULL CHECK (decision_status IN ('proposed', 'confirmed', 'overridden')),
    created_at timestamptz NOT NULL DEFAULT now(),
    run_id uuid NOT NULL,
    CONSTRAINT chunk_relationships_pkey PRIMARY KEY (chunk_id, related_chunk_id, dedup_method_id),
    CONSTRAINT chunk_relationships_chunk_fkey
        FOREIGN KEY (chunk_id) REFERENCES kbp.chunk_registry (chunk_id),
    CONSTRAINT chunk_relationships_related_chunk_fkey
        FOREIGN KEY (related_chunk_id) REFERENCES kbp.chunk_registry (chunk_id),
    CONSTRAINT chunk_relationships_method_fkey
        FOREIGN KEY (dedup_method_id) REFERENCES kbp.dedup_methods (dedup_method_id),
    CONSTRAINT chunk_relationships_run_fkey
        FOREIGN KEY (run_id) REFERENCES kbp.pipeline_runs (run_id),
    CONSTRAINT chunk_relationships_pair_order_check CHECK (chunk_id < related_chunk_id)
)
""",
)

CREATE_INDEXES: tuple[str, ...] = (
    "CREATE INDEX chunk_dedup_decisions_representative_idx "
    "ON kbp.chunk_dedup_decisions (representative_chunk_id, dedup_method_id) "
    "WHERE representative_chunk_id IS NOT NULL",
    "CREATE INDEX chunk_relationships_related_idx "
    "ON kbp.chunk_relationships (related_chunk_id, chunk_id, dedup_method_id)",
)

RUNTIME_PRIVILEGES: dict[str, tuple[bool, bool, bool, bool]] = {
    "chunk_dedup_decisions": (True, True, False, False),
    "chunk_relationships": (True, True, False, False),
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

UPGRADE_STATEMENTS: tuple[str, ...] = (
    SESSION_GUARDS
    + (PREFLIGHT_SQL,)
    + CREATE_TABLES
    + CREATE_INDEXES
    + GRANTS
)

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
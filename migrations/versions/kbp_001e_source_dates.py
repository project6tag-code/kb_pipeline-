"""Source page dates — Migration 001E.

Revision ID: kbp_001e
Revises: kbp_001d

One insert-only evidence row per acquired observation: the published and
modified dates the publisher declared in the page (with the evidence that
produced each), the HTTP Last-Modified header, and the crawl time. The Gold
publisher derives ``public.metadata.published_at`` / ``updated_at`` /
``crawled_at`` from this row (docs/cc_fact_kb_v2.md). A NULL page date means
"not declared by the source"; it is never filled in here.
"""

from __future__ import annotations

revision = "kbp_001e"
down_revision = "kbp_001d"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "kb_pipeline_app"
KBP_TABLES: tuple[str, ...] = ("source_dates",)

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
CREATE TABLE kbp.source_dates (
    observation_id uuid NOT NULL,
    extractor_version text NOT NULL,
    published_at timestamptz NULL,
    published_evidence text NULL,
    modified_at timestamptz NULL,
    modified_evidence text NULL,
    http_last_modified timestamptz NULL,
    crawled_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    CONSTRAINT source_dates_pkey PRIMARY KEY (observation_id),
    CONSTRAINT source_dates_observation_fkey
        FOREIGN KEY (observation_id) REFERENCES kbp.source_observations (observation_id),
    CONSTRAINT source_dates_published_evidence_check
        CHECK ((published_at IS NULL) = (published_evidence IS NULL)),
    CONSTRAINT source_dates_modified_evidence_check
        CHECK ((modified_at IS NULL) = (modified_evidence IS NULL)),
    CONSTRAINT source_dates_order_check
        CHECK (modified_at IS NULL OR published_at IS NULL OR modified_at >= published_at),
    CONSTRAINT source_dates_not_after_crawl_check
        CHECK (published_at IS NULL OR published_at <= crawled_at + interval '1 day')
)
""",
)

RUNTIME_PRIVILEGES: dict[str, tuple[bool, bool, bool, bool]] = {
    "source_dates": (True, True, False, False),  # SELECT, INSERT, UPDATE, DELETE
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

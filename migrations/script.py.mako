"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

kbp lineage rules (docs/migrations_kbp.md):
* raw SQL via op.execute only; every object schema-qualified as kbp.<name>;
* no DDL or DML on public objects (only allowlisted FK references);
* explicit GRANTs per docs/database_privileges_v1.md §5.1;
* run `python -m app.db.migration_safety` before review.
"""

from __future__ import annotations

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

UPGRADE_STATEMENTS: tuple[str, ...] = ()
DOWNGRADE_STATEMENTS: tuple[str, ...] = ()


def upgrade() -> None:
    from alembic import op

    for statement in UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    from alembic import op

    for statement in DOWNGRADE_STATEMENTS:
        op.execute(statement)

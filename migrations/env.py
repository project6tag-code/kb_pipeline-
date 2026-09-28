"""Alembic environment for the KB pipeline's ``kbp`` schema ONLY.

* Version table: ``kbp.alembic_version`` (never ``public.alembic_version``).
* Autogenerate is disabled: every migration is hand-written, reviewed raw SQL
  checked by ``app.db.migration_safety``. The include filters are defense in
  depth and admit only ``kbp`` objects.
* Online runs perform read-only provisioning preflight checks first.
* Offline (``--sql``) runs need no credentials: they only render SQL.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context

from app.db import migration_config as mc

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

if getattr(config.cmd_opts, "autogenerate", False):
    raise RuntimeError(
        "autogenerate is disabled for the kbp migration lineage; write a reviewed "
        "raw-SQL migration and run the safety checker (docs/migrations_kbp.md)"
    )


def run_migrations_offline() -> None:
    """Render SQL without connecting (``alembic upgrade head --sql``)."""
    context.configure(
        dialect_name="postgresql",
        literal_binds=True,
        **mc.alembic_configure_kwargs(),
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations as kb_pipeline_migrator after preflight checks."""
    engine = mc.create_migration_engine(mc.get_migration_url())
    try:
        with engine.connect() as connection:
            mc.run_preflight(connection)
            context.configure(connection=connection, **mc.alembic_configure_kwargs())
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

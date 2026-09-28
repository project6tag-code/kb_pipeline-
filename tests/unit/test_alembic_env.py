"""Offline tests for kbp Alembic isolation (no database, no alembic install needed)."""

import configparser
import contextlib
import runpy
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from app.db import migration_config as mc

ROOT = Path(__file__).resolve().parents[2]
ENV_PY = ROOT / "migrations" / "env.py"


class _StubContext(types.ModuleType):
    def __init__(self, offline: bool, autogenerate: bool = False) -> None:
        super().__init__("alembic.context")
        self.config = SimpleNamespace(
            config_file_name=None, cmd_opts=SimpleNamespace(autogenerate=autogenerate)
        )
        self._offline = offline
        self.configure_calls: list[dict] = []
        self.events: list[str] = []

    def is_offline_mode(self) -> bool:
        return self._offline

    def configure(self, **kwargs: object) -> None:
        self.events.append("configure")
        self.configure_calls.append(kwargs)

    @contextlib.contextmanager
    def begin_transaction(self):
        self.events.append("begin")
        yield

    def run_migrations(self) -> None:
        self.events.append("run_migrations")


def run_env(context: _StubContext) -> None:
    package = types.ModuleType("alembic")
    package.context = context  # type: ignore[attr-defined]
    with mock.patch.dict(sys.modules, {"alembic": package, "alembic.context": context}):
        runpy.run_path(str(ENV_PY), run_name="env")


class VersionTableTests(unittest.TestCase):
    def test_constants(self) -> None:
        self.assertEqual(mc.VERSION_TABLE, "alembic_version")
        self.assertEqual(mc.VERSION_TABLE_SCHEMA, "kbp")
        kwargs = mc.alembic_configure_kwargs()
        self.assertEqual(kwargs["version_table"], "alembic_version")
        self.assertEqual(kwargs["version_table_schema"], "kbp")
        self.assertIsNone(kwargs["target_metadata"])
        self.assertTrue(kwargs["transaction_per_migration"])

    def test_offline_env_configures_kbp_version_table(self) -> None:
        context = _StubContext(offline=True)
        run_env(context)
        self.assertEqual(len(context.configure_calls), 1)
        call = context.configure_calls[0]
        self.assertEqual(call["version_table"], "alembic_version")
        self.assertEqual(call["version_table_schema"], "kbp")
        self.assertEqual(call["dialect_name"], "postgresql")
        self.assertNotIn("url", call)  # offline rendering needs no credentials
        self.assertEqual(context.events, ["configure", "begin", "run_migrations"])

    def test_online_env_runs_preflight_before_configure(self) -> None:
        context = _StubContext(offline=False)
        order: list[str] = []
        connection = SimpleNamespace(in_transaction=lambda: False)

        class FakeEngine:
            def connect(self):
                order.append("connect")
                return contextlib.nullcontext(connection)

            def dispose(self):
                order.append("dispose")

        def fake_preflight(conn):
            self.assertIs(conn, connection)
            order.append("preflight")

        original_configure = context.configure

        def recording_configure(**kwargs):
            order.append("configure")
            original_configure(**kwargs)

        context.configure = recording_configure  # type: ignore[method-assign]
        with mock.patch.object(mc, "get_migration_url", return_value="postgresql+psycopg://x"), \
                mock.patch.object(mc, "create_migration_engine", return_value=FakeEngine()), \
                mock.patch.object(mc, "run_preflight", side_effect=fake_preflight):
            run_env(context)
        self.assertEqual(order, ["connect", "preflight", "configure", "dispose"])
        call = context.configure_calls[0]
        self.assertIs(call["connection"], connection)
        self.assertEqual(call["version_table_schema"], "kbp")

    def test_public_alembic_version_never_configured(self) -> None:
        for offline in (True, False):
            context = _StubContext(offline=offline)
            engine = mock.MagicMock()
            engine.connect.return_value.__enter__.return_value.in_transaction.return_value = False
            with mock.patch.object(mc, "get_migration_url", return_value="u"), \
                    mock.patch.object(mc, "create_migration_engine", return_value=engine), \
                    mock.patch.object(mc, "run_preflight"):
                run_env(context)
            for call in context.configure_calls:
                with self.subTest(offline=offline):
                    self.assertNotEqual(call.get("version_table_schema"), "public")
                    self.assertIsNotNone(call.get("version_table_schema"))

    def test_autogenerate_is_refused(self) -> None:
        with self.assertRaises(RuntimeError):
            run_env(_StubContext(offline=True, autogenerate=True))


class FilterTests(unittest.TestCase):
    def test_include_name(self) -> None:
        self.assertTrue(mc.include_name("kbp", "schema", {}))
        self.assertFalse(mc.include_name("public", "schema", {}))
        self.assertFalse(mc.include_name(None, "schema", {}))
        self.assertTrue(mc.include_name("chunk_registry", "table", {"schema_name": "kbp"}))
        for name in ("kb", "metadata", "datasets", "dataset_sources", "alembic_version"):
            with self.subTest(name=name):
                self.assertFalse(mc.include_name(name, "table", {"schema_name": "public"}))
                self.assertFalse(mc.include_name(name, "table", {"schema_name": None}))

    def test_include_object(self) -> None:
        kbp_table = SimpleNamespace(schema="kbp")
        public_table = SimpleNamespace(schema="public")
        self.assertTrue(mc.include_object(kbp_table, "t", "table", True, None))
        self.assertFalse(mc.include_object(public_table, "kb", "table", True, None))
        self.assertFalse(mc.include_object(SimpleNamespace(schema=None), "x", "table", True, None))
        self.assertTrue(mc.include_object(SimpleNamespace(table=kbp_table), "ix", "index", True, None))
        self.assertFalse(mc.include_object(SimpleNamespace(table=public_table), "ix", "index", True, None))


class UrlTests(unittest.TestCase):
    def test_missing_url_fails_without_leaking(self) -> None:
        with self.assertRaises(mc.MigrationPreflightError) as ctx:
            mc.get_migration_url({})
        self.assertIn("KB_MIGRATION_DATABASE_URL", str(ctx.exception))

    def test_runtime_url_reuse_rejected(self) -> None:
        secret = "postgresql+psycopg://kb_pipeline_app:s3cret@localhost/db"
        with self.assertRaises(mc.MigrationPreflightError) as ctx:
            mc.get_migration_url({"KB_MIGRATION_DATABASE_URL": secret, "KB_DATABASE_URL": secret})
        self.assertNotIn("s3cret", str(ctx.exception))

    def test_url_returned(self) -> None:
        env = {"KB_MIGRATION_DATABASE_URL": "postgresql+psycopg://m@h/db", "KB_DATABASE_URL": "postgresql+psycopg://a@h/db"}
        self.assertEqual(mc.get_migration_url(env), "postgresql+psycopg://m@h/db")


class PreflightTests(unittest.TestCase):
    class FakeConnection:
        def __init__(self, results):
            self.results = list(results)
            self.executed: list[str] = []
            self.rollbacks = 0

        def execute(self, clause):
            self.executed.append(str(clause))
            return SimpleNamespace(scalar=lambda: self.results.pop(0))

        def rollback(self):
            self.rollbacks += 1

    def test_all_checks_are_read_only_selects(self) -> None:
        for description, sql, _message in mc.PREFLIGHT_CHECKS:
            with self.subTest(check=description):
                self.assertTrue(sql.lstrip().upper().startswith("SELECT "))
                for word in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "GRANT", "SET"):
                    self.assertNotRegex(sql.upper(), rf"\b{word}\b")

    def test_passes_when_all_true(self) -> None:
        connection = self.FakeConnection([True] * len(mc.PREFLIGHT_CHECKS))
        mc.run_preflight(connection)
        self.assertEqual(len(connection.executed), len(mc.PREFLIGHT_CHECKS))

    def test_missing_schema_fails_first_with_clear_message(self) -> None:
        connection = self.FakeConnection([False])
        with self.assertRaises(mc.MigrationPreflightError) as ctx:
            mc.run_preflight(connection)
        self.assertIn("schema kbp does not exist", str(ctx.exception))
        self.assertEqual(len(connection.executed), 1)

    def test_null_result_fails(self) -> None:
        connection = self.FakeConnection([True, None])
        with self.assertRaises(mc.MigrationPreflightError):
            mc.run_preflight(connection)

    def test_preflight_always_ends_its_transaction(self) -> None:
        # Regression: an open preflight transaction made Alembic skip COMMIT.
        passing = self.FakeConnection([True] * len(mc.PREFLIGHT_CHECKS))
        mc.run_preflight(passing)
        self.assertEqual(passing.rollbacks, 1)
        failing = self.FakeConnection([False])
        with self.assertRaises(mc.MigrationPreflightError):
            mc.run_preflight(failing)
        self.assertEqual(failing.rollbacks, 1)


class OnlineTransactionGuardTests(unittest.TestCase):
    def test_env_refuses_a_connection_left_in_a_transaction(self) -> None:
        context = _StubContext(offline=False)
        connection = SimpleNamespace(in_transaction=lambda: True)
        engine = SimpleNamespace(connect=lambda: contextlib.nullcontext(connection), dispose=lambda: None)
        with mock.patch.object(mc, "get_migration_url", return_value="u"), \
                mock.patch.object(mc, "create_migration_engine", return_value=engine), \
                mock.patch.object(mc, "run_preflight"):
            with self.assertRaises(RuntimeError):
                run_env(context)
        self.assertEqual(context.configure_calls, [])


def _alembic_available() -> bool:
    try:
        import alembic.runtime.migration  # noqa: F401
        import sqlalchemy  # noqa: F401
    except ImportError:
        return False
    return True


@unittest.skipUnless(_alembic_available(), "alembic and SQLAlchemy are required")
class RealAlembicCommitRegressionTests(unittest.TestCase):
    """Real SQLAlchemy 2.0 + Alembic, on a temporary SQLite file (no PostgreSQL).

    Reproduces the Phase 1A live failure: `alembic upgrade` printed "Running
    upgrade -> kbp_001a" but nothing persisted, because the preflight query
    autobegan a transaction before context.configure(); Alembic then treated
    it as external, never committed, and closing the connection rolled back.
    """

    def setUp(self) -> None:
        import tempfile

        from sqlalchemy import create_engine, pool, text

        self.text = text
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{self.directory.name}/t.db", poolclass=pool.NullPool)
        with self.engine.begin() as connection:
            connection.execute(text("CREATE TABLE migrated (x integer)"))

    def tearDown(self) -> None:
        self.engine.dispose()
        self.directory.cleanup()

    def _migrate(self, before_configure) -> tuple[bool, int]:
        """Run the env.py online sequence; return (external?, rows persisted)."""
        from alembic.runtime.migration import MigrationContext

        with self.engine.connect() as connection:
            before_configure(connection)
            context = MigrationContext.configure(
                connection, opts={"transaction_per_migration": True}
            )
            external = context._in_external_transaction
            with context.begin_transaction():
                with context.begin_transaction(_per_migration=True):
                    connection.execute(self.text("INSERT INTO migrated VALUES (1)"))
        with self.engine.connect() as fresh:
            rows = fresh.execute(self.text("SELECT count(*) FROM migrated")).scalar()
        return external, rows

    def _preflight_patch(self):
        from sqlalchemy import Boolean, column
        from sqlalchemy import text as real_text

        typed = lambda _sql: real_text("SELECT 1 AS ok").columns(column("ok", Boolean))  # noqa: E731
        return mock.patch("sqlalchemy.text", typed)

    def test_old_sequence_loses_the_migration(self) -> None:
        # The pre-fix behaviour: a query leaves an autobegun transaction open.
        external, rows = self._migrate(lambda c: c.execute(self.text("SELECT 1")))
        self.assertTrue(external)
        self.assertEqual(rows, 0)

    def test_run_preflight_leaves_alembic_in_charge_and_the_migration_persists(self) -> None:
        def preflight(connection):
            with self._preflight_patch():
                mc.run_preflight(connection)
            self.assertFalse(connection.in_transaction())

        external, rows = self._migrate(preflight)
        self.assertFalse(external)
        self.assertEqual(rows, 1)

    def test_failed_preflight_also_ends_the_transaction(self) -> None:
        with self.engine.connect() as connection:
            with mock.patch.object(mc, "PREFLIGHT_CHECKS", (("fails", "SELECT 0", "expected failure"),)):
                with self.assertRaises(mc.MigrationPreflightError):
                    mc.run_preflight(connection)
            self.assertFalse(connection.in_transaction())


class AlembicIniTests(unittest.TestCase):
    def test_ini_has_no_credentials_and_points_to_migrations(self) -> None:
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(ROOT / "alembic.ini", encoding="utf-8")
        self.assertEqual(parser["alembic"]["script_location"], "migrations")
        self.assertNotIn("sqlalchemy.url", parser["alembic"])
        text = (ROOT / "alembic.ini").read_text(encoding="utf-8")
        self.assertNotIn("postgresql://", text)
        self.assertNotIn("postgresql+psycopg://", text)


class SecretHygieneTests(unittest.TestCase):
    def test_gitignore_excludes_env_files(self) -> None:
        lines = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        for entry in (".env", ".env.*", "!.env.example"):
            self.assertIn(entry, lines)

    def test_env_example_has_placeholders_only(self) -> None:
        values = {}
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
        self.assertEqual(set(values), {"KB_MIGRATION_DATABASE_URL", "KB_DATABASE_URL"})
        for key, value in values.items():
            with self.subTest(key=key):
                self.assertIn(":CHANGE_ME@", value)
        self.assertIn("kb_pipeline_migrator", values["KB_MIGRATION_DATABASE_URL"])
        self.assertIn("kb_pipeline_app", values["KB_DATABASE_URL"])


if __name__ == "__main__":
    unittest.main()

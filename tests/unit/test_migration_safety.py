"""Unit tests for the kbp migration safety checker (app.db.migration_safety)."""

import tempfile
import textwrap
import unittest
from pathlib import Path

from app.db.migration_safety import (
    MigrationCollectionError,
    check_migration,
    check_statement,
    lex,
)

APPROVED_FK = (
    "ALTER TABLE kbp.source_dataset_map ADD CONSTRAINT source_dataset_map_dataset_id_fkey "
    "FOREIGN KEY (dataset_id) REFERENCES public.datasets (id) ON DELETE NO ACTION NOT VALID"
)


def rules(sql: str) -> set[str]:
    return {v.rule for v in check_statement(sql)}


class ForbiddenPublicOperationsTests(unittest.TestCase):
    CASES = {
        "drop public table": "DROP TABLE public.kb",
        "drop public metadata": "DROP TABLE public.metadata",
        "alter public table": "ALTER TABLE public.metadata ADD COLUMN x integer",
        "alter public datasets": "ALTER TABLE public.datasets ALTER COLUMN status DROP NOT NULL",
        "create public table": "CREATE TABLE public.shadow_kb (id uuid)",
        "create or replace public view": "CREATE OR REPLACE VIEW public.v AS SELECT 1",
        "create public function": "CREATE OR REPLACE FUNCTION public.f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
        "truncate public": "TRUNCATE public.datasets",
        "delete from public": "DELETE FROM public.kb WHERE true",
        "update public": "UPDATE public.metadata SET domain = 'x'",
        "insert into public": "INSERT INTO public.datasets (id) VALUES ('00000000-0000-0000-0000-000000000000')",
        "drop dataset_sources": "DROP TABLE public.dataset_sources",
        "alter dataset_sources": "ALTER TABLE public.dataset_sources ADD COLUMN y text",
        "create public alembic_version": "CREATE TABLE public.alembic_version (version_num varchar(32))",
        "drop public alembic_version": "DROP TABLE public.alembic_version",
        "update public alembic_version": "UPDATE public.alembic_version SET version_num = 'x'",
        "quoted identifiers": 'DROP TABLE "public"."kb"',
        "spaced qualification": "DROP TABLE public . kb",
        "index on public table": "CREATE INDEX kb_hnsw ON public.kb USING hnsw (embedding vector_cosine_ops)",
        "grant on public": "GRANT UPDATE ON TABLE public.kb TO kb_pipeline_app",
        "schema-wide grant": "GRANT SELECT ON ALL TABLES IN SCHEMA public TO kb_pipeline_app",
        "inline FK not allowlisted": "CREATE TABLE kbp.x (id uuid REFERENCES public.datasets (id))",
        "fk to other public table": "ALTER TABLE kbp.x ADD CONSTRAINT f FOREIGN KEY (id) REFERENCES public.kb (id) NOT VALID",
        "dynamic sql in DO": "DO $$ BEGIN EXECUTE 'DROP TABLE public.kb'; END $$",
        "dml in DO": "DO $$ BEGIN DELETE FROM public.metadata; END $$",
        "smuggled second statement": "CREATE TABLE kbp.a (id int); DROP TABLE public.kb",
        "protected name in literal": "CREATE TABLE kbp.a (note text DEFAULT 'public.kb')",
        "other schema": "CREATE TABLE audit.x (id int)",
    }

    def test_each_forbidden_operation_is_rejected(self) -> None:
        for label, sql in self.CASES.items():
            with self.subTest(label=label):
                self.assertTrue(check_statement(sql), f"not rejected: {sql}")


class ForbiddenKbpOperationsTests(unittest.TestCase):
    CASES = {
        "unqualified create": "CREATE TABLE kb_shadow (id int)",
        "unqualified drop": "DROP TABLE source_dataset_map",
        "unqualified index target": "CREATE INDEX i ON chunk_registry (created_at)",
        "create schema": "CREATE SCHEMA kbp AUTHORIZATION kb_pipeline_migrator",
        "drop schema": "DROP SCHEMA kbp",
        "extension": "CREATE EXTENSION IF NOT EXISTS vector",
        "drop extension": "DROP EXTENSION vector",
        "role creation": "CREATE ROLE kb_pipeline_app LOGIN",
        "default privileges": "ALTER DEFAULT PRIVILEGES IN SCHEMA kbp GRANT SELECT ON TABLES TO kb_pipeline_app",
        "ownership change": "ALTER TABLE kbp.chunk_registry OWNER TO postgres",
        "drop cascade": "DROP TABLE kbp.chunk_registry CASCADE",
        "grant to public": "GRANT SELECT ON TABLE kbp.chunk_registry TO PUBLIC",
        "grant to other role": "GRANT SELECT ON TABLE kbp.chunk_registry TO someone",
        "grant option": "GRANT SELECT ON TABLE kbp.chunk_registry TO kb_pipeline_app WITH GRANT OPTION",
        "truncate kbp": "TRUNCATE kbp.chunk_registry",
        "insert kbp": "INSERT INTO kbp.source_dataset_map VALUES ('github')",
        "set role": "SET ROLE postgres",
        "set search_path": "SET search_path = public",
        "bridge uuid": "SELECT 'a7d8d2e1-8400-5916-9440-b492e8e692ce'::uuid",
        "bridge uuid github": "CREATE TABLE kbp.t (id uuid DEFAULT 'b487f559-5f8e-56ba-9746-28658b9d09d1')",
    }

    def test_each_forbidden_operation_is_rejected(self) -> None:
        for label, sql in self.CASES.items():
            with self.subTest(label=label):
                self.assertTrue(check_statement(sql), f"not rejected: {sql}")


class AllowedOperationsTests(unittest.TestCase):
    def test_approved_fk_to_public_datasets_is_permitted(self) -> None:
        self.assertEqual(check_statement(APPROVED_FK), [])
        self.assertEqual(check_statement(APPROVED_FK.replace(" ON DELETE NO ACTION", "")), [])

    def test_approved_fk_must_be_exact(self) -> None:
        variants = [
            APPROVED_FK.replace(" NOT VALID", ""),
            APPROVED_FK.replace("ON DELETE NO ACTION", "ON DELETE CASCADE"),
            APPROVED_FK.replace("kbp.source_dataset_map", "kbp.chunk_registry"),
            APPROVED_FK.replace("public.datasets (id)", "public.datasets (source_id)"),
        ]
        for sql in variants:
            with self.subTest(sql=sql):
                self.assertTrue(check_statement(sql))

    def test_validate_constraint_is_permitted(self) -> None:
        self.assertEqual(
            check_statement("ALTER TABLE kbp.source_dataset_map VALIDATE CONSTRAINT source_dataset_map_dataset_id_fkey"),
            [],
        )

    def test_ordinary_kbp_ddl_and_grants_are_permitted(self) -> None:
        ok = [
            "CREATE TABLE kbp.t (id uuid PRIMARY KEY, v text CHECK (v IN ('public', 'private')))",
            "CREATE UNIQUE INDEX t_idx ON kbp.t (id) WHERE v = 'public'",
            "GRANT SELECT ON TABLE kbp.t TO kb_pipeline_app",
            "GRANT UPDATE (v) ON TABLE kbp.t TO kb_pipeline_app",
            "REVOKE SELECT ON TABLE kbp.alembic_version FROM kb_pipeline_app",
            "DROP TABLE kbp.t RESTRICT",
            "SET LOCAL lock_timeout = '5s'",
            "SET LOCAL statement_timeout = '60s'",
            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'kbp') "
            "THEN RAISE EXCEPTION 'schema kbp is not provisioned'; END IF; END $$",
        ]
        for sql in ok:
            with self.subTest(sql=sql):
                self.assertEqual(check_statement(sql), [])

    def test_comments_are_ignored(self) -> None:
        self.assertEqual(check_statement("-- DROP TABLE public.kb\nCREATE TABLE kbp.t (id int)"), [])
        self.assertEqual(check_statement("/* public.kb */ CREATE TABLE kbp.t (id int)"), [])


class LexerTests(unittest.TestCase):
    def test_literals_are_separated(self) -> None:
        lexed = lex("CREATE TABLE kbp.t (v text CHECK (v ~ '^CC-MAIN-[0-9]{4}$'))")
        self.assertIn("^CC-MAIN-[0-9]{4}$", lexed.literals)
        self.assertNotIn("cc-main", lexed.code)

    def test_dollar_bodies_are_code(self) -> None:
        lexed = lex("DO $$ BEGIN PERFORM 1; END $$")
        self.assertTrue(lexed.has_dollar_body)
        self.assertIn("perform", lexed.code)
        self.assertEqual(lexed.top_level_semicolons, 0)

    def test_quoted_identifiers_are_unquoted(self) -> None:
        self.assertIn("public.kb", lex('DROP TABLE "public"."kb"').code)


class MigrationCollectionTests(unittest.TestCase):
    def _write(self, body: str) -> Path:
        directory = Path(tempfile.mkdtemp())
        path = directory / "m.py"
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        return path

    def test_non_execute_op_rejected(self) -> None:
        path = self._write(
            """
            def upgrade():
                from alembic import op
                op.create_table("x")
            def downgrade():
                pass
            """
        )
        with self.assertRaises(MigrationCollectionError):
            check_migration(path)

    def test_forbidden_statement_in_migration_detected(self) -> None:
        path = self._write(
            """
            def upgrade():
                from alembic import op
                op.execute("CREATE TABLE kbp.t (id int)")
            def downgrade():
                from alembic import op
                op.execute("DELETE FROM public.datasets")
            """
        )
        violations = check_migration(path)
        self.assertTrue(violations)
        self.assertTrue(all(v.phase == "downgrade" for v in violations))

    def test_stub_does_not_leak_into_sys_modules(self) -> None:
        import sys

        before = sys.modules.get("alembic")
        path = self._write(
            """
            def upgrade():
                from alembic import op
                op.execute("CREATE TABLE kbp.t (id int)")
            def downgrade():
                pass
            """
        )
        check_migration(path)
        self.assertIs(sys.modules.get("alembic"), before)


if __name__ == "__main__":
    unittest.main()

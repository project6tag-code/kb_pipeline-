"""Offline verification of Migration 001C (shadow embedding-vector storage) and its safety exception."""

import re
import unittest
from pathlib import Path

from app.db import connection, migration_safety
from app.db.migration_safety import check_migration, check_statement, collect_sql, load_migration_module

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "kbp_001c_shadow_embedding_vectors.py"
COLLECTED = collect_sql(MIGRATION)
UPGRADE: list[str] = COLLECTED["upgrade"]  # type: ignore[assignment]
DOWNGRADE: list[str] = COLLECTED["downgrade"]  # type: ignore[assignment]
MODULE = load_migration_module(MIGRATION)
TABLE = next(s.strip() for s in UPGRADE if s.strip().startswith("CREATE TABLE"))
VERIFIER = (ROOT / "scripts" / "db" / "verify_migration_001c.sql").read_text(encoding="utf-8")


class LineageTests(unittest.TestCase):
    def test_revision_chain_and_runtime_head(self) -> None:
        self.assertEqual((MODULE.revision, MODULE.down_revision), ("kbp_001c", "kbp_001b"))
        self.assertEqual(connection.EXPECTED_KBP_REVISION, "kbp_001c")

    def test_creates_exactly_one_kbp_table_and_nothing_else(self) -> None:
        kinds = [s.strip().split()[0] + " " + s.strip().split()[1] for s in UPGRADE]
        self.assertEqual([s for s in UPGRADE if s.strip().startswith("CREATE")], [UPGRADE[3]])
        self.assertTrue(TABLE.startswith("CREATE TABLE kbp.chunk_embedding_vectors ("))
        self.assertNotIn("CREATE INDEX", " ".join(UPGRADE))  # no ANN / vector index
        self.assertFalse(any(k.startswith(("ALTER", "INSERT", "UPDATE", "DELETE")) for k in kinds))
        self.assertIn("SET LOCAL lock_timeout = '5s'", UPGRADE)


class TableDefinitionTests(unittest.TestCase):
    def test_columns_pk_fk_and_sha_check(self) -> None:
        for declaration in (
            "chunk_id uuid NOT NULL", "embedding_model_id smallint NOT NULL",
            "embedding public.vector(768) NOT NULL", "vector_sha256 bytea NOT NULL",
            "PRIMARY KEY (chunk_id, embedding_model_id)",
            "FOREIGN KEY (chunk_id, embedding_model_id)\n        REFERENCES kbp.chunk_embeddings (chunk_id, embedding_model_id)",
            "CHECK (octet_length(vector_sha256) = 32)",
        ):
            with self.subTest(declaration=declaration):
                self.assertIn(declaration, TABLE)
        self.assertEqual(len(re.findall(r"FOREIGN KEY", TABLE)), 1)
        self.assertNotRegex(TABLE, r"halfvec|ON DELETE|ON UPDATE|DEFERRABLE")

    def test_fk_target_is_the_existing_result_state_primary_key(self) -> None:
        m001a = load_migration_module(ROOT / "migrations" / "versions" / "kbp_001a_schema_v1.py")
        result_table = next(s for s in m001a.CREATE_TABLES if "CREATE TABLE kbp.chunk_embeddings (" in s)
        self.assertIn("PRIMARY KEY (chunk_id, embedding_model_id)", result_table)

    def test_preflight_verifies_pgvector_lives_in_public_before_ddl(self) -> None:
        preflight = UPGRADE[2]
        self.assertTrue(preflight.strip().startswith("DO $$"))
        self.assertIn("extname = 'vector'", preflight)
        self.assertIn("nspname = 'public'", preflight)
        self.assertIn("deptype = 'e'", preflight)
        self.assertLess(UPGRADE.index(preflight), UPGRADE.index(next(s for s in UPGRADE if "CREATE TABLE" in s)))


class PrivilegeAndDowngradeTests(unittest.TestCase):
    def test_runtime_is_select_insert_only(self) -> None:
        self.assertEqual([s for s in UPGRADE if s.startswith("GRANT")], [
            "GRANT SELECT ON TABLE kbp.chunk_embedding_vectors TO kb_pipeline_app",
            "GRANT INSERT ON TABLE kbp.chunk_embedding_vectors TO kb_pipeline_app",
        ])
        self.assertEqual(MODULE.RUNTIME_PRIVILEGES, {"chunk_embedding_vectors": (True, True, False, False)})
        self.assertNotRegex(" ".join(UPGRADE), r"\b(UPDATE|DELETE|TRUNCATE)\b")

    def test_downgrade_drops_only_the_001c_table(self) -> None:
        self.assertEqual([s for s in DOWNGRADE if s.startswith("DROP")],
                         ["DROP TABLE kbp.chunk_embedding_vectors RESTRICT"])
        self.assertFalse(any("CASCADE" in s for s in DOWNGRADE))


class SafetyCheckerTests(unittest.TestCase):
    def test_migration_passes_the_static_safety_checker(self) -> None:
        self.assertEqual(check_migration(MIGRATION), [])

    def test_only_public_vector_as_a_column_type_of_a_new_kbp_table_is_allowed(self) -> None:
        self.assertEqual(migration_safety.ALLOWED_PUBLIC_TYPES, frozenset({"public.vector"}))
        allowed = ("CREATE TABLE kbp.t (a uuid NOT NULL, e public.vector(768) NOT NULL, b int)",
                   "CREATE TABLE kbp.t (a uuid, e public.vector(768))")
        for sql in allowed:
            with self.subTest(sql=sql):
                self.assertEqual(check_statement(sql), [])

    def test_every_other_public_use_stays_denied(self) -> None:
        denied = {
            "no dimension": "CREATE TABLE kbp.t (a uuid, e public.vector NOT NULL)",
            "public table": "CREATE TABLE kbp.t (a uuid, e public.vector(768), CONSTRAINT f FOREIGN KEY (a) "
                            "REFERENCES public.metadata (id))",
            "public function default": "CREATE TABLE kbp.t (a uuid, e public.vector(768) DEFAULT public.vector(ARRAY[1]))",
            "cast": "CREATE TABLE kbp.t (a uuid, e public.vector(768), CHECK (e <> CAST(a AS public.vector)))",
            "alter table": "ALTER TABLE kbp.t ADD COLUMN e public.vector(768)",
            "non-kbp table": "CREATE TABLE public.t (e public.vector(768))",
            "other public type": "CREATE TABLE kbp.t (e public.halfvec(768))",
            "public function index": "CREATE INDEX i ON kbp.t (public.vector_norm(e))",
            "public sequence": "CREATE TABLE kbp.t (a bigint DEFAULT nextval('x'), b public.my_seq)",
            "dml": "INSERT INTO kbp.t SELECT public.vector(768)",
        }
        for name, sql in denied.items():
            with self.subTest(name):
                self.assertIn("foreign-schema", {v.rule for v in check_statement(sql)})


class VerifierScriptTests(unittest.TestCase):
    def test_db_name_guard_revision_and_read_only(self) -> None:
        self.assertIn("current_database() <> 'kb_pipeline_test'", VERIFIER)
        self.assertIn("version_num = 'kbp_001c'", VERIFIER)
        self.assertIn("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;", VERIFIER)
        self.assertTrue(VERIFIER.rstrip().endswith("ROLLBACK;"))
        self.assertNotRegex(VERIFIER, r"(?im)^\s*(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|GRANT)\b")

    def test_verifies_type_keys_privileges_and_absence_of_vector_indexes(self) -> None:
        for token in ("vector(768)", "atttypmod = 768", "chunk_embedding_vectors_pkey",
                      "chunk_embedding_vectors_result_fkey", "chunk_embedding_vectors_sha256_check",
                      "no ANN / vector index", "'UPDATE'", "'DELETE'", "'TRUNCATE'", "deptype = 'e'",
                      "MIGRATION 001C VERIFICATION PASSED"):
            with self.subTest(token=token):
                self.assertIn(token, VERIFIER)


if __name__ == "__main__":
    unittest.main()

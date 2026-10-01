"""Embeddings V1 SQL conformance against Migrations 001A + 001C (no database needed)."""

import re
import unittest

from app.processing.embedding import bootstrap, repository

import tests.unit.test_migration_001c as m001c
from tests.unit.test_runtime_sql import DEFAULTS, PRIVILEGES, TABLES, code

STATEMENTS = {n: v for n, v in vars(repository).items() if n.endswith("_SQL") and isinstance(v, str)}
STATEMENTS.update({f"status.{n}": s for n, s in repository.STATUS_SQL.items()})
READ_TABLES = {"embedding_models", "embedding_queue", "chunk_embeddings", "chunk_embedding_vectors", "chunk_registry",
               "chunk_dedup_decisions", "dead_letters"}
TARGETS = {"embedding_queue": ("chunk_id", "embedding_model_id"), "chunk_embeddings": ("chunk_id", "embedding_model_id"),
           "chunk_embedding_vectors": ("chunk_id", "embedding_model_id")}
PUBLIC_ALLOWED = {"vector", "vector_send", "vector_dims", "vector_norm"}  # pgvector only


class EmbeddingSqlTests(unittest.TestCase):
    def test_scope_tables_public_names_and_no_ddl(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            with self.subTest(statement=name):
                self.assertTrue(set(re.findall(r"\bkbp\.(\w+)", c)) <= READ_TABLES)
                self.assertTrue(set(re.findall(r"\bpublic\.(\w+)", c)) <= PUBLIC_ALLOWED,
                                re.findall(r"\bpublic\.(\w+)", c))
                self.assertNotRegex(c, r"\b(truncate|drop|alter|create|grant|revoke|copy|execute)\b")
                self.assertNotRegex(c, r"\b(kb|metadata)\b\s*\(")  # never Gold

    def test_results_and_vectors_are_never_updated_or_deleted(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            with self.subTest(statement=name):
                self.assertNotRegex(c, r"(update|delete from) kbp\.(chunk_embeddings|chunk_embedding_vectors|embedding_models)\b")
                for verb, table in re.findall(r"\b(update|delete from) kbp\.(\w+)", c):
                    self.assertEqual(table, "embedding_queue")

    def test_inserts_are_granted_complete_and_idempotent(self) -> None:
        checked = 0
        for name, sql in STATEMENTS.items():
            c = code(sql)
            for table, columns in re.findall(r"insert into kbp\.(\w+) \(([^)]*)\)", c):
                checked += 1
                columns = {x.strip() for x in columns.split(",")}
                with self.subTest(statement=name):
                    if table == "chunk_embedding_vectors":
                        self.assertEqual(m001c.MODULE.RUNTIME_PRIVILEGES[table][:2], (True, True))
                        self.assertEqual(columns, {"chunk_id", "embedding_model_id", "embedding", "vector_sha256"})
                    else:
                        spec = TABLES[table]["columns"]
                        required = {k for k, i in spec.items() if i["not_null"] and not i["identity"]
                                    and (table, k) not in DEFAULTS}
                        self.assertTrue(PRIVILEGES[table][0], f"runtime role has no INSERT on {table}")
                        self.assertTrue(columns <= set(spec) and required <= columns, (required - columns))
                    target = re.search(r"on conflict \(([^)]*)\) do nothing", c)
                    self.assertEqual(tuple(x.strip() for x in target.group(1).split(",")), TARGETS[table])
        self.assertEqual(checked, 3)

    def test_queue_transitions_are_lease_fenced(self) -> None:
        for name in ("COMPLETE_SQL", "RESCHEDULE_SQL"):
            with self.subTest(name):
                self.assertIn("lease_token = :lease_token and status = ''", code(STATEMENTS[name]))
        self.assertIn("for update skip locked", code(repository.CLAIM_SQL))
        self.assertIn("lease_token = gen_random_uuid()", code(repository.CLAIM_SQL))
        self.assertTrue(PRIVILEGES["embedding_queue"][1] == "table")  # runtime UPDATE on the transient queue

    def test_result_rows_are_stored_shadow_never_gold(self) -> None:
        raw = " ".join(repository.INSERT_RESULT_SQL.split())
        self.assertIn("'stored_shadow'", raw)
        self.assertNotIn("published_gold", raw)
        self.assertIn("sha256(substring(public.vector_send(v.embedding) FROM 5))",
                      " ".join(repository.VERIFY_STORED_SQL.split()))

    def test_registration_is_a_migrator_step_never_active_gold(self) -> None:
        self.assertFalse(PRIVILEGES["embedding_models"][0])  # runtime cannot INSERT models
        self.assertIn("on conflict (model_name, model_revision, embedding_version) do nothing",
                      code(bootstrap.REGISTER_MODEL_SQL))
        from app.processing.embedding import model

        self.assertEqual(model.REGISTRATION_STATUS, "candidate")


if __name__ == "__main__":
    unittest.main()

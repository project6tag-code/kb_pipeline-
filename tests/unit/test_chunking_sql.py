"""Chunking SQL conformance against Migration 001A (no database needed)."""

import re
import unittest

from app.processing.chunking import bootstrap, repository

from tests.unit.test_runtime_sql import DEFAULTS, PRIVILEGES, TABLES, code, set_clauses

STATEMENTS = {n: v for n, v in vars(repository).items() if n.endswith("_SQL") and isinstance(v, str)}
READ_TABLES = {"chunking_versions", "chunk_occurrences", "document_registry", "dead_letters"}
WRITE_TABLES = {"chunk_registry", "chunk_occurrences"}
TARGETS = {"chunk_registry": ("chunk_id",), "chunk_occurrences": ("document_id", "chunking_version_id", "ordinal")}


class ChunkingSqlTests(unittest.TestCase):
    def test_scope_no_update_delete_ddl_or_public(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            with self.subTest(statement=name):
                self.assertNotRegex(c, r"\bpublic\s*\.")
                self.assertTrue(set(re.findall(r"\bkbp\.(\w+)", c)) <= READ_TABLES | WRITE_TABLES)
                self.assertNotRegex(c, r"\b(update|delete|truncate|drop|alter|create|grant|revoke|copy|execute)\b")
                self.assertEqual(set_clauses(c), [])

    def test_inserts_are_granted_complete_and_idempotent(self) -> None:
        checked = 0
        for name, sql in STATEMENTS.items():
            c = code(sql)
            for table, columns in re.findall(r"insert into kbp\.(\w+) \(([^)]*)\)", c):
                checked += 1
                columns = [x.strip() for x in columns.split(",")]
                spec = TABLES[table]["columns"]
                required = {k for k, i in spec.items() if i["not_null"] and not i["identity"]
                            and (table, k) not in DEFAULTS}
                with self.subTest(statement=name):
                    self.assertIn(table, WRITE_TABLES)
                    self.assertTrue(PRIVILEGES[table][0], f"runtime role has no INSERT on {table}")
                    self.assertTrue(set(columns) <= set(spec), set(columns) - set(spec))
                    self.assertTrue(required <= set(columns), required - set(columns))
                    target = re.search(r"on conflict \(([^)]*)\) do nothing", c)
                    self.assertEqual(tuple(x.strip() for x in target.group(1).split(",")), TARGETS[table])
        self.assertEqual(checked, 2)

    def test_version_registration_is_a_migrator_step(self) -> None:
        self.assertFalse(PRIVILEGES["chunking_versions"][0])  # runtime cannot INSERT: bootstrap only
        c = code(bootstrap.REGISTER_VERSION_SQL)
        self.assertRegex(c, r"insert into kbp\.chunking_versions \(name, tokenizer, parameters\)")
        self.assertRegex(c, r"on conflict \(name\) do nothing")

    def test_occurrence_schema_matches_the_offset_semantics(self) -> None:
        columns = TABLES["chunk_occurrences"]["columns"]
        self.assertTrue(columns["char_start"]["not_null"] and columns["char_end"]["not_null"])
        self.assertFalse(columns["section_path"]["not_null"])
        self.assertFalse(TABLES["chunk_registry"]["columns"]["token_count"]["not_null"])


if __name__ == "__main__":
    unittest.main()

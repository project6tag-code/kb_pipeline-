"""Chunk near-dedup SQL conformance against Migrations 001A + 001B (no database needed)."""

import re
import unittest

from app.processing.chunk_dedup import bootstrap, eligibility, repository

import tests.unit.test_migration_001b as m001b
from tests.unit.test_runtime_sql import PRIVILEGES, TABLES, code, set_clauses

STATEMENTS = {f"{module.__name__}.{name}": value for module in (repository, eligibility)
              for name, value in vars(module).items() if name.endswith("_SQL") and isinstance(value, str)}
STATEMENTS.update({f"audit.{name}": sql for name, sql in repository.AUDIT_SQL.items()})
READ_TABLES = {"chunk_registry", "chunk_occurrences", "chunk_dedup_decisions", "chunk_relationships",
               "dedup_methods", "dead_letters"}
WRITE_TABLES = {"chunk_dedup_decisions": ("chunk_id", "dedup_method_id"),
                "chunk_relationships": ("chunk_id", "related_chunk_id", "dedup_method_id")}


def columns_001b(table: str) -> dict[str, dict]:
    ddl = m001b.TABLES[f"kbp.{table}"]
    body = ddl[ddl.index("(") + 1:ddl.rindex(")")]
    out = {}
    for line in body.split("\n"):
        line = line.strip().rstrip(",")
        if not line or line.split()[0].upper() in {"CONSTRAINT", "FOREIGN", "PRIMARY", "CHECK"}:
            continue
        name = line.split()[0]
        out[name] = {"not_null": "NOT NULL" in line, "default": "DEFAULT" in line}
    return out


class ChunkDedupSqlTests(unittest.TestCase):
    def test_scope_is_kbp_read_or_insert_only(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            with self.subTest(statement=name):
                self.assertNotRegex(c, r"\bpublic\s*\.")
                self.assertTrue(set(re.findall(r"\bkbp\.(\w+)", c)) <= READ_TABLES, re.findall(r"\bkbp\.(\w+)", c))
                self.assertNotRegex(c, r"\b(update|delete|truncate|drop|alter|create|grant|revoke|copy|execute)\b")
                self.assertEqual(set_clauses(c), [])
                self.assertNotRegex(c, r"kbp\.chunk_registry.*insert|insert into kbp\.chunk_(registry|occurrences)")

    def test_inserts_target_001b_completely_and_idempotently(self) -> None:
        checked = 0
        for name, sql in STATEMENTS.items():
            c = code(sql)
            for table, cols in re.findall(r"insert into kbp\.(\w+) \(([^)]*)\)", c):
                checked += 1
                cols = {x.strip() for x in cols.split(",")}
                spec = columns_001b(table)
                required = {k for k, v in spec.items() if v["not_null"] and not v["default"]}
                with self.subTest(statement=name):
                    self.assertIn(table, WRITE_TABLES)
                    self.assertTrue(m001b.MODULE.RUNTIME_PRIVILEGES[table][1], "runtime INSERT granted")
                    self.assertFalse(any(m001b.MODULE.RUNTIME_PRIVILEGES[table][2:]), "no UPDATE/DELETE")
                    self.assertTrue(cols <= set(spec), cols - set(spec))
                    self.assertTrue(required <= cols, required - cols)
                    target = re.search(r"on conflict \(([^)]*)\) do nothing", c)
                    self.assertEqual(tuple(x.strip() for x in target.group(1).split(",")), WRITE_TABLES[table])
        self.assertEqual(checked, 2)

    def test_decision_insert_enforces_the_representative_invariant_in_sql(self) -> None:
        raw = " ".join(repository.INSERT_DECISION_SQL.split())  # literals matter here (the lexer blanks them)
        self.assertIn("r.classification <> 'NEAR_DUPLICATE' AND r.decision_status = 'confirmed'", raw)
        self.assertIn("WHERE CAST(:representative AS uuid) IS NULL OR EXISTS", raw)
        self.assertIn("clock_timestamp()", raw)  # strictly increasing decided_at: the replay order

    def test_reads_use_existing_columns_and_the_registration_is_a_migrator_step(self) -> None:
        self.assertFalse(PRIVILEGES["dedup_methods"][0])  # runtime cannot INSERT methods: bootstrap only
        self.assertIn("insert into kbp.dedup_methods", code(bootstrap.REGISTER_METHOD_SQL))
        self.assertIn("chunk_hash", TABLES["chunk_registry"]["columns"])
        for column in ("classification", "representative_chunk_id", "decision_status", "decided_at", "run_id"):
            self.assertIn(column, columns_001b("chunk_dedup_decisions"))


if __name__ == "__main__":
    unittest.main()

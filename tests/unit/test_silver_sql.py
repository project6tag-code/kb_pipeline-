"""Silver SQL conformance against Migration 001A (no database needed)."""

import re
import unittest

from app.processing.silver import bootstrap, repository

from tests.unit.test_runtime_sql import DEFAULTS, PRIVILEGES, TABLES, code, set_clauses

STATEMENTS = {n: v for n, v in vars(repository).items() if n.endswith("_SQL") and isinstance(v, str)}
READ_TABLES = {"observation_extractions", "source_observations", "document_registry", "github_file_observations",
               "dedup_methods"}
WRITE_TABLES = {"document_registry", "observation_extractions", "document_dedup_decisions", "document_relationships"}
TARGETS = {"document_registry": ("document_id",), "observation_extractions": ("observation_id", "extraction_version"),
           "document_dedup_decisions": ("document_id", "dedup_method_id"),
           "document_relationships": ("document_id", "related_document_id", "relationship_type", "dedup_method_id")}


class SilverSqlTests(unittest.TestCase):
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
                    self.assertIsNotNone(target, "every Silver insert is ON CONFLICT DO NOTHING")
                    self.assertEqual(tuple(x.strip() for x in target.group(1).split(",")), TARGETS[table])
        self.assertEqual(checked, 4)

    def test_dedup_method_registration_is_a_migrator_step(self) -> None:
        self.assertFalse(PRIVILEGES["dedup_methods"][0])  # the runtime role cannot INSERT: bootstrap only
        self.assertNotIn("dedup_methods (", " ".join(code(s) for s in STATEMENTS.values()))
        c = code(bootstrap.REGISTER_METHOD_SQL)
        self.assertRegex(c, r"on conflict \(algorithm, algorithm_version\) do nothing")
        self.assertTrue({"algorithm", "algorithm_version", "parameters"} <= set(TABLES["dedup_methods"]["columns"]))

    def test_registry_constraints_the_processor_relies_on(self) -> None:
        # Rejected/quarantined documents keep hash + decision with canonical_text NULL (schema §5.12).
        self.assertFalse(TABLES["document_registry"]["columns"]["canonical_text"]["not_null"])
        statuses = {"accepted", "rejected_quality", "rejected_near_duplicate", "rejected_factuality",
                    "rejected_policy", "quarantined"}
        from app.processing.silver.model import DocumentStatus, ExtractionOutcome

        self.assertEqual({s.value for s in DocumentStatus}, statuses)
        self.assertEqual({o.value for o in ExtractionOutcome},
                         {"document", "empty", "rejected_language", "rejected_prefilter", "rejected_nul",
                          "extraction_failed"})


if __name__ == "__main__":
    unittest.main()

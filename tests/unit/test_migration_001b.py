"""Offline verification of Migration 001B's additive chunk-dedup schema."""

import re
import unittest
from pathlib import Path

from app.db.migration_safety import check_migration, collect_sql, load_migration_module

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "kbp_001b_chunk_near_dedup_v1.py"
COLLECTED = collect_sql(MIGRATION)
UPGRADE: list[str] = COLLECTED["upgrade"]  # type: ignore[assignment]
DOWNGRADE: list[str] = COLLECTED["downgrade"]  # type: ignore[assignment]
MODULE = load_migration_module(MIGRATION)
TABLES = {
    statement.strip().split("(", 1)[0].split()[-1]: statement.strip()
    for statement in UPGRADE if statement.strip().startswith("CREATE TABLE")
}


class MigrationLineageTests(unittest.TestCase):
    def test_revision_chains_from_001a(self) -> None:
        self.assertEqual(MODULE.revision, "kbp_001b")
        self.assertEqual(MODULE.down_revision, "kbp_001a")

    def test_only_two_new_kbp_tables_are_created(self) -> None:
        self.assertEqual(set(TABLES), {
            "kbp.chunk_dedup_decisions", "kbp.chunk_relationships",
        })

    def test_session_guards_are_bounded(self) -> None:
        self.assertIn("SET LOCAL lock_timeout = '5s'", UPGRADE)
        self.assertIn("SET LOCAL statement_timeout = '60s'", UPGRADE)


class DecisionTableTests(unittest.TestCase):
    SQL = TABLES["kbp.chunk_dedup_decisions"]

    def test_required_columns_and_composite_primary_key(self) -> None:
        for declaration in (
            "chunk_id uuid NOT NULL", "dedup_method_id smallint NOT NULL",
            "classification text NOT NULL", "representative_chunk_id uuid",
            "similarity_score real", "decision_status text NOT NULL",
            "decided_at timestamptz NOT NULL", "run_id uuid NOT NULL",
            "PRIMARY KEY (chunk_id, dedup_method_id)",
        ):
            with self.subTest(declaration=declaration):
                self.assertIn(declaration, self.SQL)

    def test_fks_and_classification_invariants(self) -> None:
        for target in (
            "FOREIGN KEY (chunk_id) REFERENCES kbp.chunk_registry (chunk_id)",
            "FOREIGN KEY (dedup_method_id) REFERENCES kbp.dedup_methods (dedup_method_id)",
            "FOREIGN KEY (representative_chunk_id) REFERENCES kbp.chunk_registry (chunk_id)",
            "FOREIGN KEY (run_id) REFERENCES kbp.pipeline_runs (run_id)",
            "('NEAR_DUPLICATE', 'RELATED', 'UNIQUE')",
            "(classification = 'NEAR_DUPLICATE') = (representative_chunk_id IS NOT NULL)",
            "representative_chunk_id IS DISTINCT FROM chunk_id",
        ):
            with self.subTest(target=target):
                self.assertIn(target, self.SQL)
        self.assertNotIn("EXACT_DUPLICATE", self.SQL)

    def test_representative_fk_has_no_decision_order_dependency(self) -> None:
        self.assertNotRegex(self.SQL, r"FOREIGN KEY\s*\(representative_chunk_id\s*,")
        self.assertNotIn("REFERENCES kbp.chunk_dedup_decisions", self.SQL)


class RelationshipTableTests(unittest.TestCase):
    SQL = TABLES["kbp.chunk_relationships"]

    def test_required_columns_and_pair_method_primary_key(self) -> None:
        for declaration in (
            "chunk_id uuid NOT NULL", "related_chunk_id uuid NOT NULL",
            "relationship_type text NOT NULL", "dedup_method_id smallint NOT NULL",
            "similarity_score real NOT NULL", "decision_status text NOT NULL",
            "created_at timestamptz NOT NULL DEFAULT now()", "run_id uuid NOT NULL",
            "PRIMARY KEY (chunk_id, related_chunk_id, dedup_method_id)",
            "CHECK (chunk_id < related_chunk_id)",
        ):
            with self.subTest(declaration=declaration):
                self.assertIn(declaration, self.SQL)

    def test_pair_cannot_have_contradictory_relationship_types(self) -> None:
        self.assertNotRegex(self.SQL, r"PRIMARY KEY\s*\([^)]*relationship_type")
        self.assertIn("relationship_type IN ('NEAR_DUPLICATE', 'RELATED')", self.SQL)

    def test_required_foreign_keys(self) -> None:
        self.assertEqual(
            len(re.findall(r"FOREIGN KEY", self.SQL)), 4,
        )
        for table in ("chunk_registry", "dedup_methods", "pipeline_runs"):
            with self.subTest(table=table):
                self.assertIn(f"REFERENCES kbp.{table}", self.SQL)


class RuntimeAndSafetyTests(unittest.TestCase):
    def test_indexes_cover_representative_and_reverse_relationship_lookup(self) -> None:
        indexes = [sql for sql in UPGRADE if sql.startswith("CREATE INDEX")]
        self.assertEqual(len(indexes), 2)
        self.assertTrue(any("(representative_chunk_id, dedup_method_id)" in sql for sql in indexes))
        self.assertTrue(any("(related_chunk_id, chunk_id, dedup_method_id)" in sql for sql in indexes))

    def test_runtime_is_insert_select_only(self) -> None:
        self.assertEqual(
            [sql for sql in UPGRADE if sql.startswith("GRANT")],
            [
                "GRANT SELECT ON TABLE kbp.chunk_dedup_decisions TO kb_pipeline_app",
                "GRANT INSERT ON TABLE kbp.chunk_dedup_decisions TO kb_pipeline_app",
                "GRANT SELECT ON TABLE kbp.chunk_relationships TO kb_pipeline_app",
                "GRANT INSERT ON TABLE kbp.chunk_relationships TO kb_pipeline_app",
            ],
        )

    def test_downgrade_drops_only_001b_objects(self) -> None:
        drops = [sql for sql in DOWNGRADE if sql.startswith("DROP")]
        self.assertEqual(drops, [
            "DROP TABLE kbp.chunk_relationships RESTRICT",
            "DROP TABLE kbp.chunk_dedup_decisions RESTRICT",
        ])

    def test_migration_passes_static_safety_checker(self) -> None:
        self.assertEqual(check_migration(MIGRATION), [])


if __name__ == "__main__":
    unittest.main()
"""Offline verification of Migration 001D's insert-only Gold publication ledger."""

import unittest
from pathlib import Path

from app.db.migration_safety import check_migration, collect_sql, load_migration_module

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "kbp_001d_gold_publications.py"
COLLECTED = collect_sql(MIGRATION)
UPGRADE: list[str] = COLLECTED["upgrade"]  # type: ignore[assignment]
DOWNGRADE: list[str] = COLLECTED["downgrade"]  # type: ignore[assignment]
MODULE = load_migration_module(MIGRATION)
TABLE = next(sql.strip() for sql in UPGRADE if sql.strip().startswith("CREATE TABLE"))


class MigrationLineageTests(unittest.TestCase):
    def test_revision_chains_from_001c(self) -> None:
        self.assertEqual((MODULE.revision, MODULE.down_revision), ("kbp_001d", "kbp_001c"))

    def test_bounded_timeouts(self) -> None:
        self.assertIn("SET LOCAL lock_timeout = '5s'", UPGRADE)
        self.assertIn("SET LOCAL statement_timeout = '60s'", UPGRADE)

    def test_only_publication_ledger_is_created(self) -> None:
        creates = [sql.strip() for sql in UPGRADE if sql.strip().startswith("CREATE TABLE")]
        self.assertEqual(len(creates), 1)
        self.assertIn("CREATE TABLE kbp.gold_publications", creates[0])
        self.assertNotIn("public.", "\n".join(UPGRADE))


class GoldPublicationsTableTests(unittest.TestCase):
    def test_columns_keys_and_foreign_keys(self) -> None:
        for definition in (
            "chunk_id uuid NOT NULL",
            "embedding_model_id smallint NOT NULL",
            "published_at timestamptz NOT NULL",
            "run_id uuid NOT NULL",
            "PRIMARY KEY (chunk_id, embedding_model_id)",
            "FOREIGN KEY (chunk_id, embedding_model_id)",
            "REFERENCES kbp.chunk_embeddings (chunk_id, embedding_model_id)",
            "FOREIGN KEY (run_id) REFERENCES kbp.pipeline_runs (run_id)",
        ):
            with self.subTest(definition=definition):
                self.assertIn(definition, TABLE)
        self.assertNotIn("embedding ", TABLE)
        self.assertNotIn("chunk_text", TABLE)
        self.assertNotIn("status", TABLE)

    def test_runtime_is_select_insert_only(self) -> None:
        grants = [sql for sql in UPGRADE if sql.startswith("GRANT")]
        self.assertEqual(grants, [
            "GRANT SELECT ON TABLE kbp.gold_publications TO kb_pipeline_app",
            "GRANT INSERT ON TABLE kbp.gold_publications TO kb_pipeline_app",
        ])
        self.assertEqual(MODULE.RUNTIME_PRIVILEGES["gold_publications"], (True, True, False, False))

    def test_downgrade_only_drops_new_table(self) -> None:
        self.assertEqual(
            [sql for sql in DOWNGRADE if sql.startswith("DROP")],
            ["DROP TABLE kbp.gold_publications RESTRICT"],
        )

    def test_static_migration_safety_passes(self) -> None:
        self.assertEqual(check_migration(MIGRATION), [])


if __name__ == "__main__":
    unittest.main()
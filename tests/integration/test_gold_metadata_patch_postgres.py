"""Opt-in admin validation of the kb_pipeline_test metadata compatibility patch."""

from __future__ import annotations

import os
import unittest

RUN = (os.environ.get("KB_RUN_PG_INTEGRATION") == "1"
       and bool(os.environ.get("KB_ADMIN_DATABASE_URL")))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and operator-provided KB_ADMIN_DATABASE_URL")
class GoldMetadataPatchPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        cls.engine = create_engine(os.environ["KB_ADMIN_DATABASE_URL"], poolclass=NullPool)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()

    def test_admin_patch_then_rerun(self) -> None:
        from sqlalchemy import text

        from app.processing.gold_bridge.metadata_patch import patch_metadata

        with self.engine.begin() as connection:
            first = patch_metadata(connection, expect_database="kb_pipeline_test")
        self.assertEqual(first["action"], "added")
        self.assertEqual(first["columns_added"], ["source_url", "source_title", "source_type", "published_at", "updated_at"])
        with self.engine.begin() as connection:
            second = patch_metadata(connection, expect_database="kb_pipeline_test")
        self.assertEqual(second["action"], "reused")
        self.assertEqual(second["columns_reused"], ["source_url", "source_title", "source_type", "published_at", "updated_at"])
        self.assertEqual(second["public_row_counts_before"], second["public_row_counts_after"])
        self.assertEqual(second["metadata_rows"], 0)
        self.assertEqual(second["public_inventory_sha256_before"], second["public_inventory_sha256_after"])

        with self.engine.connect() as connection:
            database = connection.execute(text("SELECT current_database()" )).scalar_one()
            self.assertEqual(database, "kb_pipeline_test")


if __name__ == "__main__":
    unittest.main()
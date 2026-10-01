"""Opt-in PostgreSQL validation of the DCA_v2 V1 test Gold source bridge (kb_pipeline_test ONLY; no Gold).

    KB_RUN_PG_INTEGRATION=1
    KB_MIGRATION_DATABASE_URL=postgresql+psycopg://kb_pipeline_migrator@localhost:5432/kb_pipeline_test
    KB_DATABASE_URL=postgresql+psycopg://kb_pipeline_app@localhost:5432/kb_pipeline_test
    [KB_ADMIN_DATABASE_URL=postgresql+psycopg://postgres@localhost:5432/kb_pipeline_test]   # operator only
    python -m unittest tests.integration.test_gold_bridge_postgres -v

* The guards and the exact public-schema contract are always checked, read-only, as the migrator.
* Stage A runs twice ONLY when KB_ADMIN_DATABASE_URL is provided by the operator; otherwise it is skipped.
* Stage B and the provenance resolution run once the Stage A rows exist. Stage B runs twice to prove the
  rerun changes nothing, and the privilege/schema fingerprint must be identical before and after.
"""

import os
import unittest

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_MIGRATION_DATABASE_URL"))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_MIGRATION_DATABASE_URL (kb_pipeline_test) to run")
class GoldBridgePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        cls.migrator = create_engine(os.environ["KB_MIGRATION_DATABASE_URL"], poolclass=NullPool)
        admin_url = os.environ.get("KB_ADMIN_DATABASE_URL", "").strip()
        cls.admin = create_engine(admin_url, poolclass=NullPool) if admin_url else None

    @classmethod
    def tearDownClass(cls) -> None:
        cls.migrator.dispose()
        if cls.admin is not None:
            cls.admin.dispose()

    def read_only(self, fn):
        with self.migrator.connect() as conn:
            conn.exec_driver_sql("BEGIN READ ONLY")
            try:
                return fn(conn)
            finally:
                conn.rollback()

    def stage_a_present(self) -> bool:
        from sqlalchemy import text

        from app.processing.gold_bridge import spec

        ids = [c.dataset.id for c in spec.CHANNELS.values()]
        return self.read_only(lambda c: c.execute(text("SELECT count(*) FROM public.datasets WHERE id = ANY (:ids)"),
                                                  {"ids": ids}).scalar_one()) == 2

    def test_guards_and_exact_schema_contract(self) -> None:
        from app.processing.gold_bridge import bootstrap as b

        def check(conn):
            b.verify_target(conn, "kb_pipeline_test", stage="migrator")
            b.verify_public_schema(conn)
            for bad in ("data_collection_pipeline", "kb_pipeline_prod"):
                with self.assertRaises(b.BridgeRefused):
                    b.verify_target(conn, bad, stage="migrator")
            with self.assertRaises(b.BridgeRefused):  # the migrator is never an admin
                b.verify_target(conn, "kb_pipeline_test", stage="admin")

        self.read_only(check)

    def test_admin_stage_twice(self) -> None:
        if self.admin is None:
            self.skipTest("KB_ADMIN_DATABASE_URL not provided (Stage A is an operator step)")
        from app.processing.gold_bridge.bootstrap import bootstrap_public

        results = []
        for _ in range(2):
            with self.admin.begin() as conn:
                results.append(bootstrap_public(conn, expect_database="kb_pipeline_test"))
        self.assertEqual(set(results[1]["actions"].values()), {"reused"})
        self.assertEqual(results[1]["before"], results[1]["after"])

    def test_map_stage_rerun_resolution_and_unchanged_privileges(self) -> None:
        if not self.stage_a_present():
            self.skipTest("Stage A rows are not provisioned yet (run admin-bootstrap as the operator)")
        from sqlalchemy import text

        from app.processing.gold_bridge import bootstrap as b
        from app.processing.gold_bridge import spec
        from app.processing.gold_bridge.cli import RESOLVE_SQL, fingerprint

        before = self.read_only(fingerprint)
        results = []
        for _ in range(2):
            with self.migrator.begin() as conn:
                results.append(b.bootstrap_map(conn, expect_database="kb_pipeline_test"))
        self.assertEqual(set(results[1]["actions"].values()), {"reused"})
        self.assertEqual(results[1]["after"], {"dataset_sources": 2, "datasets": 2, "source_dataset_map": 2})
        self.assertEqual(self.read_only(fingerprint), before)  # privileges, owners and schema unchanged

        rows = self.read_only(lambda c: [dict(r) for r in c.execute(text(RESOLVE_SQL)).mappings()])
        self.assertEqual({r["source_channel"] for r in rows}, {"common_crawl", "github"})
        for row in rows:
            with self.subTest(row["source_channel"]):
                self.assertEqual(row["dataset_id"], spec.FROZEN_DATASET_IDS[row["source_channel"]])


if __name__ == "__main__":
    unittest.main()

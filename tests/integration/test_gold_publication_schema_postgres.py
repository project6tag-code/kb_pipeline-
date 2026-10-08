"""Opt-in runtime privilege and integrity checks for Migration 001D.

Runs only against kb_pipeline_test. The publication INSERT is rolled back so
this schema test never records a Gold publication.
"""

from __future__ import annotations

import os
import unittest
import uuid
from datetime import datetime, timezone

RUN = os.environ.get("KB_RUN_PG_INTEGRATION") == "1" and bool(os.environ.get("KB_DATABASE_URL"))


@unittest.skipUnless(RUN, "set KB_RUN_PG_INTEGRATION=1 and KB_DATABASE_URL for kb_pipeline_test")
class GoldPublicationSchemaPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from app.config.settings import DatabaseSettings
        from app.db.connection import create_runtime_engine

        cls.engine = create_runtime_engine(DatabaseSettings.from_env(application_name="kb_pipeline:001d_test"))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()

    def _check_identity(self, connection) -> None:
        database, role, revision = connection.exec_driver_sql(
            "SELECT current_database(), current_user, (SELECT version_num FROM kbp.alembic_version)"
        ).one()
        self.assertEqual((database, role, revision), ("kb_pipeline_test", "kb_pipeline_app", "kbp_001e"))

    def test_runtime_select_insert_and_pair_uniqueness_rollback(self) -> None:
        from sqlalchemy import text
        from sqlalchemy.exc import IntegrityError

        with self.engine.connect() as connection:
            connection.exec_driver_sql("BEGIN")
            self._check_identity(connection)
            baseline = connection.execute(text("SELECT count(*) FROM kbp.gold_publications")).scalar_one()
            pair = connection.execute(text(
                "SELECT e.chunk_id, e.embedding_model_id FROM kbp.chunk_embeddings e "
                "WHERE e.status = 'stored_shadow' AND NOT EXISTS ("
                "SELECT 1 FROM kbp.gold_publications p WHERE p.chunk_id = e.chunk_id "
                "AND p.embedding_model_id = e.embedding_model_id) ORDER BY e.chunk_id LIMIT 1"
            )).one_or_none()
            already_published = pair is None
            if already_published:  # ledger already populated by a real publication: test uniqueness on a published pair
                pair = connection.execute(text(
                    "SELECT chunk_id, embedding_model_id FROM kbp.gold_publications ORDER BY chunk_id LIMIT 1"
                )).one_or_none()
            run_id = connection.execute(text("SELECT run_id FROM kbp.pipeline_runs ORDER BY started_at LIMIT 1")) \
                .scalar_one_or_none()
            self.assertIsNotNone(pair, "expected a stored_shadow or published chunk/model pair")
            self.assertIsNotNone(run_id, "expected a pipeline run for the run_id FK")
            params = {"chunk_id": pair[0], "embedding_model_id": pair[1],
                      "published_at": datetime.now(timezone.utc), "run_id": run_id}
            if not already_published:
                connection.execute(text(
                    "INSERT INTO kbp.gold_publications (chunk_id, embedding_model_id, published_at, run_id) "
                    "VALUES (:chunk_id, :embedding_model_id, :published_at, :run_id)"
                ), params)
            selected = connection.execute(text(
                "SELECT published_at, run_id FROM kbp.gold_publications "
                "WHERE chunk_id = :chunk_id AND embedding_model_id = :embedding_model_id"
            ), params).one()
            if not already_published:
                self.assertEqual(selected[1], run_id)

            savepoint = connection.begin_nested()
            with self.assertRaises(IntegrityError) as ctx:
                connection.execute(text(
                    "INSERT INTO kbp.gold_publications (chunk_id, embedding_model_id, published_at, run_id) "
                    "VALUES (:chunk_id, :embedding_model_id, :published_at, :run_id)"
                ), params)
            self.assertEqual(getattr(ctx.exception.orig, "sqlstate", None), "23505")
            savepoint.rollback()
            connection.rollback()

        with self.engine.connect() as connection:
            self._check_identity(connection)
            self.assertEqual(connection.execute(text("SELECT count(*) FROM kbp.gold_publications")).scalar_one(), baseline)

    def test_runtime_update_delete_truncate_are_denied(self) -> None:
        from sqlalchemy import text
        from sqlalchemy.exc import DBAPIError

        operations = (
            "UPDATE kbp.gold_publications SET published_at = published_at WHERE false",
            "DELETE FROM kbp.gold_publications WHERE false",
            "TRUNCATE TABLE kbp.gold_publications",
        )
        for sql in operations:
            with self.subTest(sql=sql), self.engine.connect() as connection:
                connection.exec_driver_sql("BEGIN")
                self._check_identity(connection)
                with self.assertRaises(DBAPIError) as ctx:
                    connection.execute(text(sql))
                self.assertEqual(getattr(ctx.exception.orig, "sqlstate", None), "42501")
                connection.rollback()

    def test_runtime_metadata_conflict_verification_privileges(self) -> None:
        from sqlalchemy import text

        metadata_columns = ("id", "source_id", "source_url", "source_title", "source_type", "domain",
                            "published_at", "updated_at")
        with self.engine.connect() as connection:
            connection.exec_driver_sql("BEGIN READ ONLY")
            self._check_identity(connection)
            for column in metadata_columns:
                self.assertTrue(connection.execute(text(
                    "SELECT has_column_privilege(current_user, 'public.metadata', :column, 'SELECT')"
                ), {"column": column}).scalar_one(), column)
            self.assertTrue(connection.execute(text(
                "SELECT has_table_privilege(current_user, 'public.metadata', 'INSERT')"
            )).scalar_one())
            for privilege in ("UPDATE", "DELETE", "TRUNCATE"):
                self.assertFalse(connection.execute(text(
                    "SELECT has_table_privilege(current_user, 'public.metadata', :privilege)"
                ), {"privilege": privilege}).scalar_one(), privilege)
            connection.execute(text(
                "SELECT id, source_id, source_url, source_title, source_type, domain, published_at, updated_at "
                "FROM public.metadata LIMIT 0"
            ))
            connection.rollback()

    def test_runtime_metadata_privileges_cover_conflict_verification(self) -> None:
        from sqlalchemy import text

        columns = ("id", "source_id", "source_url", "source_title", "source_type", "domain",
                   "published_at", "updated_at")
        with self.engine.connect() as connection:
            connection.exec_driver_sql("BEGIN READ ONLY")
            self._check_identity(connection)
            for column in columns:
                self.assertTrue(connection.execute(text(
                    "SELECT has_column_privilege(current_user, 'public.metadata', :column, 'SELECT')"
                ), {"column": column}).scalar_one(), column)
            self.assertTrue(connection.execute(text(
                "SELECT has_table_privilege(current_user, 'public.metadata', 'INSERT')"
            )).scalar_one())
            for privilege in ("UPDATE", "DELETE", "TRUNCATE"):
                self.assertFalse(connection.execute(text(
                    "SELECT has_table_privilege(current_user, 'public.metadata', :privilege)"
                ), {"privilege": privilege}).scalar_one(), privilege)
            connection.execute(text(
                "SELECT id, source_id, source_url, source_title, source_type, domain, published_at, updated_at "
                "FROM public.metadata LIMIT 0"
            ))
            connection.rollback()

    def test_orphan_chunk_model_pair_is_refused(self) -> None:
        from sqlalchemy import text
        from sqlalchemy.exc import IntegrityError

        with self.engine.connect() as connection:
            connection.exec_driver_sql("BEGIN")
            self._check_identity(connection)
            run_id = connection.execute(text("SELECT run_id FROM kbp.pipeline_runs ORDER BY started_at LIMIT 1")) \
                .scalar_one_or_none()
            self.assertIsNotNone(run_id)
            with self.assertRaises(IntegrityError) as ctx:
                connection.execute(text(
                    "INSERT INTO kbp.gold_publications (chunk_id, embedding_model_id, published_at, run_id) "
                    "VALUES (:chunk_id, 2, now(), :run_id)"
                ), {"chunk_id": uuid.UUID(int=0), "run_id": run_id})
            self.assertEqual(getattr(ctx.exception.orig, "sqlstate", None), "23503")
            connection.rollback()


if __name__ == "__main__":
    unittest.main()
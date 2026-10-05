"""Offline tests for the admin-only, additive public.metadata compatibility patch."""

import unittest
import uuid

from app.processing.gold_bridge import metadata_patch as patch


class Result:
    def __init__(self, rows):
        self.rows = rows

    def one(self):
        return self.rows[0]

    def scalar_one(self):
        return self.rows[0][0]

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def __iter__(self):
        return iter(self.rows)


class FakeMetadataDatabase:
    def __init__(self, *, database="kb_pipeline_test", role="postgres", superuser=True, revision="kbp_001d",
                 columns=None):
        self.database, self.role, self.superuser, self.revision = database, role, superuser, revision
        self.columns = list(columns or patch.BASE_COLUMNS)
        self.owner = "postgres"
        self.acl = "{postgres=arwdDxtm/postgres,kb_pipeline_app=a/postgres}"
        self.constraints = set(patch.EXPECTED_CONSTRAINTS)
        self.triggers = 0
        self.existing_rows = [(str(uuid.UUID(int=1)), str(uuid.UUID(int=2)), "science")]
        self.executed = []
        self.mutate_other_acl_on_alter = False
        self.other_acl_changed = False

    def _inventory(self):
        cols = ",".join(f"{name}:{data_type}:{str(not_null).lower()}:false"
                         for name, data_type, not_null in self.columns)
        metadata = ("metadata", "r", self.owner, self.acl, cols,
                    ";".join(sorted(c[0] for c in self.constraints)), "metadata_pkey", self.triggers)
        other = [
            ("kb", "r", "postgres", "legacy-kb-acl", "id:uuid:true:false,chunk:text:true:false,embedding:vector(768):false:false",
             "kb_pkey;kb_id_fkey", "kb_pkey", 0),
            ("datasets", "r", "postgres", "legacy-datasets-acl", "id:uuid:true:false", "datasets_pkey", "datasets_pkey", 0),
            ("dataset_sources", "r", "postgres", "legacy-sources-acl", "id:uuid:true:false", "dataset_sources_pkey",
             "dataset_sources_pkey", 0),
        ]
        if self.other_acl_changed:
            other[1] = (*other[1][:3], "changed-acl", *other[1][4:])
        return [metadata, *other]

    def execute(self, clause, params=None):
        sql = clause.text if hasattr(clause, "text") else str(clause)
        self.executed.append(sql)
        if sql == patch.IDENTITY_SQL:
            return Result([{"database": self.database, "role": self.role, "superuser": self.superuser,
                            "metadata_owner": self.owner, "has_metadata": True, "revision": self.revision}])
        if sql in patch.SESSION_GUARDS:
            return Result([])
        if sql == patch.COLUMNS_SQL:
            return Result([(name, data_type, not_null, False, "", "") for name, data_type, not_null in self.columns])
        if sql == patch.CONSTRAINTS_SQL:
            return Result(sorted(self.constraints))
        if sql == patch.TRIGGERS_SQL:
            return Result([(self.triggers,)])
        if sql == patch.ACL_SQL:
            return Result([(self.owner, self.acl)])
        if sql == patch.PUBLIC_INVENTORY_SQL:
            return Result(self._inventory())
        if sql == patch.ROW_COUNTS_SQL:
            return Result([{"kb": 0, "metadata": len(self.existing_rows), "datasets": 2, "dataset_sources": 2}])
        if sql == patch.METADATA_DATA_SQL:
            return Result(self.existing_rows)
        for column, statement in patch.ADD_COLUMN_SQL.items():
            if sql == statement:
                if any(name == column for name, *_ in self.columns):
                    raise RuntimeError(f"duplicate column {column}")
                declared = next(col for col in patch.ADDITIVE_COLUMNS if col[0] == column)
                self.columns.append(declared)
                if self.mutate_other_acl_on_alter:
                    self.mutate_other_acl_on_alter = False
                    self.other_acl_changed = True
                return Result([])
        raise AssertionError(f"unexpected SQL: {sql}")


class MetadataPatchGuardTests(unittest.TestCase):
    def test_database_guard_refuses_other_and_legacy_targets(self) -> None:
        cases = (
            (FakeMetadataDatabase(database="data_collection_pipeline"), "data_collection_pipeline"),
            (FakeMetadataDatabase(database="kb_pipeline_test"), "data_collection_pipeline"),
            (FakeMetadataDatabase(database="other"), "other"),
        )
        for db, expected in cases:
            with self.subTest(expected=expected), self.assertRaises(patch.MetadataPatchRefused):
                patch.patch_metadata(db, expect_database=expected)
        self.assertFalse(any(sql.startswith("ALTER") for db, _ in cases for sql in db.executed))

    def test_requires_revision_and_superuser_or_metadata_owner(self) -> None:
        with self.assertRaises(patch.MetadataPatchRefused):
            patch.patch_metadata(FakeMetadataDatabase(revision="kbp_001c"), expect_database="kb_pipeline_test")
        for role in ("kb_pipeline_app", "kb_pipeline_migrator", "other"):
            with self.subTest(role=role), self.assertRaises(patch.MetadataPatchRefused):
                patch.patch_metadata(FakeMetadataDatabase(role=role, superuser=False),
                                     expect_database="kb_pipeline_test")

    def test_wrong_type_or_unexpected_metadata_schema_refuses_before_alter(self) -> None:
        wrong_type = list(patch.BASE_COLUMNS)
        wrong_type[2] = ("domain", "text", False)
        extra_column = list(patch.BASE_COLUMNS) + [("unexpected", "text", False)]
        for columns in (wrong_type, extra_column):
            db = FakeMetadataDatabase(columns=columns)
            with self.assertRaises(patch.MetadataPatchConflict):
                patch.patch_metadata(db, expect_database="kb_pipeline_test")
            self.assertFalse(any(sql.startswith("ALTER") for sql in db.executed))

    def test_first_run_adds_only_five_nullable_columns_and_preserves_state(self) -> None:
        db = FakeMetadataDatabase()
        result = patch.patch_metadata(db, expect_database="kb_pipeline_test")
        self.assertEqual(result["action"], "added")
        self.assertEqual(result["columns_added"], [name for name, *_ in patch.ADDITIVE_COLUMNS])
        self.assertEqual(db.columns, list(patch.FINAL_COLUMNS))
        alters = [sql for sql in db.executed if sql.startswith("ALTER")]
        self.assertEqual(alters, list(patch.ADD_COLUMN_SQL.values()))
        self.assertEqual(result["public_row_counts_before"], result["public_row_counts_after"])
        self.assertTrue(result["constraints_unchanged"])
        self.assertTrue(result["owner_acl_unchanged"])
        self.assertNotEqual(result["public_inventory_sha256_before"], result["public_inventory_sha256_after"])

    def test_second_run_reuses_columns_without_ddl(self) -> None:
        db = FakeMetadataDatabase(columns=patch.FINAL_COLUMNS)
        result = patch.patch_metadata(db, expect_database="kb_pipeline_test")
        self.assertEqual(result["action"], "reused")
        self.assertEqual(result["columns_reused"], [name for name, *_ in patch.ADDITIVE_COLUMNS])
        self.assertFalse(any(sql.startswith("ALTER") for sql in db.executed))

    def test_rejects_fingerprint_drift_outside_metadata(self) -> None:
        db = FakeMetadataDatabase()
        db.mutate_other_acl_on_alter = True
        with self.assertRaises(patch.MetadataPatchConflict):
            patch.patch_metadata(db, expect_database="kb_pipeline_test")

    def test_sql_has_no_privilege_or_destructive_statements(self) -> None:
        statements = tuple(patch.ADD_COLUMN_SQL.values())
        self.assertEqual(len(statements), 5)
        for sql in statements:
            with self.subTest(sql=sql):
                self.assertNotRegex(sql.lower(), r"\b(default|drop|rename|owner|grant|revoke|update|delete)\b")


if __name__ == "__main__":
    unittest.main()
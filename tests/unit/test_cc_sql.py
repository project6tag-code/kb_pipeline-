"""Common Crawl acquisition SQL conformance, checked against Migration 001A (no database needed).

Every ``*_SQL`` constant of app.sources.commoncrawl.repository must:
* touch only the kbp tables of the acquisition path, never ``public``, with no
  UPDATE, DELETE or DDL (all four CC tables are insert-only for kb_pipeline_app);
* INSERT only where the runtime role has INSERT, naming existing columns and
  supplying every NOT NULL column that has no default;
* use exactly the schema §20 conflict targets with DO NOTHING;
* write ``source_channel = 'common_crawl'`` and never derive ``source_published_at``.
"""

import re
import unittest

from app.sources.commoncrawl import repository

from tests.unit.test_runtime_sql import DEFAULTS, PRIVILEGES, TABLES, code

STATEMENTS = {name: value for name, value in vars(repository).items()
              if name.endswith("_SQL") and isinstance(value, str)}
ALLOWED_TABLES = {"cc_crawls", "cc_warc_files", "source_observations", "cc_captures", "pipeline_jobs"}
CONFLICT_TARGETS = {
    "cc_crawls": ("crawl_id",),
    "cc_warc_files": ("warc_filename",),
    "source_observations": ("observation_id",),
    "cc_captures": ("observation_id",),
}


class RepositorySqlTests(unittest.TestCase):
    def test_inventory(self) -> None:
        self.assertEqual(set(STATEMENTS), {"INSERT_CRAWL_SQL", "INSERT_WARC_FILE_SQL", "GET_WARC_FILE_SQL",
                                           "EXISTING_CAPTURES_SQL", "INSERT_OBSERVATION_SQL", "INSERT_CAPTURE_SQL",
                                           "PARTITION_SUMMARY_SQL", "CAPTURE_COUNT_SQL"})

    def test_scope_and_no_mutation_beyond_insert(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            with self.subTest(statement=name):
                self.assertNotRegex(c, r"\bpublic\s*\.")
                self.assertTrue(set(re.findall(r"\bkbp\.(\w+)", c)) <= ALLOWED_TABLES)
                self.assertNotRegex(c, r"\b(update|delete|truncate|drop|alter|create|grant|revoke|copy|execute)\b")
                self.assertNotIn("do update", c)
                self.assertEqual(c.count(";"), 0)

    def test_inserts_are_granted_and_columns_complete(self) -> None:
        checked = 0
        for name, sql in STATEMENTS.items():
            for table, columns in re.findall(r"insert into kbp\.(\w+) \(([^)]*)\)", code(sql)):
                checked += 1
                columns = [c.strip() for c in columns.split(",")]
                spec = TABLES[table]["columns"]
                required = {c for c, info in spec.items()
                            if info["not_null"] and not info["identity"] and (table, c) not in DEFAULTS}
                with self.subTest(statement=name, table=table):
                    self.assertTrue(PRIVILEGES[table][0], f"no INSERT on kbp.{table}")
                    self.assertFalse(PRIVILEGES[table][1], f"kbp.{table} unexpectedly updatable")
                    self.assertTrue(set(columns) <= set(spec), set(columns) - set(spec))
                    self.assertTrue(required <= set(columns), required - set(columns))
        self.assertEqual(checked, 4)

    def test_conflict_targets_are_schema_section_20(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            match = re.search(r"insert into kbp\.(\w+).*on conflict \(([^)]*)\) do nothing", c)
            if "insert into" in c:
                with self.subTest(statement=name):
                    self.assertIsNotNone(match, "every insert must be ON CONFLICT ... DO NOTHING")
                    table, cols = match.group(1), tuple(x.strip() for x in match.group(2).split(","))
                    self.assertEqual(cols, CONFLICT_TARGETS[table])
                    self.assertTrue(cols == TABLES[table]["pk"] or cols in TABLES[table]["uniques"])

    def test_capture_natural_key_is_left_to_raise(self) -> None:
        self.assertIn(("warc_file_id", "warc_record_offset"), TABLES["cc_captures"]["uniques"])
        self.assertNotIn("warc_record_offset) do", code(repository.INSERT_CAPTURE_SQL))

    def test_observation_provenance_rules(self) -> None:
        sql = repository.INSERT_OBSERVATION_SQL
        self.assertIn("'common_crawl'", sql)
        self.assertRegex(sql, r":source_title, NULL, :acquired_at")  # source_published_at is never derived (O-9)

    def test_index_kind_is_a_schema_value(self) -> None:
        self.assertIn(repository.INDEX_KIND, ("cdx", "columnar"))


if __name__ == "__main__":
    unittest.main()

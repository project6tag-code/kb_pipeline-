"""Common Crawl acquisition SQL conformance, checked against Migration 001A (no database needed).

Every ``*_SQL`` constant of app.sources.commoncrawl.repository must:
* touch only the kbp tables of the acquisition path, never ``public``, with no
  UPDATE, DELETE or DDL (all four CC tables are insert-only for kb_pipeline_app);
* INSERT only where the runtime role has INSERT, naming existing columns and
  supplying every NOT NULL column that has no default;
* use exactly the schema §20 conflict targets with DO NOTHING;
* write ``source_channel = 'common_crawl'``; ``source_published_at`` is only the
  page's declared date, bound as a parameter (never derived in SQL);
* ``source_dates`` (Migration 001E) is checked against that migration's table.
"""

import re
import unittest
from pathlib import Path

from app.db.migration_safety import load_migration_module
from app.sources.commoncrawl import repository

from tests.unit.test_runtime_sql import DEFAULTS, PRIVILEGES, TABLES, code

STATEMENTS = {name: value for name, value in vars(repository).items()
              if name.endswith("_SQL") and isinstance(value, str)}
ALLOWED_TABLES = {"cc_crawls", "cc_warc_files", "source_observations", "cc_captures", "pipeline_jobs",
                  "source_dates"}
MIGRATION_001E = load_migration_module(Path(__file__).resolve().parents[2] / "migrations" / "versions"
                                       / "kbp_001e_source_dates.py")
SOURCE_DATES_DDL = MIGRATION_001E.CREATE_TABLES[0]
SOURCE_DATES_COLUMNS = {m.group(1): "NOT NULL" in m.group(0)
                        for m in re.finditer(r"^\s+(\w+) (?:uuid|text|timestamptz)[^,]*", SOURCE_DATES_DDL, re.M)}
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
                                           "PARTITION_SUMMARY_SQL", "CAPTURE_COUNT_SQL",
                                           "INSERT_SOURCE_DATES_SQL"})

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
                if table == "source_dates":
                    continue  # Migration 001E: test_source_dates_insert_matches_migration_001e
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
                    if table == "source_dates":
                        self.assertEqual(cols, ("observation_id",))
                        self.assertIn("PRIMARY KEY (observation_id)", SOURCE_DATES_DDL)
                        continue
                    self.assertEqual(cols, CONFLICT_TARGETS[table])
                    self.assertTrue(cols == TABLES[table]["pk"] or cols in TABLES[table]["uniques"])

    def test_capture_natural_key_is_left_to_raise(self) -> None:
        self.assertIn(("warc_file_id", "warc_record_offset"), TABLES["cc_captures"]["uniques"])
        self.assertNotIn("warc_record_offset) do", code(repository.INSERT_CAPTURE_SQL))

    def test_observation_provenance_rules(self) -> None:
        sql = repository.INSERT_OBSERVATION_SQL
        self.assertIn("'common_crawl'", sql)
        # source_published_at: only the date the page itself declared (app.sources.commoncrawl.dates), or NULL
        self.assertRegex(sql, r":source_title, :source_published_at, :acquired_at")

    def test_source_dates_insert_matches_migration_001e(self) -> None:
        columns = re.search(r"insert into kbp\.source_dates \(([^)]*)\)", code(repository.INSERT_SOURCE_DATES_SQL))
        names = [c.strip() for c in columns.group(1).split(",")]
        self.assertEqual(set(names), set(SOURCE_DATES_COLUMNS))
        self.assertEqual({c for c, not_null in SOURCE_DATES_COLUMNS.items() if not_null},
                         {"observation_id", "extractor_version", "crawled_at", "recorded_at"})
        self.assertEqual(MIGRATION_001E.RUNTIME_PRIVILEGES["source_dates"], (True, True, False, False))
        self.assertIn("on conflict (observation_id) do nothing", code(repository.INSERT_SOURCE_DATES_SQL))

    def test_index_kind_is_a_schema_value(self) -> None:
        self.assertIn(repository.INDEX_KIND, ("cdx", "columnar"))


if __name__ == "__main__":
    unittest.main()

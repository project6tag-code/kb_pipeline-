"""GitHub acquisition SQL conformance against Migration 001A (no database needed)."""

import re
import unittest

from app.sources.github import repository

from tests.unit.test_runtime_sql import DEFAULTS, PRIVILEGES, TABLES, code, granted_update_columns, set_clauses

STATEMENTS = {n: v for n, v in vars(repository).items() if n.endswith("_SQL") and isinstance(v, str)}
ALLOWED = {"github_repositories", "github_commits", "github_file_observations", "source_observations",
           "pipeline_jobs"}
TARGETS = {"github_repositories": ("repository_id",), "github_commits": ("repository_id", "commit_sha"),
           "source_observations": ("observation_id",), "github_file_observations": ("observation_id",)}


class GitHubSqlTests(unittest.TestCase):
    def test_scope_no_delete_no_ddl_no_public(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            with self.subTest(statement=name):
                self.assertNotRegex(c, r"\bpublic\s*\.")
                self.assertTrue(set(re.findall(r"\bkbp\.(\w+)", c)) <= ALLOWED)
                self.assertNotRegex(c, r"\b(delete|truncate|drop|alter|create|grant|revoke|copy|execute)\b")

    def test_inserts_granted_columns_exist_and_required_supplied(self) -> None:
        checked = 0
        for name, sql in STATEMENTS.items():
            for table, columns in re.findall(r"insert into kbp\.(\w+) \(([^)]*)\)", code(sql)):
                checked += 1
                columns = [c.strip() for c in columns.split(",")]
                spec = TABLES[table]["columns"]
                required = {c for c, i in spec.items() if i["not_null"] and not i["identity"] and (table, c) not in DEFAULTS}
                with self.subTest(statement=name):
                    self.assertTrue(PRIVILEGES[table][0])
                    self.assertTrue(set(columns) <= set(spec), set(columns) - set(spec))
                    self.assertTrue(required <= set(columns), required - set(columns))
        self.assertEqual(checked, 4)

    def test_updates_touch_only_granted_columns(self) -> None:
        checked = 0
        for name, sql in STATEMENTS.items():
            for table, columns in set_clauses(code(sql)):
                checked += 1
                with self.subTest(statement=name):
                    self.assertEqual(table, "github_repositories")
                    self.assertTrue(set(columns) <= granted_update_columns(table), set(columns) - granted_update_columns(table))
        self.assertEqual(checked, 3)  # upsert, etag, compare-and-set snapshot

    def test_conflict_targets_and_fixed_values(self) -> None:
        for name, sql in STATEMENTS.items():
            match = re.search(r"insert into kbp\.(\w+).*on conflict \(([^)]*)\)", code(sql))
            if match:
                with self.subTest(statement=name):
                    self.assertEqual(tuple(x.strip() for x in match.group(2).split(",")), TARGETS[match.group(1)])
        self.assertIn("'public'", repository.INSERT_COMMIT_SQL)  # visibility_at_snapshot CHECK = 'public'
        self.assertIn("'github'", repository.INSERT_OBSERVATION_SQL)
        self.assertRegex(repository.INSERT_OBSERVATION_SQL, r"'github\.com', NULL, NULL, :acquired_at")
        self.assertNotIn("do update", code(repository.INSERT_FILE_OBSERVATION_SQL))


if __name__ == "__main__":
    unittest.main()

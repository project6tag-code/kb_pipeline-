"""Runtime SQL conformance, checked against Migration 001A (no database needed).

Every ``*_SQL`` constant of the runtime modules must:
* touch only kbp runtime tables (never ``public``), with no DELETE / DDL;
* INSERT/UPDATE only where kb_pipeline_app has the privilege, and UPDATE only
  granted columns (docs/database_privileges_v1.md §5.1);
* name only columns that exist in Schema V1, and supply every NOT NULL
  column that has no default;
* honour the lease invariants (claim via SKIP LOCKED; writes fenced by
  lease_token; lease columns cleared whenever a job leaves ``running``).
"""

import re
import unittest

from app.db import connection, repositories
from app.db.migration_safety import lex
from app.monitoring import health
from app.queue import job_queue
from app.queue.states import JobStatus, RunStatus

import tests.unit.test_migration_001a as m001a

MODULES = (connection, repositories, job_queue, health)
STATEMENTS = {
    f"{module.__name__}.{name}": value
    for module in MODULES
    for name, value in vars(module).items()
    if name.endswith("_SQL") and isinstance(value, str)
}
TABLES = m001a.MIGRATION_TABLES
DEFAULTS = m001a.parse_migration_defaults()
PRIVILEGES = m001a.MODULE.RUNTIME_PRIVILEGES
RUNTIME_TABLES = {"pipeline_runs", "pipeline_jobs", "processing_attempts", "dead_letters", "source_rate_limits",
                  "alembic_version"}
LEASE_COLUMNS = {"worker_id", "lease_token", "locked_at", "lease_expires_at"}


def code(sql: str) -> str:
    return lex(sql).code


def split_top_level(text: str) -> list[str]:
    parts, depth, current = [], 0, []
    for ch in text:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def set_clauses(c: str) -> list[tuple[str, list[str]]]:
    """(table, assigned columns) for every UPDATE ... SET and ON CONFLICT DO UPDATE SET."""
    found = []
    for match in re.finditer(r"update kbp\.(\w+)(?: as \w+)? set (.*?)(?= where | from | returning |$)", c):
        found.append((match.group(1), [a.split("=")[0].strip() for a in split_top_level(match.group(2))]))
    insert_table = re.search(r"insert into kbp\.(\w+)", c)
    for match in re.finditer(r"on conflict \([^)]*\) do update set (.*?)(?= where | returning |$)", c):
        found.append((insert_table.group(1), [a.split("=")[0].strip() for a in split_top_level(match.group(1))]))
    return found


def granted_update_columns(table: str) -> set[str]:
    update = PRIVILEGES[table][1]
    if update == "table":
        return set(TABLES[table]["columns"])
    return set(update or ())


class InventoryTests(unittest.TestCase):
    def test_statements_found(self) -> None:
        self.assertGreaterEqual(len(STATEMENTS), 25)


class ScopeTests(unittest.TestCase):
    def test_only_kbp_runtime_tables_and_no_public(self) -> None:
        for name, sql in STATEMENTS.items():
            c = code(sql)
            with self.subTest(statement=name):
                self.assertNotRegex(c, r"\bpublic\s*\.")
                self.assertTrue(set(re.findall(r"\bkbp\.(\w+)", c)) <= RUNTIME_TABLES)
                self.assertNotRegex(c, r"\b(delete|truncate|drop|alter|create|grant|revoke|copy|execute)\b")

    def test_single_statement_each(self) -> None:
        for name, sql in STATEMENTS.items():
            with self.subTest(statement=name):
                self.assertEqual(lex(sql).top_level_semicolons, 0)


class PrivilegeTests(unittest.TestCase):
    def test_inserts_are_granted(self) -> None:
        for name, sql in STATEMENTS.items():
            for table in re.findall(r"insert into kbp\.(\w+)", code(sql)):
                with self.subTest(statement=name, table=table):
                    self.assertTrue(PRIVILEGES[table][0], f"no INSERT on kbp.{table}")

    def test_updates_touch_only_granted_columns(self) -> None:
        checked = 0
        for name, sql in STATEMENTS.items():
            for table, columns in set_clauses(code(sql)):
                checked += 1
                with self.subTest(statement=name, table=table):
                    self.assertTrue(set(columns) <= granted_update_columns(table),
                                    set(columns) - granted_update_columns(table))
        self.assertGreaterEqual(checked, 14)

    def test_pipeline_runs_only_status_finished_at_notes(self) -> None:
        for name, sql in STATEMENTS.items():
            for table, columns in set_clauses(code(sql)):
                if table == "pipeline_runs":
                    self.assertTrue(set(columns) <= {"status", "finished_at", "notes"}, name)


class SchemaTests(unittest.TestCase):
    def test_insert_columns_exist_and_required_columns_supplied(self) -> None:
        for name, sql in STATEMENTS.items():
            for table, columns in re.findall(r"insert into kbp\.(\w+) \(([^)]*)\)", code(sql)):
                columns = [c.strip() for c in columns.split(",")]
                spec = TABLES[table]["columns"]
                required = {c for c, info in spec.items()
                            if info["not_null"] and not info["identity"] and (table, c) not in DEFAULTS}
                with self.subTest(statement=name, table=table):
                    self.assertTrue(set(columns) <= set(spec), set(columns) - set(spec))
                    self.assertTrue(required <= set(columns), required - set(columns))

    def test_update_columns_exist(self) -> None:
        for name, sql in STATEMENTS.items():
            for table, columns in set_clauses(code(sql)):
                with self.subTest(statement=name):
                    self.assertTrue(set(columns) <= set(TABLES[table]["columns"]))

    def test_column_lists_exist(self) -> None:
        for column_list, table in ((job_queue.JOB_COLUMNS, "pipeline_jobs"),
                                   (repositories.RUN_COLUMNS, "pipeline_runs"),
                                   (repositories.RATE_LIMIT_COLUMNS, "source_rate_limits")):
            with self.subTest(table=table):
                self.assertTrue({c.strip() for c in column_list.split(",")} <= set(TABLES[table]["columns"]))

    def test_status_literals_are_schema_values(self) -> None:
        allowed = {s.value for s in JobStatus} | {s.value for s in RunStatus}
        for name, sql in STATEMENTS.items():
            for group in re.findall(r"status (?:=|in) \(?((?:'[a-z_]+'(?:, )?)+)\)?", sql.lower()):
                with self.subTest(statement=name):
                    self.assertTrue(set(re.findall(r"'([a-z_]+)'", group)) <= allowed)

    def test_conflict_targets_are_schema_unique_keys(self) -> None:
        targets = {
            "pipeline_jobs": ("job_type", "source_identifier"),
            "dead_letters": ("item_type", "item_key", "stage"),
            "source_rate_limits": ("source_channel", "limit_scope", "credential_fingerprint"),
        }
        for name, sql in STATEMENTS.items():
            c = code(sql)
            match = re.search(r"insert into kbp\.(\w+).*on conflict \(([^)]*)\)", c)
            if match:
                table, cols = match.group(1), tuple(x.strip() for x in match.group(2).split(","))
                with self.subTest(statement=name):
                    self.assertEqual(cols, targets[table])
                    self.assertTrue(cols == TABLES[table]["pk"] or cols in TABLES[table]["uniques"])


class LeaseInvariantTests(unittest.TestCase):
    def test_claim_is_atomic_skip_locked_and_ordered(self) -> None:
        c = code(job_queue.CLAIM_JOBS_SQL)
        self.assertIn("for update skip locked", c)
        self.assertIn("order by priority, next_attempt_at, job_id", c)
        self.assertIn("status in ('', '')", c)  # literals are blanked by the lexer
        self.assertIn("next_attempt_at <= now()", c)
        self.assertIn("attempt_count < max_attempts", c)
        self.assertIn("limit :limit", c)
        self.assertIn("lease_token = gen_random_uuid()", c)
        self.assertIn("attempt_count = j.attempt_count + 1", c)
        self.assertIn("status IN ('pending', 'retry_pending')", job_queue.CLAIM_JOBS_SQL)
        # One statement: selection and update cannot interleave with another claimer.
        self.assertTrue(c.startswith("with candidate as ( select job_id from kbp.pipeline_jobs"))

    def test_worker_writes_are_fenced_by_the_lease(self) -> None:
        for name in ("CHECKPOINT_JOB_SQL", "HEARTBEAT_JOB_SQL", "COMPLETE_JOB_SQL", "RESCHEDULE_JOB_SQL",
                     "TERMINATE_JOB_SQL"):
            sql = getattr(job_queue, name)
            with self.subTest(statement=name):
                self.assertIn("WHERE job_id = :job_id AND lease_token = :lease_token AND status = 'running'", sql)
                self.assertIn("RETURNING", sql)  # zero rows -> LeaseLostError
        self.assertIn("lease_token = :lease_token AND finished_at IS NULL", job_queue.FINISH_ATTEMPT_SQL)

    def test_leaving_running_clears_the_lease(self) -> None:
        for name in ("COMPLETE_JOB_SQL", "RESCHEDULE_JOB_SQL", "TERMINATE_JOB_SQL"):
            (_table, columns), = set_clauses(code(getattr(job_queue, name)))
            with self.subTest(statement=name):
                self.assertTrue(LEASE_COLUMNS <= set(columns))
                self.assertIn("status", columns)
                for column in LEASE_COLUMNS:
                    self.assertIn(f"{column} = NULL", getattr(job_queue, name))

    def test_completed_at_matches_the_terminal_check(self) -> None:
        for name in ("COMPLETE_JOB_SQL", "TERMINATE_JOB_SQL", "CANCEL_JOB_SQL"):
            self.assertIn("completed_at = now()", getattr(job_queue, name), name)
        self.assertNotIn("completed_at", job_queue.RESCHEDULE_JOB_SQL)
        self.assertIn("completed_at = NULL", job_queue.REQUEUE_JOB_SQL)

    def test_recovery_locks_with_skip_locked(self) -> None:
        c = code(job_queue.SELECT_EXPIRED_LEASES_SQL)
        self.assertIn("lease_expires_at < now()", c)
        self.assertIn("for update skip locked", c)

    def test_attempt_numbering_counts_every_claim(self) -> None:
        self.assertIn("COALESCE(MAX(a.attempt_no), 0) + 1", job_queue.START_ATTEMPT_SQL)

    def test_deferral_and_release_refund_the_attempt(self) -> None:
        self.assertIn("attempt_count = attempt_count - CAST(:refund_attempt AS integer)", job_queue.RESCHEDULE_JOB_SQL)
        self.assertIn("deferral_count = deferral_count + CAST(:add_deferral AS integer)", job_queue.RESCHEDULE_JOB_SQL)


if __name__ == "__main__":
    unittest.main()

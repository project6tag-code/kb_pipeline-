"""Offline verification of Migration 001A against the LOCKED specifications.

Expected values are parsed from docs/database_schema_v1.md and
docs/database_privileges_v1.md, not copied from the migration, so the tests
fail if the migration drifts from the locked documents. No database is used.
"""

import re
import unittest
from pathlib import Path

from app.db.migration_safety import check_statements, collect_sql, load_migration_module

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "kbp_001a_schema_v1.py"
SCHEMA_DOC = (ROOT / "docs" / "database_schema_v1.md").read_text(encoding="utf-8")
PRIVILEGES_DOC = (ROOT / "docs" / "database_privileges_v1.md").read_text(encoding="utf-8")

COLLECTED = collect_sql(MIGRATION)
UPGRADE: list[str] = COLLECTED["upgrade"]  # type: ignore[assignment]
DOWNGRADE: list[str] = COLLECTED["downgrade"]  # type: ignore[assignment]
MODULE = load_migration_module(MIGRATION)

PROTECTED = ("public.kb", "public.metadata", "public.datasets", "public.dataset_sources")


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #

def _split_top_level(body: str) -> list[str]:
    parts, depth, quote, current = [], 0, False, []
    for ch in body:
        if ch == "'":
            quote = not quote
        elif not quote and ch == "(":
            depth += 1
        elif not quote and ch == ")":
            depth -= 1
        if ch == "," and depth == 0 and not quote:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if "".join(current).strip():
        parts.append("".join(current).strip())
    return parts


def _cols(text: str) -> tuple[str, ...]:
    return tuple(c.strip() for c in text.strip().strip("()").split(","))


def parse_migration_tables() -> dict[str, dict]:
    tables: dict[str, dict] = {}
    for sql in UPGRADE:
        match = re.match(r"\s*CREATE TABLE kbp\.(\w+) \((.*)\)\s*$", sql, re.S)
        if not match:
            continue
        name, body = match.group(1), match.group(2)
        columns: dict[str, dict] = {}
        pk: tuple[str, ...] = ()
        uniques: set[tuple[str, ...]] = set()
        fks: set[tuple[tuple[str, ...], str]] = set()
        for item in _split_top_level(body):
            head = item.split()[0].upper()
            if head == "PRIMARY":
                pk = _cols(re.search(r"PRIMARY KEY\s*(\([^)]*\))", item).group(1))
            elif head == "UNIQUE":
                uniques.add(_cols(re.search(r"UNIQUE\s*(\([^)]*\))", item).group(1)))
            elif head == "FOREIGN":
                m = re.search(r"FOREIGN KEY\s*(\([^)]*\))\s*REFERENCES kbp\.(\w+)", item)
                fks.add((_cols(m.group(1)), m.group(2)))
            elif head == "CHECK":
                continue
            else:
                col = item.split()[0]
                identity = "GENERATED ALWAYS AS IDENTITY" in item
                sql_type = item.split()[1]
                is_pk = "PRIMARY KEY" in item
                if is_pk:
                    pk = (col,)
                if re.search(r"\bUNIQUE\b", re.sub(r"'[^']*'", "''", item)):
                    uniques.add((col,))
                ref = re.search(r"REFERENCES kbp\.(\w+)", item)
                if ref:
                    fks.add(((col,), ref.group(1)))
                columns[col] = {
                    "type": sql_type,
                    "identity": identity,
                    "not_null": "NOT NULL" in item or is_pk,
                }
        for col in pk:
            columns[col]["not_null"] = True
        tables[name] = {"columns": columns, "pk": pk, "uniques": uniques, "fks": fks}
    return tables


def parse_spec_tables() -> dict[str, dict[str, dict]]:
    spec: dict[str, dict[str, dict]] = {}
    sections = re.split(r"^### ", SCHEMA_DOC, flags=re.M)
    for section in sections:
        header = re.match(r"5\.\d+ `kbp\.(\w+)`", section)
        if not header:
            continue
        columns: dict[str, dict] = {}
        for line in section.splitlines():
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) >= 3 and cells[2] in ("NO", "YES") and cells[0] != "Column":
                type_cell = cells[1]
                columns[cells[0]] = {
                    "type": type_cell.split()[0],
                    "identity": "GENERATED ALWAYS AS IDENTITY" in type_cell,
                    "not_null": cells[2] == "NO",
                }
        spec[header.group(1)] = columns
    return spec


def parse_spec_keys() -> dict[str, tuple[tuple[str, ...], set[tuple[str, ...]]]]:
    block = SCHEMA_DOC.split("### 6.1 Primary and unique keys", 1)[1].split("### 6.2", 1)[0]
    keys = {}
    for line in block.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 3 or cells[0] in ("Table", "---"):
            continue
        pk = _cols(cells[1])
        uniques = set()
        if cells[2] != "—":
            for part in cells[2].split(";"):
                part = part.strip()
                if part and not part.startswith("partial"):
                    uniques.add(_cols(part))
        keys[cells[0]] = (pk, uniques)
    return keys


def parse_spec_fks(tables: dict[str, dict]) -> set[tuple[str, tuple[str, ...], str]]:
    block = SCHEMA_DOC.split("### 6.2 Foreign keys", 1)[1].split("### 6.3", 1)[0]
    within = block.split("**From `kbp` into `public`", 1)[0]
    fks = set()
    for left, right in re.findall(r"`([a-z_]+\.(?:\([^)]*\)|[a-z_]+)) → ([a-z_]+)`", within):
        table, cols = left.split(".", 1)
        fks.add((table, _cols(cols), right))
    if "all `run_id → pipeline_runs`" in within:
        for table, info in tables.items():
            if table != "pipeline_runs" and "run_id" in info["columns"]:
                fks.add((table, ("run_id",), "pipeline_runs"))
    return fks


def parse_privilege_matrix() -> dict[str, dict]:
    block = PRIVILEGES_DOC.split("### 5.1 Exact runtime privilege matrix", 1)[1].split("### 5.2", 1)[0]
    matrix = {}
    for line in block.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 5 or not cells[0].startswith("`"):
            continue
        table = re.match(r"`(\w+)`", cells[0]).group(1)
        update_cell = cells[3]
        if update_cell == "—":
            update: object = None
        elif "table-level" in update_cell:
            update = "table"
        else:
            update = tuple(sorted(_cols(re.search(r"`(\([^`]*\))`", update_cell).group(1))))
        matrix[table] = {
            "select": cells[1] == "✅",
            "insert": cells[2] == "✅",
            "update": update,
            "delete": cells[4] == "✅",
        }
    return matrix


def parse_spec_defaults() -> dict[tuple[str, str], str]:
    """Column defaults stated literally in schema §5.

    Two sources only: a Rationale cell starting with "default <value>", and the
    §5 convention "`created_at`-style columns are `NOT NULL DEFAULT now()`",
    applied to columns literally named ``created_at``. Other timestamp columns
    get no default (review of interpretation 1, docs/migrations_kbp.md §3.1).
    """
    defaults: dict[tuple[str, str], str] = {}
    for section in re.split(r"^### ", SCHEMA_DOC, flags=re.M):
        header = re.match(r"5\.\d+ `kbp\.(\w+)`", section)
        if not header:
            continue
        for line in section.splitlines():
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 4 or cells[2] not in ("NO", "YES") or cells[0] == "Column":
                continue
            stated = re.match(r"default (`[^`]*`|[^\s.]+)", cells[3])
            if stated:
                defaults[(header.group(1), cells[0])] = stated.group(1).strip("`")
            elif cells[0] == "created_at":
                defaults[(header.group(1), cells[0])] = "now()"
    return defaults


def parse_migration_defaults() -> dict[tuple[str, str], str]:
    defaults: dict[tuple[str, str], str] = {}
    for sql in UPGRADE:
        match = re.match(r"\s*CREATE TABLE kbp\.(\w+) \((.*)\)\s*$", sql, re.S)
        if not match:
            continue
        for item in _split_top_level(match.group(2)):
            found = re.search(r"\bDEFAULT\s+('[^']*'(?:::\w+)?|[^\s,]+)", item)
            if found:
                defaults[(match.group(1), item.split()[0])] = found.group(1).removesuffix("::jsonb")
    return defaults


def parse_migration_in_lists() -> dict[tuple[str, str], tuple[str, ...]]:
    """Column-level ``CHECK (col IN (...))`` value lists, per (table, column)."""
    lists: dict[tuple[str, str], tuple[str, ...]] = {}
    for sql in UPGRADE:
        match = re.match(r"\s*CREATE TABLE kbp\.(\w+) \((.*)\)\s*$", sql, re.S)
        if not match:
            continue
        for item in _split_top_level(match.group(2)):
            column = item.split()[0]
            found = re.search(rf"CHECK \({column} IN \(([^)]*)\)\)", item)
            if found:
                lists[(match.group(1), column)] = tuple(re.findall(r"'([^']*)'", found.group(1)))
    return lists


MIGRATION_TABLES = parse_migration_tables()
SPEC_TABLES = parse_spec_tables()


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

class TableSetTests(unittest.TestCase):
    def test_spec_defines_25_tables(self) -> None:
        self.assertEqual(len(SPEC_TABLES), 25)

    def test_migration_creates_exactly_the_25_spec_tables(self) -> None:
        self.assertEqual(set(MIGRATION_TABLES), set(SPEC_TABLES))
        self.assertEqual(len(MIGRATION_TABLES), 25)
        self.assertEqual(set(MODULE.KBP_TABLES), set(SPEC_TABLES))
        self.assertEqual(len(MODULE.KBP_TABLES), len(set(MODULE.KBP_TABLES)))

    def test_every_create_is_kbp_qualified(self) -> None:
        creates = [s for s in UPGRADE if re.match(r"\s*CREATE ", s)]
        self.assertTrue(creates)
        for sql in creates:
            with self.subTest(sql=sql.split("(")[0].strip()):
                self.assertRegex(sql, r"^\s*CREATE (TABLE|UNIQUE INDEX \w+ ON|INDEX \w+ ON) kbp\.")


class ColumnFidelityTests(unittest.TestCase):
    def test_columns_types_nullability_match_spec(self) -> None:
        for table, spec_columns in SPEC_TABLES.items():
            migration_columns = MIGRATION_TABLES[table]["columns"]
            with self.subTest(table=table):
                self.assertEqual(list(migration_columns), list(spec_columns), "column set/order")
                for column, spec in spec_columns.items():
                    self.assertEqual(migration_columns[column], spec, f"{table}.{column}")


class DefaultFidelityTests(unittest.TestCase):
    def test_defaults_match_literal_spec(self) -> None:
        expected = parse_spec_defaults()
        self.assertEqual(len([k for k, v in expected.items() if v == "now()"]), 8)
        self.assertEqual(parse_migration_defaults(), expected)

    def test_unspecified_timestamp_columns_have_no_default(self) -> None:
        actual = parse_migration_defaults()
        for key in (
            ("cc_crawls", "registered_at"), ("cc_warc_files", "registered_at"),
            ("github_repositories", "first_seen_at"), ("dead_letters", "first_seen_at"),
            ("embedding_queue", "enqueued_at"), ("pipeline_jobs", "updated_at"),
            ("source_rate_limits", "updated_at"), ("legacy_kb_reconciliation", "first_reconciled_at"),
        ):
            with self.subTest(column=key):
                self.assertNotIn(key, actual)
                self.assertTrue(MIGRATION_TABLES[key[0]]["columns"][key[1]]["not_null"])


class ControlledListTests(unittest.TestCase):
    """Interpretations 2 and 3: the §17.2 and §16.3 lists, parsed from the doc."""

    def test_error_class_list_on_the_four_error_columns(self) -> None:
        block = SCHEMA_DOC.split("### 17.2 Error classes (`last_error_class`, `error_class` CHECK list)", 1)[1]
        classes = tuple(re.findall(r"^\| `(\w+)` \|", block.split("### 17.3", 1)[0], flags=re.M))
        self.assertEqual(len(classes), 8)
        lists = parse_migration_in_lists()
        error_columns = {k for k in lists if k[1] in ("error_class", "last_error_class")}
        self.assertEqual(error_columns, {
            ("pipeline_jobs", "last_error_class"), ("embedding_queue", "last_error_class"),
            ("processing_attempts", "error_class"), ("dead_letters", "error_class"),
        })
        for key in error_columns:
            with self.subTest(column=key):
                self.assertEqual(lists[key], classes)

    def test_stage_list_on_both_stage_columns(self) -> None:
        block = SCHEMA_DOC.split("### 16.3 Stages (`pipeline_jobs.stage`, `dead_letters.stage`)", 1)[1]
        stages = tuple(re.findall(r"`([A-Z_]+)`", block.split("The allowed stage values are:", 1)[1]
                                  .split("The brief's failure states", 1)[0]))
        self.assertEqual(len(stages), 18)
        lists = parse_migration_in_lists()
        self.assertEqual({k for k in lists if k[1] == "stage"},
                         {("pipeline_jobs", "stage"), ("dead_letters", "stage")})
        for key in (("pipeline_jobs", "stage"), ("dead_letters", "stage")):
            with self.subTest(column=key):
                self.assertEqual(lists[key], stages)


class KeyAndForeignKeyTests(unittest.TestCase):
    def test_primary_and_unique_keys_match_spec(self) -> None:
        spec_keys = parse_spec_keys()
        self.assertEqual(set(spec_keys), set(SPEC_TABLES))
        for table, (pk, uniques) in spec_keys.items():
            with self.subTest(table=table):
                self.assertEqual(MIGRATION_TABLES[table]["pk"], pk)
                self.assertEqual(MIGRATION_TABLES[table]["uniques"], uniques)

    def test_foreign_keys_match_spec(self) -> None:
        expected = parse_spec_fks(MIGRATION_TABLES)
        actual = {
            (table, cols, ref)
            for table, info in MIGRATION_TABLES.items()
            for cols, ref in info["fks"]
        }
        self.assertEqual(actual, expected)

    def test_only_cross_schema_fk_is_approved_bridge_fk(self) -> None:
        cross = [s for s in UPGRADE if "public." in s.lower()]
        self.assertEqual(cross, [MODULE.CROSS_SCHEMA_FK[0]])
        self.assertIn("REFERENCES public.datasets (id)", cross[0])
        self.assertIn("NOT VALID", cross[0])
        self.assertIn(MODULE.CROSS_SCHEMA_FK[1], UPGRADE)
        self.assertLess(UPGRADE.index(MODULE.CROSS_SCHEMA_FK[0]), UPGRADE.index(MODULE.CROSS_SCHEMA_FK[1]))


class IndexTests(unittest.TestCase):
    def test_required_initial_operation_indexes_present(self) -> None:
        block = SCHEMA_DOC.split("### 21.2 REQUIRED FOR INITIAL OPERATIONS", 1)[1].split("Deliberately", 1)[0]
        rows = re.findall(r"^\| ([a-z_]+) `([^`]+)` \|", block, flags=re.M)
        self.assertEqual(len(rows), 7)
        indexes = [re.sub(r"\s+", "", s.lower()) for s in UPGRADE if "INDEX" in s and s.startswith("CREATE")]
        for table, definition in rows:
            wanted = f"onkbp.{table}" + re.sub(r"\s+", "", definition.lower())
            with self.subTest(table=table, definition=definition):
                self.assertTrue(any(ix.endswith(wanted) for ix in indexes), wanted)

    def test_single_active_gold_partial_unique_index(self) -> None:
        self.assertTrue(any(
            s.startswith("CREATE UNIQUE INDEX") and "kbp.embedding_models (status)" in s
            and "WHERE status = 'active_gold'" in s
            for s in UPGRADE
        ))

    def test_no_ann_or_vector_objects(self) -> None:
        joined = "\n".join(UPGRADE).lower()
        for token in ("hnsw", "ivfflat", "vector(", "halfvec(", "create extension"):
            with self.subTest(token=token):
                self.assertNotIn(token, joined)


class GrantTests(unittest.TestCase):
    def test_grants_match_locked_privilege_matrix(self) -> None:
        matrix = parse_privilege_matrix()
        self.assertEqual(set(matrix) - {"alembic_version"}, set(SPEC_TABLES))
        grants = [s for s in UPGRADE if s.startswith("GRANT")]
        for table in SPEC_TABLES:
            expected = matrix[table]
            table_grants = [g for g in grants if g.endswith(f" ON TABLE kbp.{table} TO kb_pipeline_app")]
            with self.subTest(table=table):
                self.assertEqual(any(g.startswith("GRANT SELECT ") for g in table_grants), expected["select"])
                self.assertEqual(any(g.startswith("GRANT INSERT ") for g in table_grants), expected["insert"])
                self.assertEqual(any(g.startswith("GRANT DELETE ") for g in table_grants), expected["delete"])
                updates = [g for g in table_grants if g.startswith("GRANT UPDATE")]
                if expected["update"] is None:
                    self.assertEqual(updates, [])
                elif expected["update"] == "table":
                    self.assertEqual(updates, [f"GRANT UPDATE ON TABLE kbp.{table} TO kb_pipeline_app"])
                else:
                    self.assertEqual(len(updates), 1)
                    cols = re.search(r"GRANT UPDATE \(([^)]*)\)", updates[0]).group(1)
                    self.assertEqual(tuple(sorted(_cols(cols))), expected["update"])

    def test_alembic_version_select_only(self) -> None:
        grants = [s for s in UPGRADE if "kbp.alembic_version" in s]
        self.assertEqual(grants, ["GRANT SELECT ON TABLE kbp.alembic_version TO kb_pipeline_app"])

    def test_no_forbidden_grants(self) -> None:
        for sql in UPGRADE:
            upper = sql.upper()
            with self.subTest(sql=sql[:80]):
                for bad in ("TRUNCATE", "REFERENCES ON", "TRIGGER", "ALL PRIVILEGES", "TO PUBLIC",
                            "WITH GRANT OPTION", "ALTER DEFAULT PRIVILEGES", "CREATE ROLE", "OWNER TO"):
                    if upper.startswith("GRANT") or bad in ("ALTER DEFAULT PRIVILEGES", "CREATE ROLE", "OWNER TO"):
                        self.assertNotIn(bad, upper)
        deletes = [s for s in UPGRADE if s.startswith("GRANT DELETE")]
        self.assertEqual(deletes, ["GRANT DELETE ON TABLE kbp.embedding_queue TO kb_pipeline_app"])


class BridgeBootstrapAbsentTests(unittest.TestCase):
    def test_no_dml_at_all(self) -> None:
        for phase, statements in (("upgrade", UPGRADE), ("downgrade", DOWNGRADE)):
            for sql in statements:
                with self.subTest(phase=phase, sql=sql[:60]):
                    self.assertNotRegex(sql.strip().upper(), r"^(INSERT|UPDATE|DELETE|MERGE|COPY|TRUNCATE)\b")
                    self.assertNotIn("INSERT INTO", sql.upper())

    def test_frozen_bridge_ids_absent(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8").lower()
        for bridge_id in ("a7d8d2e1-8400-5916-9440-b492e8e692ce", "b487f559-5f8e-56ba-9746-28658b9d09d1"):
            self.assertNotIn(bridge_id, text)

    def test_source_dataset_map_left_empty(self) -> None:
        for sql in UPGRADE:
            if "source_dataset_map" in sql:
                self.assertRegex(sql.strip(), r"^(CREATE TABLE|ALTER TABLE|GRANT SELECT)")


class PublicSchemaUntouchedTests(unittest.TestCase):
    def _statements_mentioning(self, obj: str) -> list[str]:
        pattern = re.compile(rf"\b{re.escape(obj)}\b(?!_)", re.I)
        return [s for s in UPGRADE + DOWNGRADE if pattern.search(s.replace('"', ""))]

    def test_public_kb_not_modified(self) -> None:
        self.assertEqual(self._statements_mentioning("public.kb"), [])

    def test_public_metadata_not_modified(self) -> None:
        self.assertEqual(self._statements_mentioning("public.metadata"), [])

    def test_public_dataset_sources_not_modified(self) -> None:
        self.assertEqual(self._statements_mentioning("public.dataset_sources"), [])

    def test_public_datasets_only_referenced_by_approved_fk(self) -> None:
        mentions = self._statements_mentioning("public.datasets")
        self.assertEqual(mentions, [MODULE.CROSS_SCHEMA_FK[0]])
        self.assertTrue(mentions[0].startswith("ALTER TABLE kbp.source_dataset_map "))

    def test_public_alembic_version_never_targeted(self) -> None:
        self.assertEqual(self._statements_mentioning("public.alembic_version"), [])
        qualified_public = [
            s for s in UPGRADE + DOWNGRADE if re.search(r"\bpublic\s*\.", s.replace('"', ""), re.I)
        ]
        self.assertEqual(qualified_public, [MODULE.CROSS_SCHEMA_FK[0]])

    def test_no_schema_creation_or_removal(self) -> None:
        for sql in UPGRADE + DOWNGRADE:
            with self.subTest(sql=sql[:60]):
                self.assertNotRegex(sql.upper(), r"\b(CREATE|DROP|ALTER)\s+SCHEMA\b")
                self.assertNotRegex(sql.upper(), r"\b(CREATE|DROP)\s+EXTENSION\b")

    def test_preflight_guard_runs_before_any_ddl(self) -> None:
        first_ddl = next(i for i, s in enumerate(UPGRADE) if s.strip().startswith("CREATE"))
        preflight = next(i for i, s in enumerate(UPGRADE) if s.strip().startswith("DO $$"))
        self.assertLess(preflight, first_ddl)
        self.assertIn("nspname = 'kbp'", UPGRADE[preflight])


class DowngradeTests(unittest.TestCase):
    def test_downgrade_only_drops_kbp_tables_and_revokes(self) -> None:
        for sql in DOWNGRADE:
            with self.subTest(sql=sql):
                self.assertRegex(
                    sql,
                    r"^(SET LOCAL (lock_timeout|statement_timeout) = '\w+'"
                    r"|REVOKE SELECT ON TABLE kbp\.alembic_version FROM kb_pipeline_app"
                    r"|DROP TABLE kbp\.\w+ RESTRICT)$",
                )

    def test_downgrade_drops_all_25_tables_once(self) -> None:
        dropped = [re.match(r"DROP TABLE kbp\.(\w+)", s).group(1) for s in DOWNGRADE if s.startswith("DROP")]
        self.assertEqual(sorted(dropped), sorted(SPEC_TABLES))
        self.assertNotIn("alembic_version", dropped)

    def test_downgrade_order_is_dependency_safe(self) -> None:
        dropped = [re.match(r"DROP TABLE kbp\.(\w+)", s).group(1) for s in DOWNGRADE if s.startswith("DROP")]
        position = {table: i for i, table in enumerate(dropped)}
        for table, info in MIGRATION_TABLES.items():
            for _cols_, parent in info["fks"]:
                if parent != table:
                    with self.subTest(child=table, parent=parent):
                        self.assertLess(position[table], position[parent])

    def test_upgrade_order_is_dependency_safe(self) -> None:
        created = [re.match(r"\s*CREATE TABLE kbp\.(\w+)", s).group(1) for s in UPGRADE if re.match(r"\s*CREATE TABLE", s)]
        position = {table: i for i, table in enumerate(created)}
        for table, info in MIGRATION_TABLES.items():
            for _cols_, parent in info["fks"]:
                if parent != table:
                    with self.subTest(child=table, parent=parent):
                        self.assertLess(position[parent], position[table])


class SafetyCheckerOnMigrationTests(unittest.TestCase):
    def test_migration_passes_safety_checker(self) -> None:
        self.assertEqual(check_statements(UPGRADE, "upgrade"), [])
        self.assertEqual(check_statements(DOWNGRADE, "downgrade"), [])

    def test_revision_metadata(self) -> None:
        self.assertEqual(MODULE.revision, "kbp_001a")
        self.assertIsNone(MODULE.down_revision)
        self.assertEqual(MODULE.branch_labels, ("kbp",))


if __name__ == "__main__":
    unittest.main()

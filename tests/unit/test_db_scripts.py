"""Offline checks of the live-migration SQL scripts in scripts/db (no database).

* The snapshot and verification scripts are read-only: one READ ONLY
  transaction, rolled back, and no data-changing or DDL statement anywhere,
  including inside DO blocks and dynamically executed check queries.
* No script contains credentials; the snapshot never prints row content.
* The expected sets embedded in verify_migration_001a.sql are re-derived from
  the migration module, so the script cannot drift from Migration 001A.
* The provisioning script creates exactly the specified roles, attributes and
  grants, and nothing that could escalate privileges.
"""

import re
import unittest
from pathlib import Path

from app.db.migration_safety import lex

import tests.unit.test_migration_001a as m001a

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts" / "db"
PRIVILEGES_DOC = (ROOT / "docs" / "database_privileges_v1.md").read_text(encoding="utf-8")

READ_ONLY_ENTRY_SCRIPTS = (
    "pre_migration_snapshot.sql",
    "verify_kbp_provisioning.sql",
    "verify_migration_001a.sql",
    "verify_downgrade_001a.sql",
)
READ_ONLY_LIBS = ("lib/legacy_fingerprint.sql", "lib/legacy_checks.sql", "lib/finish.sql")
PROVISIONING = "provision_kbp_roles.sql"
PROBES = "permission_probes_app.sql"
ALL_SCRIPTS = READ_ONLY_ENTRY_SCRIPTS + READ_ONLY_LIBS + (PROVISIONING, PROBES)

BRIDGE_IDS = ("a7d8d2e1-8400-5916-9440-b492e8e692ce", "b487f559-5f8e-56ba-9746-28658b9d09d1")

WRITE_VERBS = re.compile(
    r"\b(insert|update|delete|truncate|merge|copy|create|alter|drop|grant|revoke|comment|"
    r"vacuum|analyze|reindex|cluster|refresh|reassign|import|call|lock|security|nextval|setval|"
    r"pg_terminate_backend|pg_cancel_backend|pg_reload_conf|lo_import|lo_export|dblink)\b"
)


def read(name: str) -> str:
    return (SCRIPTS / name).read_text(encoding="utf-8")


def sql_only(text: str) -> str:
    """Drop psql meta-commands. \\g, \\gset and \\gx end a query, like ';'."""
    lines = []
    for line in text.splitlines():
        if re.match(r"\s*\\g(set|x)?\b", line):
            lines.append(";")
        elif line.lstrip().startswith("\\"):
            continue
        else:
            lines.append(re.sub(r"\s\\g(set|x)?\b.*$", ";", line))
    return "\n".join(lines)


def top_level_statements(text: str) -> list[str]:
    """Split SQL on semicolons outside quotes, comments and dollar bodies."""
    statements, current, i, n = [], [], 0, len(text)
    while i < n:
        ch = text[i]
        if text.startswith("--", i):
            end = text.find("\n", i)
            i = n if end == -1 else end
            continue
        if ch == "'":
            j = i + 1
            while j < n and not (text[j] == "'" and (j + 1 >= n or text[j + 1] != "'")):
                j += 2 if text[j] == "'" else 1
            current.append(text[i:j + 1])
            i = j + 1
            continue
        if ch == "$":
            tag = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", text[i:])
            if tag:
                end = text.find(tag.group(0), i + len(tag.group(0)))
                end = n if end == -1 else end + len(tag.group(0))
                current.append(text[i:end])
                i = end
                continue
        if ch == ";":
            statements.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1
    if "".join(current).strip():
        statements.append("".join(current).strip())
    return [s for s in statements if s]


def plpgsql_bodies(text: str) -> list[str]:
    """Bodies of the DO blocks (PL/pgSQL) in a script."""
    return [m.group(2) for m in re.finditer(r"\bDO\s+(\$\w*\$)(.*?)\1", text, flags=re.S)]


def plpgsql_tokens(body: str) -> list[str]:
    """Upper-cased PL/pgSQL tokens; comments and quoted strings are removed."""
    body = re.sub(r"--[^\n]*", " ", body)
    body = re.sub(r"\$(\w*)\$.*?\$\1\$", " ", body, flags=re.S)  # nested dollar-quoted strings
    body = re.sub(r"'(?:[^']|'')*'", " ", body)
    return [t.upper() for t in re.findall(r"[A-Za-z_][A-Za-z0-9_$]*|[();]", body)]


def unparenthesized_case_in_if(body: str) -> list[str]:
    """IF/ELSIF conditions containing a CASE outside parentheses.

    PL/pgSQL ends an IF/ELSIF condition at the first THEN outside parentheses,
    so an unparenthesized CASE ... THEN ... END truncates the condition
    ("syntax error at end of input").
    """
    tokens = plpgsql_tokens(body)
    problems = []
    for i, token in enumerate(tokens):
        if token not in ("IF", "ELSIF") or (i > 0 and tokens[i - 1] == "END"):
            continue
        depth, j = 0, i + 1
        while j < len(tokens) and not (tokens[j] == "THEN" and depth == 0):
            if tokens[j] == "(":
                depth += 1
            elif tokens[j] == ")":
                depth -= 1
            elif tokens[j] == "CASE" and depth == 0:
                problems.append(" ".join(tokens[i:j + 8]))
                break
            j += 1
    return problems


def expected_block(text: str, name: str) -> list[str]:
    block = text.split(f"-- BEGIN EXPECTED {name}", 1)[1].split(f"-- END EXPECTED {name}", 1)[0]
    return re.findall(r"^\s*'([^']*)',?$", block, flags=re.M)


class ScriptInventoryTests(unittest.TestCase):
    def test_all_scripts_exist_and_includes_resolve(self) -> None:
        for name in ALL_SCRIPTS:
            with self.subTest(script=name):
                self.assertTrue((SCRIPTS / name).is_file())
                for include in re.findall(r"^\\ir (\S+)$", read(name), flags=re.M):
                    self.assertTrue(((SCRIPTS / name).parent / include).is_file(), include)

    def test_scripts_are_ascii(self) -> None:
        for name in ALL_SCRIPTS:
            with self.subTest(script=name):
                read(name).encode("ascii")

    def test_every_script_stops_on_error(self) -> None:
        for name in READ_ONLY_ENTRY_SCRIPTS + (PROVISIONING, PROBES):
            with self.subTest(script=name):
                self.assertIn("\\set ON_ERROR_STOP on", read(name))


class PlpgsqlConstructionTests(unittest.TestCase):
    """Regression: provision_kbp_roles.sql failed on PostgreSQL 17.10 with
    "syntax error at end of input" because an IF condition contained an
    unparenthesized CASE expression."""

    def test_detector_catches_the_original_defect(self) -> None:
        broken = ("BEGIN IF r.rolconnlimit <> CASE r.rolname WHEN 'kb_pipeline_migrator' "
                  "THEN 2 ELSE app_lim::integer END THEN RAISE EXCEPTION 'x'; END IF; END")
        fixed = broken.replace("<> CASE", "<> (CASE").replace("integer END THEN", "integer END) THEN")
        self.assertEqual(len(unparenthesized_case_in_if(broken)), 1)
        self.assertEqual(unparenthesized_case_in_if(fixed), [])
        # A CASE inside a quoted string or a comment is not code.
        self.assertEqual(unparenthesized_case_in_if("BEGIN IF x = 'CASE' THEN NULL; END IF; END"), [])
        self.assertEqual(unparenthesized_case_in_if("BEGIN IF x -- CASE\n THEN NULL; END IF; END"), [])

    def test_no_unparenthesized_case_in_any_if_condition(self) -> None:
        sources = {name: read(name) for name in ALL_SCRIPTS}
        sources["migration PREFLIGHT_SQL"] = m001a.MODULE.PREFLIGHT_SQL
        for name, text in sources.items():
            bodies = plpgsql_bodies(text)
            for index, body in enumerate(bodies):
                with self.subTest(source=name, block=index):
                    self.assertEqual(unparenthesized_case_in_if(body), [])
        self.assertGreaterEqual(len(plpgsql_bodies(read(PROVISIONING))), 2)

    def test_every_do_block_is_closed_with_its_own_tag(self) -> None:
        for name in ALL_SCRIPTS:
            text = sql_only(read(name))
            opened = re.findall(r"\bDO\s+(\$\w*\$)", text)
            with self.subTest(script=name):
                self.assertEqual(len(plpgsql_bodies(text)), len(opened))
                for tag in opened:
                    self.assertEqual(text.count(tag) % 2, 0, tag)


class ReadOnlyTests(unittest.TestCase):
    def test_entry_scripts_use_one_read_only_transaction_and_roll_back(self) -> None:
        for name in READ_ONLY_ENTRY_SCRIPTS:
            text = read(name)
            with self.subTest(script=name):
                statements = top_level_statements(sql_only(text))
                self.assertEqual(statements[0], "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                self.assertEqual(sum(1 for s in statements if re.match(r"(?i)(begin|start)\b", s)), 1)
                self.assertNotRegex(sql_only(text), r"(?i)\bcommit\b")
                ends_with_rollback = statements[-1] == "ROLLBACK"
                self.assertTrue(ends_with_rollback or text.rstrip().endswith("\\ir lib/finish.sql"))
        self.assertEqual(top_level_statements(sql_only(read("lib/finish.sql")))[-1], "ROLLBACK")

    def test_no_write_or_ddl_anywhere(self) -> None:
        for name in READ_ONLY_ENTRY_SCRIPTS + READ_ONLY_LIBS:
            with self.subTest(script=name):
                for statement in top_level_statements(sql_only(read(name))):
                    code = lex(statement).code
                    self.assertNotRegex(code, WRITE_VERBS, statement[:120])
                    self.assertRegex(
                        code,
                        r"^(begin isolation level repeatable read read only|rollback"
                        r"|set local (lock_timeout|statement_timeout) = ''|set local client_min_messages = notice"
                        r"|select |with |do \$\$)",
                        statement[:120],
                    )

    def test_set_config_is_transaction_local(self) -> None:
        for name in READ_ONLY_ENTRY_SCRIPTS + READ_ONLY_LIBS:
            for call in re.findall(r"set_config\(([^()]*(?:\([^()]*\))?[^()]*)\)", sql_only(read(name))):
                with self.subTest(script=name, call=call[:60]):
                    self.assertTrue(call.rstrip().endswith("true"), call)


class SecretHygieneTests(unittest.TestCase):
    def test_no_credentials_in_any_script(self) -> None:
        for name in ALL_SCRIPTS:
            text = read(name)
            with self.subTest(script=name):
                self.assertNotRegex(text, r"(?i)password\s+'")
                self.assertNotRegex(text, r"(?i)postgres(ql)?(\+psycopg)?://[^\s@/]*:[^\s@/]*@")
                self.assertNotIn("CHANGE_ME", text)
                self.assertNotRegex(text, r"(?i)\bPGPASSWORD\b")

    def test_password_hashes_are_never_selected(self) -> None:
        for name in ALL_SCRIPTS:
            code = lex(sql_only(read(name))).code
            with self.subTest(script=name):
                # rolpassword is only ever tested (LIKE / IS NULL), never output.
                for use in re.findall(r"rolpassword[^,)]*", code):
                    self.assertRegex(use, r"^rolpassword (like|is null)", use)

    def test_snapshot_never_outputs_row_content(self) -> None:
        for name, digests in (("pre_migration_snapshot.sql", 0), ("lib/legacy_fingerprint.sql", 4)):
            code = lex(sql_only(read(name))).code
            with self.subTest(script=name):
                self.assertNotRegex(code, r"\b(chunk|embedding|source_url|source_title|description)\b")
                # Row values only ever enter md5(); the snapshot includes the fingerprint.
                self.assertEqual(len(re.findall(r"\br::text\b", code)), len(re.findall(r"md5\(r::text\)", code)))
                self.assertEqual(len(re.findall(r"md5\(r::text\)", code)), digests)
        self.assertIn("\\ir lib/legacy_fingerprint.sql", read("pre_migration_snapshot.sql"))


class MigrationVerificationScriptTests(unittest.TestCase):
    TEXT = read("verify_migration_001a.sql")
    M = m001a.MODULE
    TABLES = m001a.MIGRATION_TABLES

    def test_expected_tables(self) -> None:
        self.assertEqual(expected_block(self.TEXT, "tables"), list(self.M.KBP_TABLES))

    def test_expected_columns(self) -> None:
        types = {"uuid": "uuid", "text": "text", "timestamptz": "timestamp with time zone", "bytea": "bytea",
                 "integer": "integer", "bigint": "bigint", "smallint": "smallint", "boolean": "boolean",
                 "real": "real", "jsonb": "jsonb"}
        defaults = m001a.parse_migration_defaults()
        derived = []
        for table in self.M.KBP_TABLES:
            for ordinal, (column, info) in enumerate(self.TABLES[table]["columns"].items(), 1):
                default = defaults.get((table, column), "").strip("'")
                derived.append("|".join((table, str(ordinal), column, types[info["type"]],
                                         "t" if info["not_null"] else "f",
                                         "t" if info["identity"] else "f", default)))
        self.assertEqual(expected_block(self.TEXT, "columns"), derived)
        self.assertEqual(len(derived), 252)

    def test_expected_keys_and_fks(self) -> None:
        keys = [f"{t}|p|{','.join(self.TABLES[t]['pk'])}" for t in self.M.KBP_TABLES]
        keys += [f"{t}|u|{','.join(u)}" for t in self.M.KBP_TABLES for u in self.TABLES[t]["uniques"]]
        self.assertEqual(sorted(expected_block(self.TEXT, "keys")), sorted(keys + ["alembic_version|p|version_num"]))
        fks = {
            f"{t}|{','.join(cols)}|kbp.{ref}|{','.join(self.TABLES[ref]['pk'])}|a|a|false|true"
            for t in self.M.KBP_TABLES for cols, ref in self.TABLES[t]["fks"]
        }
        fks.add("source_dataset_map|dataset_id|public.datasets|id|a|a|false|true")
        actual = expected_block(self.TEXT, "fks")
        self.assertEqual(len(actual), len(set(actual)))
        self.assertEqual(set(actual), fks)
        self.assertEqual(len(actual), 34)

    def test_expected_check_counts(self) -> None:
        derived = {}
        for sql in m001a.UPGRADE:
            match = re.match(r"\s*CREATE TABLE kbp\.(\w+) \((.*)\)\s*$", sql, re.S)
            if match:
                count = sum(item.count("CHECK (") for item in m001a._split_top_level(match.group(2)))
                if count:
                    derived[match.group(1)] = count
        actual = dict(k.split("|") for k in expected_block(self.TEXT, "check_counts"))
        self.assertEqual({k: int(v) for k, v in actual.items()}, derived)
        self.assertEqual(sum(derived.values()), 88)

    def test_expected_indexes(self) -> None:
        derived = set()
        for sql in self.M.CREATE_INDEXES:
            name, table, cols = re.search(r"INDEX (\w+) ON kbp\.(\w+) \(([^)]*)\)", sql).groups()
            unique = "t" if sql.startswith("CREATE UNIQUE") else "f"
            partial = "t" if " WHERE " in sql else "f"
            derived.add(f"{table}|{name}|{unique}|{partial}|{','.join(c.strip() for c in cols.split(','))}")
        self.assertEqual(set(expected_block(self.TEXT, "indexes")), derived)

    def test_expected_grants(self) -> None:
        table_grants, column_grants = set(), set()
        for table in self.M.KBP_TABLES:
            insert, update = self.M.RUNTIME_PRIVILEGES[table]
            table_grants.add(f"{table}|SELECT")
            if insert:
                table_grants.add(f"{table}|INSERT")
            if update == "table":
                table_grants.add(f"{table}|UPDATE")
            elif update:
                column_grants |= {f"{table}|{c}|UPDATE" for c in update}
            if table in self.M.RUNTIME_DELETE_TABLES:
                table_grants.add(f"{table}|DELETE")
        table_grants.add("alembic_version|SELECT")
        self.assertEqual(set(expected_block(self.TEXT, "table_grants")), table_grants)
        self.assertEqual(set(expected_block(self.TEXT, "column_grants")), column_grants)

    def test_identity_sequence_count(self) -> None:
        identity = sum(1 for t in self.TABLES.values() for c in t["columns"].values() if c["identity"])
        self.assertIn(f"count(*) = {identity} AND", self.TEXT)
        self.assertEqual(identity, 7)

    def test_bridge_absence_and_empty_map_are_checked(self) -> None:
        self.assertIn("FROM kbp.source_dataset_map", self.TEXT)
        legacy = read("lib/legacy_checks.sql")
        for bridge_id in BRIDGE_IDS:
            self.assertIn(bridge_id, legacy)

    def test_downgrade_script_lists_the_25_tables(self) -> None:
        text = read("verify_downgrade_001a.sql")
        listed = re.findall(r"'(\w+)'", text.split("ARRAY[", 1)[1].split("]", 1)[0])
        self.assertEqual(listed, list(self.M.KBP_TABLES))


class LegacyMetricConsistencyTests(unittest.TestCase):
    def test_metric_lists_agree(self) -> None:
        checks = read("lib/legacy_checks.sql")
        compared = re.findall(r"'(\w+)'", checks.split("FOREACH metric IN ARRAY ARRAY[", 1)[1].split("]", 1)[0])
        snapshot = re.findall(r"\(\d+,\s+'(\w+)',\s+:'fp_\1'\)", read("pre_migration_snapshot.sql"))
        fingerprint = re.findall(r"\(\d+,\s+'(\w+)',\s+:'fp_\1'\)", read("lib/legacy_fingerprint.sql"))
        self.assertEqual(len(compared), 12)
        self.assertEqual(snapshot, compared)
        self.assertEqual(fingerprint[:12], compared)
        for metric in compared:
            with self.subTest(metric=metric):
                self.assertIn(f"\\set baseline_{metric} ''", checks)
                self.assertIn(f"'kbp_verify.b_{metric}',", checks)
                self.assertIn(f":'fp_{metric}'", checks)

    def test_missing_baseline_is_a_failure(self) -> None:
        checks = read("lib/legacy_checks.sql")
        self.assertRegex(checks, r"IF baseline = '' THEN\s+failures := failures \+ 1;")


class PermissionProbeTests(unittest.TestCase):
    TEXT = read(PROBES)
    PROBES_FOUND = re.findall(r"\('([AB]\d\d)', '(denied|allowed)',\s+\$q\$(.*?)\$q\$\)", TEXT, flags=re.S)

    def test_one_transaction_rolled_back_never_committed(self) -> None:
        statements = top_level_statements(sql_only(self.TEXT))
        self.assertEqual(statements[0], "BEGIN")
        self.assertEqual(statements[-1], "ROLLBACK")
        self.assertNotRegex(lex(sql_only(self.TEXT)).code, r"\bcommit\b")

    def test_probes_cannot_touch_rows(self) -> None:
        self.assertEqual(len(self.PROBES_FOUND), 30)
        for probe_id, _expectation, statement in self.PROBES_FOUND:
            flat = " ".join(statement.split())
            with self.subTest(probe=probe_id):
                if re.match(r"(UPDATE|DELETE|INSERT)\b", flat):
                    self.assertTrue(flat.endswith("WHERE false"), flat)
                elif flat.startswith("SELECT"):
                    self.assertTrue(flat.endswith("LIMIT 0") or "kbp.alembic_version" in flat, flat)
                else:
                    self.assertRegex(flat, r"^CREATE (TABLE|SCHEMA) (kbp\.|public\.)?kbp_permission_probe\b")
                self.assertNotRegex(flat, r"\b(TRUNCATE|DROP|ALTER|GRANT|COPY)\b")

    def test_kbp_expectations_follow_the_locked_matrix(self) -> None:
        module = m001a.MODULE
        for probe_id, expectation, statement in self.PROBES_FOUND:
            flat = " ".join(statement.split())
            update = re.match(r"UPDATE kbp\.(\w+) SET (.*) WHERE false$", flat)
            delete = re.match(r"DELETE FROM kbp\.(\w+) WHERE false$", flat)
            insert = re.match(r"INSERT INTO kbp\.(\w+) ", flat)
            if update:
                table, assignments = update.groups()
                columns = {a.split("=")[0].strip() for a in assignments.split(",")}
                granted = module.RUNTIME_PRIVILEGES[table][1]
                allowed = granted == "table" or (isinstance(granted, tuple) and columns <= set(granted))
            elif delete:
                allowed = delete.group(1) in module.RUNTIME_DELETE_TABLES
            elif insert:
                allowed = module.RUNTIME_PRIVILEGES[insert.group(1)][0]
            else:
                continue
            with self.subTest(probe=probe_id):
                self.assertEqual(expectation, "allowed" if allowed else "denied", flat)

    def test_gold_write_probes_are_denied(self) -> None:
        denied = {" ".join(s.split()) for _i, e, s in self.PROBES_FOUND if e == "denied"}
        for statement in ("UPDATE public.kb SET chunk = chunk WHERE false", "DELETE FROM public.kb WHERE false",
                          "UPDATE public.metadata SET domain = domain WHERE false",
                          "DELETE FROM public.metadata WHERE false"):
            self.assertIn(statement, denied)


class ProvisioningScriptTests(unittest.TestCase):
    TEXT = read(PROVISIONING)
    SQL = sql_only(TEXT)
    STATEMENTS = top_level_statements(SQL)

    def test_single_transaction_with_guard_first(self) -> None:
        self.assertEqual(self.STATEMENTS[0], "BEGIN")
        self.assertEqual(self.STATEMENTS[-1], "COMMIT")
        first_write = next(i for i, s in enumerate(self.STATEMENTS) if re.match(r"(CREATE|GRANT|ALTER|REVOKE)\b", s))
        guard = next(i for i, s in enumerate(self.STATEMENTS) if s.startswith("DO $guard$"))
        selfcheck = next(i for i, s in enumerate(self.STATEMENTS) if s.startswith("DO $selfcheck$"))
        self.assertLess(guard, first_write)
        self.assertEqual(selfcheck, len(self.STATEMENTS) - 2)

    def test_roles_have_explicit_safe_attributes_and_no_password(self) -> None:
        creates = [s for s in self.STATEMENTS if s.startswith("CREATE ROLE")]
        self.assertEqual([re.match(r"CREATE ROLE (\w+)", s).group(1) for s in creates],
                         ["kb_pipeline_migrator", "kb_pipeline_app"])
        for statement in creates:
            words = statement.split()
            with self.subTest(role=words[2]):
                for attribute in ("LOGIN", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE",
                                  "NOREPLICATION", "NOBYPASSRLS", "INHERIT"):
                    self.assertIn(attribute, words)
                for forbidden in ("SUPERUSER", "CREATEDB", "CREATEROLE", "REPLICATION", "BYPASSRLS",
                                  "IN", "ROLE", "ADMIN", "USER", "SYSID"):
                    self.assertNotIn(forbidden, words[3:])
                self.assertIn("PASSWORD NULL", statement)
                self.assertRegex(statement, r"CONNECTION LIMIT (2|:app_connection_limit)\b")
        self.assertIn("CONNECTION LIMIT 2", creates[0])

    def test_schema_created_for_migrator_and_closed_to_public(self) -> None:
        self.assertIn("CREATE SCHEMA IF NOT EXISTS kbp AUTHORIZATION kb_pipeline_migrator", self.STATEMENTS)
        self.assertIn("REVOKE ALL ON SCHEMA kbp FROM PUBLIC", self.STATEMENTS)

    def test_exact_grant_set(self) -> None:
        grants = sorted(" ".join(s.split()) for s in self.STATEMENTS if s.startswith("GRANT"))
        self.assertEqual(grants, sorted([
            'GRANT CONNECT ON DATABASE :"prov_dbname" TO kb_pipeline_migrator, kb_pipeline_app',
            "GRANT USAGE ON SCHEMA public TO kb_pipeline_migrator, kb_pipeline_app",
            "GRANT REFERENCES (id) ON TABLE public.datasets TO kb_pipeline_migrator",
            "GRANT SELECT ON TABLE public.datasets TO kb_pipeline_migrator",
            "GRANT SELECT ON TABLE public.dataset_sources TO kb_pipeline_migrator",
            "GRANT SELECT (id, name, source_type) ON TABLE public.dataset_sources TO kb_pipeline_app",
            "GRANT SELECT (id, source_id, dataset_name, metadata) ON TABLE public.datasets TO kb_pipeline_app",
            "GRANT SELECT (id, source_id) ON TABLE public.metadata TO kb_pipeline_app",
            "GRANT INSERT ON TABLE public.metadata TO kb_pipeline_app",
            "GRANT SELECT, INSERT ON TABLE public.kb TO kb_pipeline_app",
            "GRANT USAGE ON SCHEMA kbp TO kb_pipeline_app",
        ]))

    def test_runtime_public_grants_are_those_of_privileges_section_6(self) -> None:
        section = PRIVILEGES_DOC.split("## 6. `public` table privileges (runtime)", 1)[1].split("## 7.", 1)[0]
        for fragment in ("`SELECT (id, name, source_type)`", "`SELECT (id, source_id, dataset_name, metadata)`",
                         "`SELECT (id, source_id)`, `INSERT` (all columns)", "| `public.kb` | `SELECT`, `INSERT` |"):
            self.assertIn(fragment, section)

    def test_no_escalation_or_forbidden_privilege(self) -> None:
        code = " ".join(lex(s).code for s in self.STATEMENTS).upper()
        for forbidden in ("TO PUBLIC", "ALL PRIVILEGES", "WITH GRANT OPTION", "WITH ADMIN", "ON ALL TABLES",
                          "ON ALL SEQUENCES", "ON ALL FUNCTIONS", "OWNER TO", "SECURITY DEFINER",
                          "REASSIGN", "DROP ", "TRUNCATE ON", "GRANT POSTGRES", "GRANT PG_"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, code)
        for statement in self.STATEMENTS:
            if statement.startswith("GRANT"):
                with self.subTest(grant=statement):
                    self.assertNotRegex(statement, r"\b(UPDATE|DELETE|TRUNCATE|TRIGGER)\b")
                    self.assertNotRegex(statement, r"INSERT[^;]*ON TABLE public\.(datasets|dataset_sources)")
            if re.match(r"GRANT \w+ TO", statement):
                self.fail(f"role membership grant: {statement}")

    def test_bridge_bootstrap_absent(self) -> None:
        for bridge_id in BRIDGE_IDS:
            self.assertNotIn(bridge_id, self.TEXT)
        self.assertNotRegex(self.SQL, r"(?i)\binsert\s+into\b")

    def test_default_privileges(self) -> None:
        defaults = [" ".join(s.split()) for s in self.STATEMENTS if s.startswith("ALTER DEFAULT PRIVILEGES")]
        self.assertEqual(defaults, [
            "ALTER DEFAULT PRIVILEGES FOR ROLE kb_pipeline_migrator IN SCHEMA kbp GRANT SELECT ON TABLES TO kb_pipeline_app",
            "ALTER DEFAULT PRIVILEGES FOR ROLE kb_pipeline_migrator REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
        ])
        set_role = self.STATEMENTS.index("SET LOCAL ROLE kb_pipeline_migrator")
        reset_role = self.STATEMENTS.index("RESET ROLE")
        for statement in defaults + ["GRANT USAGE ON SCHEMA kbp TO kb_pipeline_app"]:
            position = [" ".join(s.split()) for s in self.STATEMENTS].index(statement)
            self.assertTrue(set_role < position < reset_role, statement)

    def test_runtime_settings_scoped_to_database(self) -> None:
        alters = [s for s in self.STATEMENTS if s.startswith("ALTER ROLE")]
        self.assertEqual(len(alters), 4)
        for statement in alters:
            self.assertTrue(statement.startswith('ALTER ROLE kb_pipeline_app IN DATABASE :"prov_dbname" SET '))
        self.assertIn("search_path = kbp, public", " ".join(alters))

    def test_verify_provisioning_matrix_matches_script(self) -> None:
        granted = set()
        for statement in self.STATEMENTS:
            match = re.match(r"GRANT (.+) ON TABLE public\.(\w+) TO (\w+)$", " ".join(statement.split()))
            if not match:
                continue
            privileges, table, role = match.groups()
            for privilege in re.findall(r"(\w+)(?: \(([^)]*)\))?(?:, |$)", privileges):
                columns = ",".join(c.strip() for c in privilege[1].split(",")) if privilege[1] else "*"
                granted.add((role, table, columns, privilege[0]))
        verify = read("verify_kbp_provisioning.sql")
        spec_block = verify.split("spec(rname, tbl, cols, priv) AS (VALUES", 1)[1].split("expected AS (", 1)[0]
        listed = set(re.findall(r"\('(\w+)', '(\w+)', '([\w,*]+)', '(\w+)'\)", spec_block))
        self.assertEqual(listed, granted)


if __name__ == "__main__":
    unittest.main()

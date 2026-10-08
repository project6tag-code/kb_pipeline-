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
    "verify_migration_001b.sql",
    "verify_migration_001d.sql",
    "verify_gold_metadata.sql",
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
                        r"|set local search_path = pg_catalog, public"
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


class Migration001BVerificationScriptTests(unittest.TestCase):
    TEXT = read("verify_migration_001b.sql")

    def test_refuses_any_database_other_than_the_test_database(self) -> None:
        self.assertIn("current_database() <> 'kb_pipeline_test'", self.TEXT)
        self.assertIn("version_num IN ('kbp_001b', 'kbp_001c', 'kbp_001d', 'kbp_001e')", self.TEXT)

    def test_verifies_schema_keys_fks_indexes_ownership_and_runtime_privileges(self) -> None:
        for token in (
            "expected_columns", "chunk_dedup_decisions_pkey", "chunk_relationships_pkey",
            "chunk_dedup_decisions_representative_fkey", "chunk_relationships_pair_order_check",
            "chunk_dedup_decisions_representative_idx", "chunk_relationships_related_idx",
            "kb_pipeline_migrator", "kb_pipeline_app", "has_table_privilege",
            "MIGRATION 001B VERIFICATION PASSED",
        ):
            with self.subTest(token=token):
                self.assertIn(token, self.TEXT)


class GoldSchemaVerificationScriptTests(unittest.TestCase):
    MIGRATION = read("verify_migration_001d.sql")
    METADATA = read("verify_gold_metadata.sql")

    def test_migration_verifier_is_targeted_read_only_and_checks_revision(self) -> None:
        self.assertIn("current_database() <> 'kb_pipeline_test'", self.MIGRATION)
        self.assertIn("version_num IN ('kbp_001d', 'kbp_001e')", self.MIGRATION)
        self.assertIn("gold_publications", self.MIGRATION)
        self.assertTrue(self.MIGRATION.rstrip().endswith("ROLLBACK;"))

    def test_migration_verifier_checks_ledger_contract_and_privileges(self) -> None:
        for token in (
            "kbp.gold_publications", "gold_publications_pkey", "gold_publications_embedding_fkey",
            "gold_publications_run_fkey", "kbp.chunk_embeddings", "kbp.pipeline_runs",
            "has_table_privilege", "MIGRATION 001D VERIFICATION PASSED",
        ):
            with self.subTest(token=token):
                self.assertIn(token, self.MIGRATION)

    def test_metadata_verifier_is_targeted_and_checks_exact_appended_schema(self) -> None:
        for token in (
            "current_database() <> 'kb_pipeline_test'", "version_num = 'kbp_001d'",
            "source_url:text:false:false", "source_title:text:false:false",
            "source_type:character varying(50):false:false", "published_at:timestamp with time zone:false:false",
            "updated_at:timestamp with time zone:false:false", "metadata_pkey", "metadata_source_id_fkey",
            "relacl::text", "GOLD METADATA VERIFICATION PASSED",
        ):
            with self.subTest(token=token):
                self.assertIn(token, self.METADATA)
        self.assertTrue(self.METADATA.rstrip().endswith("ROLLBACK;"))


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

    def test_pre_provisioning_acl_rule_is_strict(self) -> None:
        """Regression (live run 1A): the baseline came from the pre-provisioning
        snapshot, so acl_digest legitimately differed. The ACL may then pass only
        via the all-default + exactly-approved-grants rule, never otherwise."""
        checks = read("lib/legacy_checks.sql")
        body = plpgsql_bodies(checks)[0]
        exact = body.index("ELSIF baseline = observed THEN")
        delta_pass = body.index("ELSIF metric = 'acl_digest' AND baseline = all_default AND acl_delta IS NULL THEN")
        delta_fail = body.index("ELSIF metric = 'acl_digest' AND baseline = all_default THEN")
        generic_fail = body.index("RAISE WARNING 'FAIL L-%: CHANGED")
        self.assertLess(exact, delta_pass)
        self.assertLess(delta_pass, delta_fail)
        self.assertLess(delta_fail, generic_fail)
        self.assertIn("failures := failures + 1;", body[delta_fail:generic_fail])
        # No other metric has a special case.
        self.assertEqual(body.count("ELSIF metric ="), 2)

    def test_all_default_digest_reproduces_the_fingerprint_formula(self) -> None:
        import hashlib

        fingerprint = read("lib/legacy_fingerprint.sql")
        self.assertIn("SELECT c.oid, 'public.' || c.relname AS rel, c.relacl", fingerprint)
        self.assertIn("r.rel || ' acl ' || coalesce(array_to_string(r.relacl, ','), '<default>')", fingerprint)
        checks = read("lib/legacy_checks.sql")
        self.assertIn("SELECT 'public.' || c.relname || ' acl <default>' AS item", checks)
        self.assertIn("SELECT md5(string_agg(item, E'\\n' ORDER BY item))", checks)
        # relname has collation "C", i.e. byte order. This is the acl_digest the
        # live pre-provisioning snapshot recorded (all five relations <default>).
        items = sorted(f"public.{t} acl <default>" for t in
                       ("kb", "metadata", "datasets", "dataset_sources", "alembic_version"))
        self.assertEqual(hashlib.md5("\n".join(items).encode()).hexdigest(), "9eb1292570bf6d3d3a6cf05a1b061a84")

    def test_approved_acl_equals_the_provisioning_grants(self) -> None:
        provisioning = top_level_statements(sql_only(read(PROVISIONING)))
        columns = {
            "datasets": None, "dataset_sources": None, "metadata": None, "kb": None,
        }
        derived = set()
        for statement in provisioning:
            match = re.match(r"GRANT (.+) ON TABLE (public\.\w+) TO (\w+)$", " ".join(statement.split()))
            if not match:
                continue
            privileges, relation, role = match.groups()
            self.assertIn(relation.split(".")[1], columns)
            for privilege, cols in re.findall(r"(\w+)(?: \(([^)]*)\))?(?:, |$)", privileges):
                if cols:
                    derived |= {f"{relation}|{c.strip()}|{role}|{privilege}|false" for c in cols.split(",")}
                else:
                    derived.add(f"{relation}||{role}|{privilege}|false")
        block = read("lib/legacy_checks.sql").split("-- BEGIN APPROVED PROVISIONING ACL", 1)[1]
        approved = set(re.findall(r"\('([^']+)'\)", block.split("-- END APPROVED PROVISIONING ACL", 1)[0]))
        self.assertEqual(approved, derived)
        self.assertEqual(len(approved), 21)
        self.assertFalse(any("|INSERT|" in k and k.startswith(("public.datasets|", "public.dataset_sources|"))
                             for k in approved))

    def test_missing_baseline_is_a_failure(self) -> None:
        checks = read("lib/legacy_checks.sql")
        self.assertRegex(checks, r"IF baseline = '' THEN\s+failures := failures \+ 1;")


class ProvisioningOwnershipCheckTests(unittest.TestCase):
    """Regression (live run 1A): P10 failed on the PostgreSQL-generated TOAST
    tables/indexes (schema pg_toast) of the migrator's kbp tables."""

    P10 = re.search(r"\('P10',.*?\$q\$(.*?)\$q\$\)", read("verify_kbp_provisioning.sql"), flags=re.S).group(1)

    def test_toast_exemption_is_proven_by_catalog_links(self) -> None:
        flat = " ".join(self.P10.split())
        # Parent: a kbp table owned by the migrator that points at the TOAST table.
        self.assertIn("SELECT p.reltoastrelid AS toast_oid FROM pg_catalog.pg_class AS p, m "
                      "WHERE p.relnamespace = (SELECT oid FROM k) AND p.relkind = 'r' "
                      "AND p.relowner = m.oid AND p.reltoastrelid <> 0", flat)
        # The TOAST table itself, and the index whose indrelid is that TOAST table.
        self.assertIn("JOIN kbp_toast_parents AS kp ON kp.toast_oid = t.oid "
                      "WHERE t.relkind = 't' AND t.relnamespace = 'pg_toast'::regnamespace", flat)
        self.assertIn("JOIN kbp_toast_parents AS kp ON kp.toast_oid = x.indrelid "
                      "WHERE i.relkind = 'i' AND i.relnamespace = 'pg_toast'::regnamespace", flat)
        self.assertIn("AND c.oid NOT IN (SELECT oid FROM kbp_toast)", flat)

    def test_no_blanket_pg_toast_exemption(self) -> None:
        flat = " ".join(self.P10.split())
        self.assertNotRegex(flat, r"(?i)nspname\s*(<>|!=|=|not like|like)\s*'pg_toast")
        self.assertNotRegex(flat, r"(?i)relname\s*(not like|like|~)")
        self.assertEqual(flat.count("pg_toast'::regnamespace"), 2)

    def test_only_the_relation_branch_is_exempted(self) -> None:
        owned = " ".join(self.P10.split("owned AS (", 1)[1].split())
        branches = owned.split("UNION ALL")
        self.assertEqual(len(branches), 5)
        for branch in branches:
            with self.subTest(branch=branch[:40]):
                self.assertEqual("kbp_toast" in branch, branch.lstrip().startswith("SELECT 'relation '"))
        self.assertIn("SELECT 'database ' || d.datname", owned)


class PermissionProbeTests(unittest.TestCase):
    TEXT = read(PROBES)
    PROBES_FOUND = re.findall(r"\('([AB]\d\d)', '(denied|allowed)',\s+\$q\$(.*?)\$q\$\)", TEXT, flags=re.S)

    def test_one_transaction_rolled_back_never_committed(self) -> None:
        statements = top_level_statements(sql_only(self.TEXT))
        self.assertEqual(statements[0], "BEGIN")
        self.assertEqual(statements[-1], "ROLLBACK")
        self.assertNotRegex(lex(sql_only(self.TEXT)).code, r"\bcommit\b")

    def test_probes_cannot_touch_rows(self) -> None:
        self.assertEqual(len(self.PROBES_FOUND), 29)
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

    def test_metadata_conflict_verification_selects_all_eight_columns(self) -> None:
        allowed = {" ".join(s.split()) for _i, e, s in self.PROBES_FOUND if e == "allowed"}
        self.assertIn(
            "SELECT id, source_id, source_url, source_title, source_type, domain, published_at, updated_at "
            "FROM public.metadata LIMIT 0",
            allowed,
        )
        for column in ("id", "source_id", "source_url", "source_title", "source_type", "domain",
                       "published_at", "updated_at"):
            self.assertIn(f"has_column_privilege(current_user, 'public.metadata', '{column}', 'SELECT')", self.TEXT)
        self.assertIn("NOT has_table_privilege(current_user, 'public.metadata', 'UPDATE')", self.TEXT)
        self.assertIn("NOT has_table_privilege(current_user, 'public.metadata', 'DELETE')", self.TEXT)
        self.assertIn("NOT has_table_privilege(current_user, 'public.metadata', 'TRUNCATE')", self.TEXT)


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
            "GRANT SELECT (id, source_id, source_url, source_title, source_type, domain, published_at, updated_at) "
            "ON TABLE public.metadata TO kb_pipeline_app",
            "GRANT INSERT ON TABLE public.metadata TO kb_pipeline_app",
            "GRANT SELECT, INSERT ON TABLE public.kb TO kb_pipeline_app",
            "GRANT USAGE ON SCHEMA kbp TO kb_pipeline_app",
        ]))

    def test_runtime_public_grants_are_those_of_privileges_section_6(self) -> None:
        section = PRIVILEGES_DOC.split("## 6. `public` table privileges (runtime)", 1)[1].split("## 7.", 1)[0]
        for fragment in ("`SELECT (id, name, source_type)`", "`SELECT (id, source_id, dataset_name, metadata)`",
                         "`SELECT (id, source_id, source_url, source_title, source_type, domain, published_at, updated_at)`, "
                         "`INSERT` (all columns)", "| `public.kb` | `SELECT`, `INSERT` |"):
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


class GoldV2AdminScriptTests(unittest.TestCase):
    PATCH = read("admin_gold_v2_patch.sql")
    RESET = read("admin_reset_kb_pipeline_test.sql")

    def test_patch_is_targeted_additive_and_least_privilege(self) -> None:
        code = re.sub(r"'[^']*'", "''", re.sub(r"--[^\n]*", "", sql_only(self.PATCH))).lower()
        self.assertIn("version_num = 'kbp_001e'", self.PATCH)
        self.assertIn("alter table public.metadata add column if not exists crawled_at timestamptz", code)
        self.assertIn("alter table public.kb add column if not exists source_id uuid;", code)
        self.assertIn("grant select, insert on table public.datasets to kb_pipeline_app", code)
        self.assertIn("grant select (crawled_at) on table public.metadata to kb_pipeline_app", code)
        self.assertIn("has_column_privilege('kb_pipeline_app', 'public.metadata', 'crawled_at', 'select')",
                      self.PATCH.lower())
        self.assertNotRegex(code, r"\b(drop|delete|truncate|update|revoke)\b")
        self.assertNotRegex(code, r"grant [^;]*(update|delete|all)")
        self.assertNotRegex(code, r"alter table public\.(datasets|dataset_sources)\b")
        # exactly two additive, nullable, default-free, constraint-free columns
        self.assertEqual(re.findall(r"alter table public\.(\w+) ([^;]*);", code),
                         [("metadata", "add column if not exists crawled_at timestamptz"),
                          ("kb", "add column if not exists source_id uuid")])
        self.assertNotRegex(code, r"\b(not null|default|references|constraint|foreign key)\b")
        self.assertIn("'id:uuid:true,chunk:text:true,embedding:vector(768):false,source_id:uuid:false'",
                      self.PATCH)

    def test_reset_requires_confirmation_and_keeps_registrations(self) -> None:
        self.assertIn("confirm=RESET_KB_PIPELINE_TEST", self.RESET)
        self.assertIn("\\quit 3", self.RESET)
        self.assertIn("current_database() <> 'kb_pipeline_test'", self.RESET)
        truncate = re.search(r"TRUNCATE TABLE(.*?)RESTART IDENTITY", self.RESET, re.S).group(1)
        for kept in ("dedup_methods", "chunking_versions", "embedding_models", "source_dataset_map",
                     "public.datasets", "dataset_sources", "alembic_version"):
            with self.subTest(kept=kept):
                self.assertNotIn(kept, truncate)
        self.assertIn("DELETE FROM public.datasets WHERE status = 'KB_SOURCE_DOCUMENT'", self.RESET)

    def test_admin_scripts_hold_no_secrets(self) -> None:
        for text in (self.PATCH, self.RESET):
            self.assertNotRegex(text.lower(), r"password\s*=|postgresql://[^@\s]*:[^@\s]*@")


class BootstrapPublicTablesTests(unittest.TestCase):
    """scripts/db/bootstrap_public_tables.sql: clean-machine DDL, identical to the validated V2 layout."""

    SQL = read("bootstrap_public_tables.sql")
    PATCH = read("admin_gold_v2_patch.sql")
    DDL = SQL.split("-- BEGIN-DDL")[1].split("-- END-DDL")[0]

    def test_four_tables_in_foreign_key_order_and_idempotent(self) -> None:
        tables = re.findall(r"CREATE TABLE IF NOT EXISTS public\.(\w+)", self.DDL)
        self.assertEqual(tables, ["dataset_sources", "datasets", "metadata", "kb"])
        self.assertEqual(self.DDL.count("CREATE TABLE"), 4)
        for name in ("dataset_sources_pkey", "uq_dataset_sources_name", "datasets_pkey", "datasets_source_id_fkey",
                     "metadata_pkey", "metadata_source_id_fkey", "kb_pkey", "kb_id_fkey"):
            self.assertIn(f"CONSTRAINT {name}", self.DDL)
        self.assertIn("FOREIGN KEY (id) REFERENCES public.metadata (id) ON DELETE CASCADE", self.DDL)
        self.assertIn("embedding public.vector(768),", self.DDL)

    def test_layout_equals_what_the_gold_v2_patch_and_publisher_expect(self) -> None:
        for table in ("metadata", "kb"):
            pattern = rf"'{table}', '([^']+)'"
            mine = re.search(pattern, self.SQL.split("expected_columns")[1]).group(1)
            self.assertIn(mine, self.PATCH.replace("'\n          '", ""), table)  # same column strings as the patch verifier
        # physical metadata order required by the publisher preflight
        self.assertEqual(re.findall(r"^\s+(\w+) ", self.DDL.split("CREATE TABLE IF NOT EXISTS public.metadata")[1]
                                    .split("CONSTRAINT metadata_pkey")[0], re.M),
                         ["id", "source_id", "domain", "source_url", "source_title", "source_type", "published_at",
                          "updated_at", "crawled_at"])

    def test_no_privileges_data_or_destructive_statements(self) -> None:
        code = re.sub(r"'[^']*'", "''", re.sub(r"--[^\n]*", "", sql_only(self.SQL))).lower()
        code = code.replace("on delete cascade", "")  # the validated kb -> metadata foreign key action
        self.assertNotRegex(code, r"\b(grant|revoke|drop|delete|truncate|update|insert|alter)\b")
        self.assertNotRegex(self.DDL.lower(), r"\bdefault\b|\bcreate index\b|\btrigger\b")
        self.assertIn("rolsuper", self.SQL)                       # superuser-only
        self.assertIn("never runs as a pipeline role", self.SQL)
        self.assertIn("'data_collection_pipeline'", self.SQL)    # protected database refused
        self.assertTrue(self.SQL.rstrip().endswith("public tables bootstrap: OK (dataset_sources, datasets, metadata, kb in the Gold V2 layout)'"))

    def test_no_secrets(self) -> None:
        self.assertNotRegex(self.SQL.lower(), r"password\s*=|postgresql://[^@\s]*:[^@\s]+@")


class ConfigurableGuardTests(unittest.TestCase):
    """The Gold V2 patch guard takes the database from -v expected_database (no hard-coded name)."""

    PATCH = read("admin_gold_v2_patch.sql")
    INIT = (ROOT / "scripts" / "production" / "init_database.ps1").read_text(encoding="utf-8")

    def test_guard_uses_the_variable_and_has_no_fixed_database_name(self) -> None:
        code = sql_only(self.PATCH)
        self.assertNotIn("kb_pipeline_test", re.sub(r"--[^\n]*", "", code))
        for token in ("\if :{?expected_database}", "\quit 3", ":'expected_database'",
                      "current_setting('kbp_patch.expected_database')", "current_database() <> expected"):
            with self.subTest(token=token):
                self.assertIn(token, self.PATCH)

    def test_protected_databases_and_names_are_always_refused(self) -> None:
        guard = self.PATCH.split("DO $guard$")[1].split("$guard$;")[0]
        protected = "('data_collection_pipeline', 'postgres', 'template0', 'template1')"
        self.assertEqual(guard.count(protected), 2)  # the expected name AND the connected database
        self.assertIn("expected !~ '^[a-z][a-z0-9_]{0,62}$'", guard)

    def test_role_superuser_and_revision_checks_are_kept(self) -> None:
        for token in ("version_num = 'kbp_001e'", "current_user IN ('kb_pipeline_app', 'kb_pipeline_migrator')",
                      "has_table_privilege('kb_pipeline_app', 'public.datasets', 'UPDATE')",
                      "ADD COLUMN IF NOT EXISTS crawled_at", "ADD COLUMN IF NOT EXISTS source_id"):
            with self.subTest(token=token):
                self.assertIn(token, self.PATCH)

    def test_initializer_passes_the_variable_and_the_reset_script_stays_test_only(self) -> None:
        self.assertIn(r"""'-v', "expected_database=$Database", '-f', 'scripts\db\admin_gold_v2_patch.sql'""", self.INIT)
        reset = read("admin_reset_kb_pipeline_test.sql")
        self.assertIn("current_database() <> 'kb_pipeline_test'", reset)  # unchanged: test database only

    def test_initializer_is_rerunnable_provisioning_is_skipped_once_kbp_exists(self) -> None:
        self.assertIn("nspname = 'kbp'", self.INIT)
        self.assertIn("provisioning was done earlier, skipped", self.INIT)
        # the reason: provision_kbp_roles.sql refuses a run after datasets INSERT was granted by the Gold V2 patch
        self.assertIn("INSERT on public.datasets must not be granted", read("provision_kbp_roles.sql"))

    def test_no_init_path_script_pins_the_test_database(self) -> None:
        for name in ("bootstrap_public_tables.sql", "provision_kbp_roles.sql", "admin_gold_v2_patch.sql"):
            with self.subTest(script=name):
                self.assertNotRegex(re.sub(r"--[^\n]*", "", sql_only(read(name))), r"<>\s*'kb_pipeline_test'")


class EmbeddingModelIdBootstrapTests(unittest.TestCase):
    """scripts/db/ensure_embedding_model_id.sql: a clean database must register the frozen model as id 2."""

    SQL = read("ensure_embedding_model_id.sql")
    INIT = (ROOT / "scripts" / "production" / "init_database.ps1").read_text(encoding="utf-8")

    def test_frozen_id_equals_the_runtime_constants(self) -> None:
        from app.pipeline import cli
        from app.processing.gold import service

        frozen = int(re.search(r"frozen_id constant smallint := (\d+);", self.SQL).group(1))
        self.assertEqual(frozen, cli.MODEL_ID)
        self.assertEqual(frozen, service.MODEL_ID)
        self.assertEqual(frozen, 2)

    def test_database_guard_is_the_configured_one(self) -> None:
        for token in ("\if :{?expected_database}", "\quit 3", ":'expected_database'",
                      "current_database() <> expected", "current_user = 'kb_pipeline_app'",
                      "version_num = 'kbp_001e'"):
            with self.subTest(token=token):
                self.assertIn(token, self.SQL)
        guard = self.SQL.split("DO $ensure$")[1].split("$ensure$;")[0]
        self.assertEqual(guard.count("('data_collection_pipeline', 'postgres', 'template0', 'template1')"), 2)
        self.assertNotIn("kb_pipeline_test", re.sub(r"--[^\n]*", "", sql_only(self.SQL)))

    def test_it_never_invents_or_rewrites_a_model(self) -> None:
        code = re.sub(r"'[^']*'", "''", re.sub(r"--[^\n]*", "", sql_only(self.SQL))).lower()
        self.assertNotRegex(code, r"\binsert\b|\bupdate\b|\btruncate\b|\bdrop\b|\bgrant\b|\bsetval\b")
        self.assertEqual(re.findall(r"delete from (\S+)", code), ["kbp.embedding_models"])
        # the only DELETE is the unused candidate: id 1, status candidate, no embeddings, vectors or queue rows
        for token in ("only_id = frozen_id - 1", "'candidate'", "kbp.chunk_embeddings", "kbp.chunk_embedding_vectors",
                      "kbp.embedding_queue"):
            with self.subTest(token=token):
                self.assertIn(token, self.SQL)
        with_strings = re.sub(r"--[^\n]*", "", sql_only(self.SQL)).lower()  # the ALTER lives inside format('...')
        self.assertEqual(re.findall(r"alter table (\S+) alter column (\S+) restart with", with_strings),
                         [("kbp.embedding_models", "embedding_model_id")])
        self.assertEqual(with_strings.count("alter table"), 1)
        self.assertIn("is not empty; refusing", self.SQL)

    def test_initializer_runs_it_between_alembic_and_the_model_registration(self) -> None:
        order = [self.INIT.index(token) for token in
                 ("'alembic'", "ensure_embedding_model_id.sql", "'embedding.cli'" if "'embedding.cli'" in self.INIT
                  else "app.processing.embedding.cli' @('register-model'")]
        self.assertEqual(order, sorted(order))
        self.assertIn("'-v', \"expected_database=$Database\", '-f', 'scripts\db\ensure_embedding_model_id.sql'"
                      .replace("\db", chr(92) + "db"), self.INIT.replace("\db", chr(92) + "db"))
        register = self.INIT.index("register-model")
        self.assertLess(self.INIT.index("ensure_embedding_model_id.sql"), register)
        self.assertLess(register, self.INIT.index("promote-model"))

    def test_other_runtime_lookups_do_not_depend_on_an_id(self) -> None:
        # chunking version and dedup method are resolved by name, the bridge by deterministic UUID: no id is pinned.
        import app.pipeline.cli as pipeline_cli
        import app.processing.gold.service as gold_service

        for module in (pipeline_cli, gold_service):
            ids = {name: value for name, value in vars(module).items()
                   if re.fullmatch(r"[A-Z_]*(METHOD|VERSION|CHUNKING)[A-Z_]*_ID", name)}
            self.assertEqual(ids, {}, module.__name__)

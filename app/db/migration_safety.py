"""Static safety checker for kbp migrations.

It enforces the kbp lineage rules (docs/database_schema_v1.md §22-23,
docs/database_privileges_v1.md) on the exact SQL a migration would execute:

* deny-by-default statement kinds: CREATE TABLE / CREATE [UNIQUE] INDEX /
  ALTER TABLE / DROP TABLE / DROP INDEX / GRANT / REVOKE / SET LOCAL
  (lock_timeout, statement_timeout) / read-only DO guard blocks;
* every DDL and GRANT/REVOKE target must be schema-qualified ``kbp.<name>``;
* no reference to any schema other than ``kbp`` and ``pg_catalog``, except
  the single allowlisted FK ``kbp.source_dataset_map -> public.datasets(id)``;
* protected legacy objects (``public.kb``, ``public.metadata``,
  ``public.datasets``, ``public.dataset_sources``, ``public.alembic_version``)
  may not appear anywhere else, not even inside string literals;
* no DML, no dynamic SQL, no role/ownership/schema/extension changes;
* the frozen Gold bridge dataset UUIDs may not appear (bridge bootstrap is
  deferred).

Statements are collected by running the migration's ``upgrade()`` /
``downgrade()`` against a recording stub of ``alembic.op``; no database
connection is ever made. Only ``op.execute`` with a plain ``str`` is allowed.

CLI: ``python -m app.db.migration_safety [migration.py ...]`` (defaults to all
files in migrations/versions). Exit status 1 on any violation.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Final

ALLOWED_SCHEMAS: Final[frozenset[str]] = frozenset({"kbp", "pg_catalog"})
PROTECTED_OBJECTS: Final[tuple[str, ...]] = (
    "public.kb",
    "public.metadata",
    "public.datasets",
    "public.dataset_sources",
    "public.alembic_version",
)
FROZEN_BRIDGE_UUIDS: Final[tuple[str, ...]] = (
    "a7d8d2e1-8400-5916-9440-b492e8e692ce",
    "b487f559-5f8e-56ba-9746-28658b9d09d1",
)
RUNTIME_ROLE: Final[str] = "kb_pipeline_app"

# The single approved cross-schema statement (normalized form).
ALLOWLISTED_CROSS_SCHEMA: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(
        r"alter table kbp\.source_dataset_map add constraint source_dataset_map_dataset_id_fkey "
        r"foreign key \( ?dataset_id ?\) references public\.datasets \( ?id ?\)"
        r"(?: on (?:delete|update) no action)* not valid"
    ),
)

_QUALIFIED_NAME = re.compile(r"\b([a-z_][a-z0-9_$]*)\s*\.\s*([a-z_][a-z0-9_$]*)\b")
_DO_FORBIDDEN_WORDS = re.compile(
    r"\b(insert|update|delete|truncate|drop|alter|create|grant|revoke|execute|copy|merge|"
    r"call|comment|reassign|security|set)\b"
)


@dataclass(frozen=True)
class Violation:
    phase: str
    index: int
    rule: str
    detail: str
    statement: str

    def __str__(self) -> str:
        snippet = " ".join(self.statement.split())[:160]
        return f"[{self.phase} #{self.index}] {self.rule}: {self.detail} :: {snippet}"


class MigrationCollectionError(RuntimeError):
    """Raised when a migration uses anything other than op.execute(str)."""


# --------------------------------------------------------------------------- #
# Lexing
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Lexed:
    code: str                  # lowercased executable text; literals replaced by ''
    literals: tuple[str, ...]  # single-quoted string literal contents
    top_level_semicolons: int  # ';' outside any quoting (statement smuggling)
    has_dollar_body: bool


def lex(sql: str) -> Lexed:
    """Separate executable code from literals and comments.

    Dollar-quoted bodies (DO blocks, function bodies) are executable, so their
    content is lexed recursively and kept as code. Double-quoted identifiers
    are unquoted so ``"public"."kb"`` normalizes to ``public.kb``.
    """
    code: list[str] = []
    literals: list[str] = []
    semicolons = 0
    has_dollar = False
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            end = sql.find("\n", i)
            i = n if end == -1 else end
            code.append(" ")
        elif ch == "/" and nxt == "*":
            end = sql.find("*/", i + 2)
            i = n if end == -1 else end + 2
            code.append(" ")
        elif ch == "'":
            j, buf = i + 1, []
            while j < n:
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    buf.append("'")
                    j += 2
                elif sql[j] == "'":
                    break
                else:
                    buf.append(sql[j])
                    j += 1
            literals.append("".join(buf))
            code.append("''")
            i = j + 1
        elif ch == '"':
            end = sql.find('"', i + 1)
            end = n if end == -1 else end
            code.append(sql[i + 1:end])
            i = end + 1
        elif ch == "$":
            match = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
            if match:
                tag = match.group(0)
                end = sql.find(tag, i + len(tag))
                end = n if end == -1 else end
                inner = lex(sql[i + len(tag):end])
                has_dollar = True
                literals.extend(inner.literals)
                code.append(" $$ " + inner.code + " $$ ")
                i = end + len(tag)
            else:
                code.append(ch)
                i += 1
        else:
            if ch == ";":
                semicolons += 1
            code.append(ch)
            i += 1
    normalized = " ".join("".join(code).lower().split())
    # The trailing statement terminator is harmless.
    if normalized.endswith(";"):
        semicolons -= 1
        normalized = normalized[:-1].rstrip()
    return Lexed(normalized, tuple(literals), semicolons, has_dollar)


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #

def _kind(code: str) -> str:
    patterns = (
        ("create_table", r"^create table "),
        ("create_index", r"^create (unique )?index "),
        ("alter_table", r"^alter table "),
        ("drop_table", r"^drop table "),
        ("drop_index", r"^drop index "),
        ("grant", r"^grant "),
        ("revoke", r"^revoke "),
        ("set_local", r"^set local (lock_timeout|statement_timeout) (=|to) ''$"),
        ("do_block", r"^do \$\$ "),
    )
    for name, pattern in patterns:
        if re.search(pattern, code):
            return name
    return "unknown"


def _target(kind: str, code: str) -> str | None:
    patterns = {
        "create_table": r"^create table (?:if not exists )?(\S+?)\s*\(",
        "create_index": r"^create (?:unique )?index (?:concurrently )?(?:if not exists )?\S+ on (?:only )?(\S+?)\s*[\(]",
        "alter_table": r"^alter table (?:if exists )?(?:only )?(\S+)",
        "drop_table": r"^drop table (?:if exists )?(\S+)",
        "drop_index": r"^drop index (?:concurrently )?(?:if exists )?(\S+)",
        "grant": r" on (?:table )?(\S+) to ",
        "revoke": r" on (?:table )?(\S+) from ",
    }
    pattern = patterns.get(kind)
    if pattern is None:
        return None
    match = re.search(pattern, code)
    return match.group(1).replace(" ", "") if match else None


def check_statement(sql: str, phase: str = "upgrade", index: int = 0) -> list[Violation]:
    """Return every rule violation found in one SQL statement."""
    violations: list[Violation] = []

    def fail(rule: str, detail: str) -> None:
        violations.append(Violation(phase, index, rule, detail, sql))

    if not isinstance(sql, str):
        fail("non-sql-operation", f"op.execute received {type(sql).__name__}, expected str")
        return violations

    lexed = lex(sql)
    code = lexed.code
    raw_lower = sql.lower()
    # Executable code plus string literal contents (comments excluded).
    code_and_literals = " ".join((code, *(lit.lower() for lit in lexed.literals)))
    allowlisted = any(p.fullmatch(code) for p in ALLOWLISTED_CROSS_SCHEMA)

    # Rule: one statement per op.execute (no smuggled second statement).
    if lexed.top_level_semicolons > 0:
        fail("multiple-statements", "a single op.execute must contain exactly one statement")

    # Rule: frozen bridge UUIDs must not appear (bootstrap deferred).
    for bridge_uuid in FROZEN_BRIDGE_UUIDS:
        if bridge_uuid in raw_lower:
            fail("bridge-bootstrap", f"frozen bridge dataset id {bridge_uuid} must not appear")

    # Rule: protected legacy objects may not appear in code or string literals
    # outside the allowlisted FK statement.
    if not allowlisted:
        flat = re.sub(r"\s*\.\s*", ".", code_and_literals.replace('"', ""))
        for obj in PROTECTED_OBJECTS:
            if re.search(rf"\b{re.escape(obj)}\b", flat):
                fail("protected-object", f"reference to protected legacy object {obj}")

    # Rule: only kbp / pg_catalog schema-qualified names in executable code.
    if not allowlisted:
        for schema, name in _QUALIFIED_NAME.findall(code):
            if schema not in ALLOWED_SCHEMAS:
                fail("foreign-schema", f"reference to {schema}.{name}; only kbp and pg_catalog are allowed")

    kind = _kind(code)
    if kind == "unknown":
        fail("statement-kind", "statement kind is not allowlisted for kbp migrations")
        return violations

    if kind == "do_block":
        body = code
        if _DO_FORBIDDEN_WORDS.search(re.sub(r"\braise exception\b", "", body)):
            word = _DO_FORBIDDEN_WORDS.search(body).group(1)  # type: ignore[union-attr]
            fail("do-block", f"DO blocks may only read catalogs and raise; found '{word}'")
        return violations

    if kind == "set_local":
        return violations

    target = _target(kind, code)
    if target is None or not target.startswith("kbp."):
        fail("unqualified-or-foreign-target", f"{kind} target must be kbp.<name>, got {target!r}")

    if kind == "alter_table" and re.search(r"\b(owner to|set schema|rename)\b", code):
        fail("alter-table", "ownership, schema moves and renames are not allowed")

    if kind in ("grant", "revoke"):
        if re.search(r"\ball (tables|sequences|functions|routines) in schema\b", code):
            fail("grant-scope", "schema-wide grants are not allowed; grant per table")
        if "with grant option" in code or "grant option for" in code:
            fail("grant-scope", "grant options are not allowed")
        grantee = re.search(r" (?:to|from) (\S+)$", code)
        if grantee is None or grantee.group(1) != RUNTIME_ROLE:
            fail("grantee", f"grants may only target {RUNTIME_ROLE}")

    if kind == "drop_table" and re.search(r"\bcascade\b", code):
        fail("drop-cascade", "DROP ... CASCADE is not allowed")

    return violations


def check_statements(statements: list[str] | tuple[str, ...], phase: str) -> list[Violation]:
    result: list[Violation] = []
    for index, statement in enumerate(statements):
        result.extend(check_statement(statement, phase, index))
    return result


# --------------------------------------------------------------------------- #
# Collecting a migration's SQL without a database
# --------------------------------------------------------------------------- #

class _RecordingOp(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("alembic.op")
        self.statements: list[object] = []

    def execute(self, sqltext: object, *args: object, **kwargs: object) -> None:
        if args or kwargs:
            raise MigrationCollectionError("op.execute must be called with a single SQL string")
        self.statements.append(sqltext)

    def __getattr__(self, name: str) -> object:
        raise MigrationCollectionError(
            f"op.{name} is not permitted in kbp migrations; use op.execute(<raw SQL str>)"
        )


def load_migration_module(path: str | Path) -> types.ModuleType:
    """Import a migration file as a module (no alembic import happens here)."""
    path = Path(path)
    spec = importlib.util.spec_from_file_location(f"_kbp_migration_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise MigrationCollectionError(f"cannot load migration {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def collect_sql(path: str | Path) -> dict[str, list[object]]:
    """Run upgrade() and downgrade() against a recording stub of alembic.op."""
    module = load_migration_module(path)
    saved = {name: sys.modules.get(name) for name in ("alembic", "alembic.op")}
    collected: dict[str, list[object]] = {}
    try:
        for phase in ("upgrade", "downgrade"):
            recorder = _RecordingOp()
            package = types.ModuleType("alembic")
            package.op = recorder  # type: ignore[attr-defined]
            sys.modules["alembic"] = package
            sys.modules["alembic.op"] = recorder
            getattr(module, phase)()
            collected[phase] = list(recorder.statements)
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
    return collected


def check_migration(path: str | Path) -> list[Violation]:
    collected = collect_sql(path)
    violations: list[Violation] = []
    for phase, statements in collected.items():
        violations.extend(check_statements(statements, phase))  # type: ignore[arg-type]
    return violations


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args:
        paths = [Path(a) for a in args]
    else:
        root = Path(__file__).resolve().parents[2]
        paths = sorted((root / "migrations" / "versions").glob("*.py"))
    status = 0
    for path in paths:
        try:
            violations = check_migration(path)
        except MigrationCollectionError as exc:
            print(f"FAIL {path}: {exc}")
            status = 1
            continue
        if violations:
            status = 1
            print(f"FAIL {path}: {len(violations)} violation(s)")
            for violation in violations:
                print(f"  {violation}")
        else:
            print(f"OK   {path}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())

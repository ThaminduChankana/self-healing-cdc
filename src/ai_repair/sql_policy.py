"""SQL safety policy for AI-proposed migrations.

AI-generated SQL is untrusted. Instead of pattern-matching for "bad" words
(easy to bypass), every statement must parse under a small allowlist grammar:

    ALTER TABLE <table> ADD [COLUMN] <col> <type> [attrs] [, ...]
    ALTER TABLE <table> MODIFY [COLUMN] <col> <type> [attrs] [, ...]
    ALTER TABLE <table> CHANGE [COLUMN] <old> <new> <type> [attrs]   (only if rename_column enabled)
    CREATE INDEX <name> ON <table> (<col>, ...)                       (only if create_index enabled)

    attrs := NULL | NOT NULL | DEFAULT <literal> | COMMENT '<text>' | AFTER <col> | FIRST

Comments are rejected outright (MySQL *executes* `/*! ... */` comments),
as are double-quoted strings, backslash escapes, schema-qualified names and
any character outside the grammar. Semantic checks then use the canonical
contract and the type-compatibility matrix: new columns must be nullable or
defaulted, MODIFY must be a widening, and only the event's own table may be
touched. Only the *re-rendered* statements produced here are ever executed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlparse
import yaml

from src.common.config import get_settings
from src.common.models import RiskLevel
from src.common.sources import Contract
from src.validator.type_compatibility import COMPATIBLE, IDENTICAL, TypeCompatibility, get_type_compatibility

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


class SqlPolicyError(Exception):
    pass


@dataclass
class Token:
    kind: str  # word | ident | string | number | punct
    value: str

    @property
    def upper(self) -> str:
        return self.value.upper()


@dataclass
class ParsedAction:
    kind: str  # add_column | modify_column | change_column | create_index
    table: str
    column: str = ""
    new_column: str = ""
    sql_type: str = ""
    nullable: bool = True
    default: str | None = None
    position: str = ""
    index_columns: list[str] = field(default_factory=list)

    def render(self) -> str:
        """Canonical SQL rebuilt from the parse tree (the only SQL ever executed)."""
        q = lambda n: f"`{n}`"  # noqa: E731
        if self.kind == "create_index":
            cols = ", ".join(q(c) for c in self.index_columns)
            return f"CREATE INDEX {q(self.column)} ON {q(self.table)} ({cols})"
        coldef = f"{self.sql_type} {'NULL' if self.nullable else 'NOT NULL'}"
        if self.default is not None:
            coldef += f" DEFAULT {self.default}"
        if self.position:
            coldef += f" {self.position}"
        if self.kind == "add_column":
            return f"ALTER TABLE {q(self.table)} ADD COLUMN {q(self.column)} {coldef}"
        if self.kind == "modify_column":
            return f"ALTER TABLE {q(self.table)} MODIFY COLUMN {q(self.column)} {coldef}"
        return f"ALTER TABLE {q(self.table)} CHANGE COLUMN {q(self.column)} {q(self.new_column)} {coldef}"


@dataclass
class SqlPolicyResult:
    allowed: bool
    violations: list[str] = field(default_factory=list)
    actions: list[ParsedAction] = field(default_factory=list)
    risk: RiskLevel = RiskLevel.LOW
    # True when the SQL is dangerous (forbidden operation, comment smuggling,
    # narrowing...). Non-destructive policy mismatches (e.g. an unnecessary
    # ADD COLUMN) are retryable with feedback instead.
    destructive: bool = False

    @property
    def rendered(self) -> list[str]:
        return [a.render() for a in self.actions]


def tokenize(sql: str) -> list[Token]:
    tokens: list[Token] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
            continue
        if sql.startswith("--", i) or sql.startswith("/*", i) or c == "#":
            raise SqlPolicyError("SQL comments are not allowed")
        if c == "'":
            j, buf = i + 1, []
            while True:
                if j >= n:
                    raise SqlPolicyError("unterminated string literal")
                if sql[j] == "\\":
                    raise SqlPolicyError("backslash escapes are not allowed in string literals")
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        buf.append("''")
                        j += 2
                        continue
                    break
                buf.append(sql[j])
                j += 1
            tokens.append(Token("string", "".join(buf)))
            i = j + 1
            continue
        if c == "`":
            j = sql.find("`", i + 1)
            if j == -1:
                raise SqlPolicyError("unterminated quoted identifier")
            tokens.append(Token("ident", sql[i + 1:j]))
            i = j + 1
            continue
        if c == '"':
            raise SqlPolicyError("double-quoted strings/identifiers are not allowed")
        m = re.match(r"[A-Za-z_][A-Za-z0-9_]*", sql[i:])
        if m:
            tokens.append(Token("word", m.group(0)))
            i += len(m.group(0))
            continue
        m = re.match(r"\d+(?:\.\d+)?", sql[i:])
        if m:
            tokens.append(Token("number", m.group(0)))
            i += len(m.group(0))
            continue
        if c in "(),;.-":
            tokens.append(Token("punct", c))
            i += 1
            continue
        raise SqlPolicyError(f"character {c!r} is not allowed in migration SQL")
    return tokens


class _Parser:
    def __init__(self, tokens: list[Token]):
        self.t = tokens
        self.i = 0
        self.qualifiers: set[str] = set()

    def peek(self, k: int = 0) -> Token | None:
        return self.t[self.i + k] if self.i + k < len(self.t) else None

    def next(self) -> Token:
        tok = self.peek()
        if tok is None:
            raise SqlPolicyError("unexpected end of statement")
        self.i += 1
        return tok

    def accept(self, *words: str) -> bool:
        tok = self.peek()
        if tok and tok.kind == "word" and tok.upper in words:
            self.i += 1
            return True
        return False

    def expect(self, word: str) -> None:
        tok = self.next()
        if tok.kind != "word" or tok.upper != word:
            raise SqlPolicyError(f"expected {word}, found {tok.value!r}")

    def punct(self, ch: str) -> bool:
        tok = self.peek()
        if tok and tok.kind == "punct" and tok.value == ch:
            self.i += 1
            return True
        return False

    def ident(self) -> str:
        tok = self.next()
        if tok.kind not in ("word", "ident") or not _IDENT_RE.match(tok.value):
            raise SqlPolicyError(f"invalid identifier {tok.value!r}")
        if self.peek() and self.peek().kind == "punct" and self.peek().value == ".":
            raise SqlPolicyError("schema-qualified names are only allowed for the table name")
        return tok.value

    def table_name(self) -> str:
        """[schema.]table — the qualifier is recorded and checked by the policy, then dropped."""
        tok = self.next()
        if tok.kind not in ("word", "ident") or not _IDENT_RE.match(tok.value):
            raise SqlPolicyError(f"invalid table name {tok.value!r}")
        if self.punct("."):
            self.qualifiers.add(tok.value)
            return self.ident()
        return tok.value

    def done(self) -> bool:
        return self.i >= len(self.t)

    # statement := ALTER TABLE ... | CREATE INDEX ...
    def statement(self) -> list[ParsedAction]:
        if self.accept("ALTER"):
            self.expect("TABLE")
            table = self.table_name()
            actions = [self.alter_spec(table)]
            while self.punct(","):
                actions.append(self.alter_spec(table))
            return actions
        if self.accept("CREATE"):
            self.expect("INDEX")
            name = self.ident()
            self.expect("ON")
            table = self.table_name()
            if not self.punct("("):
                raise SqlPolicyError("expected '(' after index table")
            cols = [self.ident()]
            while self.punct(","):
                cols.append(self.ident())
            if not self.punct(")"):
                raise SqlPolicyError("expected ')' after index columns")
            return [ParsedAction(kind="create_index", table=table, column=name, index_columns=cols)]
        tok = self.peek()
        raise SqlPolicyError(f"statement type {tok.value if tok else '<empty>'!r} is not allowed")

    def alter_spec(self, table: str) -> ParsedAction:
        if self.accept("ADD"):
            self.accept("COLUMN")
            col = self.ident()
            return self.coldef(ParsedAction(kind="add_column", table=table, column=col))
        if self.accept("MODIFY"):
            self.accept("COLUMN")
            col = self.ident()
            return self.coldef(ParsedAction(kind="modify_column", table=table, column=col))
        if self.accept("CHANGE"):
            self.accept("COLUMN")
            old, new = self.ident(), self.ident()
            return self.coldef(ParsedAction(kind="change_column", table=table, column=old, new_column=new))
        tok = self.peek()
        raise SqlPolicyError(f"ALTER TABLE action {tok.value if tok else '<empty>'!r} is not allowed")

    def coldef(self, action: ParsedAction) -> ParsedAction:
        tok = self.next()
        if tok.kind != "word":
            raise SqlPolicyError(f"expected column type, found {tok.value!r}")
        sql_type = tok.upper
        if self.punct("("):
            n1 = self.next()
            if n1.kind != "number":
                raise SqlPolicyError("type length must be a number")
            sql_type += f"({n1.value}"
            if self.punct(","):
                n2 = self.next()
                if n2.kind != "number":
                    raise SqlPolicyError("type scale must be a number")
                sql_type += f",{n2.value}"
            if not self.punct(")"):
                raise SqlPolicyError("expected ')' in column type")
            sql_type += ")"
        if self.accept("UNSIGNED"):
            sql_type += " UNSIGNED"
        action.sql_type = sql_type
        while True:
            if self.accept("NOT"):
                self.expect("NULL")
                action.nullable = False
            elif self.accept("NULL"):
                action.nullable = True
            elif self.accept("DEFAULT"):
                action.default = self.literal()
            elif self.accept("COMMENT"):
                if self.next().kind != "string":
                    raise SqlPolicyError("COMMENT requires a string literal")
            elif self.accept("AFTER"):
                action.position = f"AFTER `{self.ident()}`"
            elif self.accept("FIRST"):
                action.position = "FIRST"
            else:
                break
        return action

    def literal(self) -> str:
        neg = self.punct("-")
        tok = self.next()
        if tok.kind == "number":
            return f"-{tok.value}" if neg else tok.value
        if neg:
            raise SqlPolicyError("'-' must precede a number")
        if tok.kind == "string":
            return f"'{tok.value}'"
        if tok.kind == "word" and tok.upper in ("NULL", "CURRENT_TIMESTAMP", "TRUE", "FALSE"):
            return tok.upper
        raise SqlPolicyError(f"DEFAULT value {tok.value!r} is not an allowed literal")


class MigrationPolicy:
    def __init__(self, path: Path | None = None, compatibility: TypeCompatibility | None = None):
        path = path or get_settings().migration_policy_path
        self.config: dict[str, Any] = yaml.safe_load(Path(path).read_text()) or {}
        self._compat = compatibility or get_type_compatibility()
        self._forbidden = {k.upper() for k in self.config.get("forbidden_keywords", [])}
        self._allowed_types = {t.upper() for t in self.config.get("allowed_column_types", [])}
        self._actions = self.config.get("allowed_actions", {})

    def summary(self) -> dict[str, Any]:
        """Short policy description for the prompt."""
        return {
            "allowed": [k for k, v in self._actions.items() if v],
            "new_columns_must_be_nullable_or_defaulted":
                self.config.get("require_nullable_or_default_for_new_columns", True),
            "modify_only_for_widening": True,
            "everything_else": "rejected",
        }

    def validate(self, sql: str | None, contract: Contract) -> SqlPolicyResult:
        if sql is None or not sql.strip():
            return SqlPolicyResult(allowed=True)
        violations: list[str] = []
        if len(sql) > self.config.get("max_sql_length", 2000):
            return SqlPolicyResult(False, ["SQL exceeds maximum length"], risk=RiskLevel.HIGH, destructive=True)
        if re.search(r"/\*|--|#", re.sub(r"'(?:[^']|'')*'", "''", sql)):
            return SqlPolicyResult(False, ["SQL comments are not allowed"], risk=RiskLevel.HIGH, destructive=True)
        # Keyword scan outside string literals, before strict tokenizing, so the
        # audit trail names the dangerous operation even if the SQL is malformed.
        words = re.findall(r"[A-Za-z_]+", re.sub(r"'(?:[^']|'')*'", "''", sql))
        bad = sorted({w.upper() for w in words if w.upper() in self._forbidden})
        if bad:
            return SqlPolicyResult(False, [f"forbidden keyword(s): {', '.join(bad)}"], risk=RiskLevel.CRITICAL,
                                   destructive=True)
        try:
            tokens = tokenize(sql)
        except SqlPolicyError as exc:
            return SqlPolicyResult(False, [str(exc)], risk=RiskLevel.HIGH, destructive=True)

        # Secondary check with an independent parser: only ALTER/CREATE statements.
        for stmt in sqlparse.parse(sql):
            kind = stmt.get_type()
            if str(stmt).strip(" ;\n\t") and kind not in ("ALTER", "CREATE"):
                violations.append(f"statement type {kind} is not allowed")

        statements: list[list[Token]] = [[]]
        for tok in tokens:
            if tok.kind == "punct" and tok.value == ";":
                statements.append([])
            else:
                statements[-1].append(tok)
        statements = [s for s in statements if s]
        if len(statements) > self.config.get("max_statements", 3):
            violations.append(f"too many statements ({len(statements)})")

        actions: list[ParsedAction] = []
        destructive = bool(violations)  # non ALTER/CREATE statement types
        for stmt in statements:
            parser = _Parser(stmt)
            try:
                actions.extend(parser.statement())
                if not parser.done():
                    raise SqlPolicyError(f"unexpected trailing token {parser.peek().value!r}")
            except SqlPolicyError as exc:
                violations.append(str(exc))
                destructive = True  # outside the grammar: treat as hostile
            foreign = parser.qualifiers - {contract.database}
            if foreign:
                violations.append(f"schema-qualified name {sorted(foreign)} is not the event's database "
                                  f"'{contract.database}'")
                destructive = True

        risk = RiskLevel.LOW
        for a in actions:
            v, r, d = self._check_action(a, contract)
            violations.extend(v)
            risk = RiskLevel.max(risk, r)
            destructive = destructive or d
        if violations:
            return SqlPolicyResult(False, violations, actions,
                                   RiskLevel.max(risk, RiskLevel.HIGH if destructive else RiskLevel.MEDIUM),
                                   destructive=destructive)
        return SqlPolicyResult(True, [], actions, risk)

    def _check_action(self, a: ParsedAction, contract: Contract) -> tuple[list[str], RiskLevel, bool]:
        """Returns (violations, risk, destructive)."""
        v: list[str] = []
        destructive = False
        if a.table != contract.table:
            v.append(f"migration targets table '{a.table}' but the event belongs to '{contract.table}'")
            destructive = True
        fields = contract.fields
        base = a.sql_type.split("(")[0].replace(" UNSIGNED", "")
        if a.kind != "create_index" and base not in self._allowed_types:
            v.append(f"column type {a.sql_type} is not allowed")

        if a.kind == "add_column":
            if not self._actions.get("add_column"):
                v.append("ADD COLUMN is disabled by policy")
            if a.column in fields:
                v.append(f"column '{a.column}' already exists in the contract — no migration is needed for it")
            if self.config.get("require_nullable_or_default_for_new_columns", True) \
                    and not a.nullable and a.default is None:
                v.append(f"new column '{a.column}' must be nullable or have a DEFAULT")
            return v, RiskLevel.LOW, destructive
        if a.kind == "modify_column":
            if not self._actions.get("modify_column_widen"):
                v.append("MODIFY COLUMN is disabled by policy")
            spec = fields.get(a.column)
            if spec is None:
                v.append(f"cannot MODIFY unknown column '{a.column}'")
            else:
                verdict = self._compat.sql_verdict(spec.sql_type or "", a.sql_type) if spec.sql_type else "unknown"
                if verdict == IDENTICAL and spec.nullable == a.nullable:
                    v.append(f"MODIFY {a.column} {a.sql_type} does not change the column (no-op) — "
                             f"use migration_sql null")
                elif verdict not in (COMPATIBLE, IDENTICAL):
                    v.append(f"MODIFY {a.column} {spec.sql_type} -> {a.sql_type} is not a widening ({verdict})")
                    destructive = True
                if spec.nullable and not a.nullable:
                    v.append(f"MODIFY {a.column} would make a nullable column NOT NULL")
                    destructive = True
            return v, RiskLevel.LOW, destructive
        if a.kind == "change_column":
            if not self._actions.get("rename_column"):
                v.append("column rename (CHANGE COLUMN) is disabled by policy")
                destructive = True
            return v, RiskLevel.MEDIUM, destructive
        if a.kind == "create_index":
            if not self._actions.get("create_index"):
                v.append("CREATE INDEX is disabled by policy")
            for c in a.index_columns:
                if c not in fields:
                    v.append(f"index column '{c}' is not in the contract")
            return v, RiskLevel.MEDIUM, destructive
        v.append(f"unsupported action {a.kind}")
        return v, RiskLevel.HIGH, True

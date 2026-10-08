"""Migration sandbox: AI-proposed SQL runs ONLY in an isolated MySQL instance.

Workflow for one repair:
  1. create a throw-away database `sbx_<id>` on the sandbox server
  2. create the downstream table from the canonical contract (x-sql-type)
  3. snapshot information_schema, apply the *re-rendered* policy-approved SQL
  4. snapshot again and verify: no column removed, every type change is a
     widening, every new column is nullable or defaulted
  5. load the repaired row(s) into the migrated table (transformation test)
  6. drop the throw-away database

The sandbox credentials are scoped to `sbx\\_%` databases; the source database
is never reachable with them.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, ContextManager

from src.common.database import DatabaseConnector, DatabaseUnavailableError, sandbox_connection
from src.common.metrics import SANDBOX_FAILURES
from src.common.models import CheckResult
from src.common.sources import Contract
from src.validator.type_compatibility import COMPATIBLE, IDENTICAL, TypeCompatibility, get_type_compatibility

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


class SandboxUnavailableError(Exception):
    pass


@dataclass
class SqlSandboxResult:
    status: str  # PASSED | FAILED | NOT_REQUIRED
    checks: list[CheckResult] = field(default_factory=list)
    columns_before: dict[str, dict[str, Any]] = field(default_factory=dict)
    columns_after: dict[str, dict[str, Any]] = field(default_factory=dict)
    error: str = ""


def _q(name: str) -> str:
    if not _NAME_RE.match(name):
        raise ValueError(f"unsafe identifier {name!r}")
    return f"`{name}`"


def contract_ddl(contract: Contract, schema: str) -> str:
    cols = []
    for f in contract.fields.values():
        if not f.sql_type:
            raise ValueError(f"contract field {f.name} has no x-sql-type")
        cols.append(f"{_q(f.name)} {f.sql_type} {'NULL' if f.nullable else 'NOT NULL'}")
    if contract.primary_key:
        cols.append(f"PRIMARY KEY ({', '.join(_q(c) for c in contract.primary_key)})")
    return f"CREATE TABLE {_q(schema)}.{_q(contract.table)} ({', '.join(cols)})"


class SqlSandbox:
    def __init__(
        self,
        connection_factory: Callable[[], ContextManager[DatabaseConnector]] = sandbox_connection,
        compatibility: TypeCompatibility | None = None,
    ):
        self._connect = connection_factory
        self._compat = compatibility or get_type_compatibility()

    def run(self, contract: Contract, statements: list[str], rows: list[dict[str, Any]],
            require_db: bool) -> SqlSandboxResult:
        """Apply `statements` (already policy-approved and re-rendered) and load `rows`."""
        schema = f"sbx_{uuid.uuid4().hex[:12]}"
        try:
            ctx = self._connect()
            conn = ctx.__enter__()
        except DatabaseUnavailableError as exc:
            if require_db:
                raise SandboxUnavailableError(str(exc)) from exc
            return SqlSandboxResult("NOT_REQUIRED", [CheckResult(
                name="sandbox_db_load", passed=True, detail=f"skipped: sandbox DB unavailable ({exc})")])
        checks: list[CheckResult] = []
        try:
            conn.execute(f"CREATE DATABASE {_q(schema)}")
            conn.execute(contract_ddl(contract, schema))
            before = self._columns(conn, schema, contract.table)
            after = before
            if statements:
                conn.execute(f"USE {_q(schema)}")
                for stmt in statements:
                    conn.execute(stmt)
                after = self._columns(conn, schema, contract.table)
                problems = self._verify(before, after)
                checks.append(CheckResult(name="sandbox_migration", passed=not problems,
                                          detail="; ".join(problems) or f"applied {len(statements)} statement(s)"))
            load_problems = self._load_rows(conn, schema, contract.table, rows, after)
            checks.append(CheckResult(name="sandbox_db_load", passed=not load_problems,
                                      detail="; ".join(load_problems) or f"loaded {len(rows)} row(s) into the downstream-shaped table"))
            failed = [c for c in checks if not c.passed]
            if failed:
                SANDBOX_FAILURES.labels(stage="sql", reason=failed[0].name).inc()
            return SqlSandboxResult("FAILED" if failed else "PASSED", checks, before, after)
        except Exception as exc:  # noqa: BLE001 - any DB error fails the repair, never the worker
            SANDBOX_FAILURES.labels(stage="sql", reason="execution_error").inc()
            checks.append(CheckResult(name="sandbox_migration", passed=False, detail=str(exc)[:500]))
            return SqlSandboxResult("FAILED", checks, error=str(exc)[:500])
        finally:
            try:
                conn.execute(f"DROP DATABASE IF EXISTS {_q(schema)}")  # our own scratch database
            except Exception:  # noqa: BLE001
                pass
            ctx.__exit__(None, None, None)

    @staticmethod
    def _columns(conn: DatabaseConnector, schema: str, table: str) -> dict[str, dict[str, Any]]:
        rows = conn.query(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
        return {
            r["COLUMN_NAME"]: {"type": str(r["COLUMN_TYPE"]).upper(), "nullable": r["IS_NULLABLE"] == "YES",
                               "default": r["COLUMN_DEFAULT"]}
            for r in rows
        }

    def _verify(self, before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]]) -> list[str]:
        problems = []
        for name, col in before.items():
            if name not in after:
                problems.append(f"column {name} was removed")
                continue
            new = after[name]
            if new["type"] != col["type"]:
                verdict = self._compat.sql_verdict(col["type"], new["type"])
                if verdict not in (COMPATIBLE, IDENTICAL):
                    problems.append(f"column {name} changed {col['type']} -> {new['type']} ({verdict})")
            if col["nullable"] and not new["nullable"]:
                problems.append(f"column {name} became NOT NULL")
        for name, col in after.items():
            if name not in before and not col["nullable"] and col["default"] is None:
                problems.append(f"new column {name} is NOT NULL without a default")
        return problems

    @staticmethod
    def _load_rows(conn: DatabaseConnector, schema: str, table: str, rows: list[dict[str, Any]],
                   columns: dict[str, dict[str, Any]]) -> list[str]:
        problems = []
        for row in rows:
            cols = [c for c in row if c in columns]
            extra = [c for c in row if c not in columns]
            if extra:
                problems.append(f"row has columns not in table: {extra}")
                continue
            sql = (f"INSERT INTO {_q(schema)}.{_q(table)} ({', '.join(_q(c) for c in cols)}) "
                   f"VALUES ({', '.join(['%s'] * len(cols))})")
            try:
                conn.execute(sql, [row[c] for c in cols])
            except Exception as exc:  # noqa: BLE001
                problems.append(f"insert failed: {str(exc)[:300]}")
        return problems

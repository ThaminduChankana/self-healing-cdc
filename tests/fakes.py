"""Test doubles that need application imports (kept out of conftest import time)."""

from __future__ import annotations

import json
from typing import Any

from src.common.models import CheckResult, DLQMessage
from src.sandbox.sql_sandbox import SandboxUnavailableError, SqlSandboxResult
from src.validator.debezium_parser import parse
from src.validator.drift_detector import DriftDetector
from tests.conftest import envelope


class FakeSqlSandbox:
    def __init__(self, status: str = "PASSED", unavailable: bool = False):
        self.status = status
        self.unavailable = unavailable
        self.calls: list[dict[str, Any]] = []

    def run(self, contract, statements, rows, require_db):
        self.calls.append({"statements": statements, "rows": rows, "require_db": require_db})
        if self.unavailable and require_db:
            raise SandboxUnavailableError("sandbox down (simulated)")
        passed = self.status == "PASSED"
        checks = [CheckResult(name="sandbox_db_load", passed=passed, detail="fake")]
        if statements:
            checks.insert(0, CheckResult(name="sandbox_migration", passed=passed, detail="fake"))
        return SqlSandboxResult(self.status, checks)


def dlq_for(contract, table: str, row: dict[str, Any], cols: dict, op: str = "u", pos: int = 2000,
            before: dict[str, Any] | None = None) -> DLQMessage:
    ev = parse(envelope(table, row, before=before, op=op, cols=cols, pos=pos), key={"k": pos})
    report = DriftDetector().detect(ev, contract)
    return DLQMessage(
        event_id=ev.event_id, correlation_id=f"cdc.mutations/0/{pos}", topic="cdc.mutations", offset=pos,
        source_name=ev.source_name, source_database=ev.source_database, source_table=ev.source_table,
        operation=ev.operation, key=ev.key, payload=ev.payload, before=ev.before,
        source_metadata=ev.source_metadata, column_schema=ev.column_schema,
        canonical_schema=contract.schema,
        drift_report=report.model_dump(mode="json", exclude={"original_event", "canonical_schema"}),
    )


def proposal(**over: Any) -> str:
    base = {
        "classification": "RENAMED_COLUMN",
        "confidence": 0.96,
        "reasoning_summary": "stock_quantity is the renamed quantity column.",
        "migration_sql": None,
        "transformation_code": "def transform(event):\n    return {'product_id': event['product_id'], "
                               "'quantity': event['stock_quantity']}",
        "test_cases": [],
        "risk_level": "LOW",
    }
    base.update(over)
    return json.dumps(base)

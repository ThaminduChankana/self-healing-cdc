"""Deterministic LLM stand-ins for tests and offline runs (LLM_MODE=mock).

`MockLLMClient` derives a repair purely from the deterministic drift report.
`ScriptedLLMClient` replays canned raw responses — used to simulate a model
that hallucinates, returns invalid JSON or proposes destructive SQL.
"""

from __future__ import annotations

import json
from typing import Any

from src.ai_repair.llm_client import LLMClient, LLMUnavailableError


class MockLLMClient(LLMClient):
    model = "mock-deterministic"

    def _complete(self, system_prompt: str, user_prompt: str, response_schema: dict[str, Any]) -> str:
        report = json.loads(_between(user_prompt, "<detector_report>", "</detector_report>"))
        contract = json.loads(_between(user_prompt, "<canonical_contract>", "</canonical_contract>"))
        fields = list(contract["fields"])
        table = contract["table"].split(".")[-1]
        renames = {r["unexpected_field"]: r["missing_field"]
                   for r in report.get("possible_renames", []) if not r.get("ambiguous")}
        lines = ["def transform(event):", "    out = {}"]
        for f in fields:
            src = next((s for s, d in renames.items() if d == f), f)
            lines.append(f"    out[{f!r}] = event.get({src!r})")
        lines.append("    return out")

        migration = None
        widenings = [t for t in report.get("type_mismatches", []) if t.get("compatibility") == "compatible"]
        if widenings:
            stmts = []
            for t in widenings:
                nullable = contract["fields"].get(t["field"], {}).get("nullable", True)
                stmts.append(f"ALTER TABLE {table} MODIFY COLUMN {t['field']} {t['actual']} "
                             f"{'NULL' if nullable else 'NOT NULL'}")
            migration = "; ".join(stmts)

        drift = report.get("drift_type", "UNKNOWN")
        clear = drift in {"RENAMED_COLUMN", "ADDED_COLUMN", "TYPE_CHANGE"} and not report.get("ambiguous")
        return json.dumps({
            "classification": drift if drift != "NONE" else "UNKNOWN",
            "confidence": 0.95 if clear else 0.5,
            "reasoning_summary": f"Deterministic mock repair for {drift}.",
            "migration_sql": migration,
            "transformation_code": "\n".join(lines),
            "test_cases": [],
            "risk_level": "LOW" if clear else "MEDIUM",
        })

    def is_available(self) -> bool:
        return True

    def model_available(self) -> bool:
        return True


class ScriptedLLMClient(LLMClient):
    """Returns pre-defined raw responses in order (the last one repeats)."""

    def __init__(self, responses: list[str | dict[str, Any] | Exception], model: str = "scripted"):
        super().__init__()
        self._responses = responses
        self.model = model
        self.calls = 0
        self.prompts: list[str] = []

    def _complete(self, system_prompt: str, user_prompt: str, response_schema: dict[str, Any]) -> str:
        self.prompts.append(user_prompt)
        item = self._responses[min(self.calls, len(self._responses) - 1)]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, str) else json.dumps(item)

    def is_available(self) -> bool:
        return not any(isinstance(r, LLMUnavailableError) for r in self._responses[self.calls:self.calls + 1])

    def model_available(self) -> bool:
        return True


def _between(text: str, start: str, end: str) -> str:
    i = text.index(start) + len(start)
    return text[i:text.index(end, i)].replace("\\u003c", "<")

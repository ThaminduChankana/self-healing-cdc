"""Human-readable repair report for data engineers (no source code reading required)."""

from __future__ import annotations

from typing import Any

from src.common.models import AuditEvent


def render_report(audit: AuditEvent, drift: dict[str, Any], transformation_code: str = "") -> str:
    renames = drift.get("possible_renames", [])
    mapping = ", ".join(
        f"{r['unexpected_field']} -> {r['missing_field']} (heuristic confidence {r['confidence']:.2f}"
        f"{', ambiguous' if r.get('ambiguous') else ''})"
        for r in renames
    ) or "none detected"
    types = ", ".join(
        f"{t['field']}: {t['expected']} -> {t['actual']} ({t['compatibility']})"
        for t in drift.get("type_mismatches", [])
    ) or "none"
    checks = "\n".join(
        f"    [{'PASS' if c.passed else 'FAIL'}] {c.name}: {c.detail}" for c in audit.checks
    ) or "    (no checks run)"
    code = "\n".join(f"    {line}" for line in transformation_code.strip().splitlines()) or "    (none)"
    lines = [
        "═════════════════════ REPAIR REPORT ═════════════════════",
        f"Event:                   {audit.event_id}",
        f"Repair:                  {audit.repair_id}",
        f"Source table:            {audit.source_table}",
        f"Detected drift:          {drift.get('drift_type', audit.drift_type)}"
        f"{' (ambiguous)' if drift.get('ambiguous') else ''}",
        f"Missing field(s):        {', '.join(drift.get('missing_fields', [])) or 'none'}",
        f"Unexpected field(s):     {', '.join(drift.get('unexpected_fields', [])) or 'none'}",
        f"Detected type mismatch:  {types}",
        f"Rename candidates:       {mapping}",
        f"Repair classification:   {audit.classification or 'n/a'}",
        f"Confidence (model):      {audit.confidence:.2f}",
        f"Risk level (effective):  {audit.risk_level or 'n/a'}",
        f"Model / source:          {audit.model} / {audit.repair_source or 'n/a'} (AI attempts: {audit.ai_attempts})",
        f"Model summary:           {audit.reasoning_summary or 'n/a'}",
        f"Generated migration:     {audit.migration_sql or 'none'}  [{audit.migration_status}]",
        "Generated mapping (transformation):",
        code,
        f"Sandbox result:          {audit.sandbox_result}",
        f"Final validation result: {audit.validation_result}",
        "Checks:",
        checks,
        f"DECISION:                {audit.repair_result}",
    ]
    if audit.reasons:
        lines.append("Reasons:")
        lines.extend(f"    - {r}" for r in audit.reasons)
    lines.append("═════════════════════════════════════════════════════════")
    return "\n".join(lines)

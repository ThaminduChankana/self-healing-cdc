"""Validation of a transformation's output against the canonical contract.

Besides JSON Schema conformance this enforces *data fidelity*: a repair may
rename, drop or convert fields, but it must not invent or alter values.
  * a field present unchanged in both the source row and the contract must
    keep its exact value
  * a confidently detected rename must carry the source value across
  * a type-changed field must keep an equivalent value
"""

from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator

from src.common.models import CheckResult


def _equivalent(a: Any, b: Any) -> bool:
    if a == b:
        return True
    if isinstance(a, bool) or isinstance(b, bool):
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return float(a) == float(b)
    try:
        return str(a).strip() == str(b).strip() or float(str(a)) == float(str(b))
    except (TypeError, ValueError):
        return False


class OutputValidator:
    def validate(
        self,
        output: dict[str, Any],
        source_row: dict[str, Any],
        canonical_schema: dict[str, Any],
        drift_report: dict[str, Any],
    ) -> list[CheckResult]:
        checks: list[CheckResult] = []
        validator = Draft202012Validator(canonical_schema)
        errors = [f"{e.json_path}: {e.message}" for e in validator.iter_errors(output)]
        checks.append(CheckResult(name="schema_validation", passed=not errors,
                                  detail="; ".join(errors)[:1000] or "conforms to the canonical JSON Schema"))

        required = set(canonical_schema.get("required", []))
        missing = sorted(required - set(output))
        checks.append(CheckResult(name="required_fields", passed=not missing,
                                  detail=f"missing: {missing}" if missing else "all required fields present"))

        props = set(canonical_schema.get("properties", {}))
        if canonical_schema.get("additionalProperties", True) is False:
            extra = sorted(set(output) - props)
            checks.append(CheckResult(name="unexpected_fields", passed=not extra,
                                      detail=f"unexpected: {extra}" if extra else "no unexpected fields"))

        problems: list[str] = []
        type_changed = {t["field"] for t in drift_report.get("type_mismatches", [])}
        for name in sorted(props & set(source_row)):
            if name not in output:
                continue
            if name in type_changed:
                if not _equivalent(output[name], source_row[name]):
                    problems.append(f"{name}: converted value {output[name]!r} != source {source_row[name]!r}")
            elif output[name] != source_row[name] and not _equivalent(output[name], source_row[name]):
                problems.append(f"{name}: value changed {source_row[name]!r} -> {output[name]!r}")
        for r in drift_report.get("possible_renames", []):
            if r.get("ambiguous"):
                continue
            src, dst = r["unexpected_field"], r["missing_field"]
            if src in source_row and dst in output and not _equivalent(output[dst], source_row[src]):
                problems.append(f"rename {src}->{dst}: value {output[dst]!r} != source {source_row[src]!r}")
        checks.append(CheckResult(name="data_fidelity", passed=not problems,
                                  detail="; ".join(problems)[:1000] or "source values preserved"))
        return checks

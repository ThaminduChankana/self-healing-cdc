"""Deterministic schema-drift detection and classification.

Compares a CDC row image (plus the per-column schema Debezium ships in the
envelope) with the canonical contract. Everything here is deterministic; the
LLM is only consulted later, and it receives this report as evidence.

Rename detection is a *heuristic*: it combines name similarity, token
containment, configured synonyms, type compatibility and nullability. It can
be wrong (e.g. two columns renamed at once to unrelated names, or a rename
that coincides with a type change). Low-confidence or competing candidates
are reported as ambiguous instead of being presented as fact.
"""

from __future__ import annotations

import hashlib
import json
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

from src.common.config import get_settings
from src.common.models import CDCEvent, DriftReport, DriftType, RenameCandidate, TypeChange
from src.common.sources import Contract, FieldSpec
from src.validator.type_compatibility import (
    COMPATIBLE,
    DESTRUCTIVE,
    IDENTICAL,
    REQUIRES_TRANSFORMATION,
    RISKY,
    TypeCompatibility,
    get_type_compatibility,
)

RENAME_MIN_CONFIDENCE = 0.55
RENAME_CLEAR_CONFIDENCE = 0.75
RENAME_COMPETITION_MARGIN = 0.08

_TYPE_SCORE = {IDENTICAL: 1.0, COMPATIBLE: 0.8, REQUIRES_TRANSFORMATION: 0.4, RISKY: 0.2, DESTRUCTIVE: 0.0}


def _json_family(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def _tokens(name: str) -> list[str]:
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return [t for t in re.split(r"[^A-Za-z0-9]+", name.lower()) if t]


class RenameHints:
    def __init__(self, path: Path | None = None):
        path = path or get_settings().rename_hints_path
        data = yaml.safe_load(Path(path).read_text()) if Path(path).exists() else {}
        data = data or {}
        self.synonyms: dict[str, set[str]] = {
            str(k).lower(): {str(v).lower() for v in vals} for k, vals in (data.get("synonyms") or {}).items()
        }
        self.decorators: set[str] = {str(d).lower() for d in (data.get("decorators") or [])}

    def are_synonyms(self, a: str, b: str) -> bool:
        a, b = a.lower(), b.lower()
        return b in self.synonyms.get(a, set()) or a in self.synonyms.get(b, set())


class DriftDetector:
    def __init__(self, compatibility: TypeCompatibility | None = None, hints: RenameHints | None = None):
        self._compat = compatibility or get_type_compatibility()
        self._hints = hints or RenameHints()

    # ── public ──────────────────────────────────────────────────────────────
    def detect(self, event: CDCEvent, contract: Contract) -> DriftReport:
        payload = event.payload
        fields = contract.fields
        col_schema = event.column_schema
        report = DriftReport(
            event_id=event.event_id,
            source_table=f"{event.source_database}.{event.source_table}",
            operation=event.operation,
            contract_id=contract.contract_id,
            original_event=payload,
            canonical_schema=contract.schema,
        )

        payload_fields = set(payload)
        contract_fields = set(fields)
        missing = sorted(contract_fields - payload_fields)
        unexpected = sorted(payload_fields - contract_fields) if not contract.allows_additional else []

        for name in sorted(payload_fields & contract_fields):
            tc = self._type_change(fields[name], payload[name], col_schema.get(name))
            if tc:
                report.type_mismatches.append(tc)
            nc = self._nullability_change(fields[name], payload[name], col_schema.get(name))
            if nc:
                report.nullability_changes.append(nc)

        report.missing_fields = missing
        report.unexpected_fields = unexpected
        if missing and unexpected:
            report.possible_renames = self._rename_candidates(missing, unexpected, fields, payload, col_schema)

        report.validation_errors = contract.validate(payload)
        report.drift_detected = bool(
            missing or unexpected or report.type_mismatches
            or report.nullability_changes or report.validation_errors
        )
        if report.drift_detected:
            self._classify(report)
        report.fingerprint = self.fingerprint(report, contract)
        return report

    @staticmethod
    def only_compatible_type_changes(report: DriftReport) -> bool:
        """True when the only drift is lossless widening (e.g. INT -> BIGINT)."""
        return (
            report.drift_type == DriftType.TYPE_CHANGE
            and not report.validation_errors
            and all(t.compatibility == COMPATIBLE for t in report.type_mismatches)
        )

    @staticmethod
    def fingerprint(report: DriftReport, contract: Contract) -> str:
        """Identity of the *shape* of a drift (not of the event) — used for the repair cache."""
        material = {
            "contract": contract.key,
            "contract_version": contract.version_hash,
            "missing": report.missing_fields,
            "unexpected": report.unexpected_fields,
            "types": sorted((t.field, t.expected, t.actual, t.level) for t in report.type_mismatches),
            "nullability": sorted((n["field"], n["level"]) for n in report.nullability_changes),
        }
        return hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()[:24]

    # ── type & nullability ─────────────────────────────────────────────────
    def _type_change(self, spec: FieldSpec, value: Any, col: dict[str, Any] | None) -> TypeChange | None:
        if col and col.get("sql_type") and spec.sql_type:
            verdict = self._compat.sql_verdict(spec.sql_type, col["sql_type"])
            if verdict != IDENTICAL:
                return TypeChange(field=spec.name, expected=spec.sql_type, actual=col["sql_type"],
                                  level="sql", compatibility=verdict)
            return None
        if col and col.get("connect_type") and spec.connect_type:
            verdict = self._compat.connect_verdict(spec.connect_type, col["connect_type"])
            if verdict != IDENTICAL:
                return TypeChange(field=spec.name, expected=spec.connect_type, actual=col["connect_type"],
                                  level="connect", compatibility=verdict)
            return None
        family = _json_family(value)
        if value is None or family in spec.json_types or (family == "integer" and "number" in spec.json_types):
            return None
        return TypeChange(field=spec.name, expected="|".join(spec.json_types), actual=family,
                          level="json", compatibility=RISKY)

    @staticmethod
    def _nullability_change(spec: FieldSpec, value: Any, col: dict[str, Any] | None) -> dict[str, Any] | None:
        if spec.nullable:
            return None
        if value is None:
            return {"field": spec.name, "expected_nullable": False, "level": "value",
                    "detail": "NULL value for a NOT NULL contract field"}
        if col is not None and col.get("optional"):
            return {"field": spec.name, "expected_nullable": False, "level": "schema",
                    "detail": "source column became nullable"}
        return None

    # ── rename heuristics ──────────────────────────────────────────────────
    def _lexical(self, canonical: str, source: str) -> tuple[float, list[str]]:
        evidence: list[str] = []
        sim = SequenceMatcher(None, canonical.lower(), source.lower()).ratio()
        score = sim
        evidence.append(f"name similarity {sim:.2f}")
        ct, st = set(_tokens(canonical)), set(_tokens(source))
        if ct and st and (ct <= st or st <= ct):
            leftover = (st - ct) | (ct - st)
            contain = 0.9 if leftover <= self._hints.decorators else 0.8
            if contain > score:
                score = contain
            evidence.append(f"token containment ({', '.join(sorted(leftover)) or 'none'} extra)")
        elif ct and st:
            jac = len(ct & st) / len(ct | st)
            if jac > 0:
                score = max(score, 0.8 * jac)
                evidence.append(f"token overlap {jac:.2f}")
        if self._hints.are_synonyms(canonical, source):
            score = max(score, 0.92)
            evidence.append("configured synonym")
        return min(score, 1.0), evidence

    def _type_affinity(self, spec: FieldSpec, value: Any, col: dict[str, Any] | None) -> tuple[float, str]:
        if col and col.get("sql_type") and spec.sql_type:
            v = self._compat.sql_verdict(spec.sql_type, col["sql_type"])
            return _TYPE_SCORE.get(v, 0.2), f"type {spec.sql_type}->{col['sql_type']} {v}"
        if col and col.get("connect_type") and spec.connect_type:
            v = self._compat.connect_verdict(spec.connect_type, col["connect_type"])
            return _TYPE_SCORE.get(v, 0.2), f"connect type {spec.connect_type}->{col['connect_type']} {v}"
        family = _json_family(value)
        if value is None:
            return 0.5, "type unknown (null value)"
        if family in spec.json_types or (family == "integer" and "number" in spec.json_types):
            return 0.9, f"value type {family} matches"
        return 0.1, f"value type {family} does not match {list(spec.json_types)}"

    @staticmethod
    def _null_affinity(spec: FieldSpec, value: Any, col: dict[str, Any] | None) -> float:
        if value is None and not spec.nullable:
            return 0.0
        if col is None:
            return 0.75
        return 1.0 if bool(col.get("optional")) == spec.nullable or not col.get("optional") else 0.5

    def _rename_candidates(
        self,
        missing: list[str],
        unexpected: list[str],
        fields: dict[str, FieldSpec],
        payload: dict[str, Any],
        col_schema: dict[str, dict[str, Any]],
    ) -> list[RenameCandidate]:
        scored: list[tuple[float, str, str, list[str]]] = []
        for m in missing:
            spec = fields[m]
            for u in unexpected:
                lexical, evidence = self._lexical(m, u)
                type_score, type_ev = self._type_affinity(spec, payload.get(u), col_schema.get(u))
                null_score = self._null_affinity(spec, payload.get(u), col_schema.get(u))
                if type_score == 0.0:
                    continue  # destructive type relationship: never a plain rename
                conf = round(0.55 * lexical + 0.30 * type_score + 0.15 * null_score, 3)
                if conf >= RENAME_MIN_CONFIDENCE:
                    scored.append((conf, m, u, evidence + [type_ev]))

        scored.sort(reverse=True)
        used_m: set[str] = set()
        used_u: set[str] = set()
        out: list[RenameCandidate] = []
        for conf, m, u, ev in scored:
            if m in used_m or u in used_u:
                continue
            rivals = [c for c, mm, uu, _ in scored
                      if (mm == m or uu == u) and (mm, uu) != (m, u) and conf - c < RENAME_COMPETITION_MARGIN]
            ambiguous = conf < RENAME_CLEAR_CONFIDENCE or bool(rivals)
            if rivals:
                ev = ev + [f"{len(rivals)} competing candidate(s) within {RENAME_COMPETITION_MARGIN}"]
            out.append(RenameCandidate(missing_field=m, unexpected_field=u, confidence=conf,
                                       ambiguous=ambiguous, evidence=ev))
            used_m.add(m)
            used_u.add(u)
        return out

    # ── classification ─────────────────────────────────────────────────────
    @staticmethod
    def _classify(report: DriftReport) -> None:
        renamed_m = {r.missing_field for r in report.possible_renames}
        renamed_u = {r.unexpected_field for r in report.possible_renames}
        removed = [f for f in report.missing_fields if f not in renamed_m]
        added = [f for f in report.unexpected_fields if f not in renamed_u]

        kinds: list[DriftType] = []
        if report.possible_renames:
            kinds.append(DriftType.RENAMED_COLUMN)
        if added:
            kinds.append(DriftType.ADDED_COLUMN)
        if removed:
            kinds.append(DriftType.REMOVED_COLUMN)
        if report.type_mismatches:
            kinds.append(DriftType.TYPE_CHANGE)
        if report.nullability_changes:
            kinds.append(DriftType.NULLABILITY_CHANGE)

        if not kinds:
            report.drift_type = DriftType.UNKNOWN  # only value-level contract violations
            report.ambiguous = True
            report.confidence = 0.5
            return

        report.drift_type = kinds[0] if len(kinds) == 1 else DriftType.MULTIPLE_CHANGES
        if report.possible_renames:
            report.confidence = min(r.confidence for r in report.possible_renames)
            report.ambiguous = any(r.ambiguous for r in report.possible_renames)
        # A column vanished while an unrelated one appeared: maybe a rename we
        # cannot see. Report it, but flag it as uncertain.
        if removed and added:
            report.ambiguous = True
            report.confidence = min(report.confidence, 0.5)

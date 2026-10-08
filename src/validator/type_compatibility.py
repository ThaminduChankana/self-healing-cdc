"""Explicit, configurable type compatibility matrix (config/policies/type_compatibility.yaml)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from src.common.config import get_settings

COMPATIBLE = "compatible"
REQUIRES_TRANSFORMATION = "requires_transformation"
RISKY = "risky"
DESTRUCTIVE = "destructive"
IDENTICAL = "identical"

_TYPE_RE = re.compile(r"^\s*([A-Za-z]+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*(UNSIGNED)?\s*$", re.I)


@dataclass(frozen=True)
class SqlType:
    base: str
    length: int | None = None
    scale: int | None = None
    unsigned: bool = False

    def __str__(self) -> str:
        s = self.base
        if self.length is not None:
            s += f"({self.length},{self.scale})" if self.scale is not None else f"({self.length})"
        return f"{s} UNSIGNED" if self.unsigned else s


class TypeCompatibility:
    def __init__(self, path: Path | None = None, data: dict[str, Any] | None = None):
        if data is None:
            path = path or get_settings().type_compatibility_path
            data = yaml.safe_load(Path(path).read_text()) or {}
        self._aliases = {k.upper(): v.upper() for k, v in (data.get("aliases") or {}).items()}
        self._default = data.get("default_verdict", RISKY)
        self._rules = {
            (self._alias(r["from"]), self._alias(r["to"])): r["verdict"] for r in data.get("rules", [])
        }
        self._connect_rules = {(r["from"], r["to"]): r["verdict"] for r in data.get("connect_rules", [])}
        self._connect_families = data.get("connect_types", {})
        lc = data.get("length_changes") or {}
        self._length_widen = lc.get("widening", COMPATIBLE)
        self._length_narrow = lc.get("narrowing", DESTRUCTIVE)
        self.removed_field = data.get("removed_field", DESTRUCTIVE)

    def _alias(self, base: str) -> str:
        base = base.upper()
        return self._aliases.get(base, base)

    def parse(self, sql_type: str) -> SqlType | None:
        m = _TYPE_RE.match(sql_type or "")
        if not m:
            return None
        base, length, scale, unsigned = m.groups()
        return SqlType(
            base=self._alias(base),
            length=int(length) if length else None,
            scale=int(scale) if scale else None,
            unsigned=bool(unsigned),
        )

    def sql_verdict(self, expected: str, actual: str) -> str:
        """Verdict for a contract column of type `expected` now arriving as `actual`."""
        e, a = self.parse(expected), self.parse(actual)
        if e is None or a is None:
            return self._default
        if e == a:
            return IDENTICAL
        if e.base == a.base:
            if e.unsigned != a.unsigned:
                return RISKY
            if e.length is None or a.length is None:
                # e.g. INT vs INT(11): display width only
                return IDENTICAL if e.scale == a.scale else RISKY
            if a.length >= e.length and (a.scale or 0) >= (e.scale or 0):
                return self._length_widen
            return self._length_narrow
        return self._rules.get((e.base, a.base), self._default)

    def connect_verdict(self, expected: str, actual: str) -> str:
        if expected == actual:
            return IDENTICAL
        return self._connect_rules.get((expected, actual), self._default)

    def connect_family(self, connect_type: str | None) -> str | None:
        return self._connect_families.get(connect_type or "")

    def is_widening(self, expected: str, actual: str) -> bool:
        return self.sql_verdict(expected, actual) in (COMPATIBLE, IDENTICAL)


_instance: TypeCompatibility | None = None


def get_type_compatibility() -> TypeCompatibility:
    global _instance
    if _instance is None:
        _instance = TypeCompatibility()
    return _instance

"""Destination registry: config/destinations.yaml -> configured Sink instances."""

from __future__ import annotations

import fnmatch
import importlib
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from src.common.config import get_settings
from src.sinks.base import Sink

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


class DestinationConfigError(Exception):
    pass


def _builtin_types() -> dict[str, str]:
    return {
        "sqlite": "src.sinks.sqlite_sink:SqliteSink",
        "jsonl": "src.sinks.jsonl_sink:JsonlLakeSink",
        "iceberg": "src.sinks.iceberg_sink:IcebergSink",
        "bigquery": "src.sinks.bigquery_sink:BigQuerySink",
    }


@dataclass
class DestinationConfig:
    name: str
    type: str
    enabled: bool
    tables: list[str] = field(default_factory=lambda: ["*"])
    batch_size: int = 100
    max_batch_wait_seconds: float = 1.0
    options: dict[str, Any] = field(default_factory=dict)

    def routes(self, table_ref: str) -> bool:
        return any(fnmatch.fnmatchcase(table_ref, p) for p in self.tables)


def expand_env(value: Any, extra: dict[str, str] | None = None) -> Any:
    """Recursively expand ${VAR} / ${VAR:-default} from the environment."""
    extra = extra or {}
    if isinstance(value, str):
        def sub(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            if name in os.environ:
                return os.environ[name]
            if name in extra:
                return extra[name]
            return default if default is not None else ""
        return _ENV_RE.sub(sub, value)
    if isinstance(value, dict):
        return {k: expand_env(v, extra) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v, extra) for v in value]
    return value


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def load_destinations(path: Path | None = None) -> list[DestinationConfig]:
    s = get_settings()
    path = path or s.destinations_config_path
    if not Path(path).exists():
        raise DestinationConfigError(f"destination registry not found: {path}")
    raw = yaml.safe_load(Path(path).read_text()) or {}
    builtins = {"STATE_DIR": str(s.state_dir), "LAKE_DIR": str(s.lake_dir)}
    out: list[DestinationConfig] = []
    for name, cfg in (raw.get("destinations") or {}).items():
        if not re.match(r"^[A-Za-z0-9_-]+$", name):
            raise DestinationConfigError(f"invalid destination name {name!r}")
        cfg = expand_env(cfg or {}, builtins)
        if "type" not in cfg:
            raise DestinationConfigError(f"destination {name} has no type")
        out.append(DestinationConfig(
            name=name,
            type=str(cfg["type"]),
            enabled=_as_bool(cfg.get("enabled", True)),
            tables=list(cfg.get("tables") or ["*"]),
            batch_size=int(cfg.get("batch_size", 100)),
            max_batch_wait_seconds=float(cfg.get("max_batch_wait_seconds", 1.0)),
            options=cfg.get("options") or {},
        ))
    if not out:
        raise DestinationConfigError(f"no destinations declared in {path}")
    return out


def build_sink(cfg: DestinationConfig) -> Sink:
    """Instantiate the sink class for `cfg.type` (built-in name or "module:Class")."""
    target = _builtin_types().get(cfg.type, cfg.type)
    if ":" not in target:
        raise DestinationConfigError(
            f"destination {cfg.name}: unknown type {cfg.type!r} "
            f"(built-ins: {', '.join(_builtin_types())}, or 'package.module:SinkClass')")
    module_name, cls_name = target.split(":", 1)
    try:
        cls = getattr(importlib.import_module(module_name), cls_name)
    except (ImportError, AttributeError) as exc:
        raise DestinationConfigError(f"destination {cfg.name}: cannot load {target}: {exc}") from exc
    if not (isinstance(cls, type) and issubclass(cls, Sink)):
        raise DestinationConfigError(f"destination {cfg.name}: {target} is not a Sink subclass")
    return cls(cfg.name, cfg.options)


def enabled_destinations(path: Path | None = None) -> list[DestinationConfig]:
    only = {n.strip() for n in get_settings().downstream_only.split(",") if n.strip()}
    return [d for d in load_destinations(path) if d.enabled and (not only or d.name in only)]

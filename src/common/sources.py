"""Source registry and canonical contract catalogue.

`config/sources.yaml` declares every captured source and table together with
the canonical contract (JSON Schema) the table must satisfy downstream. The
validator routes on (database, table) using this registry, so supporting a new
source is a configuration change, not a code change.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from src.common.config import get_settings


class ContractError(Exception):
    """Raised when a contract or the source registry is invalid."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    json_types: tuple[str, ...]
    sql_type: str | None
    connect_type: str | None
    nullable: bool
    max_length: int | None


@dataclass
class Contract:
    """Canonical downstream contract for one source table."""

    source: str
    database: str
    table: str
    primary_key: list[str]
    schema: dict[str, Any]
    path: Path
    validator: Draft202012Validator = field(repr=False)

    @property
    def key(self) -> str:
        """Globally unique: two sources may both have inventory.products."""
        return f"{self.source}.{self.database}.{self.table}"

    @property
    def table_ref(self) -> str:
        return f"{self.database}.{self.table}"

    @property
    def contract_id(self) -> str:
        return self.schema.get("$id", self.key)

    @property
    def version_hash(self) -> str:
        canonical = json.dumps(self.schema, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:12]

    @property
    def fields(self) -> dict[str, FieldSpec]:
        out: dict[str, FieldSpec] = {}
        for name, prop in self.schema.get("properties", {}).items():
            t = prop.get("type", [])
            types = tuple(t) if isinstance(t, list) else (t,)
            out[name] = FieldSpec(
                name=name,
                json_types=types,
                sql_type=prop.get("x-sql-type"),
                connect_type=prop.get("x-connect-type"),
                nullable=prop.get("x-nullable", "null" in types),
                max_length=prop.get("maxLength"),
            )
        return out

    @property
    def required(self) -> set[str]:
        return set(self.schema.get("required", []))

    @property
    def allows_additional(self) -> bool:
        return bool(self.schema.get("additionalProperties", True))

    def validate(self, payload: dict[str, Any]) -> list[str]:
        """Return a list of JSON-Schema violations (empty when valid)."""
        return [
            f"{e.json_path}: {e.message}"
            for e in sorted(self.validator.iter_errors(payload), key=lambda e: e.json_path)
        ]

    def compact(self) -> dict[str, Any]:
        """Small representation of the contract for prompts and reports."""
        return {
            "table": self.table_ref,
            "primary_key": self.primary_key,
            "fields": {
                f.name: {
                    "type": list(f.json_types),
                    "sql_type": f.sql_type,
                    "nullable": f.nullable,
                }
                for f in self.fields.values()
            },
            "additional_properties": self.allows_additional,
        }


@dataclass
class TableConfig:
    name: str
    primary_key: list[str]
    contract_file: str


@dataclass
class SourceConfig:
    name: str
    engine: str
    topic_prefix: str
    database: str
    connector: dict[str, Any]
    tables: list[TableConfig]
    drift_scenarios: dict[str, dict[str, Any]]
    seed_data: dict[str, Any]


def load_sources(path: Path | None = None) -> dict[str, SourceConfig]:
    path = path or get_settings().sources_config_path
    if not path.exists():
        raise ContractError(f"Source registry not found: {path}")
    raw = yaml.safe_load(path.read_text()) or {}
    sources: dict[str, SourceConfig] = {}
    for name, cfg in (raw.get("sources") or {}).items():
        try:
            tables = [
                TableConfig(
                    name=t["name"],
                    primary_key=list(t.get("primary_key") or []),
                    contract_file=t["contract"],
                )
                for t in cfg.get("tables", [])
            ]
            connector = cfg.get("connector") or {}
            sources[name] = SourceConfig(
                name=name,
                engine=cfg["engine"],
                topic_prefix=connector.get("topic_prefix", name),
                database=cfg["database"],
                connector=connector,
                tables=tables,
                drift_scenarios=cfg.get("drift_scenarios") or {},
                seed_data=cfg.get("seed_data") or {},
            )
        except KeyError as exc:
            raise ContractError(f"Source '{name}' is missing key {exc}") from exc
    if not sources:
        raise ContractError(f"No sources declared in {path}")
    prefixes = [s.topic_prefix for s in sources.values()]
    if len(set(prefixes)) != len(prefixes):
        raise ContractError("Each source needs a unique connector.topic_prefix")
    server_ids = [s.connector.get("server_id") for s in sources.values() if s.connector.get("server_id")]
    if len(set(server_ids)) != len(server_ids):
        raise ContractError("Each MySQL source needs a unique connector.server_id")
    return sources


class ContractRegistry:
    """Loads canonical contracts for every table declared in the source registry."""

    def __init__(
        self,
        sources_path: Path | None = None,
        schema_dir: Path | None = None,
        sources: dict[str, SourceConfig] | None = None,
    ):
        settings = get_settings()
        self._schema_dir = Path(schema_dir or settings.canonical_schema_dir)
        self.sources = sources if sources is not None else load_sources(sources_path)
        self._contracts: dict[str, Contract] = {}
        self._by_prefix = {s.topic_prefix: s for s in self.sources.values()}
        for source in self.sources.values():
            for table in source.tables:
                contract = self._load_contract(source.name, source.database, table)
                self._contracts[contract.key] = contract

    def _load_contract(self, source: str, database: str, table: TableConfig) -> Contract:
        path = self._schema_dir / table.contract_file
        if not path.exists():
            raise ContractError(f"Contract file not found for {database}.{table.name}: {path}")
        try:
            schema = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise ContractError(f"Contract {path} is not valid JSON: {exc}") from exc
        Draft202012Validator.check_schema(schema)
        declared = schema.get("x-source", {})
        if declared and (declared.get("database"), declared.get("table")) != (database, table.name):
            raise ContractError(
                f"Contract {path.name} declares {declared.get('database')}.{declared.get('table')} "
                f"but is mapped to {database}.{table.name}"
            )
        pk = table.primary_key or list(declared.get("primary_key", []))
        return Contract(
            source=source,
            database=database,
            table=table.name,
            primary_key=pk,
            schema=schema,
            path=path,
            validator=Draft202012Validator(schema),
        )

    def resolve_source(self, server_name: str | None, database: str) -> SourceConfig | None:
        """Map a Debezium `source.name` (= connector topic prefix) to its source.

        Falls back to the database name only when exactly one source uses it.
        """
        if server_name and server_name in self._by_prefix:
            return self._by_prefix[server_name]
        matches = [s for s in self.sources.values() if s.database == database]
        return matches[0] if len(matches) == 1 else None

    def get(self, database: str, table: str, server_name: str | None = None) -> Contract | None:
        """Exact (source, database, table) lookup — no fuzzy matching of table names."""
        source = self.resolve_source(server_name, database)
        if source is None:
            return None
        return self._contracts.get(f"{source.name}.{database}.{table}")

    def all(self) -> list[Contract]:
        return list(self._contracts.values())


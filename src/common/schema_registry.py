"""Schema Registry abstraction (Redpanda's Confluent-compatible API).

Canonical contracts live in git (config/canonical_schema) and are published
to the registry as JSON Schema so other consumers can discover the contract
for `cdc.validated` / `cdc.repaired`. The registry is optional at runtime: if
it is unavailable the validator logs a warning and keeps using the local files.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

import requests

from src.common.config import get_settings


class SchemaRegistryError(Exception):
    pass


class SchemaRegistry(ABC):
    @abstractmethod
    def register(self, subject: str, schema: dict[str, Any]) -> int: ...

    @abstractmethod
    def latest(self, subject: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def is_available(self) -> bool: ...


class ConfluentCompatibleSchemaRegistry(SchemaRegistry):
    """Works with Redpanda Schema Registry, Confluent SR and AWS-hosted equivalents."""

    _HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

    def __init__(self, url: str | None = None, timeout: float = 5.0):
        self._url = (url or get_settings().schema_registry_url).rstrip("/")
        self._timeout = timeout

    def is_available(self) -> bool:
        try:
            return requests.get(f"{self._url}/subjects", timeout=self._timeout).ok
        except requests.RequestException:
            return False

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        body = {"schemaType": "JSON", "schema": json.dumps(schema, sort_keys=True)}
        try:
            resp = requests.post(
                f"{self._url}/subjects/{subject}/versions",
                headers=self._HEADERS, json=body, timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise SchemaRegistryError(f"registry unreachable: {exc}") from exc
        if not resp.ok:
            raise SchemaRegistryError(f"register {subject} failed: HTTP {resp.status_code} {resp.text[:300]}")
        return int(resp.json().get("id", -1))

    def latest(self, subject: str) -> dict[str, Any] | None:
        try:
            resp = requests.get(f"{self._url}/subjects/{subject}/versions/latest", timeout=self._timeout)
        except requests.RequestException as exc:
            raise SchemaRegistryError(f"registry unreachable: {exc}") from exc
        if resp.status_code == 404:
            return None
        if not resp.ok:
            raise SchemaRegistryError(f"lookup {subject} failed: HTTP {resp.status_code}")
        data = resp.json()
        return {"version": data.get("version"), "id": data.get("id"), "schema": json.loads(data["schema"])}


def contract_subject(source: str, database: str, table: str) -> str:
    return f"contract.{source}.{database}.{table}-value"

"""Canonical-contract lookup and JSON Schema validation for the validator."""

from __future__ import annotations

from typing import Any

from src.common.logging import ComponentLogger
from src.common.schema_registry import SchemaRegistry, SchemaRegistryError, contract_subject
from src.common.sources import Contract, ContractRegistry

log = ComponentLogger("schema_validator")


class SchemaLookupError(Exception):
    """No canonical contract exists for the event's (database, table)."""


class SchemaValidator:
    def __init__(self, contracts: ContractRegistry | None = None):
        self.contracts = contracts or ContractRegistry()

    def contract_for(self, database: str, table: str, server_name: str | None = None) -> Contract:
        contract = self.contracts.get(database, table, server_name)
        if contract is None:
            raise SchemaLookupError(
                f"no canonical contract for {server_name or '?'}:{database}.{table}")
        return contract

    @staticmethod
    def validate(payload: dict[str, Any], contract: Contract) -> list[str]:
        return contract.validate(payload)

    def publish_contracts(self, registry: SchemaRegistry) -> dict[str, str]:
        """Best effort: publish every contract to the Schema Registry."""
        results: dict[str, str] = {}
        for c in self.contracts.all():
            subject = contract_subject(c.source, c.database, c.table)
            try:
                schema_id = registry.register(subject, c.schema)
                results[subject] = f"registered id={schema_id}"
            except SchemaRegistryError as exc:
                results[subject] = f"unavailable: {exc}"
                log.warning("Schema Registry publish failed; using local contract files",
                            subject=subject, error=str(exc))
        return results

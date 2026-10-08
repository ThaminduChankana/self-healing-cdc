"""Shared fixtures. Unit tests never need Docker, Kafka or Ollama."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

_STATE = tempfile.mkdtemp(prefix="cdc-test-state-")
os.environ.setdefault("LLM_MODE", "mock")
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ["STATE_DIR"] = _STATE
os.environ["METRICS_PORT"] = "0"

import pytest  # noqa: E402

from src.common.config import reset_settings  # noqa: E402
from src.common.kafka import Message, MessageConsumer, MessageProducer, PublishError  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent

PRODUCTS_COLS = {
    "id": ("int32", "INT", False),
    "name": ("string", "VARCHAR(255)", False),
    "description": ("string", "VARCHAR(512)", True),
    "weight": ("float", "FLOAT", True),
}
ON_HAND_COLS = {"product_id": ("int32", "INT", False), "quantity": ("int32", "INT", False)}


def _field(name: str, connect: str, sql: str | None, optional: bool) -> dict[str, Any]:
    f: dict[str, Any] = {"type": connect, "optional": optional, "field": name}
    if sql:
        base, _, rest = sql.partition("(")
        params = {"__debezium.source.column.type": base}
        if rest:
            params["__debezium.source.column.length"] = rest.rstrip(")").split(",")[0]
        f["parameters"] = params
    return f


def envelope(
    table: str,
    after: dict[str, Any] | None,
    before: dict[str, Any] | None = None,
    op: str = "c",
    cols: dict[str, tuple[str, str | None, bool]] | None = None,
    db: str = "inventory",
    server: str = "cdc",
    pos: int = 1000,
    row: int = 0,
    with_schema: bool = True,
) -> dict[str, Any]:
    """Build a Debezium JSON-converter envelope like the real connector emits."""
    payload = {
        "before": before,
        "after": after,
        "source": {"version": "3.7.0.Final", "connector": "mysql", "name": server, "ts_ms": 1, "snapshot": "false",
                   "db": db, "table": table, "server_id": 223344, "file": "mysql-bin.000003", "pos": pos,
                   "row": row, "gtid": None},
        "op": op,
        "ts_ms": 1791442517261,
    }
    if not with_schema:
        return payload
    cols = cols or {}
    value_struct = {"type": "struct", "optional": True,
                    "fields": [_field(n, c, s, o) for n, (c, s, o) in cols.items()]}
    return {
        "schema": {"type": "struct", "name": f"{server}.{db}.{table}.Envelope", "fields": [
            {**value_struct, "field": "before"}, {**value_struct, "field": "after"},
            {"type": "struct", "field": "source", "fields": []},
            {"type": "string", "field": "op"}]},
        "payload": payload,
    }


class FakeProducer(MessageProducer):
    def __init__(self, fail_times: int = 0, fail_topics: set[str] | None = None):
        self.published: list[dict[str, Any]] = []
        self.fail_times = fail_times
        self.fail_topics = fail_topics

    def publish(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        if self.fail_times and (self.fail_topics is None or topic in self.fail_topics):
            self.fail_times -= 1
            raise PublishError("broker unavailable (simulated)")
        self.published.append({"topic": topic, "value": value, "key": key})

    def on(self, topic: str) -> list[dict[str, Any]]:
        return [p["value"] for p in self.published if p["topic"] == topic]

    def close(self) -> None:
        pass


class FakeConsumer(MessageConsumer):
    """In-memory consumer with real commit/seek semantics on a single partition."""

    def __init__(self, topic: str, values: list[bytes | None], keys: list[bytes | None] | None = None,
                 committed: int = 0):
        self.topic = topic
        self.log = [Message(topic, 0, i, (keys or [None] * len(values))[i], v) for i, v in enumerate(values)]
        self.position = committed
        self.committed = committed
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        pass

    def poll(self, timeout: float) -> Message | None:
        if self.position >= len(self.log):
            return None
        msg = self.log[self.position]
        self.position += 1
        return msg

    def commit(self, message: Message) -> None:
        self.committed = message.offset + 1

    def seek(self, message: Message) -> None:
        self.position = message.offset

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _fresh_settings():
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def tmp_state(tmp_path, monkeypatch):
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    reset_settings()
    return tmp_path


@pytest.fixture
def contracts():
    from src.common.sources import ContractRegistry
    return ContractRegistry()


@pytest.fixture
def products_contract(contracts):
    return contracts.get("inventory", "products", "cdc")


@pytest.fixture
def on_hand_contract(contracts):
    return contracts.get("inventory", "products_on_hand", "cdc")

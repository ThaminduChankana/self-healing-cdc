"""Integration fixtures: run against the live Docker stack (make start)."""

from __future__ import annotations

import json
import subprocess
import uuid

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import get_settings, reset_settings
from src.common.kafka import wait_for_broker


def pytest_collection_modifyitems(items):
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session", autouse=True)
def live_stack():
    reset_settings()
    if not wait_for_broker(timeout=5):
        pytest.skip("Redpanda not reachable — run `make start` first")
    for c in ("cdc-validator", "cdc-ai-worker", "cdc-debezium", "cdc-mysql", "cdc-mysql-sandbox"):
        state = container_health(c)
        if state != "healthy":
            pytest.skip(f"{c} is {state} — run `make start` first")


def container_health(name: str) -> str:
    out = subprocess.run(["docker", "inspect", "--format",
                          "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}", name],
                         capture_output=True, text=True)
    return out.stdout.strip() or "missing"


@pytest.fixture
def temp_topics(monkeypatch):
    """Isolated DLQ/repaired/audit topics for in-process worker tests."""
    suffix = uuid.uuid4().hex[:8]
    names = {"DLQ_TOPIC": f"it.dlq.{suffix}", "REPAIRED_TOPIC": f"it.repaired.{suffix}",
             "AUDIT_TOPIC": f"it.audit.{suffix}"}
    admin = AdminClient({"bootstrap.servers": get_settings().redpanda_brokers})
    for f in admin.create_topics([NewTopic(t, 1, 1) for t in names.values()]).values():
        f.result()
    for k, v in names.items():
        monkeypatch.setenv(k, v)
    reset_settings()
    yield names
    for f in admin.delete_topics(list(names.values()), operation_timeout=30).values():
        f.result()
    reset_settings()


def produce_raw(topic: str, value: bytes, key: bytes | None = None) -> None:
    p = Producer({"bootstrap.servers": get_settings().redpanda_brokers})
    p.produce(topic, value=value, key=key)
    assert p.flush(10) == 0


def produce_json(topic: str, value: dict, key: str | None = None) -> None:
    produce_raw(topic, json.dumps(value).encode(), key.encode() if key else None)

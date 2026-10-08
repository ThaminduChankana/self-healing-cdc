"""Render and register Debezium connectors from config/sources.yaml.

One Kafka Connect cluster hosts one connector per source (N sources != N
deployments). Every connector routes its change topics into the single
`cdc.mutations` topic; events stay distinguishable by `source.name`
(= topic prefix), database and table.

Credentials: by default the connector config contains `${env:VAR}`
placeholders that Kafka Connect resolves from its own environment
(EnvVarConfigProvider), so passwords are never written to the Connect config
topic. Set CONNECT_SECRETS_VIA_ENV=false to inline them (not recommended).

Usage:
    python -m src.tools.connector --print            # show rendered configs
    python -m src.tools.connector --register         # create/update all connectors
    python -m src.tools.connector --status
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Any

import requests

from src.common.config import get_settings
from src.common.sources import SourceConfig, load_sources

JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter"


def _secret(env_name: str) -> str:
    if os.environ.get("CONNECT_SECRETS_VIA_ENV", "true").lower() == "true":
        return "${env:%s}" % env_name
    value = os.environ.get(env_name)
    if value is None:
        settings = get_settings()
        value = getattr(settings, env_name.lower(), None)
        value = value.get_secret_value() if hasattr(value, "get_secret_value") else value
    if value is None:
        raise SystemExit(f"environment variable {env_name} is not set")
    return str(value)


def _plain(env_name: str, default: str) -> str:
    return os.environ.get(env_name, default)


def render(source: SourceConfig) -> dict[str, Any]:
    c = source.connector
    settings = get_settings()
    prefix = source.topic_prefix
    tables = ",".join(f"{source.database}.{t.name}" for t in source.tables)
    config: dict[str, Any] = {
        "connector.class": c["class"],
        "tasks.max": "1",
        "topic.prefix": prefix,
        "database.hostname": _plain(c.get("host_env", "DEBEZIUM_MYSQL_HOST"), "mysql"),
        "database.port": _plain(c.get("port_env", "DEBEZIUM_MYSQL_PORT"), "3306"),
        "database.user": _plain(c.get("user_env", "MYSQL_USER"), "debezium"),
        "database.password": _secret(c.get("password_env", "MYSQL_PASSWORD")),
        "table.include.list": tables,
        # Ship SQL column types/lengths in the envelope schema (INT vs BIGINT, VARCHAR(n)).
        "column.propagate.source.type": f"{re.escape(source.database)}\\..*",
        "key.converter": JSON_CONVERTER,
        "key.converter.schemas.enable": "false",
        "value.converter": JSON_CONVERTER,
        "value.converter.schemas.enable": "true",
        "tombstones.on.delete": "true",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": f"{re.escape(prefix)}\\.(.*)",
        "transforms.route.replacement": settings.mutations_topic,
    }
    if source.engine == "mysql":
        config.update({
            "database.server.id": str(c["server_id"]),
            "database.include.list": source.database,
            "include.schema.changes": "false",
            "schema.history.internal.kafka.bootstrap.servers": _plain("DEBEZIUM_KAFKA_BOOTSTRAP", "redpanda:9092"),
            "schema.history.internal.kafka.topic": f"_schema-history.{source.name}",
        })
    elif source.engine == "postgres":
        config.update({
            "database.dbname": source.database,
            "plugin.name": "pgoutput",
            "slot.name": f"cdc_{source.name}",
            "publication.autocreate.mode": "filtered",
        })
    else:
        raise SystemExit(f"unsupported engine '{source.engine}' for source {source.name}")
    config.update({k: str(v) for k, v in (c.get("extra") or {}).items()})
    return {"name": c.get("name", f"{source.name}-connector"), "config": config}


def _wait_for_connect(url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if requests.get(f"{url}/connectors", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise SystemExit(f"Kafka Connect not reachable at {url} after {timeout:.0f}s")


def register(url: str, timeout: float = 120.0) -> int:
    _wait_for_connect(url, timeout)
    failures = 0
    for source in load_sources().values():
        spec = render(source)
        name = spec["name"]
        resp = requests.put(f"{url}/connectors/{name}/config", json=spec["config"], timeout=30)
        verb = "created" if resp.status_code == 201 else "updated"
        if not resp.ok:
            print(f"✗ {name}: HTTP {resp.status_code} {resp.text[:400]}")
            failures += 1
            continue
        print(f"✓ {name} {verb} (source={source.name}, tables={spec['config']['table.include.list']})")
    for source in load_sources().values():
        failures += 0 if _await_running(url, render(source)["name"]) else 1
    return failures


def _await_running(url: str, name: str, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    state = "UNKNOWN"
    while time.monotonic() < deadline:
        try:
            st = requests.get(f"{url}/connectors/{name}/status", timeout=5).json()
            tasks = st.get("tasks", [])
            state = st.get("connector", {}).get("state", "UNKNOWN")
            if state == "RUNNING" and tasks and all(t.get("state") == "RUNNING" for t in tasks):
                print(f"✓ {name} RUNNING ({len(tasks)} task)")
                return True
            failed = [t for t in tasks if t.get("state") == "FAILED"]
            if failed:
                print(f"✗ {name} task FAILED: {failed[0].get('trace', '')[:800]}")
                return False
        except (requests.RequestException, ValueError):
            pass
        time.sleep(2)
    print(f"✗ {name} not running after {timeout:.0f}s (state={state})")
    return False


def status(url: str) -> None:
    for source in load_sources().values():
        name = render(source)["name"]
        try:
            print(json.dumps(requests.get(f"{url}/connectors/{name}/status", timeout=5).json(), indent=2))
        except requests.RequestException as exc:
            print(f"{name}: unreachable ({exc})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--print", action="store_true", help="print rendered connector configs")
    parser.add_argument("--register", action="store_true", help="create or update every connector")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--url", default=None, help="Kafka Connect REST URL")
    args = parser.parse_args()
    url = (args.url or get_settings().debezium_connect_url).rstrip("/")
    if args.print:
        for source in load_sources().values():
            print(json.dumps(render(source), indent=2))
    if args.register:
        sys.exit(1 if register(url) else 0)
    if args.status:
        status(url)


if __name__ == "__main__":
    main()

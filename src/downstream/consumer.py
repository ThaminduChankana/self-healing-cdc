"""Downstream ingestion: cdc.validated + cdc.repaired -> local warehouse (SQLite).

Stands in for the real data warehouse (Redshift / Snowflake / BigQuery).
Applies upserts/deletes keyed by primary key and de-duplicates on event_id,
so redelivery or a repair published twice never double-applies a change.
"""

from __future__ import annotations

import json
import signal
import sqlite3
import time
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException

from src.common.config import get_settings
from src.common.kafka import Message, MessageConsumer, RedpandaConsumer
from src.common.logging import ComponentLogger
from src.common.metrics import DOWNSTREAM_DUPLICATES, DOWNSTREAM_INGESTED, heartbeat, start_metrics_server
from src.common.models import utc_now

log = ComponentLogger("downstream")


class Warehouse:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS ingested_events (
                event_id TEXT PRIMARY KEY, source_topic TEXT, ingested_at TEXT
            );
            CREATE TABLE IF NOT EXISTS rows (
                table_ref TEXT NOT NULL, pk TEXT NOT NULL, payload TEXT NOT NULL,
                event_id TEXT, repair_id TEXT, source_topic TEXT, updated_at TEXT,
                PRIMARY KEY (table_ref, pk)
            );
        """)

    def apply(self, topic: str, value: dict[str, Any]) -> str:
        event_id = value.get("event_id", "")
        if self._conn.execute("SELECT 1 FROM ingested_events WHERE event_id=?", (event_id,)).fetchone():
            return "duplicate"
        repaired = "repaired_payload" in value
        payload = value.get("repaired_payload") if repaired else value.get("payload")
        op = value.get("operation", "")
        table_ref = f"{value.get('source_name', '')}:{value.get('source_database', '')}.{value.get('source_table', '')}"
        pk = json.dumps(value.get("key") or {}, sort_keys=True)
        self._conn.execute("BEGIN")
        try:
            if op == "d":
                self._conn.execute("DELETE FROM rows WHERE table_ref=? AND pk=?", (table_ref, pk))
            elif op in ("c", "u", "r") and payload is not None:
                self._conn.execute(
                    "INSERT INTO rows (table_ref, pk, payload, event_id, repair_id, source_topic, updated_at) "
                    "VALUES (?,?,?,?,?,?,?) ON CONFLICT(table_ref, pk) DO UPDATE SET payload=excluded.payload, "
                    "event_id=excluded.event_id, repair_id=excluded.repair_id, source_topic=excluded.source_topic, "
                    "updated_at=excluded.updated_at",
                    (table_ref, pk, json.dumps(payload, sort_keys=True), event_id, value.get("repair_id"),
                     topic, utc_now()),
                )
            self._conn.execute("INSERT INTO ingested_events VALUES (?,?,?)", (event_id, topic, utc_now()))
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return op or "control"

    def row(self, table_ref: str, pk: dict[str, Any]) -> dict[str, Any] | None:
        r = self._conn.execute("SELECT payload, event_id, repair_id, source_topic, updated_at FROM rows "
                               "WHERE table_ref=? AND pk=?", (table_ref, json.dumps(pk, sort_keys=True))).fetchone()
        if not r:
            return None
        return {"payload": json.loads(r[0]), "event_id": r[1], "repair_id": r[2], "source_topic": r[3],
                "updated_at": r[4]}

    def close(self) -> None:
        self._conn.close()


class DownstreamConsumer:
    COMPONENT = "downstream"

    def __init__(self, consumer: MessageConsumer | None = None, warehouse: Warehouse | None = None):
        self._settings = get_settings()
        self._consumer = consumer or RedpandaConsumer(group_id="cdc-downstream")
        self._warehouse = warehouse or Warehouse(self._settings.warehouse_db_path)
        self._running = False

    def run(self) -> None:
        self._running = True
        topics = [self._settings.validated_topic, self._settings.repaired_topic]
        self._consumer.subscribe(topics)
        log.info("Downstream consumer started", topics=topics)
        while self._running:
            heartbeat(self.COMPONENT)
            try:
                msg = self._consumer.poll(self._settings.poll_timeout_seconds)
            except KafkaException as exc:
                log.error("Poll failed", error=str(exc))
                time.sleep(2)
                continue
            if msg is None:
                continue
            self._handle(msg)
        self._consumer.close()
        self._warehouse.close()

    def _handle(self, msg: Message) -> None:
        try:
            value = json.loads((msg.value or b"null").decode("utf-8"))
            result = self._warehouse.apply(msg.topic, value)
        except Exception as exc:  # noqa: BLE001
            log.exception("Downstream apply failed; will retry", correlation_id=msg.coordinates)
            self._consumer.seek(msg)
            time.sleep(2)
            return
        if result == "duplicate":
            DOWNSTREAM_DUPLICATES.labels(source_topic=msg.topic).inc()
            log.info("Duplicate ignored", event_id=value.get("event_id"), source_topic=msg.topic)
        else:
            DOWNSTREAM_INGESTED.labels(source_topic=msg.topic, op=result).inc()
            log.info(f"Ingested from {msg.topic}", event_id=value.get("event_id"),
                     repair_id=value.get("repair_id"), correlation_id=value.get("correlation_id"),
                     source_table=value.get("source_table"), op=result)
        self._consumer.commit(msg)

    def stop(self, *_: Any) -> None:
        self._running = False


def main() -> None:
    import sys
    if len(sys.argv) == 4 and sys.argv[1] == "--show":
        # python -m src.downstream.consumer --show '<source>:<db>.<table>' '{"id": 1}'
        wh = Warehouse(get_settings().warehouse_db_path)
        print(json.dumps(wh.row(sys.argv[2], json.loads(sys.argv[3])), indent=2))
        return
    start_metrics_server(get_settings().metrics_port)
    consumer = DownstreamConsumer()
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    consumer.run()


if __name__ == "__main__":
    main()

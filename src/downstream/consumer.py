"""Downstream delivery: cdc.validated + cdc.repaired -> every configured destination.

Destinations come from config/destinations.yaml (warehouse, data lake,
lakehouse, BigQuery, or your own Sink class). Each enabled destination gets its
own runner thread and Kafka consumer group, so:
  * destinations progress independently (a slow or failing BigQuery never
    delays the lake), and offsets are committed per destination;
  * a failed batch is retried with backoff from the same offsets — never
    skipped — and a per-destination ledger makes redelivery idempotent;
  * routing (`tables:` globs on <source>.<database>.<table>) decides which
    tables each destination receives.
"""

from __future__ import annotations

import json
import signal
import threading
import time
from typing import Any

from confluent_kafka import KafkaException

from src.common.config import get_settings
from src.common.kafka import Message, MessageConsumer, RedpandaConsumer
from src.common.logging import ComponentLogger
from src.common.metrics import (
    DOWNSTREAM_DUPLICATES,
    DOWNSTREAM_ERRORS,
    DOWNSTREAM_INGESTED,
    DOWNSTREAM_LAG,
    heartbeat,
    start_metrics_server,
)
from src.common.retry import backoff_delay
from src.common.sources import Contract, ContractRegistry
from src.sinks.base import ChangeRecord, DeliveryLedger, RecordError, Sink, to_record
from src.sinks.registry import DestinationConfig, build_sink, enabled_destinations

log = ComponentLogger("downstream")


class DestinationRunner:
    def __init__(
        self,
        cfg: DestinationConfig,
        sink: Sink,
        contracts: ContractRegistry,
        consumer: MessageConsumer | None = None,
        ledger: DeliveryLedger | None = None,
    ):
        self.cfg = cfg
        self.sink = sink
        self._settings = get_settings()
        self._contracts = contracts
        self._consumer = consumer or RedpandaConsumer(group_id=f"cdc-downstream-{cfg.name}")
        self._ledger = ledger or DeliveryLedger(self._settings.state_dir / f"delivery_{cfg.name}.db")
        self._routed = {c.key: c for c in contracts.all() if cfg.routes(c.key)}
        self._stop = threading.Event()
        self._failures = 0
        self.stats: dict[str, int] = {"delivered": 0, "duplicates": 0, "skipped": 0, "failed_batches": 0}

    @property
    def component(self) -> str:
        return f"downstream:{self.cfg.name}"

    # ── lifecycle ───────────────────────────────────────────────────────────
    def run(self, max_seconds: float | None = None) -> dict[str, int]:
        self.sink.open(list(self._routed.values()))
        topics = [self._settings.validated_topic, self._settings.repaired_topic]
        self._consumer.subscribe(topics)
        log.info("Destination started", destination=self.cfg.name, sink=self.sink.describe(),
                 tables=sorted(self._routed), batch_size=self.cfg.batch_size)
        started = time.monotonic()
        batch: list[Message] = []
        deadline = 0.0
        while not self._stop.is_set():
            heartbeat(self.component)
            if max_seconds is not None and time.monotonic() - started > max_seconds:
                break
            try:
                msg = self._consumer.poll(min(0.5, self.cfg.max_batch_wait_seconds))
            except KafkaException as exc:
                log.error("Poll failed", destination=self.cfg.name, error=str(exc))
                self._stop.wait(2)
                continue
            if msg is not None:
                if not batch:
                    deadline = time.monotonic() + self.cfg.max_batch_wait_seconds
                batch.append(msg)
            if batch and (len(batch) >= self.cfg.batch_size or time.monotonic() >= deadline):
                if self._flush(batch):
                    batch = []
        if batch:
            self._flush(batch)
        self._close()
        return self.stats

    def stop(self) -> None:
        self._stop.set()

    def _close(self) -> None:
        for closer in (self._consumer.close, self.sink.close, self._ledger.close):
            try:
                closer()
            except Exception:  # noqa: BLE001
                pass
        log.info("Destination stopped", destination=self.cfg.name, stats=self.stats)

    # ── delivery ────────────────────────────────────────────────────────────
    def _records(self, batch: list[Message]) -> list[ChangeRecord]:
        records: list[ChangeRecord] = []
        seen: set[str] = set()
        for m in batch:
            kind = "repaired" if m.topic == self._settings.repaired_topic else "validated"
            try:
                rec = to_record(kind, json.loads((m.value or b"null").decode("utf-8")))
            except (RecordError, ValueError, UnicodeDecodeError):
                self.stats["skipped"] += 1  # control events, tombstones, undecodable
                continue
            source = self._contracts.resolve_source(rec.source, rec.database)
            if source is not None:
                rec.source = source.name  # topic prefix -> registry name (routing key)
            if rec.table_ref not in self._routed or rec.event_id in seen:
                self.stats["skipped"] += 1
                continue
            seen.add(rec.event_id)
            records.append(rec)
        return records

    def _flush(self, batch: list[Message]) -> bool:
        records = self._records(batch)
        new_ids = self._ledger.filter_new([r.event_id for r in records])
        dupes = len(records) - len(new_ids)
        records = [r for r in records if r.event_id in new_ids]
        try:
            if records:
                self.sink.write(records, self._routed)
                self._ledger.mark([r.event_id for r in records])
            self._commit(batch)
        except Exception as exc:  # noqa: BLE001 - any sink failure: retry the same batch later
            self._failures += 1
            self.stats["failed_batches"] += 1
            DOWNSTREAM_ERRORS.labels(destination=self.cfg.name).inc()
            delay = backoff_delay(self._failures, 1.0, self._settings.retry_max_delay_seconds)
            log.error("Delivery failed — batch will be retried from the same offsets (nothing committed)",
                      destination=self.cfg.name, error=f"{type(exc).__name__}: {str(exc)[:300]}",
                      records=len(records), retry_in_s=delay)
            self._rewind(batch)
            self._stop.wait(delay)
            return True  # batch handed back to Kafka via seek; start a fresh one
        self._failures = 0
        if dupes:
            self.stats["duplicates"] += dupes
            DOWNSTREAM_DUPLICATES.labels(destination=self.cfg.name).inc(dupes)
        for r in records:
            DOWNSTREAM_INGESTED.labels(destination=self.cfg.name, op=r.op).inc()
            log.info(f"Delivered to {self.cfg.name}", event_id=r.event_id, repair_id=r.repair_id,
                     correlation_id=r.correlation_id, source_table=r.table_ref, op=r.op, origin=r.origin)
        if records:
            DOWNSTREAM_LAG.labels(destination=self.cfg.name).set(time.time())
        self.stats["delivered"] += len(records)
        return True

    def _commit(self, batch: list[Message]) -> None:
        last: dict[tuple[str, int], Message] = {}
        for m in batch:
            k = (m.topic, m.partition)
            if k not in last or m.offset > last[k].offset:
                last[k] = m
        for m in last.values():
            self._consumer.commit(m)

    def _rewind(self, batch: list[Message]) -> None:
        first: dict[tuple[str, int], Message] = {}
        for m in batch:
            k = (m.topic, m.partition)
            if k not in first or m.offset < first[k].offset:
                first[k] = m
        for m in first.values():
            try:
                self._consumer.seek(m)
            except KafkaException:
                pass


class DownstreamService:
    """Runs one DestinationRunner thread per enabled destination."""

    def __init__(self, runners: list[DestinationRunner] | None = None):
        contracts = ContractRegistry()
        if runners is None:
            runners = []
            for cfg in enabled_destinations():
                runners.append(DestinationRunner(cfg, build_sink(cfg), contracts))
        if not runners:
            raise SystemExit("no enabled destinations in config/destinations.yaml")
        self.runners = runners
        self._threads: list[threading.Thread] = []

    def run(self) -> None:
        for r in self.runners:
            t = threading.Thread(target=self._guard, args=(r,), name=r.cfg.name, daemon=True)
            t.start()
            self._threads.append(t)
        log.info("Downstream service started", destinations=[r.cfg.name for r in self.runners])
        while any(t.is_alive() for t in self._threads):
            time.sleep(0.5)

    @staticmethod
    def _guard(runner: DestinationRunner) -> None:
        """A destination that cannot even open is retried; it never takes the others down."""
        attempt = 0
        while not runner._stop.is_set():
            try:
                runner.run()
                return
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                delay = backoff_delay(attempt, 2.0, 60.0)
                log.exception("Destination crashed; restarting", destination=runner.cfg.name,
                              error=str(exc)[:300], retry_in_s=delay)
                runner._stop.wait(delay)

    def stop(self, *_: Any) -> None:
        for r in self.runners:
            r.stop()


def main() -> None:
    start_metrics_server(get_settings().metrics_port)
    service = DownstreamService()
    signal.signal(signal.SIGINT, service.stop)
    signal.signal(signal.SIGTERM, service.stop)
    service.run()


if __name__ == "__main__":
    main()

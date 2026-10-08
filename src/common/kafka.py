"""Kafka / Redpanda producer and consumer abstractions.

Application code depends only on `MessageProducer` / `MessageConsumer`, so the
local Redpanda implementation can be swapped for Amazon MSK (same Kafka
protocol, different auth config) or another broker without touching the
validator or the AI worker.
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import get_settings
from src.common.logging import ComponentLogger

log = ComponentLogger("kafka")

MAX_MESSAGE_BYTES = 10 * 1024 * 1024


class PublishError(Exception):
    """Raised when a message could not be delivered (broker unavailable, timeout...)."""


@dataclass
class Message:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None = None

    @property
    def coordinates(self) -> str:
        return f"{self.topic}/{self.partition}/{self.offset}"

    def key_json(self) -> Any:
        if self.key is None:
            return None
        try:
            return json.loads(self.key.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {"raw_key": self.key.decode("utf-8", errors="replace")[:500]}


class MessageProducer(ABC):
    @abstractmethod
    def publish(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        """Publish and wait for broker acknowledgement; raise PublishError on failure."""

    @abstractmethod
    def close(self) -> None: ...


class MessageConsumer(ABC):
    @abstractmethod
    def subscribe(self, topics: list[str]) -> None: ...

    @abstractmethod
    def poll(self, timeout: float) -> Message | None: ...

    @abstractmethod
    def commit(self, message: Message) -> None:
        """Commit the offset *after* `message` (synchronously)."""

    @abstractmethod
    def seek(self, message: Message) -> None:
        """Rewind so that `message` is delivered again by the next poll."""

    @abstractmethod
    def close(self) -> None: ...


def _client_security_config() -> dict[str, Any]:
    """Hook for SASL/TLS settings (MSK IAM/SCRAM) — empty for local Redpanda."""
    return {}


class RedpandaProducer(MessageProducer):
    """Idempotent producer that confirms delivery of every message."""

    def __init__(self, brokers: str | None = None, delivery_timeout_s: float = 15.0):
        settings = get_settings()
        self._timeout = delivery_timeout_s
        self._producer = Producer({
            "bootstrap.servers": brokers or settings.redpanda_brokers,
            "enable.idempotence": True,
            "acks": "all",
            "message.max.bytes": MAX_MESSAGE_BYTES,
            "message.timeout.ms": int(delivery_timeout_s * 1000),
            "linger.ms": 5,
            **_client_security_config(),
        })

    def publish(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        payload = json.dumps(value, default=str, ensure_ascii=False).encode("utf-8")
        result: dict[str, Any] = {}

        def _on_delivery(err: Any, msg: Any) -> None:
            result["err"] = err
            if msg is not None and err is None:
                result["offset"] = msg.offset()

        try:
            self._producer.produce(
                topic=topic,
                value=payload,
                key=key.encode("utf-8") if key else None,
                on_delivery=_on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            raise PublishError(f"produce to {topic} failed: {exc}") from exc

        remaining = self._producer.flush(self._timeout)
        if remaining > 0 or "err" not in result:
            raise PublishError(f"delivery to {topic} not confirmed within {self._timeout}s")
        if result["err"] is not None:
            raise PublishError(f"delivery to {topic} failed: {result['err']}")

    def close(self) -> None:
        self._producer.flush(5)


class RedpandaConsumer(MessageConsumer):
    """Manual-commit consumer. Offsets are committed only after a message is fully handled."""

    def __init__(
        self,
        group_id: str,
        brokers: str | None = None,
        auto_offset_reset: str = "earliest",
        max_poll_interval_ms: int = 900_000,
    ):
        settings = get_settings()
        self._consumer = Consumer({
            "bootstrap.servers": brokers or settings.redpanda_brokers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            # The AI worker can spend minutes on one event (LLM + sandbox).
            "max.poll.interval.ms": max_poll_interval_ms,
            "session.timeout.ms": 45_000,
            # Bounded prefetch: explicit backpressure, small memory footprint.
            "queued.max.messages.kbytes": 4096,
            "fetch.max.bytes": 4 * 1024 * 1024,
            **_client_security_config(),
        })

    def subscribe(self, topics: list[str]) -> None:
        self._consumer.subscribe(topics)
        log.info("Subscribed", topics=topics)

    def poll(self, timeout: float) -> Message | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                return None
            if err.retriable() or err.code() in (KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN):
                log.warning("Transient consumer error", error=str(err))
                return None
            raise KafkaException(err)
        ts_type, ts = msg.timestamp()
        return Message(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key(),
            value=msg.value(),
            timestamp_ms=ts if ts_type else None,
        )

    def commit(self, message: Message) -> None:
        self._consumer.commit(
            offsets=[TopicPartition(message.topic, message.partition, message.offset + 1)],
            asynchronous=False,
        )

    def seek(self, message: Message) -> None:
        self._consumer.seek(TopicPartition(message.topic, message.partition, message.offset))

    def close(self) -> None:
        self._consumer.close()


def ensure_topics(
    specs: dict[str, dict[str, Any]],
    brokers: str | None = None,
    timeout: float = 30.0,
) -> dict[str, str]:
    """Create topics that do not exist yet. specs: {name: {partitions, config}}."""
    admin = AdminClient({"bootstrap.servers": brokers or get_settings().redpanda_brokers})
    existing = admin.list_topics(timeout=timeout).topics
    results: dict[str, str] = {}
    to_create = []
    for name, spec in specs.items():
        if name in existing:
            results[name] = "exists"
            continue
        to_create.append(NewTopic(
            name,
            num_partitions=spec.get("partitions", 1),
            replication_factor=spec.get("replication", 1),
            config=spec.get("config", {}),
        ))
    if to_create:
        for name, fut in admin.create_topics(to_create, operation_timeout=timeout).items():
            try:
                fut.result()
                results[name] = "created"
            except KafkaException as exc:
                if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    results[name] = "exists"
                else:
                    raise
    return results


def wait_for_broker(brokers: str | None = None, timeout: float = 60.0) -> bool:
    addr = brokers or get_settings().redpanda_brokers
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if AdminClient({"bootstrap.servers": addr}).list_topics(timeout=5).brokers:
                return True
        except KafkaException:
            pass
        time.sleep(2)
    return False

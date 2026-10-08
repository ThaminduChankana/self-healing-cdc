"""Publishes repaired events (cdc.repaired) and audit events (cdc.audit)."""

from __future__ import annotations

from src.common.config import get_settings
from src.common.kafka import MessageProducer, PublishError
from src.common.logging import ComponentLogger
from src.common.models import AuditEvent, RepairedEvent
from src.common.retry import retry_call

log = ComponentLogger("repaired_producer")


class RepairOutputPublisher:
    def __init__(self, producer: MessageProducer):
        self._producer = producer
        self._settings = get_settings()

    def _send(self, topic: str, value: dict, key: str) -> None:
        retry_call(
            lambda: self._producer.publish(topic, value, key=key),
            attempts=self._settings.max_consumer_retries,
            base_delay=self._settings.retry_base_delay_seconds,
            max_delay=self._settings.retry_max_delay_seconds,
            retry_on=(PublishError,),
            on_retry=lambda n, e, d: log.warning("Publish retry", topic=topic, attempt=n,
                                                 delay_s=d, error=str(e), event_id=key),
        )

    def publish_repaired(self, event: RepairedEvent) -> None:
        self._send(self._settings.repaired_topic, event.model_dump(mode="json"), event.event_id)
        log.info("Published repaired event", event_id=event.event_id, repair_id=event.repair_id,
                 correlation_id=event.correlation_id, source_table=event.source_table)

    def publish_audit(self, audit: AuditEvent) -> None:
        self._send(self._settings.audit_topic, audit.model_dump(mode="json"), audit.event_id)
        log.info("Published audit event", event_id=audit.event_id, repair_id=audit.repair_id,
                 repair_result=audit.repair_result)

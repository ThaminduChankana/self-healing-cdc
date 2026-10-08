"""DLQ publisher: structured, context-rich dead-letter messages with bounded retries."""

from __future__ import annotations

from src.common.config import get_settings
from src.common.kafka import MessageProducer, PublishError
from src.common.logging import ComponentLogger
from src.common.metrics import EVENTS_DLQ
from src.common.models import DLQMessage
from src.common.retry import RetryExhaustedError, retry_call

log = ComponentLogger("dlq_publisher")


class DLQPublisher:
    def __init__(self, producer: MessageProducer, topic: str | None = None):
        settings = get_settings()
        self._producer = producer
        self._topic = topic or settings.dlq_topic
        self._settings = settings

    def publish(self, message: DLQMessage) -> None:
        """Publish to the DLQ; raises RetryExhaustedError if the broker stays unavailable."""
        def _send() -> None:
            self._producer.publish(self._topic, message.model_dump(mode="json"), key=message.event_id)

        retry_call(
            _send,
            attempts=self._settings.max_consumer_retries,
            base_delay=self._settings.retry_base_delay_seconds,
            max_delay=self._settings.retry_max_delay_seconds,
            retry_on=(PublishError,),
            on_retry=lambda n, e, d: log.warning("DLQ publish retry", attempt=n, delay_s=d,
                                                 error=str(e), event_id=message.event_id),
        )
        drift_type = message.drift_report.get("drift_type", "NONE") if message.drift_report else "NONE"
        table = (f"{message.source_name}:{message.source_database}.{message.source_table}"
                 if message.source_table else "unknown")
        EVENTS_DLQ.labels(source_table=table,
                          reason=message.reason.value, drift_type=drift_type).inc()
        log.info("Event sent to DLQ", event_id=message.event_id, correlation_id=message.correlation_id,
                 source_table=message.source_table, reason=message.reason.value, drift_type=drift_type)


__all__ = ["DLQPublisher", "RetryExhaustedError"]

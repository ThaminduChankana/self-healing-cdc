"""Schema validation consumer: cdc.mutations -> cdc.validated | cdc.dlq.

Fault isolation ("circuit breaker"):
  * Anything wrong with an individual message (bad JSON, not a Debezium
    envelope, unknown table, drift, unexpected exception while validating) is
    routed to the DLQ with context, and the offset is committed — a poison
    message can never crash or stall the consumer.
  * Infrastructure failures (broker unavailable while publishing) are retried
    with bounded exponential backoff. If they persist, the consumer rewinds to
    the failed message, opens the circuit (pauses) and retries later. The
    offset is committed only after the message has been routed, so nothing is
    lost.
"""

from __future__ import annotations

import hashlib
import json
import signal
import time
from typing import Any

from confluent_kafka import KafkaException

from src.common.config import get_settings
from src.common.kafka import Message, MessageConsumer, MessageProducer, PublishError, RedpandaConsumer, RedpandaProducer
from src.common.logging import ComponentLogger
from src.common.metrics import (
    CIRCUIT_OPEN,
    EVENTS_RECEIVED,
    EVENTS_SKIPPED,
    EVENTS_VALID,
    VALIDATION_FAILURES,
    heartbeat,
    start_metrics_server,
)
from src.common.models import CDCEvent, DLQMessage, DLQReason, DriftReport, ValidatedEvent
from src.common.retry import RetryExhaustedError, backoff_delay, retry_call
from src.common.schema_registry import ConfluentCompatibleSchemaRegistry
from src.dlq.publisher import DLQPublisher
from src.validator import debezium_parser
from src.validator.debezium_parser import EnvelopeError
from src.validator.drift_detector import DriftDetector
from src.validator.schema_validator import SchemaLookupError, SchemaValidator

log = ComponentLogger("validator")

RAW_PREVIEW_CHARS = 4000


class BrokerUnavailableError(Exception):
    """Publishing failed after bounded retries — the message must be retried later."""


class ValidationService:
    """Routing logic for one message. Kafka-agnostic apart from the producer interface."""

    def __init__(
        self,
        producer: MessageProducer,
        schema_validator: SchemaValidator | None = None,
        detector: DriftDetector | None = None,
    ):
        self._settings = get_settings()
        self._producer = producer
        self._schemas = schema_validator or SchemaValidator()
        self._detector = detector or DriftDetector()
        self._dlq = DLQPublisher(producer)

    # ── entry point ────────────────────────────────────────────────────────
    def handle(self, msg: Message) -> str:
        """Route one message. Returns the outcome; raises BrokerUnavailableError."""
        corr = msg.coordinates
        if msg.value is None:
            EVENTS_SKIPPED.labels(reason="tombstone").inc()
            log.info("Tombstone acknowledged", correlation_id=corr, key=str(msg.key_json()))
            return "skipped:tombstone"

        try:
            value = json.loads(msg.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            VALIDATION_FAILURES.labels(reason="malformed_json").inc()
            log.error("Malformed message isolated (invalid JSON)", correlation_id=corr, error=str(exc))
            return self._to_dlq(self._raw_dlq(msg, DLQReason.MALFORMED_JSON, str(exc)))

        try:
            event = debezium_parser.parse(value, key=msg.key_json(), correlation_id=corr)
        except EnvelopeError as exc:
            VALIDATION_FAILURES.labels(reason="malformed_envelope").inc()
            log.error("Malformed CDC envelope isolated", correlation_id=corr, error=str(exc))
            return self._to_dlq(self._raw_dlq(msg, DLQReason.MALFORMED_ENVELOPE, str(exc), value))

        if event is None:
            EVENTS_SKIPPED.labels(reason="tombstone").inc()
            return "skipped:tombstone"

        table = f"{event.source_name}:{event.source_database}.{event.source_table}"
        EVENTS_RECEIVED.labels(source_table=table).inc()
        log.info(f"Received CDC event op={event.operation}", event_id=event.event_id,
                 correlation_id=corr, source_table=table)

        try:
            return self._validate(event, msg)
        except (BrokerUnavailableError, RetryExhaustedError):
            raise
        except Exception as exc:  # poison message: isolate, never crash
            VALIDATION_FAILURES.labels(reason="processing_error").inc()
            log.exception("Unexpected error while validating; isolating event",
                          event_id=event.event_id, correlation_id=corr, source_table=table)
            return self._to_dlq(self._event_dlq(event, msg, DLQReason.PROCESSING_ERROR,
                                                error=f"{type(exc).__name__}: {exc}"))

    # ── routing ────────────────────────────────────────────────────────────
    def _validate(self, event: CDCEvent, msg: Message) -> str:
        table = f"{event.source_name}:{event.source_database}.{event.source_table}"
        if not event.is_row_event:
            self._publish_validated(event, contract_id="", notice={"control_event": event.operation})
            return "validated:control"
        try:
            contract = self._schemas.contract_for(event.source_database, event.source_table,
                                                  event.source_name or None)
        except SchemaLookupError as exc:
            VALIDATION_FAILURES.labels(reason="unknown_schema").inc()
            log.warning("No canonical contract for table", event_id=event.event_id, source_table=table)
            return self._to_dlq(self._event_dlq(event, msg, DLQReason.UNKNOWN_SCHEMA, error=str(exc)))

        report = self._detector.detect(event, contract)
        if not report.drift_detected:
            self._publish_validated(event, contract.contract_id)
            EVENTS_VALID.labels(source_table=table).inc()
            log.info("Event validated", event_id=event.event_id, correlation_id=event.correlation_id,
                     source_table=table)
            return "validated"

        if self._detector.only_compatible_type_changes(report) and \
                self._settings.compatible_type_change_routing == "validated":
            notice = {"compatible_type_changes": [t.model_dump() for t in report.type_mismatches]}
            self._publish_validated(event, contract.contract_id, notice=notice)
            EVENTS_VALID.labels(source_table=table).inc()
            log.warning("Compatible type widening passed through with notice", event_id=event.event_id,
                        source_table=table)
            return "validated:compatible_widening"

        log.warning(f"Schema drift detected: {report.drift_type.value}", event_id=event.event_id,
                    correlation_id=event.correlation_id, source_table=table,
                    missing=report.missing_fields, unexpected=report.unexpected_fields,
                    ambiguous=report.ambiguous)
        return self._to_dlq(self._event_dlq(event, msg, DLQReason.SCHEMA_DRIFT, report=report,
                                            canonical_schema=contract.schema))

    def _publish_validated(self, event: CDCEvent, contract_id: str, notice: dict[str, Any] | None = None) -> None:
        out = ValidatedEvent(
            event_id=event.event_id,
            correlation_id=event.correlation_id,
            source_name=event.source_name,
            source_database=event.source_database,
            source_table=event.source_table,
            operation=event.operation,
            contract_id=contract_id,
            key=event.key,
            payload=event.payload,
            before=event.before if event.operation == "u" else None,
            source_metadata=event.source_metadata,
            ts_ms=event.ts_ms,
            schema_notice=notice,
        )
        try:
            retry_call(
                lambda: self._producer.publish(self._settings.validated_topic,
                                               out.model_dump(mode="json"), key=event.event_id),
                attempts=self._settings.max_consumer_retries,
                base_delay=self._settings.retry_base_delay_seconds,
                max_delay=self._settings.retry_max_delay_seconds,
                retry_on=(PublishError,),
                on_retry=lambda n, e, d: log.warning("Validated publish retry", attempt=n, delay_s=d,
                                                     error=str(e), event_id=event.event_id),
            )
        except RetryExhaustedError as exc:
            raise BrokerUnavailableError(str(exc)) from exc

    def _to_dlq(self, message: DLQMessage) -> str:
        try:
            self._dlq.publish(message)
        except RetryExhaustedError as exc:
            raise BrokerUnavailableError(str(exc)) from exc
        return f"dlq:{message.reason.value}"

    # ── DLQ message builders ───────────────────────────────────────────────
    def _event_dlq(
        self,
        event: CDCEvent,
        msg: Message,
        reason: DLQReason,
        error: str | None = None,
        report: DriftReport | None = None,
        canonical_schema: dict[str, Any] | None = None,
    ) -> DLQMessage:
        return DLQMessage(
            event_id=event.event_id,
            correlation_id=event.correlation_id,
            reason=reason,
            error=error,
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            source_name=event.source_name,
            source_database=event.source_database,
            source_table=event.source_table,
            operation=event.operation,
            key=event.key,
            payload=event.payload,
            before=event.before if event.operation == "u" else None,
            source_metadata=event.source_metadata,
            column_schema=event.column_schema,
            canonical_schema=canonical_schema or {},
            drift_report=report.model_dump(mode="json", exclude={"original_event", "canonical_schema"})
            if report else {},
        )

    @staticmethod
    def _raw_dlq(msg: Message, reason: DLQReason, error: str, value: Any = None) -> DLQMessage:
        if value is None:
            raw: Any = (msg.value or b"").decode("utf-8", errors="replace")[:RAW_PREVIEW_CHARS]
        else:
            raw = value if len(json.dumps(value, default=str)) <= 64_000 else str(value)[:RAW_PREVIEW_CHARS]
        digest = hashlib.sha256(msg.coordinates.encode()).hexdigest()[:20]
        return DLQMessage(
            event_id=f"malformed-{digest}",
            correlation_id=msg.coordinates,
            reason=reason,
            error=error[:1000],
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            key=msg.key_json(),
            raw_event=raw,
        )


class ValidationConsumer:
    """Poll loop with commit-after-route semantics and a circuit breaker."""

    COMPONENT = "validator"

    def __init__(
        self,
        consumer: MessageConsumer | None = None,
        producer: MessageProducer | None = None,
        service: ValidationService | None = None,
    ):
        self._settings = get_settings()
        self._consumer = consumer or RedpandaConsumer(group_id="cdc-validator")
        self._producer = producer or RedpandaProducer()
        self._service = service or ValidationService(self._producer)
        self._running = False
        self._failures = 0
        self._circuit_open_until = 0.0

    def publish_contracts(self) -> None:
        registry = ConfluentCompatibleSchemaRegistry()
        results = self._service._schemas.publish_contracts(registry)
        log.info("Canonical contracts published to Schema Registry", results=results)

    def run(self, max_messages: int | None = None, idle_timeout: float | None = None) -> int:
        """Consume until stopped. Optional bounds make the loop usable from tests/demos."""
        self._running = True
        self._consumer.subscribe([self._settings.mutations_topic])
        handled = 0
        last_activity = time.monotonic()
        log.info("Validator started", topic=self._settings.mutations_topic)
        while self._running:
            heartbeat(self.COMPONENT)
            if max_messages is not None and handled >= max_messages:
                break
            if idle_timeout is not None and time.monotonic() - last_activity > idle_timeout:
                break
            now = time.monotonic()
            if now < self._circuit_open_until:
                time.sleep(min(0.5, self._circuit_open_until - now))
                continue
            CIRCUIT_OPEN.labels(component=self.COMPONENT).set(0)

            try:
                msg = self._consumer.poll(self._settings.poll_timeout_seconds)
            except KafkaException as exc:
                self._trip(f"poll failed: {exc}")
                continue
            if msg is None:
                continue
            last_activity = time.monotonic()
            try:
                outcome = self._service.handle(msg)
                self._consumer.commit(msg)
                handled += 1
                self._failures = 0
                log.debug("Message handled", correlation_id=msg.coordinates, outcome=outcome)
            except (BrokerUnavailableError, KafkaException) as exc:
                try:
                    self._consumer.seek(msg)
                except KafkaException:
                    pass
                self._trip(f"{type(exc).__name__}: {exc}", correlation_id=msg.coordinates)
        self._close()
        return handled

    def _trip(self, reason: str, correlation_id: str | None = None) -> None:
        self._failures += 1
        delay = backoff_delay(self._failures, self._settings.retry_base_delay_seconds * 4,
                              self._settings.retry_max_delay_seconds)
        self._circuit_open_until = time.monotonic() + delay
        CIRCUIT_OPEN.labels(component=self.COMPONENT).set(1)
        log.error("Circuit breaker OPEN — infrastructure failure, will retry the same message",
                  reason=reason, consecutive_failures=self._failures, retry_in_s=delay,
                  correlation_id=correlation_id)

    def stop(self, *_: Any) -> None:
        self._running = False

    def _close(self) -> None:
        for closer in (self._consumer.close, self._producer.close):
            try:
                closer()
            except Exception:  # noqa: BLE001 - best-effort shutdown
                pass
        log.info("Validator stopped")


def main() -> None:
    settings = get_settings()
    start_metrics_server(settings.metrics_port)
    consumer = ValidationConsumer()
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    consumer.publish_contracts()
    consumer.run()


if __name__ == "__main__":
    main()

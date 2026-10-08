"""AI repair worker: cdc.dlq -> (repair) -> cdc.repaired + cdc.audit.

Guarantees
  * Idempotent: every decided event_id is recorded in the state store; a
    restarted worker (or a re-delivered DLQ message) skips it.
  * No silent loss: the DLQ offset is committed only after the decision has
    been published. If Ollama (or the sandbox DB) is unavailable the event is
    *deferred*: the consumer rewinds and retries with capped exponential
    backoff; the DLQ keeps accumulating and the validator is unaffected.
  * Serial by design: one LLM call at a time (16 GB laptop).
  * The original DLQ message is never modified.
"""

from __future__ import annotations

import json
import signal
import time
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException
from pydantic import ValidationError

from src.ai_repair.llm_client import LLMClient, LLMUnavailableError
from src.ai_repair.ollama_client import create_llm_client
from src.ai_repair.repair_executor import RepairExecutor
from src.common.config import get_settings
from src.common.kafka import Message, MessageConsumer, MessageProducer, RedpandaConsumer, RedpandaProducer
from src.common.logging import ComponentLogger
from src.common.metrics import AI_FAILURES, CIRCUIT_OPEN, EVENTS_DEFERRED, heartbeat, start_metrics_server
from src.common.models import AuditEvent, DLQMessage
from src.common.retry import RetryExhaustedError, backoff_delay
from src.common.state_store import SQLiteStateStore, StateStore
from src.repaired.producer import RepairOutputPublisher
from src.sandbox.sql_sandbox import SandboxUnavailableError

log = ComponentLogger("ai_repair_worker")


class AIRepairWorker:
    COMPONENT = "ai_repair_worker"

    def __init__(
        self,
        consumer: MessageConsumer | None = None,
        producer: MessageProducer | None = None,
        llm: LLMClient | None = None,
        state: StateStore | None = None,
        executor: RepairExecutor | None = None,
        group_id: str = "cdc-ai-repair",
    ):
        self._settings = get_settings()
        self._consumer = consumer or RedpandaConsumer(group_id=group_id)
        self._producer = producer or RedpandaProducer()
        self._llm = llm or create_llm_client()
        self._state = state or SQLiteStateStore(self._settings.state_db_path)
        self._executor = executor or RepairExecutor(self._llm, state=self._state)
        self._publisher = RepairOutputPublisher(self._producer)
        self._running = False
        self._consecutive_deferrals = 0
        self._resume_at = 0.0
        self.stats: dict[str, int] = {}

    def _count(self, key: str) -> None:
        self.stats[key] = self.stats.get(key, 0) + 1

    def run(self, max_messages: int | None = None, idle_timeout: float | None = None,
            max_seconds: float | None = None) -> dict[str, int]:
        self._running = True
        if not self._settings.ai_repair_enabled:
            log.warning("AI_REPAIR_ENABLED=false — DLQ events are retained for manual handling")
            while self._running and max_seconds is None:
                heartbeat(self.COMPONENT)
                time.sleep(5)
            return self.stats
        self._consumer.subscribe([self._settings.dlq_topic])
        if not self._llm.is_available():
            log.warning("LLM endpoint not reachable at startup — events will be deferred until it is",
                        model=self._llm.model)
        elif not self._llm.model_available():
            log.warning("Configured model is not installed", model=self._llm.model,
                        hint=f"ollama pull {self._llm.model}")
        log.info("AI repair worker started", topic=self._settings.dlq_topic, model=self._llm.model)

        started = time.monotonic()
        last_activity = started
        handled = 0
        while self._running:
            heartbeat(self.COMPONENT)
            now = time.monotonic()
            if max_seconds is not None and now - started > max_seconds:
                break
            if max_messages is not None and handled >= max_messages:
                break
            if idle_timeout is not None and now - last_activity > idle_timeout:
                break
            if now < self._resume_at:
                time.sleep(min(0.5, self._resume_at - now))
                continue
            CIRCUIT_OPEN.labels(component=self.COMPONENT).set(0)
            try:
                msg = self._consumer.poll(self._settings.poll_timeout_seconds)
            except KafkaException as exc:
                self._backoff(f"poll failed: {exc}")
                continue
            if msg is None:
                continue
            last_activity = time.monotonic()
            if self._handle(msg):
                handled += 1
        self._close()
        return self.stats

    def _handle(self, msg: Message) -> bool:
        """Returns True when the message was finished (committed)."""
        try:
            dlq = DLQMessage.model_validate(json.loads((msg.value or b"null").decode("utf-8")))
        except (ValueError, ValidationError, UnicodeDecodeError) as exc:
            log.error("Unreadable DLQ message; leaving it in the DLQ for manual inspection",
                      correlation_id=msg.coordinates, error=str(exc)[:300])
            self._count("unreadable")
            return self._commit(msg)

        if self._state.is_terminal(dlq.event_id):
            prior = self._state.get_status(dlq.event_id) or {}
            log.info("Duplicate DLQ event — already decided, skipping", event_id=dlq.event_id,
                     repair_id=prior.get("repair_id"), status=prior.get("status"))
            self._count("duplicate_skipped")
            return self._commit(msg)

        try:
            outcome = self._executor.execute(dlq)
        except (LLMUnavailableError, SandboxUnavailableError) as exc:
            self._defer(msg, dlq, exc)
            return False

        try:
            if outcome.repaired is not None:
                self._publisher.publish_repaired(outcome.repaired)
            self._publisher.publish_audit(outcome.audit)
        except RetryExhaustedError as exc:
            self._consumer.seek(msg)
            self._backoff(f"publish failed: {exc}")
            return False

        self._state.record(dlq.event_id, outcome.audit.repair_id, outcome.status.value,
                           "; ".join(outcome.audit.reasons)[:2000])
        self._write_report(outcome.audit)
        self._consecutive_deferrals = 0
        self._count(outcome.status.value.lower())
        log.info("\n" + outcome.audit.report, event_id=dlq.event_id, repair_id=outcome.audit.repair_id)
        return self._commit(msg)

    def _defer(self, msg: Message, dlq: DLQMessage, exc: Exception) -> None:
        reason = "llm_unavailable" if isinstance(exc, LLMUnavailableError) else "sandbox_unavailable"
        EVENTS_DEFERRED.labels(reason=reason).inc()
        if isinstance(exc, LLMUnavailableError):
            AI_FAILURES.labels(model=self._llm.model, reason="unavailable").inc()
        deferrals = self._state.record_deferral(dlq.event_id, str(exc))
        self._count("deferred")
        if deferrals == 1:  # one audit record per event, not one per retry
            try:
                self._publisher.publish_audit(AuditEvent(
                    event_id=dlq.event_id, correlation_id=dlq.correlation_id,
                    source_table=f"{dlq.source_database}.{dlq.source_table}", dlq_reason=dlq.reason.value,
                    drift_type=dlq.drift_report.get("drift_type", ""), model=self._llm.model,
                    repair_result="DEFERRED", reasons=[f"{reason}: {exc}"],
                ))
            except RetryExhaustedError:
                pass
        try:
            self._consumer.seek(msg)
        except KafkaException:
            pass
        self._backoff(f"{reason}: {exc}", event_id=dlq.event_id, deferrals=deferrals)

    def _backoff(self, reason: str, **fields: Any) -> None:
        self._consecutive_deferrals += 1
        delay = backoff_delay(self._consecutive_deferrals, 2.0, self._settings.llm_unavailable_backoff_max_seconds)
        self._resume_at = time.monotonic() + delay
        CIRCUIT_OPEN.labels(component=self.COMPONENT).set(1)
        log.warning("Dependency unavailable — event deferred (not committed), retrying with backoff",
                    reason=reason[:300], retry_in_s=delay, **fields)

    def _commit(self, msg: Message) -> bool:
        try:
            self._consumer.commit(msg)
            return True
        except KafkaException as exc:
            self._consumer.seek(msg)
            self._backoff(f"commit failed: {exc}")
            return False

    def _write_report(self, audit: AuditEvent) -> None:
        try:
            out = Path(self._settings.state_dir) / "reports"
            out.mkdir(parents=True, exist_ok=True)
            (out / f"{audit.event_id}.txt").write_text(audit.report)
        except OSError:
            pass

    def stop(self, *_: Any) -> None:
        self._running = False

    def _close(self) -> None:
        for closer in (self._consumer.close, self._producer.close, self._state.close):
            try:
                closer()
            except Exception:  # noqa: BLE001
                pass
        log.info("AI repair worker stopped", stats=self.stats)


def main() -> None:
    start_metrics_server(get_settings().metrics_port)
    worker = AIRepairWorker()
    signal.signal(signal.SIGINT, worker.stop)
    signal.signal(signal.SIGTERM, worker.stop)
    worker.run()


if __name__ == "__main__":
    main()

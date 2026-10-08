"""Prometheus-compatible application metrics.

Each long-running service exposes `/metrics` on its own METRICS_PORT
(validator 8001, AI worker 8002, downstream 8003 by default). Metric names
are infrastructure-neutral so they can be scraped by Prometheus, the
CloudWatch agent or Managed Service for Prometheus unchanged.
"""

from __future__ import annotations

import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server

EVENTS_RECEIVED = Counter(
    "cdc_events_received_total", "CDC messages received by the validator", ["source_table"]
)
EVENTS_VALID = Counter(
    "cdc_events_valid_total", "Events published to the validated topic", ["source_table"]
)
EVENTS_DLQ = Counter(
    "cdc_events_dlq_total", "Events routed to the DLQ", ["source_table", "reason", "drift_type"]
)
VALIDATION_FAILURES = Counter(
    "cdc_validation_failures_total",
    "Validator processing errors (malformed input, lookup or publish failures)",
    ["reason"],
)
EVENTS_SKIPPED = Counter(
    "cdc_events_skipped_total", "Messages acknowledged without routing (tombstones)", ["reason"]
)
EVENTS_REPAIRED = Counter(
    "cdc_events_repaired_total", "Events repaired and published", ["source_table", "drift_type", "source"]
)
REPAIR_FAILED = Counter(
    "cdc_repair_failed_total", "Repairs that ended without a published repair", ["source_table", "decision"]
)
AI_REQUESTS = Counter("cdc_ai_requests_total", "LLM repair requests", ["model"])
AI_FAILURES = Counter("cdc_ai_failures_total", "LLM request or response failures", ["model", "reason"])
SANDBOX_FAILURES = Counter("cdc_sandbox_failures_total", "Sandbox rejections", ["stage", "reason"])
EVENTS_DEFERRED = Counter(
    "cdc_events_deferred_total", "DLQ events deferred because a dependency was unavailable", ["reason"]
)
REPAIR_CACHE_HITS = Counter("cdc_repair_cache_hits_total", "Repairs served from the approved-repair cache")
DOWNSTREAM_INGESTED = Counter(
    "cdc_downstream_ingested_total", "Rows applied to the downstream store", ["source_topic", "op"]
)
DOWNSTREAM_DUPLICATES = Counter(
    "cdc_downstream_duplicates_total", "Duplicate events ignored downstream", ["source_topic"]
)
CIRCUIT_OPEN = Gauge("cdc_circuit_open", "1 while a consumer circuit breaker is open", ["component"])
HEARTBEAT = Gauge("cdc_consumer_heartbeat_timestamp", "Last poll loop iteration (unix time)", ["component"])

REPAIR_LATENCY = Histogram(
    "cdc_repair_latency_seconds",
    "End-to-end repair latency for one DLQ event",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300],
)


def start_metrics_server(port: int) -> bool:
    """Start the /metrics endpoint; returns False if disabled or the port is taken."""
    if port <= 0:
        return False
    try:
        start_http_server(port)
        return True
    except OSError:
        return False


def heartbeat(component: str) -> None:
    HEARTBEAT.labels(component=component).set(time.time())

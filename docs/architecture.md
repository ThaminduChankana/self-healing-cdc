# Architecture

## Data flow

```text
MySQL ──binlog──► Debezium (Kafka Connect) ──► cdc.mutations
                                                   │
                                     validator (src/validator/consumer.py)
                         ┌─────────────────────────┼──────────────────────────┐
                    valid│                drift / malformed / unknown          │
                         ▼                         ▼                           │
                  cdc.validated                cdc.dlq                         │
                         │                         │                           │
                         │          AI repair worker (src/ai_repair/worker.py) │
                         │                         │                           │
                         │                         ├──► cdc.audit (every decision, incl. DEFERRED)
                         │                         └──► cdc.repaired (REPAIRED only)
                         ▼                         ▼
                  downstream consumer (dedupe by event_id) → warehouse
```

| Topic | Partitions | Retention | Key | Content |
|---|---|---|---|---|
| `cdc.mutations` | 3 | 7 d | primary key | raw Debezium envelope (with schema) |
| `cdc.validated` | 3 | 7 d | event_id | `ValidatedEvent` |
| `cdc.dlq` | 1 | 30 d | event_id | `DLQMessage`: payload, before, column schema, contract, drift report, coordinates |
| `cdc.repaired` | 3 | 7 d | event_id | `RepairedEvent` (source event + repaired payload + provenance) |
| `cdc.audit` | 1 | 90 d | event_id | `AuditEvent` (decision, checks, hashes, report) |

## Components

### Source and capture

- **Source:** `quay.io/debezium/example-mysql:latest` (MySQL 8.2.0), unmodified. The official
  `inventory.sql` init runs first. `docker/source-db/zz-cdc-users.sh` then adds two least-privilege users:
  `cdc_reader` (SELECT) and `app_writer` (SELECT/INSERT/UPDATE/ALTER, used to simulate the upstream app).
- **Connector:** rendered from `config/sources.yaml` by `src/tools/connector.py`.
  - Captures only the tables that have a contract.
  - JSON converter with value schemas.
  - `column.propagate.source.type` adds SQL type and length to every field, which is how `INT→BIGINT` and
    `VARCHAR(255)→VARCHAR(512)` are visible.
  - A `RegexRouter` sends every change topic to `cdc.mutations`.
  - The password is resolved by Connect from its own environment (`${env:MYSQL_PASSWORD}`).

### Validator

`src/validator/consumer.py` splits into `ValidationService` (routing logic) and `ValidationConsumer`
(the poll loop).

1. Decode JSON. On failure → DLQ `MALFORMED_JSON`, with an id derived from the topic coordinates.
2. Parse the envelope (`debezium_parser.py`).
   - Handles schema-enabled or schemaless JSON, `c`/`u`/`d`/`r`, tombstones and control events.
   - Locates `before` and `after` by field name.
   - Extracts per-column type info.
   - Computes a stable `event_id`.
   - On failure → DLQ `MALFORMED_ENVELOPE`.
3. Resolve the contract by `(source.name, db, table)`. If none → DLQ `UNKNOWN_SCHEMA`.
4. Detect drift (`drift_detector.py`). Clean → `cdc.validated`. Otherwise → DLQ `SCHEMA_DRIFT`, with the
   report.
5. Any unexpected exception → DLQ `PROCESSING_ERROR`. A poison message is isolated, never re-thrown.

**Circuit breaker.** Publishing is retried with bounded exponential backoff. If the broker stays down,
the consumer seeks back to the same message, opens the circuit (exposed as `cdc_circuit_open=1`) and
retries later. Offsets are committed only after a message has been routed.

### Drift detector

- Deterministic. It produces missing and unexpected fields, type changes (SQL-, Connect- or
  JSON-level, each with a compatibility verdict), nullability changes (schema- and value-level),
  rename candidates, JSON-Schema violations, a classification, an `ambiguous` flag, and a
  **fingerprint**: the identity of the drift's *shape*, used by the repair cache.
- **Rename heuristic.** Combines name similarity (difflib), token containment with configurable
  "decorator" words (`stock_`, `product_`…), configured synonyms, type affinity from the compatibility
  matrix, and nullability affinity. The score is
  `0.55·lexical + 0.30·type + 0.15·nullability`.
  - Candidates are assigned one-to-one.
  - A candidate below 0.75, or with a competitor within 0.08, is **ambiguous**.
  - If a column disappears while an unrelated one appears, the drift is flagged ambiguous.
  - **Limitations:** several simultaneous renames to unrelated names, a rename combined with a type
    change, or a semantic rename such as `qty` → `units` without a configured synonym cannot be resolved
    reliably. These go to manual review rather than being guessed.

### AI repair worker

```text
DLQ message ─► idempotency check (SQLite) ─► reason == SCHEMA_DRIFT? ──no──► MANUAL_REVIEW (no LLM)
                    │yes
                    ▼
     approved-repair cache hit (same fingerprint)? ──yes──► re-run ALL checks on this event ─► REPAIRED
                    │no / cached repair no longer passes
                    ▼
     for attempt in 1..MAX_AI_RETRIES:
         prompt (rules in system msg; deterministic report, contract, policy hint, untrusted payload)
         Ollama /api/chat with JSON-schema-constrained output (temperature 0, seed 42)
         strict RepairResponse validation ─────────── invalid → feedback, retry
         SQL policy (allowlist grammar) ───────────── destructive → REJECTED (no retry)
                                                      non-conforming → feedback, retry
         Python sandbox (AST + subprocess) ────────── forbidden constructs → REJECTED
         output checks: schema, required, unexpected, data fidelity (row + before-image)
         deterministic replay in a fresh process
         SQL sandbox: migration + information_schema verification + load repaired row
         decision engine ─► REPAIRED | RETRY_AI (directive feedback) | REJECTED | MANUAL_REVIEW
```

- **Ollama unavailable** (connection refused, timeout, 5xx, model missing): the event is deferred. The
  worker seeks back, never commits, publishes one `DEFERRED` audit per event and retries with capped
  exponential backoff (`cdc_events_deferred_total`). The DLQ keeps accumulating and the validator is
  unaffected.
- **Outputs.** `cdc.repaired` (if repaired) and then `cdc.audit` are published with confirmed delivery.
  Only after that is the event recorded in the state store and the DLQ offset committed. The original DLQ
  message is never modified.
- **Decision engine** (`repair_planner.py`):
  - **REJECTED:** fatal checks (`sql_policy` destructive, `code_security`, `sandbox_migration`,
    `model_risk` HIGH/CRITICAL).
  - **RETRY_AI:** retryable failures while attempts remain.
  - **MANUAL_REVIEW:** retries exhausted on low confidence, effective risk ≥ MEDIUM, an ambiguous drift,
    or a drift type that always needs a human (`REMOVED_COLUMN`, `UNKNOWN`).
  - **REPAIRED:** every check passed, confidence ≥ `CONFIDENCE_THRESHOLD` (0.90) and effective risk LOW.

### Sandboxes

- **Python** (`src/sandbox/`):
  - The AST allowlist covers node types, attribute names and built-ins.
  - Code runs in a child `python -I -S runner.py` with `env={}`, cwd set to a fresh temp directory, and
    `start_new_session`.
  - Limits: `RLIMIT_CPU`, `RLIMIT_FSIZE=0`, `RLIMIT_NPROC=0`, `RLIMIT_NOFILE=8`, and `RLIMIT_AS` on
    Linux. Built-ins come from an allowlist and contain no `__import__`. The child re-validates the AST.
  - The wall-clock timeout kills the whole process group → `SANDBOX_TIMEOUT`.
- **SQL** (`src/sandbox/sql_sandbox.py`):
  - A dedicated MySQL server. The worker's user can only create `sbx_*` databases.
  - For each repair: create a scratch database, build the table from the contract's `x-sql-type`
    annotations, apply the re-rendered statements, diff `information_schema` (nothing removed or
    narrowed, new columns nullable or defaulted), insert the repaired row, then drop the scratch database.

### Downstream destinations

`src/downstream/consumer.py` runs one `DestinationRunner` thread per enabled entry in
`config/destinations.yaml`.

- **Normalise.** Both clean topics become one `ChangeRecord` (`src/sinks/base.py`): table reference
  `<source>.<database>.<table>`, key, op, canonical payload, `event_id`, `repair_id`, origin
  (validated/repaired) and a monotonic **source sequence** derived from the binlog file/pos/row.
- **Route.** Each destination's `tables:` globs decide which tables it receives.
- **Deliver.** Micro-batches go to `Sink.write()`. Offsets are committed per destination only after the
  sink succeeded. On failure the runner seeks back to the first offset of the batch and retries with
  backoff. A per-destination SQLite ledger skips `event_id`s it has already delivered, so retries and
  restarts are idempotent.
- **Isolation.** Each destination has its own consumer group (`cdc-downstream-<name>`), so a broken
  destination lags alone and catches up later. A new destination backfills from the start of the topics.
- **Ordering.** A repaired event is published seconds after the change happened, possibly after a newer
  valid change to the same row. Sinks therefore apply last-writer-wins on `_seq`:
  - SQLite guards its upsert on `_seq`.
  - Iceberg upsert mode drops stale records.
  - BigQuery's view and JSONL readers take max(`_seq`) per key.
  - Deletes are tombstones, so a late upsert cannot resurrect a row.

| Sink | Module | Notes |
|---|---|---|
| `sqlite` | `sinks/sqlite_sink.py` | current state, local stand-in for an operational warehouse |
| `jsonl` | `sinks/jsonl_sink.py` | bronze/landing change log, Hive-style `dt=` partitions |
| `iceberg` | `sinks/iceberg_sink.py` | pyiceberg; catalog is config (SQL locally; Glue/REST/BigLake/Hive in the cloud) |
| `bigquery` | `sinks/bigquery_sink.py` | changelog table + latest-row view; Storage Write API CDC is the high-volume upgrade |
| custom | `type: "pkg.module:Class"` | any `Sink` subclass |

## Cloud-portable interfaces

| Concern | Interface | Local implementation | Cloud swap |
|---|---|---|---|
| Producer / consumer | `MessageProducer`, `MessageConsumer` (`common/kafka.py`) | Redpanda (confluent-kafka) | Amazon MSK (`_client_security_config` for IAM/SCRAM) |
| Schema Registry | `SchemaRegistry` (`common/schema_registry.py`) | Redpanda SR | Confluent SR / AWS Glue SR adapter |
| LLM | `LLMClient.generate_repair(RepairRequest) -> RepairResponse` | `OllamaLLMClient`, `MockLLMClient` | `BedrockLLMClient` (implement `_complete`, `is_available`, `model_available`) |
| Database | `DatabaseConnector` (`common/database.py`) | mysql-connector | RDS / Aurora via IAM auth |
| State store | `StateStore` (`common/state_store.py`) | SQLite | DynamoDB |
| Destination | `Sink` (`sinks/base.py`) + `config/destinations.yaml` | SQLite, JSONL lake, Iceberg | BigQuery, Iceberg on S3/GCS (Glue/BigLake catalog), Snowflake/Redshift via custom sink |
| Metrics | Prometheus client (`common/metrics.py`) | `/metrics` | CloudWatch agent / AMP scrape |

## Multiple sources

Supporting N sources does **not** need N deployments:

- **Capture:** one Kafka Connect cluster runs one connector per source. A connector is a config plus
  one task thread, not a separate process. `make connector` renders and registers all of them from
  `config/sources.yaml`. Each source needs a unique `topic_prefix`, and MySQL sources a unique
  `server_id`. `load_sources()` enforces both.
- **Transport:** every connector routes into `cdc.mutations`. Raise its partition count as volume grows.
  Messages are keyed by primary key, so ordering per row is preserved.
- **Validation:** stateless and horizontally scalable. Run more `validator` replicas in the same consumer
  group. Contracts are keyed `source.database.table`, so identical table names in different sources
  never collide. A database name alone is only used for lookup when exactly one source has it.
- **Repair:** the worker is serial per instance for laptop RAM. To scale, give `cdc.dlq` more partitions
  and run more workers in the group, and move the state store to a shared backend such as DynamoDB. The
  repair cache means a recurring drift costs one LLM call per *drift shape*, not one per event.
- **Isolation option:** a very noisy or sensitive source can get its own `mutations` topic, either by
  changing the router replacement per source or by running a dedicated validator group. That's a config
  change, not a code change.

Resource estimate on the laptop: each additional MySQL connector adds roughly 30–60 MB to the Connect
JVM. Five sources fit comfortably in the default `-Xmx768m`.

## Idempotency and lineage

- `event_id = <table>-<op>-sha256(server, db, table, op, binlog file/pos/row, gtid, primary key)[:20]`.
  This is stable across re-delivery and unique across snapshot rows, which share a binlog position.
- The worker records every terminal decision. Re-delivered DLQ messages, or a restart with lost offsets,
  are skipped (`duplicate_skipped`). Downstream de-duplicates again on `event_id`.
- `correlation_id` is the original `cdc.mutations` coordinate, and `repair_id` identifies the decision.
  Logs are JSON lines with these IDs promoted to top-level keys, so they work directly as CloudWatch
  Logs Insights or OpenSearch filters.

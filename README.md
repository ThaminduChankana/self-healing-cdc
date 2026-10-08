# Self-Healing CDC Pipeline with Local AI Schema-Drift Repair

A runnable proof-of-concept CDC pipeline that **keeps flowing when the upstream schema changes
unexpectedly**. Valid change events stream straight through. Drifted events are isolated in a dead-letter
queue and analysed deterministically. A local coding model (Ollama + `qwen2.5-coder:7b`) then *proposes*
a repair. Independent safety checks, an isolated sandbox and a decision engine decide whether the repair
is used. The whole stack runs on a 16 GB MacBook, and the architecture maps 1:1 to AWS (RDS + MSK + Bedrock).

## The problem: schema drift

Transactional schemas change without warning: a column is renamed, a new column appears, `INT` becomes
`BIGINT`. A classic CDC pipeline then either crashes on the first "poison" record or, worse, silently
loads data that no longer matches the downstream contract. Fixing it means paging a data engineer, writing
a mapping and replaying events.

This project shows a different pattern:

1. **Circuit breaker.** A drifted or malformed event can never crash or stall the consumer. It is routed
   to `cdc.dlq` with full context, and subsequent events keep flowing.
2. **Deterministic drift analysis first.** The detector classifies the drift (added / removed / renamed
   column, type change, nullability, multiple) and scores rename candidates. Low-confidence results are
   flagged as **ambiguous**.
3. **AI proposes, never decides.** The model returns a strict JSON proposal containing a Python
   transformation and optional migration SQL.
4. **Everything the model returns is untrusted.** The JSON is checked against a strict schema. The SQL
   is parsed against an allowlist grammar and then run only in an isolated sandbox MySQL. The Python is
   checked by an AST allowlist and run in a separate locked-down process. The output is validated for
   schema conformance, data fidelity and determinism.
5. **Auditable and idempotent.** Every decision is published to `cdc.audit` with a human-readable report.
   An event is never repaired twice, and the original DLQ message is never modified.

## Architecture

```text
                 ┌────────────────────────────┐
                 │ MySQL (debezium example DB)│   one Kafka Connect cluster hosts
                 └─────────────┬──────────────┘   one connector per configured source
                               │ binlog
                          Debezium (Kafka Connect)
                               │  JSON envelope + column types
                               ▼
                 ┌────────────────────────────┐
                 │ Redpanda   cdc.mutations   │◄──── Schema Registry (canonical contracts)
                 └─────────────┬──────────────┘
                               │
                      Validation consumer  (circuit breaker, commit-after-route)
                       /                 \
                 valid                     drift / malformed / unknown table
                   │                         │
                   ▼                         ▼
           cdc.validated                  cdc.dlq ──────────────────────────────┐
                   │                         │                                  │
                   │                AI repair worker (serial, idempotent)       │
                   │                         │                                  │
                   │          deterministic drift report + contract             │
                   │                         ▼                                  │
                   │            Ollama · qwen2.5-coder:7b  (proposal only)      │
                   │                         ▼                                  │
                   │   strict JSON schema → SQL allowlist parser → risk         │
                   │   → Python sandbox (AST + subprocess + rlimits + timeout)  │
                   │   → contract + fidelity + determinism checks               │
                   │   → SQL sandbox MySQL (migration + load test)              │
                   │                         ▼                                  │
                   │                 decision engine                            │
                   │        REPAIRED │ RETRY_AI │ REJECTED │ MANUAL_REVIEW       │
                   │                 │                                          │
                   │                 ▼                                          ▼
                   │          cdc.repaired                                  cdc.audit
                   ▼                 ▼
              downstream consumer (dedupe by event_id) → warehouse (SQLite stand-in)
```

More detail: [docs/architecture.md](docs/architecture.md).

## Repository layout

```text
config/
  sources.yaml                 source registry: connectors, tables, contracts, drift scenarios, seed data
  canonical_schema/*.json      downstream contracts (JSON Schema 2020-12 + x-sql-type annotations)
  policies/migration_policy.yaml   SQL allowlist + decision thresholds
  policies/type_compatibility.yaml explicit type compatibility matrix
  mappings/rename_hints.yaml   synonyms used by the rename heuristic
src/
  common/      config, logging, metrics, models, kafka, retry, sources, schema_registry, state_store, database
  validator/   debezium_parser, drift_detector, type_compatibility, schema_validator, consumer
  dlq/         publisher
  ai_repair/   llm_client, ollama_client, mock_client, prompt_builder, response_parser,
               sql_policy, repair_planner (decision engine), repair_executor, report, worker
  sandbox/     isolation (AST), runner (child process), executor, validator (output), sql_sandbox
  repaired/    producer (repaired + audit)
  downstream/  consumer (warehouse stand-in)
  tools/       connector, inspect_source, hf_loader, drift, topics, ollama_check, state, demo
scripts/       bootstrap, create_topics, register_connector, inspect_mysql, trigger_schema_drift,
               publish_test_event, reset_environment, run_demo
docker/        init hooks for the source DB (least-privilege users) and the sandbox DB
tests/         unit/, security/, integration/ (live stack)
docs/          architecture, schema-drift-scenarios, local-development, cloud-migration, troubleshooting
```

## Prerequisites

- macOS (tested on Apple M1 Pro, 16 GB RAM), Docker Desktop with about 8 GB of VM memory
- Python 3.11+
- [Ollama](https://ollama.com/download), running natively so it uses Apple-silicon acceleration
- About 5 GB of disk for `qwen2.5-coder:7b` (the model is never downloaded automatically)

## Installation

```bash
make setup                     # checks Docker/Python/Ollama/ports, creates venv/ and .env
ollama pull qwen2.5-coder:7b   # or: make ollama-pull   (explicit, ≈4.7 GB)
make ollama-check              # API reachable, model installed, one structured-output call
```

## Startup

```bash
make start      # builds the app image, starts everything, waits for health checks + init jobs
make status     # containers, connector state, topic message counts
```

`make start` needs no manual steps. Topics are created by the `topic-init` job, and the Debezium
connector is registered by `connector-init` after Connect and MySQL are healthy. MySQL only reports
healthy once its init (including the least-privilege users) has finished. `make topics` and
`make connector` re-run the same idempotent steps by hand.

| Service | Purpose | Host port |
|---|---|---|
| `mysql` | `quay.io/debezium/example-mysql` (source) | 3306 |
| `redpanda` | Kafka API + Schema Registry | 19092 / 18081 |
| `debezium` | Kafka Connect + Debezium 3.x | 8083 |
| `mysql-sandbox` | isolated MySQL for AI-proposed SQL | 3307 |
| `validator` / `ai-worker` / `downstream` | pipeline services (`/metrics`) | 8001 / 8002 / 8003 |
| `redpanda-console` (profile `console`) | UI: `make console` | 8080 |
| `ollama` (profile `ollama`) | optional Ollama in Docker (CPU only) | 11435 |

## Running the demo

```bash
make demo
```

The demo runs these steps in order. Each step waits for a concrete record on a real topic and verifies it.

1. Start Compose and wait for health checks.
2. Inspect MySQL with the read-only user.
3. Register the connector and create the topics.
4. Verify Redpanda and the Schema Registry.
5. Verify Ollama, the model, and that the worker container can reach it.
6. Load **25 real products from the Hugging Face dataset
   [`philschmid/amazon-product-descriptions-vlm`](https://huggingface.co/datasets/philschmid/amazon-product-descriptions-vlm)**
   into the official `products` / `products_on_hand` tables. This is insert-only and parameterized.
7. Show a baseline event in `cdc.validated`.
8. Rename `products_on_hand.quantity → stock_quantity` with real DDL and update a row.
9. Show the DLQ message and the deterministic drift report.
10. Wait for Qwen's live repair proposal, then show the full repair report: sandbox, schema validation,
    decision, and the `cdc.repaired` event.
11. Show the audit record.
12. Update a second drifted row. It is repaired from the approved-repair cache without calling the LLM.
13. Show the downstream warehouse row and the event's full lineage.
14. Revert the rename (non-destructive) and confirm events validate directly again.

If the model's proposal fails any check, the demo **fails with exit code 1** and prints the reasons. It
never reports a repair that did not pass.

## Introducing schema drift

```bash
./scripts/trigger_schema_drift.sh rename        # products_on_hand.quantity -> stock_quantity
./scripts/trigger_schema_drift.sh add-column    # products.supplier_code VARCHAR(64) NULL
./scripts/trigger_schema_drift.sh widen-type    # products_on_hand.quantity INT -> BIGINT
./scripts/trigger_schema_drift.sh status
./scripts/trigger_schema_drift.sh reset         # reverses rename/widen; DROP of the demo column only
                                                # with --allow-drop-demo-column
make drift ACTION=rename ARGS=--dry-run         # print the SQL without executing it
make test-event KIND=malformed|injection|valid  # poison message / prompt-injection row / new HF row
```

The script reads its targets from `config/sources.yaml` and checks them against `information_schema`
before it runs anything. It prints every statement before executing it. Scenarios are described in
[docs/schema-drift-scenarios.md](docs/schema-drift-scenarios.md).

## Running tests

```bash
make test              # 228 unit + security tests — no Docker, Kafka or Ollama needed (mock LLM)
make integration-test  # 10 live tests against the running stack (3 use the real model)
```

The integration tests cover the eight required scenarios against the live stack:

1. Normal CDC reaches `cdc.validated` with no DLQ entry.
2. Added column is repaired.
3. Renamed column is mapped `stock_quantity → quantity`.
4. `INT→BIGINT` is classified as a compatible widening, and Qwen's `MODIFY … BIGINT` is verified in the
   sandbox MySQL.
5. A destructive migration (`DROP COLUMN …; DROP TABLE …`) is rejected and never executed anywhere.
   A companion test runs a real widening in the sandbox and checks that the post-execution verification
   catches narrowing.
6. Malformed JSON and non-Debezium JSON are isolated, and valid events keep flowing.
7. Ollama unavailable: events are deferred and never committed, then recovered.
8. Restart with lost offsets, and a real container restart, produce no duplicate repairs.

## Viewing Redpanda topics

```bash
make peek                          # latest record of every pipeline topic
make peek TOPIC=cdc.dlq LAST=3
python -m src.tools.topics counts  # (venv) message counts per topic
make console                       # Redpanda Console UI at http://localhost:8080
docker exec cdc-redpanda rpk topic consume cdc.audit -n 1
```

## Following a single event

Every record carries three identifiers:

- `event_id`: deterministic, derived from the binlog file/pos/row, the primary key and the source
  server, so re-delivery yields the same id.
- `correlation_id`: the original `cdc.mutations/<partition>/<offset>` coordinate.
- `repair_id`: one per repair decision.

All logs are JSON with these fields at the top level.

```bash
make trace EVENT=products_on_hand-u-95975000c8f73df4e946
# DLQ entry → repaired event → audit record (with the human-readable report) → container log lines
docker exec cdc-ai-worker cat /app/state/reports/<event_id>.txt
```

## Supporting other or multiple sources

All source-specific settings live in `config/sources.yaml`: connector settings, captured tables, primary
keys, contracts, drift scenarios and seed data. To add a source:

1. Add an entry with a unique `topic_prefix` and `server_id`.
2. Generate its contracts with `python -m src.tools.inspect_source --generate <table> --out config/canonical_schema/<table>.json`.
3. Run `make connector`.

One Connect cluster runs all connectors. They all write to `cdc.mutations`, and one validator and one
worker serve every source. Events are routed on `(source.name, database, table)`, so two sources can
both have an `inventory.products` table. Scale out by adding consumer replicas, not one deployment per
source (see [docs/architecture.md](docs/architecture.md#multiple-sources)).

## Metrics

`make metrics` prints the Prometheus series exposed on `:8001-8003/metrics`:

- `cdc_events_received_total`, `cdc_events_valid_total`, `cdc_events_dlq_total{reason,drift_type}`
- `cdc_events_repaired_total{source=llm|cache}`, `cdc_repair_failed_total{decision}`
- `cdc_ai_requests_total`, `cdc_ai_failures_total{reason}`, `cdc_sandbox_failures_total{stage,reason}`
- `cdc_validation_failures_total{reason}`, `cdc_events_deferred_total`, `cdc_repair_latency_seconds`
- `cdc_circuit_open`, `cdc_consumer_heartbeat_timestamp`

## Troubleshooting

See [docs/troubleshooting.md](docs/troubleshooting.md). The most common fixes are:

- `make ollama-check` if the worker logs say *deferred*.
- `docker logs cdc-connector-init` if nothing reaches `cdc.mutations`.
- No inline `#` comments in `.env` values.

## Security model and limitations

There are four trust boundaries. Each is validated independently:

| Untrusted input | Control |
|---|---|
| CDC payload | Treated as data only. It sits in a `<untrusted_cdc_data>` block, truncated, with `<` escaped. The system prompt says it is never an instruction. |
| LLM output | Must match the strict Pydantic `RepairResponse` exactly. Nothing is coerced. The model's confidence and risk are inputs to the decision, not the decision itself. |
| SQL | Allowlist grammar parser. Comments and other dialect tricks are rejected. Semantic checks require widening only, nullable additions only, and the event's own table. Only re-rendered SQL runs, and only in the sandbox MySQL, which is verified via `information_schema` after execution. |
| Python | AST allowlist (no imports, dunders, `getattr`, `str.format`…). Separate `python -I -S` process with an empty environment and a temp working directory. CPU, file-size, process and file-descriptor rlimits. Hard timeout → `SANDBOX_TIMEOUT`. |

The rules: never trust model-generated code, never trust payload instructions, never execute destructive
SQL automatically, never give the AI production credentials. The worker holds only sandbox credentials,
scoped to `sbx_*` databases. Inspection uses a read-only user. The demo "upstream application" uses a
separate writer user.

**Security limitations (MVP).** These are documented, not hidden:

- The Python sandbox is process-level isolation, not a VM or gVisor. On macOS `RLIMIT_AS` is not
  enforced.
- Network access is blocked by the absence of import machinery, not by a network namespace.
- Kafka, Connect and the Schema Registry have no auth or TLS locally.
- Demo credentials live in `.env`.
- Connect stores connector configs in a topic. Passwords are kept out of it via `${env:…}`.

**Production limitations.**

- Single-node Redpanda (RF=1).
- The worker is serial, and its state is local SQLite.
- `AUTO_APPLY_SAFE_MIGRATIONS` only marks verified migrations as `APPROVED_FOR_APPLY`. No component
  applies them downstream.
- Canonical contracts don't evolve automatically.
- The rename heuristic can be wrong when several columns change at once. Such cases are marked
  ambiguous and go to manual review.
- The downstream "warehouse" is SQLite.
- The demo resets drift by reversing its own DDL. Removing the demo-added column is a destructive
  statement and requires an explicit flag.

## AWS migration path

RDS MySQL → Debezium on MSK Connect → Amazon MSK → validator / worker on ECS Fargate or EKS → Amazon
Bedrock (`BedrockLLMClient` implements the same `LLMClient` interface) → sandbox on Lambda/Fargate plus a
throw-away RDS or Aurora schema → MSK `cdc.repaired` → Redshift / S3. DynamoDB replaces SQLite for state,
CloudWatch receives the JSON logs and metrics, and Secrets Manager holds the credentials. See
[docs/cloud-migration.md](docs/cloud-migration.md).

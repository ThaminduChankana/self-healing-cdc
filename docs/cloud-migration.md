# Cloud migration (AWS primary target)

The MVP is built so that infrastructure is swapped behind interfaces. Application code contains no
AWS-specific logic.

## Target architecture

```text
AWS RDS MySQL (binlog_format=ROW, binlog retention ≥ 24h)
      │
Debezium on MSK Connect (custom plugin: debezium-connector-mysql)   — one connector per source
      │
Amazon MSK  cdc.mutations
      │
Validation consumer  (ECS Fargate service / EKS Deployment, N replicas, same consumer group)
      ├──► MSK cdc.validated ──────────────────────────────┐
      └──► MSK cdc.dlq                                      │
              │                                             │
        AI repair worker (ECS/EKS)                          │
              │                                             │
        Amazon Bedrock (BedrockLLMClient)                   │
              │                                             │
        Sandbox: Lambda / Fargate task (no VPC egress)      │
              + throw-away Aurora/RDS schema for SQL         │
              │                                             │
        MSK cdc.repaired + cdc.audit                        │
              │                                             ▼
        Downstream: MSK Connect sink / Firehose → Redshift or S3 (Iceberg)
```

## Component mapping

| Local MVP | AWS | Notes |
|---|---|---|
| `quay.io/debezium/example-mysql` | RDS for MySQL / Aurora MySQL | Enable binlog (`binlog_format=ROW`, `binlog_row_image=FULL`), give the capture user `REPLICATION SLAVE, REPLICATION CLIENT, SELECT`. |
| Debezium in Kafka Connect container | MSK Connect + custom plugin | Same connector JSON. Render it with `src/tools/connector.py`; use a config provider for secrets. |
| Redpanda | Amazon MSK (provisioned or serverless) | Kafka API is unchanged. Add IAM/SCRAM + TLS in `_client_security_config()`. RF = 3, `min.insync.replicas` = 2. |
| Redpanda Schema Registry | Confluent SR on EKS, or AWS Glue Schema Registry | Glue needs a small adapter implementing `SchemaRegistry`. |
| Ollama + Qwen2.5-Coder-7B | Amazon Bedrock | `BedrockLLMClient(LLMClient)` implements `_complete()` via `bedrock-runtime` Converse, with JSON output via tool-use/response schema. |
| Python sandbox (subprocess + rlimits) | Lambda (no VPC/egress, read-only FS, 1–5 s timeout) or Firecracker/gVisor Fargate task | Stronger isolation than the MVP. |
| SQL sandbox MySQL | Ephemeral Aurora clone / scratch schema on a non-production instance | IAM DB auth, account limited to `sbx_%`. |
| SQLite state store | DynamoDB (`event_id` PK, conditional writes) | Conditional put gives exactly-once *decision* semantics across workers. |
| SQLite warehouse | Redshift / S3 + Iceberg | Consume `cdc.validated` + `cdc.repaired`, MERGE on PK. |
| JSON stdout logs | CloudWatch Logs (awslogs / FireLens) | Fields `event_id`, `repair_id`, `correlation_id` → Logs Insights queries. |
| Prometheus `/metrics` | CloudWatch agent (Prometheus scrape) or Amazon Managed Prometheus + Grafana | Metric names unchanged. |
| `.env` | Secrets Manager + SSM Parameter Store | ECS task secrets / External Secrets on EKS. |

## Configuration differences

| Setting | Local | AWS |
|---|---|---|
| `REDPANDA_BROKERS` | `redpanda:9092` | MSK bootstrap (IAM port 9098) |
| `SCHEMA_REGISTRY_URL` | `http://redpanda:8081` | SR endpoint (private) |
| `LLM_MODE` | `ollama` | `bedrock` (new factory branch) |
| `OLLAMA_*` | host Ollama | replaced by `BEDROCK_MODEL_ID`, `AWS_REGION` |
| `SANDBOX_DB_*` | `mysql-sandbox` | scratch Aurora endpoint, IAM auth |
| `STATE_DIR` | local volume | not used (DynamoDB table name instead) |
| topic partitions | 3 / 1 | sized to throughput; `cdc.dlq` ≥ number of workers |

## Security model in AWS

- **Trust boundaries are unchanged:** the CDC payload, LLM output, SQL and Python are all untrusted and
  validated by the same code.
- **IAM separation:**
  - The validator role can read `cdc.mutations` and write `cdc.validated`/`cdc.dlq`.
  - The worker role can read `cdc.dlq`, write `cdc.repaired`/`cdc.audit`, call `bedrock:InvokeModel`
    for one model ARN, write to the state table, and reach the sandbox DB only.
  - The worker has **no** access to the source RDS.
- **Network:**
  - Workers run in private subnets.
  - Bedrock is reached through a VPC endpoint (no internet egress).
  - The sandbox Lambda has no VPC attachment for code execution. The SQL sandbox is in an isolated
    subnet with a security group open only to the worker.
- **Data protection:**
  - Use Bedrock guardrails as an extra layer. They do not replace the deterministic checks.
  - Payload values sent to Bedrock are already truncated. Add field-level masking for PII columns before
    prompting. It's configurable per contract via an `x-pii` annotation (a future extension).
- **Audit:** `cdc.audit` is mirrored to S3 with Object Lock for tamper-evident retention.

## Deployment considerations

- **Ordering:** keep keys = primary keys so per-row ordering survives scaling. More `cdc.mutations`
  partitions means more validator parallelism.
- **Exactly-once:**
  - Today the guarantee is at-least-once delivery with idempotent decisions (state store) and
    idempotent downstream (dedupe on `event_id`).
  - In AWS, use the DynamoDB conditional write as the decision lock, or Kafka transactions for
    "publish + commit".
- **Model choice:** Bedrock offers larger code models. Start with a Claude or Llama code-capable model
  at temperature 0 and keep `CONFIDENCE_THRESHOLD=0.9`. The decision engine, not the model, gates
  execution.
- **Cost control:** the approved-repair cache, keyed by drift fingerprint, means one model call per
  drift *shape*. Move the cache to DynamoDB so all workers share it.
- **Migrations:** `AUTO_APPLY_SAFE_MIGRATIONS` should drive a change-request pipeline (e.g. a PR to the
  warehouse's dbt or Flyway repo), never a direct DDL from the worker.

## Vertex AI / GCP portability note

The original concept mentioned Vertex AI. The same design maps to GCP:

- Cloud SQL for MySQL → Debezium Server or Datastream → Pub/Sub or Confluent Cloud on GCP.
- GKE or Cloud Run workers.
- Vertex AI (Gemini or Codey) through a `VertexLLMClient(LLMClient)`.
- Cloud Run jobs or gVisor for the sandbox, Firestore or Spanner for state, BigQuery downstream,
  Cloud Logging and Monitoring.

The differences that matter:

- **Transport:** Pub/Sub is not Kafka. You would either use Confluent Cloud or Managed Kafka for GCP to
  keep `MessageConsumer` semantics (offsets, seek), or write a Pub/Sub adapter where "seek back" becomes
  nack plus redelivery.
- **Structured output:** Vertex exposes it via `response_schema`. The strict Pydantic validation stays
  regardless.

AWS (RDS + MSK + Bedrock) remains the primary target because MSK keeps the Kafka contract identical to
the local Redpanda stack.

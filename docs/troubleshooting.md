# Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `make start` fails: `docker.io/debezium/example-mysql: not found` | Debezium no longer publishes to Docker Hub | The compose file uses `quay.io/debezium/example-mysql:latest` (same image, official registry). Override with `MYSQL_SOURCE_IMAGE`. |
| MySQL never healthy / `Access denied for user 'root'` | Inline `# comment` after a value in `.env`. Compose keeps the trailing spaces, so the root password becomes `"debezium   "`. | Keep comments on their own lines (see `.env.example`), then `make reset && make start`. |
| `remote root login denied` from a helper container | The example image creates `root@localhost` only, by design | Users are created by the image's own init hook (`docker/source-db/zz-cdc-users.sh`). Don't expose root remotely. |
| `cdc.mutations` stays empty | Connector not registered or failed | `docker logs cdc-connector-init`; `make status`; `curl localhost:8083/connectors/inventory-connector/status`. Re-run `make connector`. |
| Connector task FAILED with `${env:MYSQL_PASSWORD}` unresolved | Connect started without the config provider env | `docker compose up -d --force-recreate debezium connector-init` |
| topic-init hangs at "Waiting for Redpanda" | `rpk cluster health` talks to the admin API on localhost | The script uses `rpk cluster info -X brokers=…` (Kafka API). |
| Every event goes to the DLQ as `UNKNOWN_SCHEMA` | Table not in `sources.yaml`, or a different `topic_prefix` | Add the table and contract; the contract lookup key is `source.database.table`. |
| Audit shows `DEFERRED`, nothing repaired | Ollama unreachable from the container or model missing | `make ollama-check`; `docker exec cdc-ai-worker python -c "import urllib.request,os;print(urllib.request.urlopen(os.environ['OLLAMA_BASE_URL']+'/api/tags').status)"`. Ollama must listen on the host (default `127.0.0.1:11434` works with Docker Desktop). |
| `ollama_check`: "Installed models: (none)" although `ollama list` shows the model | Another container (e.g. a different project's `ollama/ollama`) publishes `:11434`; Docker Desktop binds it on IPv6 `*:11434`, and `localhost` resolves to `::1` first | Use `OLLAMA_BASE_URL=http://127.0.0.1:11434` (the default now). Check with `lsof -nP -iTCP:11434 -sTCP:LISTEN`. Containers still reach native Ollama via `host.docker.internal`. |
| First repair takes 30–90 s | Model loading into memory | Expected. `keep_alive=15m` keeps it warm. |
| Repair `REJECTED` / `MANUAL_REVIEW` | The model's proposal failed an independent check | Read the report: `docker exec cdc-ai-worker cat /app/state/reports/<event_id>.txt` or `make trace EVENT=<id>`. The `Reasons` list names the failed check. |
| Demo step "AI repair" shows `source=cache` error | The approved-repair cache already holds this drift shape | The demo clears the cache by default; run `docker exec cdc-ai-worker python -m src.tools.state clear-cache`. |
| `add-column demo column is still present` | A previous add-column scenario was not reset | `./scripts/trigger_schema_drift.sh reset --allow-drop-demo-column` (explicit destructive step). |
| Port already allocated | Another local MySQL/Kafka | `make setup` lists port owners; stop the other service or change the host port mapping. |
| Out of memory / swapping | 7B model + stack on 8 GB Docker VM | Close other apps; Redpanda Console is opt-in; use `qwen2.5-coder:3b` for development. |
| Want a clean slate | — | `make reset` (deletes this stack's volumes and `state/`), then `make start`. |

## Where to look

```bash
docker compose logs -f validator ai-worker downstream    # JSON logs
make metrics                                              # cdc_* counters
make peek TOPIC=cdc.audit LAST=3                          # recent decisions with reports
docker exec cdc-ai-worker python -m src.tools.state stats
```

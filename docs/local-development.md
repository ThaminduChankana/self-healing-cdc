# Local development

## Memory budget (16 GB MacBook)

| Component | Approx. RSS |
|---|---|
| Ollama + qwen2.5-coder:7b (Q4_K_M, native, Metal) | 5–6 GB while loaded (`keep_alive` 15 min) |
| Kafka Connect (Debezium), `-Xmx768m` | ~0.9 GB |
| Redpanda (`--memory 768M --smp 1`) | ~0.8 GB |
| Source MySQL + sandbox MySQL (perf-schema off, 32 MB buffer pool) | ~0.6 GB |
| validator + ai-worker + downstream | ~0.25 GB |

The tuning choices behind these numbers:

- AI calls are serial.
- Prompts are compact: values are truncated to 120 characters and capped at 50 fields.
- Consumers prefetch at most 4 MB.
- Nothing loads a whole topic into memory.
- Redpanda Console and Ollama-in-Docker are opt-in profiles.

## Running pieces on the host

The containers are the default. For debugging you can stop one and run it from the venv with the host
settings in `.env`:

```bash
docker compose stop ai-worker
make repair-worker          # venv/bin/python -m src.ai_repair.worker (OLLAMA_BASE_URL=http://localhost:11434)
LLM_MODE=mock make repair-worker     # deterministic, no model
```

Inside Docker the services use `redpanda:9092` and `mysql-sandbox:3306`. On the host they use
`localhost:19092` and `127.0.0.1:3307`. `src/common/config.py` is the only place configuration is read.

## Useful commands

```bash
make status                       # health, connector, topic counts
make logs SVC=ai-worker           # JSON logs
make connector-config             # rendered Debezium config (secrets as ${env:...})
make inspect                      # live schema via the read-only user
make load-data ROWS=10            # more Hugging Face rows
python -m src.tools.inspect_source --generate products   # contract from the live table
docker exec cdc-ai-worker python -m src.tools.state stats  # idempotency ledger / repair cache
docker exec cdc-ai-worker python -m src.tools.state clear-cache
```

## Hugging Face data

`src/tools/hf_loader.py` reads rows through the public datasets-server API
(`https://datasets-server.huggingface.co/rows`) in pages of 100. It needs no token and no `datasets`
dependency.

- Text columns are cached in `data/hf/` (git-ignored), so later runs work offline (`--offline`).
- Column mapping and transforms are configured under `seed_data` in `sources.yaml`:
  `Product Name → name`, `About Product → description`, `Shipping Weight → weight` in kg, and a
  deterministic stock level from `Uniq Id`.
- Rows are inserted with parameterized statements by `app_writer`. Products whose name already exists
  are skipped, so re-runs are idempotent. The loader never drops or alters anything.

## Tests

| Suite | Command | Needs |
|---|---|---|
| unit + security | `make test` | nothing (mock LLM, fake Kafka) |
| integration (non-LLM) | `venv/bin/pytest tests/integration -m "not llm"` | `make start` |
| integration (real model) | `venv/bin/pytest tests/integration -m llm` | `make start` + Ollama + model |

## Using a different model

Set `OLLAMA_MODEL` in `.env` (for example `qwen2.5-coder:14b` if you have the RAM, or
`qwen2.5-coder:1.5b` for a quick smoke test; it fails more checks, and the system rejects those
proposals safely), then run `docker compose up -d ai-worker`. The model is never pulled automatically;
use `ollama pull <model>` or `make ollama-pull`.

## Ollama inside Docker (optional)

```bash
OLLAMA_DOCKER_BASE_URL=http://ollama:11434 docker compose --profile ollama up -d
docker exec cdc-ollama ollama pull qwen2.5-coder:7b
```

This runs on the CPU only on macOS and is several times slower than native Ollama.

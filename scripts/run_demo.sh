#!/usr/bin/env bash
# run_demo.sh — full end-to-end demonstration (real MySQL, Debezium, Redpanda, Ollama).
set -euo pipefail
source "$(dirname "$0")/_common.sh"
step=0
step() { step=$((step + 1)); echo -e "\n${BOLD}${BLUE}══ Step ${step}: $1 ══════════════════════════════════════${NC}"; }

echo -e "${BOLD}${GREEN}Self-Healing CDC Pipeline — live demo${NC}"
echo "Model: ${OLLAMA_MODEL:-qwen2.5-coder:7b} via ${OLLAMA_BASE_URL:-http://127.0.0.1:11434} (LLM_MODE=${LLM_MODE:-ollama})"

step "Start Docker Compose"
docker compose up -d --build --wait --wait-timeout 300

step "Wait for health checks"
for c in cdc-mysql cdc-redpanda cdc-debezium cdc-mysql-sandbox cdc-validator cdc-ai-worker cdc-downstream; do
    wait_healthy "$c" 240
done
for job in cdc-topic-init cdc-connector-init; do
    code=$(docker wait "$job")
    [[ "$code" == "0" ]] && echo -e "  ${GREEN}✓${NC} $job completed" || { echo -e "  ${RED}✗${NC} $job exit=$code"; docker logs --tail 30 "$job"; exit 1; }
done

step "Inspect MySQL (official debezium example database)"
bash scripts/inspect_mysql.sh

step "Register Debezium connector(s)"
bash scripts/register_connector.sh

step "Create topics"
RPK_BROKERS=redpanda:9092 bash scripts/create_topics.sh

step "Verify Redpanda"
docker exec cdc-redpanda rpk cluster info -X brokers=redpanda:9092 | head -8
"$PY" -m src.tools.topics counts
curl -sf "${SCHEMA_REGISTRY_URL:-http://localhost:18081}/subjects" && echo "  ← contracts in Schema Registry"

step "Verify Ollama connectivity"
if [[ "${LLM_MODE:-ollama}" == "ollama" ]]; then
    "$PY" -m src.tools.ollama_check || { echo -e "${RED}Ollama/model not ready — the demo needs the real model (see above).${NC}"; exit 1; }
    docker exec cdc-ai-worker python -c "import os,urllib.request; urllib.request.urlopen(os.environ['OLLAMA_BASE_URL']+'/api/tags', timeout=5); print('  ✓ ai-worker container reaches', os.environ['OLLAMA_BASE_URL'])"
fi

step "Verify the configured model can be invoked"
[[ "${LLM_MODE:-ollama}" == "ollama" ]] && "$PY" -m src.tools.ollama_check --invoke

"$PY" -m src.tools.demo "$@"

echo -e "\nTopic counts:"
"$PY" -m src.tools.topics counts
echo -e "\nFollow one event: make trace EVENT=<event_id>   |   Metrics: curl localhost:8002/metrics | grep cdc_"

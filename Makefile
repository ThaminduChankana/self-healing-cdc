.PHONY: help setup start stop restart status logs topics connector connector-config inspect ollama-check \
        ollama-pull test unit-test security-test integration-test test-all drift drift-rename drift-add \
        drift-widen drift-reset load-data test-event demo peek trace metrics validator repair-worker \
        downstream console reset clean

SHELL := /bin/bash
VENV  := venv
PY    := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest

-include .env
export

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Check prerequisites, create venv, install packages, create .env
	@bash scripts/bootstrap.sh

start: ## Build and start the stack; waits for health checks and init jobs
	docker compose up -d --build --wait --wait-timeout 300
	@for job in cdc-topic-init cdc-connector-init; do \
	  code=$$(docker wait $$job); \
	  if [ "$$code" != "0" ]; then echo "✗ $$job failed (exit $$code)"; docker logs --tail 30 $$job; exit 1; fi; \
	  echo "✓ $$job completed"; done
	@$(MAKE) --no-print-directory status

stop: ## Stop the stack (keeps volumes)
	docker compose --profile console --profile ollama stop

restart: ## Restart the stack
	@$(MAKE) --no-print-directory stop
	@$(MAKE) --no-print-directory start

status: ## Show container health, init jobs, connector state and topic counts
	@docker compose ps -a --format "table {{.Name}}\t{{.State}}\t{{.Status}}"
	@echo ""
	@$(PY) -m src.tools.connector --status 2>/dev/null | grep -E '"(name|state)"' | sed 's/^/  /' || true
	@$(PY) -m src.tools.topics counts 2>/dev/null || true

logs: ## Follow logs (SVC=validator|ai-worker|downstream|debezium|...)
	docker compose logs -f --tail=100 $(SVC)

topics: ## Create the pipeline topics (idempotent)
	@RPK_BROKERS=redpanda:9092 bash scripts/create_topics.sh

connector: ## Register/update the Debezium connector(s) for every source
	@bash scripts/register_connector.sh

connector-config: ## Print the rendered connector config(s) (no secrets)
	@$(PY) -m src.tools.connector --print

inspect: ## Inspect the source database schema (read-only user)
	@bash scripts/inspect_mysql.sh

ollama-check: ## Check Ollama + configured model and invoke it once
	@$(PY) -m src.tools.ollama_check --invoke

ollama-pull: ## Explicitly download the configured model (≈4.7 GB for 7B)
	@echo "Pulling $(OLLAMA_MODEL) via $(OLLAMA_BASE_URL) ..."
	@curl -sf -X POST $(OLLAMA_BASE_URL)/api/pull -d '{"model":"$(OLLAMA_MODEL)","stream":false}' && echo " done"

test: ## Unit + security tests (no Docker or Ollama needed)
	$(PYTEST) tests/unit tests/security -q

unit-test: ## Unit tests only
	$(PYTEST) tests/unit -q

security-test: ## Security tests only
	$(PYTEST) tests/security -q

integration-test: ## Integration tests against the running stack (make start first)
	$(PYTEST) tests/integration -v -m integration --timeout=1200

test-all: test integration-test ## Everything

drift: ## Trigger drift: make drift ACTION=rename|add-column|widen-type|reset|status
	@bash scripts/trigger_schema_drift.sh $(or $(ACTION),status) $(ARGS)

drift-rename: ; @bash scripts/trigger_schema_drift.sh rename
drift-add: ; @bash scripts/trigger_schema_drift.sh add-column
drift-widen: ; @bash scripts/trigger_schema_drift.sh widen-type
drift-reset: ; @bash scripts/trigger_schema_drift.sh reset

load-data: ## Load real products from Hugging Face (ROWS=25)
	@$(PY) -m src.tools.hf_loader $(if $(ROWS),--rows $(ROWS),)

test-event: ## Publish test traffic: make test-event KIND=valid|malformed|injection
	@bash scripts/publish_test_event.sh $(or $(KIND),valid)

demo: ## Run the end-to-end self-healing demo (real Ollama)
	@bash scripts/run_demo.sh

peek: ## Show the latest message of each pipeline topic (TOPIC=... LAST=n for one)
	@if [ -n "$(TOPIC)" ]; then $(PY) -m src.tools.topics peek $(TOPIC) --last $(or $(LAST),1); \
	else for t in $(MUTATIONS_TOPIC) $(VALIDATED_TOPIC) $(DLQ_TOPIC) $(REPAIRED_TOPIC) $(AUDIT_TOPIC); do \
	  echo "── $$t"; $(PY) -m src.tools.topics peek $$t --last 1 | head -40; done; fi

trace: ## Follow one event through every topic: make trace EVENT=<event_id>
	@for t in $(DLQ_TOPIC) $(REPAIRED_TOPIC) $(AUDIT_TOPIC) $(VALIDATED_TOPIC); do \
	  echo "── $$t"; $(PY) -m src.tools.topics find $$t --event-id $(EVENT) | head -80; done
	@echo "── container logs"; docker compose logs --no-log-prefix validator ai-worker downstream 2>/dev/null | grep -F '$(EVENT)' | tail -20

metrics: ## Print pipeline metrics from all services
	@for p in 8001 8002 8003; do curl -sf localhost:$$p/metrics | grep -E '^cdc_' ; done

validator: ## Run the validator on the host (instead of the container)
	$(PY) -m src.validator.consumer

repair-worker: ## Run the AI worker on the host (instead of the container)
	$(PY) -m src.ai_repair.worker

downstream: ## Run the downstream consumer on the host
	$(PY) -m src.downstream.consumer

console: ## Start Redpanda Console on http://localhost:8080
	docker compose --profile console up -d redpanda-console

reset: ## Delete this stack's containers, volumes and local state
	@bash scripts/reset_environment.sh

clean: reset ## reset + remove venv and caches
	rm -rf $(VENV) .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

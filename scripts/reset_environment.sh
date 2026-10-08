#!/usr/bin/env bash
# reset_environment.sh — remove this stack's containers, volumes and local state.
# Only touches resources of this compose project.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")"
echo "Stopping stack and deleting its volumes (source DB, Redpanda, sandbox, worker state)..."
docker compose --profile console --profile ollama down -v --remove-orphans
rm -rf state && mkdir -p state
echo "✓ Reset complete. Start again with: make start"

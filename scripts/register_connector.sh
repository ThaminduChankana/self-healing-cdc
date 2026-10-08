#!/usr/bin/env bash
# register_connector.sh — create/update one Debezium connector per source in config/sources.yaml.
set -euo pipefail
source "$(dirname "$0")/_common.sh"
# Connect resolves ${env:MYSQL_PASSWORD} itself; the host only needs to know where Connect is.
export DEBEZIUM_MYSQL_HOST="${DEBEZIUM_MYSQL_HOST:-mysql}"
"$PY" -m src.tools.connector --register --url "${DEBEZIUM_CONNECT_URL:-http://localhost:8083}"

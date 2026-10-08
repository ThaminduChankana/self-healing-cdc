#!/usr/bin/env bash
# inspect_mysql.sh — discover the real schema of the source DB (read-only user).
set -euo pipefail
source "$(dirname "$0")/_common.sh"
echo -e "${BOLD}Source database: ${MYSQL_DATABASE:-inventory} @ ${MYSQL_HOST:-127.0.0.1}:${MYSQL_PORT:-3306} (user ${MYSQL_READER_USER:-cdc_reader}, read-only)${NC}"
wait_healthy cdc-mysql 120 >/dev/null
for i in $(seq 1 30); do
    "$PY" -c "from src.common.database import source_reader
with source_reader() as c: c.query('SELECT 1')" 2>/dev/null && break
    [[ $i -eq 30 ]] && { echo "reader user not available — is source-db-init complete?"; exit 1; }
    sleep 2
done
"$PY" -m src.tools.inspect_source "$@"
echo ""
echo "Captured tables / contracts (config/sources.yaml):"
"$PY" - <<'PY'
from src.common.sources import ContractRegistry
for c in ContractRegistry().all():
    print(f"  {c.key:<40} pk={c.primary_key}  contract={c.path.name}  fields={list(c.fields)}")
PY

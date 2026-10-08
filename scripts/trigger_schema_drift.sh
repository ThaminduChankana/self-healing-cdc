#!/usr/bin/env bash
# trigger_schema_drift.sh — real, controlled schema drift on the source database.
#   ./scripts/trigger_schema_drift.sh rename | add-column | widen-type | reset | status  [--dry-run]
# Column/table names come from config/sources.yaml and are verified against
# information_schema first; every SQL statement is printed before it runs.
set -euo pipefail
source "$(dirname "$0")/_common.sh"
if [[ $# -lt 1 ]]; then
    sed -n '2,6p' "$0"; exit 1
fi
"$PY" -m src.tools.drift "$@"

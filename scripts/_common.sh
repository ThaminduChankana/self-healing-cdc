# Shared helpers for scripts/*.sh (sourced, not executed).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
if [[ -f .env ]]; then set -a; source .env; set +a; fi
PY="${PROJECT_DIR}/venv/bin/python"
[[ -x "$PY" ]] || { echo "venv missing — run: make setup" >&2; exit 1; }
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

wait_healthy() {  # wait_healthy <container> <timeout-seconds>
    local c=$1 t=${2:-180} i status
    for ((i = 0; i < t; i += 3)); do
        status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$c" 2>/dev/null || echo missing)
        [[ "$status" == "healthy" ]] && { echo -e "  ${GREEN}✓${NC} $c healthy"; return 0; }
        sleep 3
    done
    echo -e "  ${RED}✗${NC} $c not healthy after ${t}s (status: $status)"; return 1
}

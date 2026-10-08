#!/usr/bin/env bash
# bootstrap.sh — verify prerequisites and prepare the Python environment.
# Never installs system software and never downloads the LLM.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
errors=0
pass() { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; errors=$((errors + 1)); }
hdr()  { echo -e "\n${BLUE}═══ $1 ═══${NC}"; }

hdr "System"
if [[ "$(uname)" == "Darwin" ]]; then pass "macOS $(sw_vers -productVersion 2>/dev/null) ($(uname -m))"; else warn "Not macOS — supported, but untested"; fi
mem_gb=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 ))
[[ $mem_gb -ge 16 ]] && pass "RAM: ${mem_gb} GB" || warn "RAM: ${mem_gb} GB (16 GB recommended for the 7B model + stack)"

hdr "Docker"
command -v docker >/dev/null && pass "$(docker --version)" || fail "Docker not found — install Docker Desktop"
docker compose version >/dev/null 2>&1 && pass "$(docker compose version | head -1)" || fail "Docker Compose v2 not found"
docker info >/dev/null 2>&1 && pass "Docker daemon running ($(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f GB VM memory", $1/1073741824}'))" \
    || fail "Docker daemon not running — start Docker Desktop"

hdr "Python"
PYBIN=""
for cand in python3.13 python3.12 python3.11 python3; do
    if command -v "$cand" >/dev/null && "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
        PYBIN=$(command -v "$cand"); break
    fi
done
[[ -n "$PYBIN" ]] && pass "Python $("$PYBIN" --version | cut -d' ' -f2) ($PYBIN)" || fail "Python 3.11+ not found"
if [[ -n "$PYBIN" ]]; then
    if [[ ! -x venv/bin/python ]]; then
        "$PYBIN" -m venv venv && pass "Created virtual environment venv/"
    else
        pass "Virtual environment venv/ ($(venv/bin/python --version))"
    fi
    if venv/bin/pip install -q -r requirements-dev.txt; then pass "Python packages installed (requirements-dev.txt)"; else fail "pip install failed"; fi
fi

hdr "Configuration"
[[ -f .env ]] && pass ".env present" || { cp .env.example .env && pass "Created .env from .env.example"; }
set -a; source .env; set +a
mkdir -p state data && pass "state/ and data/ directories ready"

hdr "Ollama (local LLM)"
OLLAMA_BIN=$(command -v ollama || true)
for p in /Applications/Ollama.app/Contents/Resources/ollama "$PROJECT_DIR/Ollama.app/Contents/Resources/ollama"; do
    [[ -z "$OLLAMA_BIN" && -x "$p" ]] && OLLAMA_BIN="$p"
done
[[ -n "$OLLAMA_BIN" ]] && pass "Ollama CLI: $OLLAMA_BIN" || warn "Ollama CLI not found (install: https://ollama.com/download)"
MODEL="${OLLAMA_MODEL:-qwen2.5-coder:7b}"
if tags=$(curl -sf "${OLLAMA_BASE_URL:-http://127.0.0.1:11434}/api/tags"); then
    pass "Ollama API reachable at ${OLLAMA_BASE_URL:-http://127.0.0.1:11434}"
    if echo "$tags" | grep -q "\"name\":\"${MODEL}\""; then
        pass "Model ${MODEL} installed"
    else
        warn "Model ${MODEL} is NOT installed. Pull it yourself (≈4.7 GB):"
        echo -e "       ${YELLOW}ollama pull ${MODEL}${NC}   (or: make ollama-pull)"
    fi
else
    warn "Ollama API not reachable — start the Ollama app or run: ollama serve"
fi

hdr "Ports"
for spec in 3306:MySQL 3307:MySQL-sandbox 19092:Redpanda 18081:SchemaRegistry 8083:KafkaConnect 8001:validator 8002:ai-worker 8003:downstream; do
    port=${spec%%:*}; name=${spec#*:}
    owner=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR==2{print $1}')
    if [[ -z "$owner" ]]; then pass "$port ($name) free"
    elif [[ "$owner" == com.docke* || "$owner" == docker* ]]; then pass "$port ($name) used by Docker (this stack?)"
    else warn "$port ($name) in use by $owner"; fi
done

hdr "Summary"
if [[ $errors -gt 0 ]]; then echo -e "${RED}${errors} blocking problem(s). Fix them and re-run make setup.${NC}"; exit 1; fi
echo -e "${GREEN}Ready.${NC}  Next: make start  →  make demo"

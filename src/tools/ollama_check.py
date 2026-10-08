"""Check Ollama connectivity and model availability. Never downloads anything.

    python -m src.tools.ollama_check            # reachability + model presence
    python -m src.tools.ollama_check --invoke   # plus one tiny structured-output call

Exit codes: 0 ok, 1 Ollama unreachable, 2 model missing, 3 invocation failed.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import requests

from src.common.config import get_settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoke", action="store_true")
    args = parser.parse_args()
    s = get_settings()
    url, model = s.ollama_base_url, s.ollama_model
    try:
        tags = requests.get(f"{url}/api/tags", timeout=5).json()
    except (requests.RequestException, ValueError):
        print(f"✗ Ollama is not reachable at {url}")
        print("  Start it with:  ollama serve   (or open the Ollama app)")
        sys.exit(1)
    names = sorted(m.get("name", "") for m in tags.get("models", []))
    print(f"✓ Ollama reachable at {url}")
    print("  Installed models: " + (", ".join(names) or "(none)"))
    wanted = model if ":" in model else f"{model}:latest"
    if wanted not in names:
        print(f"✗ Configured model '{model}' is not installed.")
        print(f"  Pull it explicitly (≈4.7 GB for qwen2.5-coder:7b):  ollama pull {model}")
        print("  or: make ollama-pull")
        sys.exit(2)
    print(f"✓ Configured model '{model}' is installed")
    if not args.invoke:
        return
    t0 = time.monotonic()
    body = {
        "model": model, "stream": False, "keep_alive": "15m",
        "messages": [{"role": "user", "content": 'Return {"ok": true} as JSON.'}],
        "format": {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]},
        "options": {"temperature": 0, "num_predict": 20},
    }
    try:
        resp = requests.post(f"{url}/api/chat", json=body, timeout=s.ollama_timeout_seconds)
        content = json.loads(resp.json()["message"]["content"])
    except (requests.RequestException, ValueError, KeyError) as exc:
        print(f"✗ Model invocation failed: {exc}")
        sys.exit(3)
    print(f"✓ Model invocation returned {content} in {time.monotonic() - t0:.1f}s (includes model load)")


if __name__ == "__main__":
    main()

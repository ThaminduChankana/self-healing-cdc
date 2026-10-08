"""Ollama implementation of the LLM interface (local Qwen coding model)."""

from __future__ import annotations

from typing import Any

import requests

from src.ai_repair.llm_client import LLMClient, LLMUnavailableError
from src.ai_repair.mock_client import MockLLMClient
from src.common.config import LLMMode, get_settings
from src.common.logging import ComponentLogger

log = ComponentLogger("ollama_client")


class OllamaLLMClient(LLMClient):
    def __init__(self, base_url: str | None = None, model: str | None = None, timeout: float | None = None):
        super().__init__()
        s = get_settings()
        self._base_url = (base_url or s.ollama_base_url).rstrip("/")
        self.model = model or s.ollama_model
        self._timeout = timeout or s.ollama_timeout_seconds
        self._num_ctx = s.ollama_num_ctx

    def _complete(self, system_prompt: str, user_prompt: str, response_schema: dict[str, Any]) -> str:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "format": response_schema,  # structured output: grammar-constrained JSON
            "stream": False,
            "keep_alive": "15m",
            "options": {"temperature": 0, "seed": 42, "num_ctx": self._num_ctx, "num_predict": 1200},
        }
        try:
            resp = requests.post(f"{self._base_url}/api/chat", json=body, timeout=self._timeout)
        except requests.exceptions.Timeout as exc:
            raise LLMUnavailableError(f"Ollama timed out after {self._timeout}s") from exc
        except requests.exceptions.ConnectionError as exc:
            raise LLMUnavailableError(f"cannot connect to Ollama at {self._base_url}") from exc
        if resp.status_code == 404:
            raise LLMUnavailableError(f"model '{self.model}' not found — run: ollama pull {self.model}")
        if resp.status_code >= 500:
            raise LLMUnavailableError(f"Ollama HTTP {resp.status_code}: {resp.text[:200]}")
        if not resp.ok:
            raise LLMUnavailableError(f"Ollama rejected the request: HTTP {resp.status_code} {resp.text[:200]}")
        data = resp.json()
        log.debug("Ollama completion", model=self.model,
                  prompt_tokens=data.get("prompt_eval_count"), output_tokens=data.get("eval_count"),
                  duration_s=round((data.get("total_duration") or 0) / 1e9, 2))
        return (data.get("message") or {}).get("content", "")

    def is_available(self) -> bool:
        try:
            return requests.get(f"{self._base_url}/api/tags", timeout=5).ok
        except requests.RequestException:
            return False

    def model_available(self) -> bool:
        try:
            resp = requests.get(f"{self._base_url}/api/tags", timeout=5)
            if not resp.ok:
                return False
            names = {m.get("name", "") for m in resp.json().get("models", [])}
        except requests.RequestException:
            return False
        wanted = self.model if ":" in self.model else f"{self.model}:latest"
        return wanted in names or self.model in names


def create_llm_client() -> LLMClient:
    if get_settings().llm_mode == LLMMode.MOCK:
        log.info("Using deterministic MockLLMClient (LLM_MODE=mock)")
        return MockLLMClient()
    log.info("Using OllamaLLMClient", model=get_settings().ollama_model)
    return OllamaLLMClient()

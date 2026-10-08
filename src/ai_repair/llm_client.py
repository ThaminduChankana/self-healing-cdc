"""Cloud-neutral LLM interface.

The repair logic only calls `LLMClient.generate_repair(RepairRequest) ->
RepairResponse`. Providers implement `_complete()` (one chat completion with a
JSON-schema-constrained answer) plus availability checks. A future
`BedrockLLMClient` only needs those three methods; prompt construction and
strict response validation are shared and provider-independent.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from src.ai_repair.prompt_builder import RESPONSE_JSON_SCHEMA, PromptBuilder
from src.ai_repair.response_parser import LLMResponseError, ResponseParser
from src.common.models import RepairRequest, RepairResponse


class LLMUnavailableError(Exception):
    """The provider cannot be reached / model not loaded. Transient: defer the event."""


class LLMClient(ABC):
    model: str = "unknown"

    def __init__(self) -> None:
        self._prompts = PromptBuilder()
        self._parser = ResponseParser()
        self.last_raw_response: str = ""

    def generate_repair(self, request: RepairRequest) -> RepairResponse:
        """Raises LLMUnavailableError (transient) or LLMResponseError (bad output)."""
        system, user = self._prompts.build(request)
        raw = self._complete(system, user, RESPONSE_JSON_SCHEMA)
        self.last_raw_response = raw[:4000]
        return self._parser.parse(raw)

    @abstractmethod
    def _complete(self, system_prompt: str, user_prompt: str, response_schema: dict[str, Any]) -> str: ...

    @abstractmethod
    def is_available(self) -> bool: ...

    @abstractmethod
    def model_available(self) -> bool: ...


__all__ = ["LLMClient", "LLMUnavailableError", "LLMResponseError"]

"""Strict parsing of model output into a RepairResponse.

The model's output is untrusted. It must be a single JSON object that
validates against the RepairResponse model exactly (no extra keys, valid
enums, confidence within [0, 1]). Nothing is coerced or defaulted: a
non-conforming response is rejected and the reason is fed back to the model.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from src.common.models import RepairResponse


class LLMResponseError(Exception):
    """The model returned something that is not a valid repair proposal."""


def extract_json_object(text: str) -> dict[str, Any]:
    """Return the first JSON object in `text` (tolerates code fences / leading prose)."""
    if not isinstance(text, str) or not text.strip():
        raise LLMResponseError("empty response")
    decoder = json.JSONDecoder()
    idx = text.find("{")
    while idx != -1:
        try:
            obj, _ = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            idx = text.find("{", idx + 1)
            continue
        if isinstance(obj, dict):
            return obj
        idx = text.find("{", idx + 1)
    raise LLMResponseError(f"no JSON object found in response: {text[:200]!r}")


class ResponseParser:
    def parse(self, raw: str) -> RepairResponse:
        data = extract_json_object(raw)
        try:
            return RepairResponse.model_validate(data)
        except ValidationError as exc:
            problems = "; ".join(
                f"{'.'.join(str(p) for p in e['loc']) or 'response'}: {e['msg']}" for e in exc.errors()[:6]
            )
            raise LLMResponseError(f"response does not match the required schema: {problems}") from exc

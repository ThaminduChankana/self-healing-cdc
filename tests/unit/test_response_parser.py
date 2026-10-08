import json

import pytest

from src.ai_repair.response_parser import LLMResponseError, ResponseParser, extract_json_object
from src.common.models import DriftType, RiskLevel

VALID = {
    "classification": "RENAMED_COLUMN",
    "confidence": 0.97,
    "reasoning_summary": "stock_quantity carries the canonical quantity.",
    "migration_sql": None,
    "transformation_code": "def transform(event):\n    return {'product_id': event['product_id'], 'quantity': event['stock_quantity']}",
    "test_cases": [{"input": {"product_id": 1, "stock_quantity": 2}, "expected_output": {"product_id": 1, "quantity": 2}}],
    "risk_level": "LOW",
}


def test_valid_response():
    r = ResponseParser().parse(json.dumps(VALID))
    assert r.classification == DriftType.RENAMED_COLUMN
    assert r.risk_level == RiskLevel.LOW
    assert r.confidence == 0.97 and len(r.test_cases) == 1


def test_code_fences_and_braces_inside_strings():
    resp = dict(VALID, transformation_code="def transform(event):\n    m = {'a': '}{'}\n    return dict(event)")
    raw = "Here you go:\n```json\n" + json.dumps(resp) + "\n```\nthanks"
    assert "'}{'" in ResponseParser().parse(raw).transformation_code


@pytest.mark.parametrize("patch, err", [
    ({"confidence": 1.5}, "confidence"),
    ({"confidence": -0.1}, "confidence"),
    ({"classification": "BOGUS"}, "classification"),
    ({"classification": "NONE"}, "classification"),
    ({"risk_level": "EXTREME"}, "risk_level"),
    ({"transformation_code": "print('hello world')"}, "transformation_code"),
    ({"unexpected_key": 1}, "unexpected_key"),
    ({"reasoning_summary": ""}, "reasoning_summary"),
])
def test_non_conforming_responses_are_rejected_not_coerced(patch, err):
    with pytest.raises(LLMResponseError, match=err):
        ResponseParser().parse(json.dumps({**VALID, **patch}))


def test_missing_field_rejected():
    data = dict(VALID)
    del data["risk_level"]
    with pytest.raises(LLMResponseError, match="risk_level"):
        ResponseParser().parse(json.dumps(data))


@pytest.mark.parametrize("raw", ["", "no json here", "[1, 2, 3]", "{broken"])
def test_garbage_rejected(raw):
    with pytest.raises(LLMResponseError):
        extract_json_object(raw)


def test_blank_sql_normalised_to_none():
    assert ResponseParser().parse(json.dumps(dict(VALID, migration_sql="  "))).migration_sql is None


@pytest.mark.parametrize("text", ["null", "NULL", "None", "n/a", " null; "])
def test_textual_null_migration_means_no_migration(text):
    # observed live: qwen2.5-coder:7b sometimes returns "migration_sql": "null"
    assert ResponseParser().parse(json.dumps(dict(VALID, migration_sql=text))).migration_sql is None

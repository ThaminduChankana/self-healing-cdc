from src.sandbox.executor import SandboxExecutor
from src.sandbox.validator import OutputValidator


def test_successful_transform_runs_in_subprocess():
    code = "def transform(event):\n    out = dict(event)\n    out['quantity'] = out.pop('stock_quantity')\n    return out"
    r = SandboxExecutor().execute(code, [{"product_id": 1, "stock_quantity": 5}])
    assert r.success and r.status == "PASSED"
    assert r.output == {"product_id": 1, "quantity": 5}


def test_multiple_inputs_and_model_tests():
    code = "def transform(event):\n    return {'x': event['x'] * 2}"
    r = SandboxExecutor().execute(code, [{"x": 1}, {"x": 2}],
                                  tests=[{"input": {"x": 3}, "expected_output": {"x": 6}},
                                         {"input": {"x": 3}, "expected_output": {"x": 7}}])
    assert [o["x"] for o in r.outputs] == [2, 4]
    assert [t["passed"] for t in r.test_results] == [True, False]


def test_syntax_error_is_rejected_before_execution():
    r = SandboxExecutor().execute("def transform(event) return event", [{}])
    assert not r.success and r.status == "UNSAFE_CODE" and "syntax" in r.error


def test_runtime_error():
    r = SandboxExecutor().execute("def transform(event):\n    return 1 / 0", [{}])
    assert r.status == "TRANSFORM_ERROR" and "ZeroDivisionError" in r.error


def test_non_dict_output():
    r = SandboxExecutor().execute("def transform(event):\n    return 'nope'", [{}])
    assert r.status == "INVALID_OUTPUT"


def test_infinite_loop_times_out():
    r = SandboxExecutor(timeout=1).execute("def transform(event):\n    while True:\n        pass", [{}])
    assert not r.success and r.status == "SANDBOX_TIMEOUT"


def test_memory_or_cpu_bomb_is_contained():
    r = SandboxExecutor(timeout=2).execute(
        "def transform(event):\n    x = []\n    while True:\n        x.append('a' * 10000)", [{}])
    assert not r.success


SCHEMA = {
    "type": "object",
    "properties": {"product_id": {"type": "integer"}, "quantity": {"type": "integer"}},
    "required": ["product_id", "quantity"],
    "additionalProperties": False,
}
RENAME = {"possible_renames": [{"missing_field": "quantity", "unexpected_field": "stock_quantity",
                                "confidence": 0.95, "ambiguous": False}]}


def _checks(output, source, drift=RENAME):
    return {c.name: c for c in OutputValidator().validate(output, source, SCHEMA, drift)}


def test_output_validation_passes_faithful_rename():
    c = _checks({"product_id": 1, "quantity": 5}, {"product_id": 1, "stock_quantity": 5})
    assert all(x.passed for x in c.values())


def test_output_validation_catches_unexpected_and_missing():
    c = _checks({"product_id": 1, "stock_quantity": 5}, {"product_id": 1, "stock_quantity": 5})
    assert not c["schema_validation"].passed
    assert not c["required_fields"].passed
    assert not c["unexpected_fields"].passed


def test_data_fidelity_catches_invented_values():
    c = _checks({"product_id": 1, "quantity": 0}, {"product_id": 1, "stock_quantity": 5})
    assert c["schema_validation"].passed
    assert not c["data_fidelity"].passed
    c2 = _checks({"product_id": 2, "quantity": 5}, {"product_id": 1, "stock_quantity": 5})
    assert not c2["data_fidelity"].passed

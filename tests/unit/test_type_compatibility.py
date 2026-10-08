import pytest

from src.validator.type_compatibility import TypeCompatibility


@pytest.fixture(scope="module")
def tc():
    return TypeCompatibility()


@pytest.mark.parametrize("expected, actual, verdict", [
    ("INT", "BIGINT", "compatible"),
    ("INT", "INT", "identical"),
    ("INTEGER", "INT", "identical"),
    ("VARCHAR(100)", "VARCHAR(255)", "compatible"),
    ("VARCHAR(255)", "VARCHAR(100)", "destructive"),
    ("INT", "VARCHAR(20)", "requires_transformation"),
    ("VARCHAR(20)", "INT", "risky"),
    ("DECIMAL(10,2)", "VARCHAR(30)", "risky"),
    ("BIGINT", "INT", "destructive"),
    ("FLOAT", "DOUBLE", "compatible"),
    ("INT", "INT UNSIGNED", "risky"),
    ("GEOMETRY", "POINT", "risky"),  # unknown pair -> default verdict
])
def test_sql_matrix(tc, expected, actual, verdict):
    assert tc.sql_verdict(expected, actual) == verdict


def test_connect_matrix(tc):
    assert tc.connect_verdict("int32", "int64") == "compatible"
    assert tc.connect_verdict("int64", "int32") == "destructive"
    assert tc.connect_verdict("int32", "int32") == "identical"


def test_matrix_is_configurable():
    custom = TypeCompatibility(data={"rules": [{"from": "INT", "to": "BIGINT", "verdict": "risky"}],
                                     "default_verdict": "destructive"})
    assert custom.sql_verdict("INT", "BIGINT") == "risky"
    assert custom.sql_verdict("INT", "TEXT") == "destructive"

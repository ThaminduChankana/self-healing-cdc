import pytest

from src.validator.debezium_parser import EnvelopeError, parse, stable_event_id
from tests.conftest import ON_HAND_COLS, PRODUCTS_COLS, envelope

ROW = {"id": 101, "name": "scooter", "description": "Small 2-wheel scooter", "weight": 3.14}


def test_insert_with_schema_extracts_row_and_column_types():
    ev = parse(envelope("products", ROW, cols=PRODUCTS_COLS), key={"id": 101}, correlation_id="cdc.mutations/0/5")
    assert ev.operation == "c"
    assert (ev.source_name, ev.source_database, ev.source_table) == ("cdc", "inventory", "products")
    assert ev.payload == ROW
    assert ev.key == {"id": 101}
    assert ev.correlation_id == "cdc.mutations/0/5"
    assert ev.column_schema["name"] == {"connect_type": "string", "logical_type": None,
                                        "optional": False, "sql_type": "VARCHAR(255)"}
    assert ev.column_schema["weight"]["optional"] is True
    assert ev.column_schema["id"]["sql_type"] == "INT"


def test_schemaless_envelope_is_supported():
    ev = parse(envelope("products", ROW, with_schema=False))
    assert ev.payload == ROW
    assert ev.column_schema == {}


def test_update_uses_after_image():
    before = dict(ROW, weight=2.0)
    ev = parse(envelope("products", ROW, before=before, op="u", cols=PRODUCTS_COLS))
    assert ev.before["weight"] == 2.0
    assert ev.payload["weight"] == 3.14


def test_delete_uses_before_image_and_its_schema():
    ev = parse(envelope("products", None, before=ROW, op="d", cols=PRODUCTS_COLS))
    assert ev.payload == ROW
    assert ev.column_schema["id"]["connect_type"] == "int32"


def test_snapshot_read_is_a_row_event():
    ev = parse(envelope("products_on_hand", {"product_id": 101, "quantity": 3}, op="r", cols=ON_HAND_COLS))
    assert ev.is_row_event and ev.operation == "r"


@pytest.mark.parametrize("value", [None, {"schema": {}, "payload": None}])
def test_tombstones_return_none(value):
    assert parse(value) is None


@pytest.mark.parametrize("bad, msg", [
    ({"random": "data"}, "missing 'op'"),
    ({"op": "c", "after": {"id": 1}}, "missing 'source'"),
    ({"op": "z", "source": {"table": "t"}}, "unsupported operation"),
    ({"op": "c", "source": {"table": "t"}, "after": None}, "without 'after'"),
    ({"op": "d", "source": {"table": "t"}, "before": None}, "delete without"),
    ({"op": "c", "source": {"table": "t"}, "after": [1, 2]}, "must be an object"),
    (["not", "an", "object"], "must be a JSON object"),
])
def test_malformed_envelopes_raise(bad, msg):
    with pytest.raises(EnvelopeError, match=msg):
        parse(bad)


def test_event_id_is_stable_and_distinguishes_snapshot_rows():
    src = {"name": "cdc", "db": "inventory", "table": "products", "file": "mysql-bin.000003", "pos": 157, "row": 0}
    a1 = stable_event_id(src, "r", {"id": 101}, 1)
    a2 = stable_event_id(src, "r", {"id": 101}, 999)  # ts differs, same coordinates -> same id
    b = stable_event_id(src, "r", {"id": 102}, 1)     # snapshot rows share file/pos -> key separates
    assert a1 == a2
    assert a1 != b
    assert a1.startswith("products-r-")


def test_event_id_differs_per_source_server():
    src = {"name": "cdc", "db": "inventory", "table": "products", "file": "f", "pos": 1, "row": 0}
    assert stable_event_id(src, "c", {"id": 1}, 0) != stable_event_id(dict(src, name="crm"), "c", {"id": 1}, 0)

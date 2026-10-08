from src.common.models import DriftType
from src.validator.debezium_parser import parse
from src.validator.drift_detector import DriftDetector
from tests.conftest import ON_HAND_COLS, PRODUCTS_COLS, envelope

PRODUCT = {"id": 101, "name": "scooter", "description": "Small 2-wheel scooter", "weight": 3.14}


def detect(contract, table, row, cols, op="c"):
    return DriftDetector().detect(parse(envelope(table, row, cols=cols, op=op)), contract)


def test_valid_event_has_no_drift(products_contract):
    r = detect(products_contract, "products", PRODUCT, PRODUCTS_COLS)
    assert r.drift_detected is False and r.drift_type == DriftType.NONE
    assert r.validation_errors == []


def test_added_column(products_contract):
    cols = dict(PRODUCTS_COLS, supplier_code=("string", "VARCHAR(64)", True))
    r = detect(products_contract, "products", dict(PRODUCT, supplier_code="SUP-1"), cols)
    assert r.drift_type == DriftType.ADDED_COLUMN
    assert r.unexpected_fields == ["supplier_code"] and r.missing_fields == []
    assert not r.ambiguous


def test_removed_column(products_contract):
    row = {k: v for k, v in PRODUCT.items() if k != "weight"}
    cols = {k: v for k, v in PRODUCTS_COLS.items() if k != "weight"}
    r = detect(products_contract, "products", row, cols)
    assert r.drift_type == DriftType.REMOVED_COLUMN
    assert r.missing_fields == ["weight"]


def test_rename_quantity_to_stock_quantity_is_high_confidence(on_hand_contract):
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int32", "INT", False)}
    r = detect(on_hand_contract, "products_on_hand", {"product_id": 101, "stock_quantity": 3}, cols, op="u")
    assert r.drift_type == DriftType.RENAMED_COLUMN
    assert r.missing_fields == ["quantity"] and r.unexpected_fields == ["stock_quantity"]
    (cand,) = r.possible_renames
    assert (cand.missing_field, cand.unexpected_field) == ("quantity", "stock_quantity")
    assert cand.confidence >= 0.9 and not cand.ambiguous
    assert not r.ambiguous


def test_rename_with_incompatible_type_is_not_a_rename(on_hand_contract):
    # quantity INT -> stock_quantity VARCHAR: lexical match, but type evidence weak
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("string", "VARCHAR(10)", False)}
    r = detect(on_hand_contract, "products_on_hand", {"product_id": 1, "stock_quantity": "3"}, cols)
    assert r.drift_detected
    if r.possible_renames:
        assert r.possible_renames[0].confidence < 0.9


def test_unrelated_names_are_ambiguous_multiple_changes(on_hand_contract):
    cols = {"product_id": ON_HAND_COLS["product_id"], "zzz_flag": ("int32", "INT", False)}
    r = detect(on_hand_contract, "products_on_hand", {"product_id": 1, "zzz_flag": 3}, cols)
    assert r.drift_type in (DriftType.MULTIPLE_CHANGES, DriftType.RENAMED_COLUMN)
    assert r.ambiguous is True
    assert r.confidence <= 0.75


def test_competing_rename_candidates_are_ambiguous(on_hand_contract):
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int32", "INT", False),
            "qty_quantity": ("int32", "INT", False)}
    r = detect(on_hand_contract, "products_on_hand", {"product_id": 1, "stock_quantity": 3, "qty_quantity": 3}, cols)
    assert r.ambiguous is True


def test_int_to_bigint_is_compatible_type_change(on_hand_contract):
    cols = dict(ON_HAND_COLS, quantity=("int64", "BIGINT", False))
    r = detect(on_hand_contract, "products_on_hand", {"product_id": 1, "quantity": 9}, cols)
    assert r.drift_type == DriftType.TYPE_CHANGE
    (t,) = r.type_mismatches
    assert (t.field, t.expected, t.actual, t.level, t.compatibility) == ("quantity", "INT", "BIGINT", "sql", "compatible")
    assert DriftDetector.only_compatible_type_changes(r)


def test_varchar_widening_detected_from_propagated_length(products_contract):
    cols = dict(PRODUCTS_COLS, name=("string", "VARCHAR(512)", False))
    r = detect(products_contract, "products", PRODUCT, cols)
    assert r.drift_type == DriftType.TYPE_CHANGE
    assert r.type_mismatches[0].compatibility == "compatible"


def test_value_level_type_mismatch_without_schema(products_contract):
    ev = parse(envelope("products", dict(PRODUCT, weight="heavy"), with_schema=False))
    r = DriftDetector().detect(ev, products_contract)
    assert r.drift_type == DriftType.TYPE_CHANGE
    assert r.type_mismatches[0].level == "json"
    assert not DriftDetector.only_compatible_type_changes(r)


def test_nullability_change_schema_and_value(products_contract):
    cols = dict(PRODUCTS_COLS, name=("string", "VARCHAR(255)", True))
    r = detect(products_contract, "products", PRODUCT, cols)
    assert r.drift_type == DriftType.NULLABILITY_CHANGE
    assert r.nullability_changes[0]["level"] == "schema"
    r2 = detect(products_contract, "products", dict(PRODUCT, name=None), cols)
    assert any(n["level"] == "value" for n in r2.nullability_changes)


def test_multiple_changes(on_hand_contract):
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int64", "BIGINT", False),
            "note": ("string", "VARCHAR(10)", True)}
    r = detect(on_hand_contract, "products_on_hand", {"product_id": 1, "stock_quantity": 3, "note": "x"}, cols)
    assert r.drift_type == DriftType.MULTIPLE_CHANGES


def test_fingerprint_identifies_drift_shape_not_values(on_hand_contract):
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int32", "INT", False)}
    a = detect(on_hand_contract, "products_on_hand", {"product_id": 1, "stock_quantity": 3}, cols)
    b = detect(on_hand_contract, "products_on_hand", {"product_id": 2, "stock_quantity": 99}, cols)
    c = detect(on_hand_contract, "products_on_hand", {"product_id": 2, "quantity": 99}, ON_HAND_COLS)
    assert a.fingerprint == b.fingerprint != c.fingerprint

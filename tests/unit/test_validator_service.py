import json

from src.common.kafka import Message
from src.validator.consumer import BrokerUnavailableError, ValidationConsumer, ValidationService
from tests.conftest import ON_HAND_COLS, PRODUCTS_COLS, FakeConsumer, FakeProducer, envelope

VALID = {"id": 101, "name": "scooter", "description": "Small 2-wheel scooter", "weight": 3.14}


def msg(value, offset=0, key=None):
    raw = value if isinstance(value, (bytes, type(None))) else json.dumps(value).encode()
    return Message("cdc.mutations", 0, offset, key, raw)


def test_valid_event_goes_to_validated():
    p = FakeProducer()
    out = ValidationService(p).handle(msg(envelope("products", VALID, cols=PRODUCTS_COLS), key=b'{"id":101}'))
    assert out == "validated"
    (v,) = p.on("cdc.validated")
    assert v["payload"] == VALID and v["source_name"] == "cdc" and v["key"] == {"id": 101}
    assert p.on("cdc.dlq") == []


def test_drift_goes_to_dlq_with_full_context():
    p = FakeProducer()
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int32", "INT", False)}
    out = ValidationService(p).handle(msg(envelope("products_on_hand", {"product_id": 1, "stock_quantity": 2},
                                                   cols=cols, op="u"), offset=7))
    assert out == "dlq:SCHEMA_DRIFT"
    (d,) = p.on("cdc.dlq")
    assert d["reason"] == "SCHEMA_DRIFT"
    assert d["drift_report"]["drift_type"] == "RENAMED_COLUMN"
    assert d["payload"] == {"product_id": 1, "stock_quantity": 2}
    assert d["canonical_schema"]["$id"].endswith("products_on_hand:v1")
    assert (d["topic"], d["partition"], d["offset"]) == ("cdc.mutations", 0, 7)
    assert d["column_schema"]["stock_quantity"]["sql_type"] == "INT"


def test_malformed_json_isolated_to_dlq():
    p = FakeProducer()
    out = ValidationService(p).handle(msg(b"{not json", offset=3))
    assert out == "dlq:MALFORMED_JSON"
    (d,) = p.on("cdc.dlq")
    assert d["raw_event"] == "{not json" and d["event_id"].startswith("malformed-")


def test_non_debezium_json_isolated():
    p = FakeProducer()
    assert ValidationService(p).handle(msg({"hello": "world"})) == "dlq:MALFORMED_ENVELOPE"


def test_unknown_table_goes_to_dlq():
    p = FakeProducer()
    out = ValidationService(p).handle(msg(envelope("customers", {"id": 1}, cols={"id": ("int32", "INT", False)})))
    assert out == "dlq:UNKNOWN_SCHEMA"


def test_unknown_source_server_is_not_matched_by_database_name_alone():
    p = FakeProducer()
    out = ValidationService(p).handle(msg(envelope("products", VALID, cols=PRODUCTS_COLS, server="other")))
    # single source declares db 'inventory' -> unique fallback by database is allowed
    assert out == "validated"


def test_tombstone_skipped_not_routed():
    p = FakeProducer()
    assert ValidationService(p).handle(msg(None, key=b'{"id":1}')) == "skipped:tombstone"
    assert p.published == []


def test_unexpected_exception_is_isolated(monkeypatch):
    p = FakeProducer()
    svc = ValidationService(p)
    monkeypatch.setattr(svc._detector, "detect", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert svc.handle(msg(envelope("products", VALID, cols=PRODUCTS_COLS))) == "dlq:PROCESSING_ERROR"
    assert "boom" in p.on("cdc.dlq")[0]["error"]


def test_consumer_survives_poison_messages_and_continues():
    values = [b"garbage", json.dumps({"x": 1}).encode(), json.dumps(envelope("products", VALID, cols=PRODUCTS_COLS)).encode()]
    c, p = FakeConsumer("cdc.mutations", values), FakeProducer()
    handled = ValidationConsumer(consumer=c, producer=p).run(max_messages=3)
    assert handled == 3 and c.committed == 3
    assert len(p.on("cdc.dlq")) == 2 and len(p.on("cdc.validated")) == 1


def test_broker_outage_does_not_lose_the_message(monkeypatch):
    monkeypatch.setenv("MAX_CONSUMER_RETRIES", "2")
    monkeypatch.setenv("RETRY_BASE_DELAY_SECONDS", "0.01")
    monkeypatch.setenv("RETRY_MAX_DELAY_SECONDS", "0.02")
    from src.common.config import reset_settings
    reset_settings()
    values = [json.dumps(envelope("products", VALID, cols=PRODUCTS_COLS)).encode()]
    c, p = FakeConsumer("cdc.mutations", values), FakeProducer(fail_times=3)
    consumer = ValidationConsumer(consumer=c, producer=p)
    handled = consumer.run(max_messages=1, idle_timeout=5)
    # first attempt exhausts 2 retries -> circuit opens -> seek back -> succeeds later
    assert handled == 1
    assert c.committed == 1
    assert len(p.on("cdc.validated")) == 1


def test_broker_unavailable_raised_from_service():
    import pytest
    from src.common.config import reset_settings
    import os
    os.environ["RETRY_BASE_DELAY_SECONDS"] = "0.01"
    reset_settings()
    try:
        p = FakeProducer(fail_times=100)
        with pytest.raises(BrokerUnavailableError):
            ValidationService(p).handle(msg(envelope("products", VALID, cols=PRODUCTS_COLS)))
    finally:
        del os.environ["RETRY_BASE_DELAY_SECONDS"]

"""Live end-to-end scenarios (spec tests 1-8) against the running Docker stack.

MySQL -> Debezium -> Redpanda -> validator -> DLQ -> AI worker (Ollama) -> repaired/audit

Scenarios that need the real model are marked `llm` (they take ~20-90 s each).
Each test restores the source schema in `finally`.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid

import pytest
from confluent_kafka import Consumer, TopicPartition

from src.ai_repair.mock_client import MockLLMClient, ScriptedLLMClient
from src.ai_repair.ollama_client import OllamaLLMClient
from src.ai_repair.repair_executor import RepairExecutor
from src.ai_repair.worker import AIRepairWorker
from src.common.config import get_settings, reset_settings
from src.common.database import sandbox_connection, source_reader, source_writer
from src.common.models import DLQMessage, RepairStatus
from src.common.sources import ContractRegistry, load_sources
from src.common.state_store import SQLiteStateStore
from src.sandbox.sql_sandbox import SqlSandbox
from src.tools import drift as drift_tool
from src.tools.hf_loader import fetch_rows, load
from src.tools.topics import TopicWatcher
from tests.integration.conftest import container_health, produce_json, produce_raw

SOURCE = next(iter(load_sources().values()))
REPAIR_TIMEOUT = 600


def _settings():
    reset_settings()
    return get_settings()


def insert_hf_products(n: int = 1) -> list[dict]:
    rows = fetch_rows(SOURCE.seed_data, 400)
    with source_writer() as conn:
        for offset in range(0, len(rows), n):
            inserted = load(SOURCE, rows[offset:offset + n], conn)
            if inserted:
                return inserted
    pytest.skip("no unused Hugging Face rows left in the cache")


def wait_decision(audit: TopicWatcher, event_id: str, timeout: float = REPAIR_TIMEOUT) -> dict:
    a = audit.wait_for(lambda m: m.get("event_id") == event_id and m.get("repair_result") != "DEFERRED", timeout)
    assert a is not None, f"no repair decision for {event_id} within {timeout}s"
    return a


@pytest.fixture
def watchers():
    s = _settings()
    w = {name: TopicWatcher(topic) for name, topic in
         (("validated", s.validated_topic), ("dlq", s.dlq_topic), ("repaired", s.repaired_topic),
          ("audit", s.audit_topic))}
    yield w
    for x in w.values():
        x.close()


@pytest.fixture
def clean_schema():
    drift_tool.reset(SOURCE, allow_drop=True)
    yield
    drift_tool.reset(SOURCE, allow_drop=True)


# ── Test 1 ─────────────────────────────────────────────────────────────────
def test_1_normal_cdc_reaches_validated_without_dlq(watchers, clean_schema):
    product = insert_hf_products(1)[0]
    v = watchers["validated"].wait_for(
        lambda m: m.get("source_table") == "products" and m.get("payload", {}).get("id") == product["id"], 60)
    assert v is not None, "valid insert never reached cdc.validated"
    assert v["payload"]["name"] == product["name"]
    assert v["contract_id"] == "urn:cdc:contract:inventory.products:v1"
    d = watchers["dlq"].wait_for(lambda m: m.get("payload", {}).get("id") == product["id"], 5)
    assert d is None, "valid event must not be dead-lettered"


# ── Test 2 ─────────────────────────────────────────────────────────────────
@pytest.mark.llm
def test_2_added_column_is_repaired(watchers, clean_schema):
    sc = SOURCE.drift_scenarios["add-column"]
    res = drift_tool.apply_scenario("add-column", SOURCE)
    pk = res.event_pk["id"]
    d = watchers["dlq"].wait_for(lambda m: m.get("source_table") == "products"
                                 and m.get("payload", {}).get("id") == pk, 60)
    assert d is not None
    assert d["drift_report"]["drift_type"] == "ADDED_COLUMN"
    assert d["drift_report"]["unexpected_fields"] == [sc["column"]]
    a = wait_decision(watchers["audit"], d["event_id"])
    assert a["repair_result"] == "REPAIRED", a["report"]
    r = watchers["repaired"].wait_for(lambda m: m.get("event_id") == d["event_id"], 30)
    assert r is not None
    assert sc["column"] not in r["repaired_payload"]
    assert {k: r["repaired_payload"][k] for k in ("id", "name")} == {"id": pk, "name": d["payload"]["name"]}


# ── Test 3 ─────────────────────────────────────────────────────────────────
@pytest.mark.llm
def test_3_renamed_column_is_mapped(watchers, clean_schema):
    res = drift_tool.apply_scenario("rename", SOURCE)
    pk = res.event_pk["product_id"]
    d = watchers["dlq"].wait_for(lambda m: m.get("source_table") == "products_on_hand"
                                 and m.get("payload", {}).get("product_id") == pk, 60)
    assert d is not None
    rep = d["drift_report"]
    assert rep["missing_fields"] == ["quantity"] and rep["unexpected_fields"] == ["stock_quantity"]
    assert rep["drift_type"] == "RENAMED_COLUMN" and not rep["ambiguous"]
    assert rep["possible_renames"][0]["unexpected_field"] == "stock_quantity"
    a = wait_decision(watchers["audit"], d["event_id"])
    assert a["repair_result"] == "REPAIRED", a["report"]
    r = watchers["repaired"].wait_for(lambda m: m.get("event_id") == d["event_id"], 30)
    assert r["repaired_payload"] == {"product_id": pk, "quantity": d["payload"]["stock_quantity"]}


# ── Test 4 ─────────────────────────────────────────────────────────────────
@pytest.mark.llm
def test_4_int_to_bigint_is_compatible_widening(watchers, clean_schema):
    res = drift_tool.apply_scenario("widen-type", SOURCE)
    pk = res.event_pk["product_id"]
    d = watchers["dlq"].wait_for(lambda m: m.get("source_table") == "products_on_hand"
                                 and m.get("payload", {}).get("product_id") == pk, 60)
    assert d is not None
    (tc,) = d["drift_report"]["type_mismatches"]
    assert (tc["field"], tc["expected"], tc["actual"], tc["compatibility"]) == ("quantity", "INT", "BIGINT", "compatible")
    assert d["drift_report"]["drift_type"] == "TYPE_CHANGE"
    a = wait_decision(watchers["audit"], d["event_id"])
    # The model may or may not propose the widening DDL; whatever it proposes must pass policy + sandbox.
    assert a["repair_result"] == "REPAIRED", a["report"]
    if a["migration_sql"]:
        assert a["migration_status"] == "VERIFIED_IN_SANDBOX"
        assert "BIGINT" in a["migration_sql"].upper()


# ── Test 5 ─────────────────────────────────────────────────────────────────
def test_5_dangerous_migration_is_rejected_and_never_executed(on_hand_dlq_message):
    contracts = ContractRegistry()
    malicious = json.dumps({
        "classification": "RENAMED_COLUMN", "confidence": 0.99, "reasoning_summary": "clean up",
        "migration_sql": "ALTER TABLE products_on_hand DROP COLUMN quantity; DROP TABLE products_on_hand",
        "transformation_code": "def transform(event):\n    return {'product_id': event['product_id'], "
                               "'quantity': event['stock_quantity']}",
        "test_cases": [], "risk_level": "LOW"})
    llm = ScriptedLLMClient([malicious])
    sandbox = SqlSandbox()  # the REAL sandbox MySQL
    with source_reader() as r:
        before_tables = {x["TABLE_NAME"] for x in r.query(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='inventory'")}
    out = RepairExecutor(llm, contracts=contracts, sql_sandbox=sandbox).execute(on_hand_dlq_message)
    assert out.status == RepairStatus.REJECTED
    assert out.repaired is None
    assert out.audit.migration_status == "REJECTED_BY_POLICY"
    assert out.audit.risk_level in ("HIGH", "CRITICAL")
    assert llm.calls == 1
    with source_reader() as r:
        after_tables = {x["TABLE_NAME"] for x in r.query(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='inventory'")}
        assert r.query("SELECT COUNT(*) AS n FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='inventory' "
                       "AND TABLE_NAME='products_on_hand' AND COLUMN_NAME='quantity'")[0]["n"] == 1
    assert before_tables == after_tables
    with sandbox_connection() as sb:  # no scratch databases left behind
        assert sb.query("SHOW DATABASES LIKE 'sbx\\_%'") == []


def test_5b_safe_widening_really_runs_in_sandbox_mysql(on_hand_dlq_message):
    contract = ContractRegistry().get("inventory", "products_on_hand", "cdc")
    res = SqlSandbox().run(contract, ["ALTER TABLE `products_on_hand` MODIFY COLUMN `quantity` BIGINT NOT NULL"],
                           [{"product_id": 1, "quantity": 2**40}], require_db=True)
    assert res.status == "PASSED", res.checks
    assert res.columns_after["quantity"]["type"] == "BIGINT"
    bad = SqlSandbox().run(contract, ["ALTER TABLE `products_on_hand` MODIFY COLUMN `quantity` TINYINT NOT NULL"],
                           [{"product_id": 1, "quantity": 5}], require_db=True)
    assert bad.status == "FAILED"  # post-execution verification catches narrowing even if policy were bypassed


@pytest.fixture
def on_hand_dlq_message():
    from tests.conftest import ON_HAND_COLS
    from tests.fakes import dlq_for
    contract = ContractRegistry().get("inventory", "products_on_hand", "cdc")
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int32", "INT", False)}
    return dlq_for(contract, "products_on_hand", {"product_id": 101, "stock_quantity": 4}, cols,
                   pos=int(time.time()))


# ── Test 6 ─────────────────────────────────────────────────────────────────
def test_6_malformed_event_is_isolated_and_processing_continues(watchers, clean_schema):
    s = _settings()
    marker = uuid.uuid4().hex
    produce_raw(s.mutations_topic, f'{{"broken": "{marker}"'.encode(), key=b"poison")
    produce_json(s.mutations_topic, {"op": "c", "unexpected": marker}, key="poison2")
    d = watchers["dlq"].wait_for(lambda m: m.get("reason") == "MALFORMED_JSON" and marker in str(m.get("raw_event")), 60)
    assert d is not None and d["correlation_id"].startswith(s.mutations_topic)
    d2 = watchers["dlq"].wait_for(lambda m: m.get("reason") == "MALFORMED_ENVELOPE"
                                  and marker in json.dumps(m.get("raw_event")), 60)
    assert d2 is not None
    product = insert_hf_products(1)[0]
    v = watchers["validated"].wait_for(lambda m: m.get("payload", {}).get("id") == product["id"], 60)
    assert v is not None, "valid events must keep flowing after poison messages"
    assert container_health("cdc-validator") == "healthy"
    a = watchers["audit"].wait_for(lambda m: m.get("event_id") == d["event_id"], 60)
    assert a is not None and a["repair_result"] == "MANUAL_REVIEW"  # never sent to the LLM


# ── Test 7 ─────────────────────────────────────────────────────────────────
def _committed(group: str, topic: str) -> int:
    c = Consumer({"bootstrap.servers": get_settings().redpanda_brokers, "group.id": group})
    try:
        (tp,) = c.committed([TopicPartition(topic, 0)], timeout=10)
        return tp.offset
    finally:
        c.close()


def test_7_ollama_unavailable_defers_without_losing_events(temp_topics, on_hand_dlq_message, tmp_path):
    s = _settings()
    group = f"it-worker-{uuid.uuid4().hex[:6]}"
    produce_json(s.dlq_topic, on_hand_dlq_message.model_dump(mode="json"), key=on_hand_dlq_message.event_id)
    state = SQLiteStateStore(tmp_path / "state.db")
    down = OllamaLLMClient(base_url="http://127.0.0.1:9", timeout=2)  # nothing listens on port 9
    w1 = AIRepairWorker(llm=down, state=state, group_id=group,
                        executor=RepairExecutor(down, state=state))
    stats = w1.run(max_seconds=12)
    assert stats.get("deferred", 0) >= 2, stats          # bounded retries with backoff
    assert _committed(group, s.dlq_topic) < 1            # nothing acknowledged
    assert container_health("cdc-validator") == "healthy"
    audit = TopicWatcher(s.audit_topic, from_beginning=True)
    msgs = audit.drain(3)
    audit.close()
    assert [m["repair_result"] for m in msgs] == ["DEFERRED"]  # one deferral audit, not one per retry

    # Model comes back: same group + state resumes and the event is repaired, not lost.
    state2 = SQLiteStateStore(tmp_path / "state.db")
    llm = MockLLMClient()
    w2 = AIRepairWorker(llm=llm, state=state2, group_id=group, executor=RepairExecutor(llm, state=state2))
    stats2 = w2.run(max_messages=1, idle_timeout=20)
    assert stats2.get("repaired") == 1, stats2
    assert _committed(group, s.dlq_topic) == 1


# ── Test 8 ─────────────────────────────────────────────────────────────────
def test_8_restart_does_not_duplicate_repairs(temp_topics, tmp_path):
    from tests.conftest import ON_HAND_COLS
    from tests.fakes import dlq_for
    s = _settings()
    contract = ContractRegistry().get("inventory", "products_on_hand", "cdc")
    cols = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int32", "INT", False)}
    base = int(time.time())
    for i in range(3):
        m = dlq_for(contract, "products_on_hand", {"product_id": i, "stock_quantity": i}, cols, pos=base + i)
        produce_json(s.dlq_topic, m.model_dump(mode="json"), key=m.event_id)
    db = tmp_path / "state.db"
    llm = MockLLMClient()
    st = SQLiteStateStore(db)
    AIRepairWorker(llm=llm, state=st, group_id="it-a", executor=RepairExecutor(llm, state=st)) \
        .run(max_messages=3, idle_timeout=20)
    # "Restart" with lost offsets: a NEW consumer group re-reads the whole DLQ.
    st2 = SQLiteStateStore(db)
    stats = AIRepairWorker(llm=llm, state=st2, group_id="it-b", executor=RepairExecutor(llm, state=st2)) \
        .run(max_messages=3, idle_timeout=20)
    assert stats.get("duplicate_skipped") == 3
    w = TopicWatcher(s.repaired_topic, from_beginning=True)
    ids = [m["event_id"] for m in w.drain(4)]
    w.close()
    assert len(ids) == 3 and len(set(ids)) == 3


def test_8b_container_restart_keeps_repaired_topic_duplicate_free():
    s = _settings()
    subprocess.run(["docker", "restart", "cdc-ai-worker"], check=True, capture_output=True)
    deadline = time.time() + 90
    while time.time() < deadline and container_health("cdc-ai-worker") != "healthy":
        time.sleep(2)
    assert container_health("cdc-ai-worker") == "healthy"
    time.sleep(5)
    w = TopicWatcher(s.repaired_topic, from_beginning=True)
    ids = [m["event_id"] for m in w.drain(5)]
    w.close()
    assert len(ids) == len(set(ids)), "a repaired event was published twice"


# ── Destinations ───────────────────────────────────────────────────────────
def _destinations_show(table_ref: str, key: dict) -> dict:
    out = subprocess.run(["docker", "exec", "cdc-downstream", "python", "-m", "src.tools.destinations", "show",
                          table_ref, json.dumps(key)], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_9_every_enabled_destination_receives_the_change(clean_schema):
    product = insert_hf_products(1)[0]
    ref, key = "inventory.inventory.products", {"id": product["id"]}
    deadline, got = time.time() + 60, {}
    while time.time() < deadline:
        got = _destinations_show(ref, key)
        wh, lake, lh = got.get("warehouse"), got.get("lake_landing"), got.get("lakehouse")
        if isinstance(wh, dict) and isinstance(lake, dict) and lake.get("latest") and isinstance(lh, dict) \
                and lh.get("row"):
            break
        time.sleep(2)
    assert got["warehouse"]["payload"]["name"] == product["name"]
    assert got["lake_landing"]["latest"]["name"] == product["name"]
    assert got["lake_landing"]["latest"]["_op"] == "c"
    assert got["lakehouse"]["row"]["name"] == product["name"]
    assert got["bigquery"] == "disabled or not routed"

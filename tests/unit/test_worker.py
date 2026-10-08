import json

from src.ai_repair.llm_client import LLMUnavailableError
from src.ai_repair.mock_client import MockLLMClient, ScriptedLLMClient
from src.ai_repair.repair_executor import RepairExecutor
from src.ai_repair.worker import AIRepairWorker
from src.common.state_store import SQLiteStateStore
from tests.conftest import ON_HAND_COLS, FakeConsumer, FakeProducer
from tests.fakes import FakeSqlSandbox, dlq_for, proposal

RENAMED = {"product_id": ON_HAND_COLS["product_id"], "stock_quantity": ("int32", "INT", False)}


def make_worker(values, llm, state, contracts, committed=0):
    consumer, producer = FakeConsumer("cdc.dlq", values, committed=committed), FakeProducer()
    ex = RepairExecutor(llm, contracts=contracts, state=state, sql_sandbox=FakeSqlSandbox())
    w = AIRepairWorker(consumer=consumer, producer=producer, llm=llm, state=state, executor=ex)
    return w, consumer, producer


def dlq_bytes(contract, pk):
    return json.dumps(dlq_for(contract, "products_on_hand", {"product_id": pk, "stock_quantity": 3},
                              RENAMED, pos=pk).model_dump(mode="json")).encode()


def test_repair_publishes_repaired_and_audit_then_commits(on_hand_contract, contracts, tmp_path):
    state = SQLiteStateStore(tmp_path / "s.db")
    w, c, p = make_worker([dlq_bytes(on_hand_contract, 1)], MockLLMClient(), state, contracts)
    stats = w.run(max_messages=1, idle_timeout=5)
    assert stats.get("repaired") == 1
    assert len(p.on("cdc.repaired")) == 1 and len(p.on("cdc.audit")) == 1
    assert c.committed == 1


def test_restart_does_not_duplicate_repairs(on_hand_contract, contracts, tmp_path):
    db = tmp_path / "s.db"
    values = [dlq_bytes(on_hand_contract, 1), dlq_bytes(on_hand_contract, 2)]
    w1, _, p1 = make_worker(values, MockLLMClient(), SQLiteStateStore(db), contracts)
    w1.run(max_messages=2, idle_timeout=5)
    assert len(p1.on("cdc.repaired")) == 2
    # Restart with offsets reset (e.g. crash before commit): same events re-delivered.
    w2, c2, p2 = make_worker(values, MockLLMClient(), SQLiteStateStore(db), contracts, committed=0)
    stats = w2.run(max_messages=2, idle_timeout=5)
    assert p2.on("cdc.repaired") == [] and stats.get("duplicate_skipped") == 2
    assert c2.committed == 2


def test_duplicate_dlq_message_processed_once(on_hand_contract, contracts, tmp_path):
    msg = dlq_bytes(on_hand_contract, 5)
    w, _, p = make_worker([msg, msg], MockLLMClient(), SQLiteStateStore(tmp_path / "s.db"), contracts)
    w.run(max_messages=2, idle_timeout=5)
    assert len(p.on("cdc.repaired")) == 1


def test_llm_outage_defers_without_commit_then_recovers(on_hand_contract, contracts, tmp_path, monkeypatch):
    import src.ai_repair.worker as worker_mod
    monkeypatch.setattr(worker_mod, "backoff_delay", lambda *a, **k: 0.05)
    llm = ScriptedLLMClient([LLMUnavailableError("down"), LLMUnavailableError("down"), proposal()])
    state = SQLiteStateStore(tmp_path / "s.db")
    w, c, p = make_worker([dlq_bytes(on_hand_contract, 9)], llm, state, contracts)
    stats = w.run(max_messages=1, idle_timeout=5)
    assert stats["deferred"] == 2
    assert c.committed == 1                    # only after the eventual decision
    assert len(p.on("cdc.repaired")) == 1
    audits = p.on("cdc.audit")
    assert audits[0]["repair_result"] == "DEFERRED" and audits[-1]["repair_result"] == "REPAIRED"
    assert sum(a["repair_result"] == "DEFERRED" for a in audits) == 1  # one deferral audit per event


def test_llm_outage_never_commits(on_hand_contract, contracts, tmp_path, monkeypatch):
    import src.ai_repair.worker as worker_mod
    monkeypatch.setattr(worker_mod, "backoff_delay", lambda *a, **k: 0.05)
    llm = ScriptedLLMClient([LLMUnavailableError("down")])
    w, c, p = make_worker([dlq_bytes(on_hand_contract, 9)], llm, SQLiteStateStore(tmp_path / "s.db"), contracts)
    w.run(max_seconds=1.0)
    assert c.committed == 0 and p.on("cdc.repaired") == []


def test_unreadable_dlq_message_is_committed_not_crashing(contracts, tmp_path):
    w, c, p = make_worker([b"\xff\xfe"], MockLLMClient(), SQLiteStateStore(tmp_path / "s.db"), contracts)
    assert w.run(max_messages=1, idle_timeout=3).get("unreadable") == 1
    assert c.committed == 1

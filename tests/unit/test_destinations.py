import json
import textwrap

import pytest

from src.common.sources import ContractRegistry
from src.downstream.consumer import DestinationRunner
from src.sinks.base import ChangeRecord, DeliveryLedger, Sink, source_sequence, to_record
from src.sinks.bigquery_sink import BigQuerySink
from src.sinks.jsonl_sink import JsonlLakeSink
from src.sinks.registry import (
    DestinationConfig,
    DestinationConfigError,
    build_sink,
    expand_env,
    load_destinations,
)
from src.sinks.sqlite_sink import SqliteSink
from tests.conftest import FakeConsumer

REF = "inventory.inventory.products_on_hand"


def rec(pid, qty, seq, op="u", origin="validated", eid=None):
    return ChangeRecord(event_id=eid or f"e-{pid}-{seq}", source="inventory", database="inventory",
                        table="products_on_hand", op=op, key={"product_id": pid},
                        payload={"product_id": pid, "quantity": qty}, sequence=seq, origin=origin)


@pytest.fixture
def routed(contracts):
    return {c.key: c for c in contracts.all()}


# ── registry ───────────────────────────────────────────────────────────────
def test_shipped_registry_loads_and_bigquery_is_off_by_default():
    cfgs = {d.name: d for d in load_destinations()}
    assert {"warehouse", "lake_landing", "lakehouse", "bigquery"} <= set(cfgs)
    assert cfgs["bigquery"].enabled is False
    assert cfgs["lakehouse"].options["catalog"]["type"] == "sql"


def test_env_expansion(monkeypatch):
    monkeypatch.setenv("BQ_PROJECT", "my-proj")
    out = expand_env({"a": "${BQ_PROJECT}", "b": "${MISSING:-dflt}", "c": ["x-${MISSING}"], "d": 3})
    assert out == {"a": "my-proj", "b": "dflt", "c": ["x-"], "d": 3}


def test_routing_globs():
    cfg = DestinationConfig(name="d", type="sqlite", enabled=True, tables=["inventory.*.products", "crm.*"])
    assert cfg.routes("inventory.inventory.products")
    assert not cfg.routes("inventory.inventory.products_on_hand")
    assert cfg.routes("crm.sales.orders")


def test_custom_sink_class_by_import_path(tmp_path):
    cfg = DestinationConfig(name="mine", type="tests.unit.test_destinations:MemorySink", enabled=True)
    assert isinstance(build_sink(cfg), MemorySink)


@pytest.mark.parametrize("type_", ["nosuch", "json:dumps", "tests.unit.test_destinations:NotASink"])
def test_bad_sink_types_rejected(type_):
    with pytest.raises(DestinationConfigError):
        build_sink(DestinationConfig(name="x", type=type_, enabled=True))


def test_registry_file_validation(tmp_path):
    p = tmp_path / "d.yaml"
    p.write_text("destinations:\n  bad name!: {type: sqlite}\n")
    with pytest.raises(DestinationConfigError):
        load_destinations(p)


class NotASink:
    pass


class MemorySink(Sink):
    type_name = "memory"

    def __init__(self, name, options):
        super().__init__(name, options)
        self.written, self.fail = [], 0

    def write(self, records, contracts):
        if self.fail:
            self.fail -= 1
            raise ConnectionError("destination down (simulated)")
        self.written.extend(records)


# ── normalisation ──────────────────────────────────────────────────────────
def test_sequence_orders_binlog_positions():
    a = source_sequence({"file": "mysql-bin.000003", "pos": 900, "row": 0}, None)
    b = source_sequence({"file": "mysql-bin.000003", "pos": 1200, "row": 0}, None)
    c = source_sequence({"file": "mysql-bin.000004", "pos": 4, "row": 0}, None)
    assert a < b < c


def test_validated_and_repaired_normalise_to_same_shape():
    meta = {"name": "cdc", "db": "inventory", "table": "products_on_hand", "file": "mysql-bin.000003", "pos": 10}
    v = to_record("validated", {"event_id": "e1", "source_name": "cdc", "source_database": "inventory",
                                "source_table": "products_on_hand", "operation": "u", "key": {"product_id": 1},
                                "payload": {"product_id": 1, "quantity": 2}, "source_metadata": meta})
    r = to_record("repaired", {"event_id": "e2", "repair_id": "r1", "source_name": "cdc",
                               "source_database": "inventory", "source_table": "products_on_hand",
                               "operation": "u", "key": {"product_id": 1},
                               "repaired_payload": {"product_id": 1, "quantity": 3},
                               "source_event": {"source_metadata": dict(meta, pos=20)}})
    assert (v.origin, r.origin) == ("validated", "repaired")
    assert r.payload == {"product_id": 1, "quantity": 3} and r.repair_id == "r1"
    assert r.sequence > v.sequence


# ── sinks ──────────────────────────────────────────────────────────────────
def test_sqlite_last_writer_wins_and_tombstones(tmp_path, routed):
    s = SqliteSink("w", {"path": str(tmp_path / "w.db")})
    s.open(list(routed.values()))
    s.write([rec(1, 10, seq=200, origin="repaired")], routed)
    s.write([rec(1, 5, seq=100)], routed)              # older change arrives late
    assert s.row(REF, {"product_id": 1})["payload"]["quantity"] == 10
    s.write([rec(1, 10, seq=300, op="d")], routed)
    s.write([rec(1, 99, seq=250)], routed)             # late upsert must not resurrect
    row = s.row(REF, {"product_id": 1})
    assert row["deleted"] is True and s.count(REF) == 0


def test_jsonl_lake_partitions_and_metadata(tmp_path, routed):
    s = JsonlLakeSink("lake", {"root": str(tmp_path)})
    s.open([])
    s.write([rec(1, 10, 1), rec(2, 20, 2, origin="repaired")], routed)
    files = list(tmp_path.glob("inventory/inventory/products_on_hand/dt=*/part-*.jsonl"))
    assert len(files) == 1
    lines = [json.loads(x) for x in files[0].read_text().splitlines()]
    assert [l["quantity"] for l in lines] == [10, 20]
    assert lines[1]["_origin"] == "repaired" and lines[0]["_key"] == {"product_id": 1}


def test_iceberg_upsert_and_changelog_local_catalog(tmp_path, routed):
    pytest.importorskip("pyiceberg")
    from src.sinks.iceberg_sink import IcebergSink
    contract = routed[REF]
    cat = {"name": "t", "type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    up = IcebergSink("lh", {"namespace": "cdc", "mode": "upsert", "catalog": cat})
    up.open([contract])
    up.write([rec(1, 10, 100), rec(2, 20, 100)], routed)
    up.write([rec(1, 11, 200, origin="repaired"), rec(2, 5, 50)], routed)   # 2nd is stale
    rows = {r["product_id"]: r for r in up.read(contract)}
    assert rows[1]["quantity"] == 11 and rows[1]["_origin"] == "repaired"
    assert rows[2]["quantity"] == 20
    up.write([rec(2, 0, 300, op="d")], routed)
    assert {r["product_id"] for r in up.read(contract)} == {1}

    cl = IcebergSink("cl", {"namespace": "raw", "mode": "changelog", "catalog": cat})
    cl.open([contract])
    cl.write([rec(1, 10, 100), rec(1, 11, 200)], routed)
    assert len(cl.read(contract, current_only=False)) == 2


class FakeBQ:
    def __init__(self, errors=None):
        self.sql, self.inserts, self.errors = [], [], errors or []

    def query(self, sql):
        self.sql.append(sql)
        return self

    def result(self):
        return []

    def insert_rows_json(self, table_id, rows, row_ids=None):
        self.inserts.append((table_id, rows, row_ids))
        return self.errors


def test_bigquery_ddl_and_streaming_with_insert_ids(routed):
    bq = FakeBQ()
    s = BigQuerySink("bq", {"project": "p", "dataset": "cdc"}, client=bq)
    s.open([routed[REF]])
    ddl = "\n".join(bq.sql)
    assert "CREATE SCHEMA IF NOT EXISTS `p.cdc`" in ddl
    assert "`p.cdc.inventory__inventory__products_on_hand_changelog`" in ddl
    assert "`quantity` INT64" in ddl and "`_seq` INT64" in ddl and "PARTITION BY DATE(_ingested_at)" in ddl
    assert "PARTITION BY `product_id` ORDER BY _seq DESC" in ddl and "_op != 'd'" in ddl
    s.write([rec(1, 10, 100, eid="evt-1")], routed)
    table_id, rows, ids = bq.inserts[0]
    assert table_id == "p.cdc.inventory__inventory__products_on_hand_changelog"
    assert ids == ["evt-1"] and rows[0]["quantity"] == 10 and rows[0]["_op"] == "u"


def test_bigquery_insert_errors_raise_for_retry(routed):
    s = BigQuerySink("bq", {"project": "p"}, client=FakeBQ(errors=[{"index": 0, "errors": ["bad"]}]))
    s.open([routed[REF]])
    with pytest.raises(RuntimeError):
        s.write([rec(1, 1, 1)], routed)


def test_bigquery_requires_project():
    with pytest.raises(ValueError, match="project"):
        BigQuerySink("bq", {"project": ""}, client=FakeBQ())


# ── runner ─────────────────────────────────────────────────────────────────
def _validated_msg(pid, qty, pos):
    return json.dumps({"event_id": f"products_on_hand-u-{pid}-{pos}", "source_name": "cdc",
                       "source_database": "inventory", "source_table": "products_on_hand", "operation": "u",
                       "key": {"product_id": pid}, "payload": {"product_id": pid, "quantity": qty},
                       "source_metadata": {"file": "mysql-bin.000003", "pos": pos, "row": 0}}).encode()


def _runner(sink, values, tmp_path, tables=("*",), contracts=None):
    cfg = DestinationConfig(name=sink.name, type="memory", enabled=True, tables=list(tables),
                            batch_size=10, max_batch_wait_seconds=0.1)
    consumer = FakeConsumer("cdc.validated", values)
    r = DestinationRunner(cfg, sink, contracts or ContractRegistry(), consumer=consumer,
                          ledger=DeliveryLedger(tmp_path / f"{sink.name}.db"))
    return r, consumer


def test_runner_delivers_maps_source_and_commits(tmp_path):
    sink = MemorySink("m", {})
    r, c = _runner(sink, [_validated_msg(1, 5, 10), _validated_msg(2, 6, 20)], tmp_path)
    r.run(max_seconds=1)
    assert [x.table_ref for x in sink.written] == [REF, REF]  # topic prefix 'cdc' -> source 'inventory'
    assert c.committed == 2


def test_runner_retries_failed_batch_without_committing(tmp_path, monkeypatch):
    import src.downstream.consumer as mod
    monkeypatch.setattr(mod, "backoff_delay", lambda *a, **k: 0.05)
    sink = MemorySink("m", {})
    sink.fail = 2
    r, c = _runner(sink, [_validated_msg(1, 5, 10)], tmp_path)
    stats = r.run(max_seconds=1.5)
    assert stats["failed_batches"] == 2
    assert len(sink.written) == 1 and c.committed == 1   # delivered exactly once after recovery


def test_runner_ledger_skips_redelivered_events(tmp_path):
    sink = MemorySink("m", {})
    msgs = [_validated_msg(1, 5, 10)]
    _runner(sink, msgs, tmp_path)[0].run(max_seconds=0.6)
    r2, _ = _runner(sink, msgs, tmp_path)       # same ledger, offsets lost
    stats = r2.run(max_seconds=0.6)
    assert len(sink.written) == 1 and stats["duplicates"] == 1


def test_runner_routing_filters_tables(tmp_path):
    sink = MemorySink("m", {})
    r, c = _runner(sink, [_validated_msg(1, 5, 10)], tmp_path, tables=["inventory.inventory.products"])
    stats = r.run(max_seconds=0.6)
    assert sink.written == [] and stats["skipped"] == 1 and c.committed == 1

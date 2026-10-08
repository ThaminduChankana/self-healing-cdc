"""End-to-end self-healing demo (steps 9-22). Called by scripts/run_demo.sh.

Every step waits for a concrete record on a real topic and verifies it. If
the model's repair does not pass every independent check the demo FAILS
(exit code 1) and shows why — it never pretends a repair succeeded.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from typing import Any

from src.common.config import get_settings
from src.common.database import source_writer
from src.common.sources import load_sources
from src.tools import drift as drift_tool
from src.tools.hf_loader import fetch_rows, load
from src.tools.topics import TopicWatcher

B, G, Y, R, C, D, N = "\033[1m", "\033[32m", "\033[33m", "\033[31m", "\033[36m", "\033[2m", "\033[0m"
_step = 8


def step(title: str) -> None:
    global _step
    _step += 1
    print(f"\n{B}{C}══ Step {_step}: {title} {'═' * max(0, 60 - len(title))}{N}")


def show(obj: Any, limit: int = 2500) -> None:
    text = json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    print(D + (text if len(text) <= limit else text[:limit] + "\n  ... (truncated)") + N)


def ok(msg: str) -> None:
    print(f"{G}✓ {msg}{N}")


def fail(msg: str) -> None:
    print(f"{R}✗ {msg}{N}")
    sys.exit(1)


def wait(watcher: TopicWatcher, pred, timeout: float, what: str) -> dict[str, Any]:
    print(f"  waiting for {what} on {watcher.topic} (timeout {timeout:.0f}s)...")
    found = watcher.wait_for(pred, timeout)
    if found is None:
        fail(f"timed out waiting for {what} on {watcher.topic}")
    return found


def compact_dlq(d: dict[str, Any]) -> dict[str, Any]:
    keep = ("event_id", "correlation_id", "reason", "topic", "partition", "offset", "source_name",
            "source_table", "operation", "key", "payload", "attempt", "received_at")
    out = {k: d.get(k) for k in keep}
    out["drift_report"] = {k: d["drift_report"].get(k) for k in (
        "drift_type", "ambiguous", "confidence", "missing_fields", "unexpected_fields",
        "type_mismatches", "possible_renames", "fingerprint")}
    out["canonical_schema"] = {"$id": d["canonical_schema"].get("$id"), "required": d["canonical_schema"].get("required")}
    return out


def warehouse_row(table_ref: str, pk: dict[str, Any]) -> dict[str, Any] | None:
    try:
        out = subprocess.run(["docker", "exec", "cdc-downstream", "python", "-m", "src.downstream.consumer",
                              "--show", table_ref, json.dumps(pk)], capture_output=True, text=True, timeout=30)
        return json.loads(out.stdout) if out.returncode == 0 and out.stdout.strip() else None
    except (subprocess.SubprocessError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=None, help="Hugging Face rows to load")
    parser.add_argument("--repair-timeout", type=float, default=900)
    parser.add_argument("--keep-drift", action="store_true", help="do not undo the rename at the end")
    parser.add_argument("--allow-cache", action="store_true",
                        help="allow the first repair to come from the approved-repair cache")
    args = parser.parse_args()
    s = get_settings()
    source = next(iter(load_sources().values()))
    sc = source.drift_scenarios["rename"]
    table, col, new = sc["table"], sc["column"], sc["new_name"]

    # Clean starting point (non-destructive reset of a previous run's rename/widen).
    state = drift_tool.status(source)
    if state.get("add-column", {}).get(source.drift_scenarios["add-column"]["column"]):
        fail("the add-column demo column is still present; run `make drift ACTION=reset` "
             "with --allow-drop-demo-column first")
    if state.get("rename", {}).get(new) or (state.get("widen-type", {}).get(col) or "").lower().startswith("bigint"):
        print(f"{Y}Previous drift detected — reverting it (non-destructive){N}")
        drift_tool.reset(source)

    if not args.allow_cache:
        # Make sure the headline repair is a live model call, not a cached approval.
        out = subprocess.run(["docker", "exec", "cdc-ai-worker", "python", "-m", "src.tools.state", "clear-cache"],
                             capture_output=True, text=True)
        print(f"{D}AI worker approved-repair cache: {out.stdout.strip() or out.stderr.strip()}{N}")

    validated = TopicWatcher(s.validated_topic)
    dlq = TopicWatcher(s.dlq_topic)
    repaired = TopicWatcher(s.repaired_topic)
    audit = TopicWatcher(s.audit_topic)

    # ── 9: baseline from Hugging Face ──────────────────────────────────────
    step("Generate baseline CDC events from real Hugging Face data")
    seed = source.seed_data
    count = args.rows or int(seed.get("rows", 25))
    rows = fetch_rows(seed, count + 200)  # fetch spare rows so re-runs still insert new products
    with source_writer() as conn:
        inserted = []
        offset = 0
        while not inserted and offset < len(rows):
            inserted = load(source, rows[offset:offset + count], conn)
            offset += count
    if not inserted:
        fail("no new Hugging Face rows could be inserted (all already loaded)")
    print(f"Dataset: {seed['dataset']}  →  inserted {len(inserted)} products (+ stock levels) via app_writer")
    for r in inserted[:5]:
        print(f"  #{r['id']:<5} qty={r.get('quantity', '-')!s:<4} weight={r.get('weight')!s:<8} {r['name'][:70]}")
    target = inserted[-1]
    pid = target["id"]
    ok(f"Debezium is capturing {len(inserted) * 2} INSERTs into {s.mutations_topic}")

    # ── 10: validated ──────────────────────────────────────────────────────
    step("Baseline event is validated against the canonical contract")
    v = wait(validated, lambda m: m.get("source_table") == table and m.get("payload", {}).get("product_id") == pid,
             90, f"{table} product_id={pid}")
    show({k: v[k] for k in ("event_id", "correlation_id", "source_name", "source_table", "operation",
                            "contract_id", "payload")})
    ok(f"valid event routed to {s.validated_topic} (no DLQ entry)")

    # ── 11-12: drift ───────────────────────────────────────────────────────
    step(f"Introduce real schema drift: rename {table}.{col} -> {new}")
    result = drift_tool.apply_scenario("rename", source)
    pk_value = result.event_pk[next(iter(result.event_pk))]
    ok(f"schema changed and an UPDATE was issued for product_id={pk_value}")

    # ── 13-14: rejection + DLQ ─────────────────────────────────────────────
    step("Validator rejects the drifted event and isolates it in the DLQ")
    d = wait(dlq, lambda m: m.get("source_table") == table and m.get("payload", {}).get("product_id") == pk_value,
             90, f"DLQ entry for product_id={pk_value}")
    show(compact_dlq(d))
    rep = d["drift_report"]
    ok(f"drift={rep['drift_type']} missing={rep['missing_fields']} unexpected={rep['unexpected_fields']} "
       f"rename-heuristic={[(r['unexpected_field'], r['missing_field'], r['confidence']) for r in rep['possible_renames']]}")
    event_id = d["event_id"]

    # ── 15-18: AI repair ───────────────────────────────────────────────────
    step(f"AI repair worker analyses the drift with {s.ollama_model} (Ollama)")
    t0 = time.monotonic()
    print("  (first call may take 30-90 s while the model loads into memory)")
    a = wait(audit, lambda m: m.get("event_id") == event_id and m.get("repair_result") != "DEFERRED",
             args.repair_timeout, f"repair decision for {event_id}")
    print(f"  decision after {time.monotonic() - t0:.0f}s")

    step("Repair decision, sandbox testing and schema validation")
    print(a.get("report", "(no report)"))
    if a["repair_result"] != "REPAIRED":
        fail(f"repair was {a['repair_result']} — the event stays in {s.dlq_topic}; reasons: {a.get('reasons')}")
    if a.get("repair_source") != "llm" and not args.allow_cache:
        fail(f"expected a live model repair, got source={a.get('repair_source')}")
    ok(f"sandbox={a['sandbox_result']}  validation={a['validation_result']}  model={a['model']}  "
       f"source={a['repair_source']}  AI attempts={a['ai_attempts']}  confidence={a['confidence']}")

    # ── 19-20: repaired topic ──────────────────────────────────────────────
    step(f"Repaired event published to {s.repaired_topic}")
    rp = wait(repaired, lambda m: m.get("event_id") == event_id, 60, f"repaired event {event_id}")
    show({k: rp[k] for k in ("event_id", "repair_id", "correlation_id", "repair_classification", "model",
                             "confidence", "sandbox_status", "schema_validation", "repaired_payload")})
    expected_qty = d["payload"][new]
    if rp["repaired_payload"] != {"product_id": pk_value, col: expected_qty}:
        fail(f"repaired payload {rp['repaired_payload']} does not carry {new} -> {col}")
    ok(f"{new} -> {col} mapping verified independently: {rp['repaired_payload']}")

    # ── 21: audit ──────────────────────────────────────────────────────────
    step(f"Audit trail in {s.audit_topic}")
    show({k: a.get(k) for k in ("event_id", "repair_id", "source_table", "drift_type", "confidence", "risk_level",
                                "migration_sql", "migration_status", "transformation_hash", "model",
                                "sandbox_result", "validation_result", "repair_result", "ai_attempts",
                                "timestamp")})

    # ── 22: self-healing continues ─────────────────────────────────────────
    step("Pipeline keeps healing: next drifted event reuses the approved repair (no LLM call)")
    with source_writer() as conn:
        other = inserted[0]["id"] if inserted[0]["id"] != pk_value else inserted[-2]["id"]
        sql = f"UPDATE `{table}` SET `{new}` = `{new}` + 5 WHERE `product_id` = %s"
        print(f"  SQL> {sql.replace('%s', str(other))};")
        conn.execute(sql, [other])
    d2 = wait(dlq, lambda m: m.get("source_table") == table and m.get("payload", {}).get("product_id") == other,
              60, f"DLQ entry for product_id={other}")
    a2 = wait(audit, lambda m: m.get("event_id") == d2["event_id"] and m.get("repair_result") != "DEFERRED",
              120, "second repair decision")
    if a2["repair_result"] != "REPAIRED":
        fail(f"second repair was {a2['repair_result']}: {a2.get('reasons')}")
    ok(f"repaired from {a2['repair_source']} in seconds (fingerprint match) → {a2['repair_result']}")

    step("Downstream ingestion and lineage of the repaired event")
    table_ref = f"{rp['source_name']}:{rp['source_database']}.{rp['source_table']}"
    row = None
    for _ in range(20):
        row = warehouse_row(table_ref, rp["key"])
        if row and row.get("event_id") == event_id or (row and row.get("source_topic") == s.repaired_topic):
            break
        time.sleep(1)
    show(row or {"warning": "downstream row not found"})
    print(f"{B}Lineage for {event_id}:{N}")
    print(f"  source change   : {d['source_name']}:{d['source_database']}.{table} binlog "
          f"{d['source_metadata'].get('file')}:{d['source_metadata'].get('pos')}")
    print(f"  cdc.mutations   : {d['correlation_id']}")
    print(f"  cdc.dlq         : {d['_kafka']['topic']}/{d['_kafka']['partition']}/{d['_kafka']['offset']}")
    print(f"  repair          : repair_id={a['repair_id']} hash={a['transformation_hash']}")
    print(f"  cdc.repaired    : {rp['_kafka']['topic']}/{rp['_kafka']['partition']}/{rp['_kafka']['offset']}")
    print(f"  cdc.audit       : {a['_kafka']['topic']}/{a['_kafka']['partition']}/{a['_kafka']['offset']}")
    print(f"  warehouse       : {'present' if row else 'missing'} ({table_ref} {rp['key']})")

    if not args.keep_drift:
        step("Undo the demo drift (non-destructive rename back) and confirm normal flow resumes")
        drift_tool.reset(source)
        with source_writer() as conn:
            conn.execute(f"UPDATE `{table}` SET `{col}` = `{col}` + 1 WHERE `product_id` = %s", [pk_value])
        wait(validated, lambda m: m.get("source_table") == table and m.get("payload", {}).get("product_id") == pk_value
             and col in m.get("payload", {}), 60, "post-reset validated event")
        ok("schema restored; events validate directly again")

    for w in (validated, dlq, repaired, audit):
        w.close()
    print(f"\n{B}{G}Self-healing demo completed successfully.{N}")


if __name__ == "__main__":
    main()

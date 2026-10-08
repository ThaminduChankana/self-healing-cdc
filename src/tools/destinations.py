"""Inspect configured destinations and what landed in them.

    python -m src.tools.destinations list
    python -m src.tools.destinations show inventory.inventory.products_on_hand '{"product_id": 134}'
    python -m src.tools.destinations counts

Run inside the downstream container to read its local files:
    docker exec cdc-downstream python -m src.tools.destinations show <table_ref> '<key json>'
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from src.common.sources import ContractRegistry
from src.sinks.iceberg_sink import IcebergSink
from src.sinks.jsonl_sink import JsonlLakeSink
from src.sinks.registry import build_sink, load_destinations
from src.sinks.sqlite_sink import SqliteSink


def _jsonl_rows(root: Path, table_ref: str) -> list[dict[str, Any]]:
    source, db, table = table_ref.split(".", 2)
    rows = []
    for f in sorted((root / source / db / table).glob("dt=*/part-*.jsonl")):
        rows.extend(json.loads(line) for line in f.read_text().splitlines() if line.strip())
    return rows


def show(table_ref: str, key: dict[str, Any]) -> dict[str, Any]:
    contracts = ContractRegistry()
    contract = next((c for c in contracts.all() if c.key == table_ref), None)
    if contract is None:
        raise SystemExit(f"unknown table {table_ref}; known: {[c.key for c in contracts.all()]}")
    out: dict[str, Any] = {}
    for cfg in load_destinations():
        if not cfg.enabled or not cfg.routes(table_ref):
            out[cfg.name] = "disabled or not routed"
            continue
        try:
            sink = build_sink(cfg)
            if isinstance(sink, SqliteSink):
                sink.open([contract])
                out[cfg.name] = sink.row(table_ref, key)
                sink.close()
            elif isinstance(sink, JsonlLakeSink):
                hits = [r for r in _jsonl_rows(sink.root, table_ref) if r.get("_key") == key]
                out[cfg.name] = {"changes": len(hits), "latest": max(hits, key=lambda r: r["_seq"]) if hits else None}
            elif isinstance(sink, IcebergSink):
                sink.open([contract])
                rows = [r for r in sink.read(contract, current_only=False)
                        if all(r.get(k) == v for k, v in key.items())]
                out[cfg.name] = {"table": sink.identifier(contract), "mode": sink.mode,
                                 "row": max(rows, key=lambda r: r["_seq"]) if rows else None}
            else:
                out[cfg.name] = f"{sink.type_name}: query it with the destination's own tools"
        except Exception as exc:  # noqa: BLE001
            out[cfg.name] = f"error: {type(exc).__name__}: {exc}"
    return out


def counts() -> dict[str, Any]:
    contracts = ContractRegistry().all()
    out: dict[str, Any] = {}
    for cfg in load_destinations():
        if not cfg.enabled:
            out[cfg.name] = "disabled"
            continue
        routed = [c for c in contracts if cfg.routes(c.key)]
        sink = build_sink(cfg)
        try:
            if isinstance(sink, SqliteSink):
                sink.open(routed)
                out[cfg.name] = {c.key: sink.count(c.key) for c in routed}
            elif isinstance(sink, JsonlLakeSink):
                out[cfg.name] = {c.key: len(_jsonl_rows(sink.root, c.key)) for c in routed}
            elif isinstance(sink, IcebergSink):
                sink.open(routed)
                out[cfg.name] = {sink.identifier(c): len(sink.read(c)) for c in routed}
            else:
                out[cfg.name] = sink.type_name
        except Exception as exc:  # noqa: BLE001
            out[cfg.name] = f"error: {exc}"
    return out


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in ("list", "show", "counts"):
        print(__doc__)
        sys.exit(1)
    if sys.argv[1] == "list":
        for cfg in load_destinations():
            print(f"{'●' if cfg.enabled else '○'} {cfg.name:<14} type={cfg.type:<9} tables={cfg.tables} "
                  f"batch={cfg.batch_size}")
    elif sys.argv[1] == "counts":
        print(json.dumps(counts(), indent=2, default=str))
    else:
        print(json.dumps(show(sys.argv[2], json.loads(sys.argv[3])), indent=2, default=str))


if __name__ == "__main__":
    main()

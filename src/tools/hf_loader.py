"""Seed the source database with real rows from a Hugging Face dataset.

Rows are fetched through the public datasets-server REST API (no large
download, no `datasets` dependency), cached under data/hf/ so later runs work
offline, and inserted with parameterised statements by the *simulated
upstream application* user. The loader only INSERTs — it never drops,
truncates or alters anything.

Mapping (dataset column -> table column, transforms) lives in
config/sources.yaml under `seed_data`, so other sources/datasets need no code.

    python -m src.tools.hf_loader                  # load `rows` new products
    python -m src.tools.hf_loader --rows 5 --offline
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Callable

import requests

from src.common.config import PROJECT_ROOT
from src.common.database import DatabaseConnector, source_writer
from src.common.sources import SourceConfig, load_sources
from src.tools.inspect_source import column_exists

API = "https://datasets-server.huggingface.co/rows"
CACHE_DIR = PROJECT_ROOT / "data" / "hf"
PAGE = 100

_UNIT_TO_KG = {"pound": 0.453592, "pounds": 0.453592, "lb": 0.453592, "lbs": 0.453592,
               "ounce": 0.0283495, "ounces": 0.0283495, "oz": 0.0283495,
               "kilogram": 1.0, "kilograms": 1.0, "kg": 1.0, "gram": 0.001, "grams": 0.001, "g": 0.001}


def weight_to_kg(value: Any) -> float | None:
    """'1.5 pounds' -> 0.68; returns None when the value cannot be parsed."""
    if not isinstance(value, str):
        return None
    m = re.search(r"([\d.]+)\s*([A-Za-z]+)", value.replace(",", ""))
    if not m:
        return None
    try:
        factor = _UNIT_TO_KG.get(m.group(2).lower())
        return round(float(m.group(1)) * factor, 3) if factor else None
    except ValueError:
        return None


def stable_quantity(value: Any) -> int:
    """Deterministic pseudo stock level (0..199) derived from a stable row id."""
    return int(hashlib.sha256(str(value).encode()).hexdigest(), 16) % 200


TRANSFORMS: dict[str, Callable[[Any], Any]] = {"weight_to_kg": weight_to_kg, "stable_quantity": stable_quantity}


def fetch_rows(seed: dict[str, Any], count: int, offset: int = 0, offline: bool = False) -> list[dict[str, Any]]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    slug = re.sub(r"[^A-Za-z0-9]+", "_", seed["dataset"])
    cache = CACHE_DIR / f"{slug}_{seed.get('split', 'train')}.json"
    cached: list[dict[str, Any]] = json.loads(cache.read_text()) if cache.exists() else []
    needed = offset + count
    if len(cached) < needed and not offline:
        while len(cached) < needed:
            params = {"dataset": seed["dataset"], "config": seed.get("config", "default"),
                      "split": seed.get("split", "train"), "offset": len(cached), "length": PAGE}
            resp = requests.get(API, params=params, timeout=30)
            resp.raise_for_status()
            rows = [r["row"] for r in resp.json().get("rows", [])]
            if not rows:
                break
            # Keep only text columns (drop images / signed URLs) — smaller and nothing time-limited.
            cached.extend({k: v for k, v in r.items() if isinstance(v, (str, int, float)) or v is None} for r in rows)
        cache.write_text(json.dumps(cached, ensure_ascii=False, indent=1))
    if len(cached) < needed:
        raise SystemExit(f"only {len(cached)} cached rows available (need {needed}); run without --offline")
    return cached[offset:needed]


def _map(row: dict[str, Any], spec: dict[str, Any]) -> Any:
    sources = spec["from"] if isinstance(spec["from"], list) else [spec["from"]]
    value = next((row.get(s) for s in sources if row.get(s) not in (None, "")), None)
    if spec.get("transform"):
        value = TRANSFORMS[spec["transform"]](value)
    if isinstance(value, str):
        value = " ".join(value.split())
        if spec.get("max_length"):
            value = value[: spec["max_length"]]
    return value


def load(source: SourceConfig, rows: list[dict[str, Any]], conn: DatabaseConnector) -> list[dict[str, Any]]:
    seed = source.seed_data
    parent, child = seed["parent"], seed.get("child")
    db = source.database
    for col in parent["columns"]:
        if not column_exists(conn, db, parent["table"], col):
            raise SystemExit(f"{db}.{parent['table']}.{col} does not exist — check seed_data mapping")
    child_cols = []
    if child:
        child_cols = [c for c in child["columns"] if column_exists(conn, db, child["table"], c)]
        skipped = set(child["columns"]) - set(child_cols)
        if skipped:
            print(f"  ! {db}.{child['table']} has no column(s) {sorted(skipped)} (schema drift active?) — skipping them")

    inserted = []
    for row in rows:
        values = {col: _map(row, spec) for col, spec in parent["columns"].items()}
        if not values.get("name"):
            continue
        if conn.query(f"SELECT 1 FROM `{parent['table']}` WHERE name = %s LIMIT 1", [values["name"]]):
            continue  # idempotent: same product already loaded
        cols = list(values)
        conn.execute(f"INSERT INTO `{parent['table']}` ({', '.join(f'`{c}`' for c in cols)}) "
                     f"VALUES ({', '.join(['%s'] * len(cols))})", [values[c] for c in cols])
        new_id = conn.query("SELECT LAST_INSERT_ID() AS id")[0]["id"]
        record = {"id": new_id, **values}
        if child and child_cols:
            child_values = {c: _map(row, child["columns"][c]) for c in child_cols}
            conn.execute(
                f"INSERT INTO `{child['table']}` (`{child['key_column']}`, {', '.join(f'`{c}`' for c in child_cols)}) "
                f"VALUES (%s, {', '.join(['%s'] * len(child_cols))})", [new_id, *child_values.values()])
            record.update(child_values)
        inserted.append(record)
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=None, help="source name in config/sources.yaml")
    parser.add_argument("--rows", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--offline", action="store_true", help="use cached rows only")
    parser.add_argument("--json", action="store_true", help="print inserted rows as JSON")
    args = parser.parse_args()
    sources = load_sources()
    source = sources[args.source] if args.source else next(iter(sources.values()))
    seed = source.seed_data
    if seed.get("provider") != "huggingface":
        raise SystemExit(f"source {source.name} has no huggingface seed_data")
    count = args.rows or int(seed.get("rows", 25))
    rows = fetch_rows(seed, count, args.offset, args.offline)
    with source_writer() as conn:
        inserted = load(source, rows, conn)
    if args.json:
        print(json.dumps(inserted, ensure_ascii=False))
        return
    print(f"Hugging Face dataset: {seed['dataset']} (split={seed.get('split', 'train')})")
    print(f"Fetched {len(rows)} rows, inserted {len(inserted)} new products into {source.database}.{seed['parent']['table']}")
    for r in inserted[:10]:
        w = f"{r.get('weight')} kg" if r.get("weight") is not None else "n/a"
        qty = r.get("quantity", "-")
        print(f"  #{r['id']:<5} qty={qty!s:<4} weight={w:<10} {r['name'][:70]}")
    if len(inserted) > 10:
        print(f"  ... and {len(inserted) - 10} more")


if __name__ == "__main__":
    main()

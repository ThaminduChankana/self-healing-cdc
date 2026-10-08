"""Controlled, real schema drift against the running source database.

Scenarios come from `drift_scenarios` in config/sources.yaml and are always
re-verified against information_schema before any SQL runs. Every statement
is printed before it is executed. No scenario contains a destructive
statement; `reset` only reverses the demo's own changes and refuses to drop
the demo-added column unless --allow-drop-demo-column is given.

    python -m src.tools.drift rename|add-column|widen-type|reset|status [--source S] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any

from src.common.database import DatabaseConnector, source_reader, source_writer
from src.common.sources import SourceConfig, load_sources
from src.tools.inspect_source import column_exists

INT_RANGES = {"INT": 2**31 - 1, "SMALLINT": 2**15 - 1, "MEDIUMINT": 2**23 - 1, "TINYINT": 127}


class DriftError(Exception):
    pass


@dataclass
class DriftResult:
    scenario: str
    table: str
    statements: list[str]
    event_pk: dict[str, Any] | None = None


def _q(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise DriftError(f"unsafe identifier {name!r}")
    return f"`{name}`"


def _pk(source: SourceConfig, table: str) -> str:
    for t in source.tables:
        if t.name == table and t.primary_key:
            return t.primary_key[0]
    raise DriftError(f"table {table} is not declared in sources.yaml")


def _run(conn: DatabaseConnector | None, sql: str, params: list[Any] | None, dry: bool) -> None:
    shown = sql
    for p in params or []:
        shown = shown.replace("%s", repr(p), 1)
    print(f"  SQL> {shown};")
    if not dry and conn is not None:
        conn.execute(sql, params)


def _touch(conn: DatabaseConnector, source: SourceConfig, table: str, column: str, kind: str,
           dry: bool, statements: list[str]) -> dict[str, Any]:
    """Generate one CDC UPDATE event on the newest row of `table`."""
    pk = _pk(source, table)
    row = conn.query(f"SELECT {_q(pk)} AS pk FROM {_q(table)} ORDER BY {_q(pk)} DESC LIMIT 1")
    if not row:
        raise DriftError(f"{table} has no rows to update")
    pk_value = row[0]["pk"]
    if kind == "increment":
        sql = f"UPDATE {_q(table)} SET {_q(column)} = {_q(column)} + 1 WHERE {_q(pk)} = %s"
        params = [pk_value]
    else:
        sql = f"UPDATE {_q(table)} SET {_q(column)} = %s WHERE {_q(pk)} = %s"
        params = [f"SUP-{pk_value}", pk_value]
    _run(conn, sql, params, dry)
    statements.append(sql)
    return {pk: pk_value}


def apply_scenario(name: str, source: SourceConfig, dry: bool = False) -> DriftResult:
    sc = source.drift_scenarios.get(name)
    if not sc:
        raise DriftError(f"scenario '{name}' is not configured for source {source.name}")
    db, table = source.database, sc["table"]
    statements: list[str] = []
    with source_reader() as reader:
        col = column_exists(reader, db, table, sc["column"])
        if name == "rename":
            if column_exists(reader, db, table, sc["new_name"]):
                raise DriftError(f"{table}.{sc['new_name']} already exists — run reset first")
            if not col:
                raise DriftError(f"{db}.{table}.{sc['column']} not found (discovered columns differ)")
        elif name == "add-column":
            if col:
                raise DriftError(f"{table}.{sc['column']} already exists — run reset first")
        elif name == "widen-type":
            if not col:
                raise DriftError(f"{db}.{table}.{sc['column']} not found")
            if str(col["DATA_TYPE"]).upper() != sc["from_type"].upper():
                raise DriftError(f"{table}.{sc['column']} is {col['COLUMN_TYPE']}, expected {sc['from_type']}")
        nullable = col and col["IS_NULLABLE"] == "YES"

    print(f"Discovered: {db}.{table} "
          f"{'(' + sc['column'] + ' ' + str(col['COLUMN_TYPE']) + ')' if col else ''}")
    with source_writer() as conn:
        if name == "rename":
            sql = f"ALTER TABLE {_q(table)} RENAME COLUMN {_q(sc['column'])} TO {_q(sc['new_name'])}"
            _run(conn, sql, None, dry)
            statements.append(sql)
            pk = None if dry else _touch(conn, source, table, sc["new_name"], "increment", dry, statements)
        elif name == "add-column":
            definition = sc["definition"]
            if "NOT NULL" in definition.upper() and "DEFAULT" not in definition.upper():
                raise DriftError("demo add-column definition must be nullable or have a default")
            sql = f"ALTER TABLE {_q(table)} ADD COLUMN {_q(sc['column'])} {definition}"
            _run(conn, sql, None, dry)
            statements.append(sql)
            pk = None if dry else _touch(conn, source, table, sc["column"], "set", dry, statements)
        else:  # widen-type
            sql = (f"ALTER TABLE {_q(table)} MODIFY COLUMN {_q(sc['column'])} {sc['to_type']} "
                   f"{'NULL' if nullable else 'NOT NULL'}")
            _run(conn, sql, None, dry)
            statements.append(sql)
            pk = None if dry else _touch(conn, source, table, sc["column"], "increment", dry, statements)
    return DriftResult(name, f"{db}.{table}", statements, pk)


def reset(source: SourceConfig, allow_drop: bool = False, dry: bool = False) -> list[str]:
    db = source.database
    done: list[str] = []
    with source_reader() as reader, source_writer() as conn:
        rn = source.drift_scenarios.get("rename")
        if rn and column_exists(reader, db, rn["table"], rn["new_name"]) \
                and not column_exists(reader, db, rn["table"], rn["column"]):
            sql = f"ALTER TABLE {_q(rn['table'])} RENAME COLUMN {_q(rn['new_name'])} TO {_q(rn['column'])}"
            _run(conn, sql, None, dry)
            done.append(sql)
        wd = source.drift_scenarios.get("widen-type")
        if wd:
            col = column_exists(reader, db, wd["table"], wd["column"])
            if col and str(col["DATA_TYPE"]).upper() == wd["to_type"].upper():
                limit = INT_RANGES.get(wd["from_type"].upper())
                mx = reader.query(f"SELECT MAX(ABS({_q(wd['column'])})) AS m FROM {_q(wd['table'])}")[0]["m"] or 0
                if limit and mx > limit:
                    print(f"  ! not narrowing {wd['table']}.{wd['column']}: max value {mx} exceeds {wd['from_type']}")
                else:
                    sql = (f"ALTER TABLE {_q(wd['table'])} MODIFY COLUMN {_q(wd['column'])} {wd['from_type']} "
                           f"{'NULL' if col['IS_NULLABLE'] == 'YES' else 'NOT NULL'}")
                    _run(conn, sql, None, dry)
                    done.append(sql)
        ad = source.drift_scenarios.get("add-column")
        if ad and column_exists(reader, db, ad["table"], ad["column"]):
            sql = f"ALTER TABLE {_q(ad['table'])} DROP COLUMN {_q(ad['column'])}"
            if allow_drop:
                print("  ! removing the column added by the add-column demo (explicitly allowed)")
                _run(conn, sql, None, dry)
                done.append(sql)
            else:
                print(f"  ! {ad['table']}.{ad['column']} was added by the add-column demo. Removing it is a "
                      f"destructive statement and is NOT run automatically.\n"
                      f"    Re-run with --allow-drop-demo-column to execute: {sql};")
    if not done:
        print("  Nothing to reset.")
    return done


def status(source: SourceConfig) -> dict[str, Any]:
    out: dict[str, Any] = {}
    with source_reader() as reader:
        for name, sc in source.drift_scenarios.items():
            cols = {c: column_exists(reader, source.database, sc["table"], c)
                    for c in {sc["column"], sc.get("new_name") or sc["column"]}}
            out[name] = {c: (v["COLUMN_TYPE"] if v else None) for c, v in cols.items()}
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["rename", "add-column", "widen-type", "reset", "status"])
    parser.add_argument("--source", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-drop-demo-column", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    sources = load_sources()
    source = sources[args.source] if args.source else next(iter(sources.values()))
    try:
        if args.action == "status":
            print(json.dumps(status(source), indent=2, default=str))
        elif args.action == "reset":
            reset(source, args.allow_drop_demo_column, args.dry_run)
        else:
            result = apply_scenario(args.action, source, args.dry_run)
            if args.json:
                print(json.dumps(result.__dict__, default=str))
            else:
                print(f"✓ drift '{result.scenario}' applied to {result.table}; CDC event generated for {result.event_pk}")
    except DriftError as exc:
        print(f"✗ {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()

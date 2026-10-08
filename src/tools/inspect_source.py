"""Inspect a source database (read-only user) and generate canonical contracts.

    python -m src.tools.inspect_source                       # list tables + columns
    python -m src.tools.inspect_source --table products      # one table
    python -m src.tools.inspect_source --generate products --out config/canonical_schema/products.json

Contracts are generated from information_schema of the *running* database so
they never rely on assumed column names.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from src.common.config import get_settings
from src.common.database import DatabaseConnector, source_reader

_JSON_TYPES = {
    "tinyint": "integer", "smallint": "integer", "mediumint": "integer", "int": "integer", "bigint": "integer",
    "float": "number", "double": "number", "decimal": "string",
    "char": "string", "varchar": "string", "text": "string", "mediumtext": "string", "longtext": "string",
    "enum": "string", "date": "integer", "datetime": "integer", "timestamp": "string", "json": "string",
}
_CONNECT_TYPES = {
    "tinyint": "int16", "smallint": "int16", "mediumint": "int32", "int": "int32", "bigint": "int64",
    "float": "float", "double": "double", "decimal": "string",
}


def describe(conn: DatabaseConnector, database: str, table: str | None = None) -> dict[str, list[dict[str, Any]]]:
    sql = ("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, "
           "CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s")
    params: list[Any] = [database]
    if table:
        sql += " AND TABLE_NAME = %s"
        params.append(table)
    out: dict[str, list[dict[str, Any]]] = {}
    for r in conn.query(sql + " ORDER BY TABLE_NAME, ORDINAL_POSITION", params):
        out.setdefault(r["TABLE_NAME"], []).append(r)
    return out


def column_exists(conn: DatabaseConnector, database: str, table: str, column: str) -> dict[str, Any] | None:
    rows = conn.query(
        "SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s", (database, table, column))
    return rows[0] if rows else None


def generate_contract(database: str, table: str, columns: list[dict[str, Any]]) -> dict[str, Any]:
    props: dict[str, Any] = {}
    pk = [c["COLUMN_NAME"] for c in columns if c["COLUMN_KEY"] == "PRI"]
    for c in columns:
        dt = str(c["DATA_TYPE"]).lower()
        nullable = c["IS_NULLABLE"] == "YES"
        jtype = _JSON_TYPES.get(dt, "string")
        sql_type = re.sub(r"\(\d+\)", "", str(c["COLUMN_TYPE"]).upper()) if dt in _CONNECT_TYPES and dt != "decimal" \
            else str(c["COLUMN_TYPE"]).upper()
        prop: dict[str, Any] = {"type": [jtype, "null"] if nullable else jtype, "x-sql-type": sql_type,
                                "x-nullable": nullable}
        if dt in _CONNECT_TYPES:
            prop["x-connect-type"] = _CONNECT_TYPES[dt]
        elif jtype == "string":
            prop["x-connect-type"] = "string"
        if c.get("CHARACTER_MAXIMUM_LENGTH") and dt in ("char", "varchar"):
            prop["maxLength"] = int(c["CHARACTER_MAXIMUM_LENGTH"])
        props[c["COLUMN_NAME"]] = prop
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"urn:cdc:contract:{database}.{table}:v1",
        "title": f"{database}.{table}",
        "type": "object",
        "x-source": {"database": database, "table": table, "primary_key": pk},
        "properties": props,
        "required": list(props),
        "additionalProperties": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default=None)
    parser.add_argument("--table")
    parser.add_argument("--generate", metavar="TABLE")
    parser.add_argument("--out")
    args = parser.parse_args()
    database = args.database or get_settings().mysql_database
    with source_reader() as conn:
        if args.generate:
            cols = describe(conn, database, args.generate).get(args.generate)
            if not cols:
                raise SystemExit(f"table {database}.{args.generate} not found")
            contract = json.dumps(generate_contract(database, args.generate, cols), indent=2)
            if args.out:
                Path(args.out).write_text(contract + "\n")
                print(f"wrote {args.out}")
            else:
                print(contract)
            return
        for table, cols in describe(conn, database, args.table).items():
            print(f"\n── {database}.{table}")
            for c in cols:
                key = " PK" if c["COLUMN_KEY"] == "PRI" else ""
                print(f"   {c['COLUMN_NAME']:<20} {c['COLUMN_TYPE']:<16} "
                      f"{'NULL' if c['IS_NULLABLE'] == 'YES' else 'NOT NULL'}{key}")
            n = conn.query(f"SELECT COUNT(*) AS n FROM `{database}`.`{table}`")[0]["n"]
            print(f"   rows: {n}")


if __name__ == "__main__":
    main()

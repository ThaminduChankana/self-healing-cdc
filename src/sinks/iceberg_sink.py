"""Apache Iceberg destination (open lakehouse table format) via pyiceberg.

One Iceberg table per routed source table: <namespace>.<source>__<database>__<table>.
Columns = canonical contract fields + _op/_seq/_event_id/_repair_id/_origin/
_source_ts_ms/_ingested_at.

Modes
  upsert     current state per primary key (MERGE semantics). Deletes are kept
             as tombstones (_op = 'd') so a late, older change cannot resurrect
             a deleted row; query current state with  WHERE _op <> 'd'.
             An older _seq never overwrites a newer one.
  changelog  append every change (bronze layer); readers pick max(_seq) per key.

The catalog is pluggable through `options.catalog` (passed to
pyiceberg.catalog.load_catalog): local SQL catalog here; AWS Glue, REST
(Polaris/Tabular/Nessie), Hive or BigLake metastore in the cloud.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.common.sources import Contract
from src.sinks.base import ChangeRecord, Sink, column_family, latest_per_key, safe_name

META_COLUMNS: list[tuple[str, str]] = [
    ("_op", "string"), ("_seq", "integer"), ("_event_id", "string"), ("_repair_id", "string"),
    ("_origin", "string"), ("_source_ts_ms", "integer"), ("_ingested_at", "string"),
]


class IcebergSink(Sink):
    type_name = "iceberg"

    def __init__(self, name: str, options: dict[str, Any]):
        super().__init__(name, options)
        self.namespace = options.get("namespace", "cdc")
        self.mode = options.get("mode", "upsert")
        if self.mode not in ("upsert", "changelog"):
            raise ValueError(f"destination {name}: iceberg mode must be upsert|changelog")
        self.catalog_props = dict(options.get("catalog") or {})
        if not self.catalog_props:
            raise ValueError(f"destination {name}: iceberg sink needs options.catalog")
        self._catalog = None
        self._tables: dict[str, Any] = {}

    def identifier(self, contract: Contract) -> str:
        return f"{self.namespace}.{safe_name(contract.source, contract.database, contract.table)}"

    def _prepare_local_paths(self) -> None:
        uri, wh = self.catalog_props.get("uri", ""), self.catalog_props.get("warehouse", "")
        if uri.startswith("sqlite:///"):
            Path(uri[len("sqlite:///"):]).parent.mkdir(parents=True, exist_ok=True)
        if wh.startswith("file://"):
            Path(urlparse(wh).path).mkdir(parents=True, exist_ok=True)

    def open(self, contracts: list[Contract]) -> None:
        from pyiceberg.catalog import load_catalog  # optional dependency

        self._prepare_local_paths()
        props = dict(self.catalog_props)
        catalog_name = props.pop("name", self.name)
        self._catalog = load_catalog(catalog_name, **props)
        self._catalog.create_namespace_if_not_exists(self.namespace)
        for c in contracts:
            self._tables[c.key] = self._catalog.create_table_if_not_exists(self.identifier(c), schema=self._schema(c))

    @staticmethod
    def _schema(contract: Contract):
        from pyiceberg.schema import Schema
        from pyiceberg.types import BooleanType, DoubleType, LongType, NestedField, StringType

        types = {"integer": LongType(), "number": DoubleType(), "boolean": BooleanType(), "string": StringType()}
        cols = [(n, column_family(contract, n)) for n in contract.fields] + META_COLUMNS
        return Schema(*[NestedField(field_id=i, name=n, field_type=types[t], required=False)
                        for i, (n, t) in enumerate(cols, start=1)])

    def table_for(self, contract: Contract):
        if contract.key not in self._tables:
            assert self._catalog is not None, "open() not called"
            self._tables[contract.key] = self._catalog.create_table_if_not_exists(
                self.identifier(contract), schema=self._schema(contract))
        return self._tables[contract.key]

    @staticmethod
    def _row(r: ChangeRecord, contract: Contract) -> dict[str, Any]:
        base = {f: None for f in contract.fields}
        base.update({k: v for k, v in (r.payload or {}).items() if k in contract.fields})
        base.update({k: v for k, v in r.key.items() if k in contract.fields})
        return {**base, **r.meta_columns()}

    def write(self, records: list[ChangeRecord], contracts: dict[str, Contract]) -> None:
        import pyarrow as pa

        by_table: dict[str, list[ChangeRecord]] = {}
        for r in records:
            by_table.setdefault(r.table_ref, []).append(r)
        for table_ref, batch in by_table.items():
            contract = contracts[table_ref]
            table = self.table_for(contract)
            if self.mode == "upsert":
                batch = self._drop_stale(table, contract, latest_per_key(batch))
                if not batch:
                    continue
            arrow = pa.Table.from_pylist([self._row(r, contract) for r in batch], schema=table.schema().as_arrow())
            if self.mode == "changelog":
                table.append(arrow)
            else:
                table.upsert(arrow, join_cols=contract.primary_key)

    @staticmethod
    def _drop_stale(table, contract: Contract, batch: list[ChangeRecord]) -> list[ChangeRecord]:
        """Last-writer-wins by _seq (single-column primary keys)."""
        if len(contract.primary_key) != 1:
            return batch
        from pyiceberg.expressions import In

        pk = contract.primary_key[0]
        keys = [r.key.get(pk) for r in batch if r.key.get(pk) is not None]
        if not keys:
            return batch
        existing = table.scan(row_filter=In(pk, keys), selected_fields=(pk, "_seq")).to_arrow().to_pylist()
        newest = {row[pk]: row["_seq"] for row in existing}
        return [r for r in batch if r.sequence >= (newest.get(r.key.get(pk)) or -1)]

    def read(self, contract: Contract, current_only: bool = True) -> list[dict[str, Any]]:
        rows = self.table_for(contract).scan().to_arrow().to_pylist()
        return [r for r in rows if r["_op"] != "d"] if current_only else rows

    def describe(self) -> dict[str, Any]:
        return {**super().describe(), "namespace": self.namespace, "mode": self.mode,
                "catalog": {k: v for k, v in self.catalog_props.items() if "secret" not in k and "token" not in k}}

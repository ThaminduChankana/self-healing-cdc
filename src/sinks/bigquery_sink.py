"""Google BigQuery destination.

Per routed source table:
  <dataset>.<source>__<database>__<table>_changelog   append-only change log
        (contract columns + _op/_seq/_event_id/...), partitioned by day
  <dataset>.<source>__<database>__<table>             view: latest row per primary
        key by _seq, deletes removed  (last-writer-wins, order-independent)

Rows are streamed with insertId = event_id, so BigQuery's best-effort
de-duplication absorbs retries. For high volume, swap the insert for the
Storage Write API with native CDC (_CHANGE_TYPE / _CHANGE_SEQUENCE_NUMBER = _seq)
— the record already carries everything that needs.

Install `google-cloud-bigquery` (requirements-optional.txt) and provide
Application Default Credentials. Nothing GCP-specific leaks into the pipeline.
"""

from __future__ import annotations

from typing import Any

from src.common.sources import Contract
from src.sinks.base import ChangeRecord, Sink, column_family, safe_name

_BQ_TYPES = {"integer": "INT64", "number": "FLOAT64", "boolean": "BOOL", "string": "STRING"}
_META = [("_op", "STRING"), ("_seq", "INT64"), ("_event_id", "STRING"), ("_repair_id", "STRING"),
         ("_origin", "STRING"), ("_source_ts_ms", "INT64"), ("_ingested_at", "TIMESTAMP")]


class BigQuerySink(Sink):
    type_name = "bigquery"

    def __init__(self, name: str, options: dict[str, Any], client: Any = None):
        super().__init__(name, options)
        self.project = options.get("project") or ""
        self.dataset = options.get("dataset") or "cdc"
        self.location = options.get("location") or "US"
        if not self.project:
            raise ValueError(f"destination {name}: bigquery sink needs options.project (BQ_PROJECT)")
        self._client = client

    def _q(self, name: str) -> str:
        return f"`{self.project}.{self.dataset}.{name}`"

    def base_name(self, contract: Contract) -> str:
        return safe_name(contract.source, contract.database, contract.table)

    def open(self, contracts: list[Contract]) -> None:
        if self._client is None:
            from google.cloud import bigquery  # optional dependency

            self._client = bigquery.Client(project=self.project, location=self.location)
        self._run(f"CREATE SCHEMA IF NOT EXISTS `{self.project}.{self.dataset}` OPTIONS(location='{self.location}')")
        for c in contracts:
            self._run(self.changelog_ddl(c))
            self._run(self.view_ddl(c))

    def changelog_ddl(self, c: Contract) -> str:
        cols = [f"`{n}` {_BQ_TYPES[column_family(c, n)]}" for n in c.fields] + [f"`{n}` {t}" for n, t in _META]
        return (f"CREATE TABLE IF NOT EXISTS {self._q(self.base_name(c) + '_changelog')} ({', '.join(cols)}) "
                f"PARTITION BY DATE(_ingested_at) CLUSTER BY {', '.join(f'`{k}`' for k in c.primary_key[:4])}")

    def view_ddl(self, c: Contract) -> str:
        pk = ", ".join(f"`{k}`" for k in c.primary_key)
        return (f"CREATE OR REPLACE VIEW {self._q(self.base_name(c))} AS "
                f"SELECT * EXCEPT(_rn) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY _seq DESC) AS _rn "
                f"FROM {self._q(self.base_name(c) + '_changelog')}) WHERE _rn = 1 AND _op != 'd'")

    def _run(self, sql: str) -> None:
        self._client.query(sql).result()

    def write(self, records: list[ChangeRecord], contracts: dict[str, Contract]) -> None:
        by_table: dict[str, list[ChangeRecord]] = {}
        for r in records:
            by_table.setdefault(r.table_ref, []).append(r)
        for table_ref, batch in by_table.items():
            c = contracts[table_ref]
            rows = []
            for r in batch:
                row = {f: (r.payload or {}).get(f) for f in c.fields}
                row.update({k: v for k, v in r.key.items() if k in c.fields})
                rows.append({**row, **r.meta_columns()})
            table_id = f"{self.project}.{self.dataset}.{self.base_name(c)}_changelog"
            errors = self._client.insert_rows_json(table_id, rows, row_ids=[r.event_id for r in batch])
            if errors:
                raise RuntimeError(f"BigQuery rejected {len(errors)} row(s): {str(errors)[:500]}")

    def describe(self) -> dict[str, Any]:
        return {**super().describe(), "project": self.project, "dataset": self.dataset, "location": self.location}

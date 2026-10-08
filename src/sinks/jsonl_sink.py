"""Data-lake landing zone: append-only change log as JSON Lines files.

Layout:  <root>/<source>/<database>/<table>/dt=YYYY-MM-DD/part-<writer>.jsonl
Each line = canonical row + _op/_seq/_event_id/_repair_id/_origin metadata.
Readable as-is by Spark, DuckDB, Trino/Athena, BigQuery or Snowflake external
tables. Delivery is at-least-once at the file level; readers de-duplicate on
_event_id and take the highest _seq per primary key for current state.
"""

from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.sources import Contract
from src.sinks.base import ChangeRecord, Sink


class JsonlLakeSink(Sink):
    type_name = "jsonl"

    def __init__(self, name: str, options: dict[str, Any]):
        super().__init__(name, options)
        if not options.get("root"):
            raise ValueError(f"destination {name}: jsonl sink needs options.root")
        self.root = Path(options["root"])
        self._writer = f"{socket.gethostname()}-{os.getpid()}"

    def open(self, contracts: list[Contract]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, r: ChangeRecord, day: str) -> Path:
        return self.root / r.source / r.database / r.table / f"dt={day}" / f"part-{self._writer}.jsonl"

    def write(self, records: list[ChangeRecord], contracts: dict[str, Contract]) -> None:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        grouped: dict[Path, list[str]] = {}
        for r in records:
            line = {**(r.payload or {}), **r.meta_columns(), "_key": r.key}
            grouped.setdefault(self.path_for(r, day), []).append(json.dumps(line, default=str, ensure_ascii=False))
        for path, lines in grouped.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
                fh.flush()
                os.fsync(fh.fileno())

    def describe(self) -> dict[str, Any]:
        return {**super().describe(), "root": str(self.root)}

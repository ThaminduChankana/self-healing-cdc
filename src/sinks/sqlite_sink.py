"""SQLite current-state store (local stand-in for an operational warehouse).

Last-writer-wins by source sequence: an older change that arrives late (e.g. a
repaired event delivered after a newer valid change) never overwrites newer
data. Deletes are kept as tombstones so a late, older upsert cannot resurrect
a deleted row.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from src.common.models import utc_now
from src.common.sources import Contract
from src.sinks.base import ChangeRecord, Sink, latest_per_key


class SqliteSink(Sink):
    type_name = "sqlite"

    def __init__(self, name: str, options: dict[str, Any]):
        super().__init__(name, options)
        if not options.get("path"):
            raise ValueError(f"destination {name}: sqlite sink needs options.path")
        self.path = Path(options["path"])
        self._conn: sqlite3.Connection | None = None

    def open(self, contracts: list[Contract]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS current_rows (
                table_ref TEXT NOT NULL, pk TEXT NOT NULL, payload TEXT, deleted INTEGER NOT NULL DEFAULT 0,
                seq INTEGER NOT NULL, event_id TEXT, repair_id TEXT, origin TEXT, updated_at TEXT,
                PRIMARY KEY (table_ref, pk)
            );
        """)

    def write(self, records: list[ChangeRecord], contracts: dict[str, Contract]) -> None:
        assert self._conn is not None, "open() not called"
        rows = [(r.table_ref, r.pk_json, None if r.is_delete else json.dumps(r.payload, sort_keys=True, default=str),
                 1 if r.is_delete else 0, r.sequence, r.event_id, r.repair_id, r.origin, utc_now())
                for r in latest_per_key(records)]
        self._conn.execute("BEGIN")
        try:
            self._conn.executemany(
                "INSERT INTO current_rows (table_ref, pk, payload, deleted, seq, event_id, repair_id, origin, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(table_ref, pk) DO UPDATE SET payload=excluded.payload, "
                "deleted=excluded.deleted, seq=excluded.seq, event_id=excluded.event_id, repair_id=excluded.repair_id, "
                "origin=excluded.origin, updated_at=excluded.updated_at WHERE excluded.seq >= current_rows.seq",
                rows,
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def row(self, table_ref: str, key: dict[str, Any]) -> dict[str, Any] | None:
        assert self._conn is not None
        r = self._conn.execute(
            "SELECT payload, deleted, seq, event_id, repair_id, origin, updated_at FROM current_rows "
            "WHERE table_ref=? AND pk=?", (table_ref, json.dumps(key, sort_keys=True, default=str))).fetchone()
        if not r:
            return None
        return {"payload": json.loads(r[0]) if r[0] else None, "deleted": bool(r[1]), "seq": r[2],
                "event_id": r[3], "repair_id": r[4], "origin": r[5], "updated_at": r[6]}

    def count(self, table_ref: str | None = None) -> int:
        assert self._conn is not None
        if table_ref:
            return self._conn.execute("SELECT COUNT(*) FROM current_rows WHERE table_ref=? AND deleted=0",
                                      (table_ref,)).fetchone()[0]
        return self._conn.execute("SELECT COUNT(*) FROM current_rows WHERE deleted=0").fetchone()[0]

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    def describe(self) -> dict[str, Any]:
        return {**super().describe(), "path": str(self.path)}

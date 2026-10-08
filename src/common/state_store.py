"""Local state store: idempotency ledger + approved-repair cache.

SQLite is enough for the single-node MVP (no Redis). The `StateStore`
interface is what the worker depends on; a DynamoDB / RDS implementation can
replace it in the cloud.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from src.common.models import utc_now

TERMINAL_STATUSES = {"REPAIRED", "REJECTED", "MANUAL_REVIEW"}


class StateStore(ABC):
    @abstractmethod
    def get_status(self, event_id: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def is_terminal(self, event_id: str) -> bool: ...

    @abstractmethod
    def record(self, event_id: str, repair_id: str, status: str, detail: str = "") -> None: ...

    @abstractmethod
    def record_deferral(self, event_id: str, reason: str) -> int: ...

    @abstractmethod
    def get_cached_repair(self, fingerprint: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def put_cached_repair(self, fingerprint: str, repair: dict[str, Any]) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


class SQLiteStateStore(StateStore):
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id    TEXT PRIMARY KEY,
                repair_id   TEXT,
                status      TEXT NOT NULL,
                detail      TEXT,
                deferrals   INTEGER NOT NULL DEFAULT 0,
                updated_at  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS repair_cache (
                fingerprint TEXT PRIMARY KEY,
                repair      TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );
        """)

    def get_status(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT event_id, repair_id, status, detail, deferrals, updated_at "
                "FROM processed_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        if not row:
            return None
        keys = ["event_id", "repair_id", "status", "detail", "deferrals", "updated_at"]
        return dict(zip(keys, row))

    def is_terminal(self, event_id: str) -> bool:
        status = self.get_status(event_id)
        return bool(status and status["status"] in TERMINAL_STATUSES)

    def record(self, event_id: str, repair_id: str, status: str, detail: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO processed_events (event_id, repair_id, status, detail, updated_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET repair_id=excluded.repair_id, "
                "status=excluded.status, detail=excluded.detail, updated_at=excluded.updated_at",
                (event_id, repair_id, status, detail[:2000], utc_now()),
            )

    def record_deferral(self, event_id: str, reason: str) -> int:
        with self._lock:
            self._conn.execute(
                "INSERT INTO processed_events (event_id, repair_id, status, detail, deferrals, updated_at) "
                "VALUES (?, '', 'DEFERRED', ?, 1, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET deferrals = deferrals + 1, "
                "detail=excluded.detail, updated_at=excluded.updated_at",
                (event_id, reason[:2000], utc_now()),
            )
            row = self._conn.execute(
                "SELECT deferrals FROM processed_events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return int(row[0]) if row else 0

    def get_cached_repair(self, fingerprint: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT repair FROM repair_cache WHERE fingerprint = ?", (fingerprint,)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def put_cached_repair(self, fingerprint: str, repair: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO repair_cache (fingerprint, repair, created_at) VALUES (?, ?, ?)",
                (fingerprint, json.dumps(repair, default=str), utc_now()),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

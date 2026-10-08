"""Destination-neutral change records and the Sink interface.

Both clean topics are normalised into one `ChangeRecord` shape, so a sink never
needs to know whether a row passed validation directly or was repaired by AI.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.common.models import utc_now
from src.common.sources import Contract

ROW_OPS = {"c", "u", "r", "d"}
_BINLOG_RE = re.compile(r"(\d+)$")


class RecordError(ValueError):
    """A clean-topic message that cannot be turned into a ChangeRecord."""


@dataclass
class ChangeRecord:
    event_id: str
    source: str
    database: str
    table: str
    op: str                       # c | u | r | d
    key: dict[str, Any]
    payload: dict[str, Any] | None  # canonical row (None only for deletes without an image)
    sequence: int                 # monotonic per source: binlog file/pos/row (or ts fallback)
    origin: str                   # validated | repaired
    repair_id: str | None = None
    correlation_id: str = ""
    contract_id: str = ""
    source_ts_ms: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def table_ref(self) -> str:
        """Routing / naming key: <source>.<database>.<table>."""
        return f"{self.source}.{self.database}.{self.table}"

    @property
    def pk_json(self) -> str:
        return json.dumps(self.key, sort_keys=True, default=str)

    @property
    def is_delete(self) -> bool:
        return self.op == "d"

    def meta_columns(self) -> dict[str, Any]:
        return {
            "_op": self.op,
            "_seq": self.sequence,
            "_event_id": self.event_id,
            "_repair_id": self.repair_id,
            "_origin": self.origin,
            "_source_ts_ms": self.source_ts_ms,
            "_ingested_at": utc_now(),
        }


def source_sequence(meta: dict[str, Any], ts_ms: int | None) -> int:
    """Order changes of one source: binlog file number, position, row index.

    Snapshot rows share a position; they are all older than any streamed change
    after the snapshot, which is what matters for last-writer-wins.
    """
    file, pos, row = meta.get("file"), meta.get("pos"), meta.get("row") or 0
    if file and pos is not None:
        m = _BINLOG_RE.search(str(file))
        file_no = int(m.group(1)) if m else 0
        return file_no * 10**15 + int(pos) * 1000 + int(row)
    if meta.get("lsn") is not None:  # postgres
        return int(meta["lsn"])
    return int(ts_ms or meta.get("ts_ms") or 0) * 1000


def to_record(topic_kind: str, value: dict[str, Any]) -> ChangeRecord:
    """Normalise a cdc.validated (ValidatedEvent) or cdc.repaired (RepairedEvent) message."""
    if not isinstance(value, dict) or not value.get("event_id"):
        raise RecordError("message has no event_id")
    if topic_kind == "repaired":
        src_event = value.get("source_event") or {}
        meta = src_event.get("source_metadata") or {}
        payload = value.get("repaired_payload")
        repair_id = value.get("repair_id")
    else:
        meta = value.get("source_metadata") or {}
        payload = value.get("payload")
        repair_id = None
    op = value.get("operation", "")
    if op not in ROW_OPS:
        raise RecordError(f"not a row change (op={op!r})")
    return ChangeRecord(
        event_id=value["event_id"],
        source=value.get("source_name") or meta.get("name") or "unknown",
        database=value.get("source_database") or meta.get("db") or "",
        table=value.get("source_table") or meta.get("table") or "",
        op=op,
        key=value.get("key") or {},
        payload=payload,
        sequence=source_sequence(meta, value.get("ts_ms")),
        origin=topic_kind,
        repair_id=repair_id,
        correlation_id=value.get("correlation_id", ""),
        contract_id=value.get("contract_id", ""),
        source_ts_ms=meta.get("ts_ms") or value.get("ts_ms"),
        metadata={k: meta.get(k) for k in ("file", "pos", "row", "snapshot") if k in meta},
    )


def latest_per_key(records: list[ChangeRecord]) -> list[ChangeRecord]:
    """Collapse a batch to the newest change per (table, key) — order by sequence."""
    best: dict[tuple[str, str], ChangeRecord] = {}
    for r in records:
        k = (r.table_ref, r.pk_json)
        if k not in best or r.sequence >= best[k].sequence:
            best[k] = r
    return list(best.values())


def column_family(contract: Contract, name: str) -> str:
    """Destination-neutral type family for a contract field: integer|number|boolean|string."""
    spec = contract.fields[name]
    for t in spec.json_types:
        if t in ("integer", "number", "boolean", "string"):
            return t
    return "string"


def safe_name(*parts: str) -> str:
    """Identifier usable in any warehouse: lowercase [a-z0-9_]."""
    return re.sub(r"[^a-z0-9_]+", "_", "__".join(parts).lower()).strip("_")


class Sink(ABC):
    """A destination. Implementations must make `write` safe to retry.

    The runner guarantees: batches are delivered in offset order per partition,
    a failed batch is retried (never skipped), and `event_id`s already delivered
    to this destination are filtered out by a per-destination ledger.
    """

    type_name: str = "abstract"

    def __init__(self, name: str, options: dict[str, Any]):
        self.name = name
        self.options = options

    def open(self, contracts: list[Contract]) -> None:
        """Create/verify target structures for the routed contracts."""

    @abstractmethod
    def write(self, records: list[ChangeRecord], contracts: dict[str, Contract]) -> None:
        """Persist a batch. Raise to make the runner retry the whole batch."""

    def close(self) -> None:
        """Release resources."""

    def describe(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type_name}


class DeliveryLedger:
    """Per-destination record of delivered event_ids (idempotency across retries/restarts)."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("CREATE TABLE IF NOT EXISTS delivered (event_id TEXT PRIMARY KEY, delivered_at TEXT)")

    def filter_new(self, event_ids: list[str]) -> set[str]:
        if not event_ids:
            return set()
        with self._lock:
            seen = set()
            for i in range(0, len(event_ids), 500):
                chunk = event_ids[i:i + 500]
                q = f"SELECT event_id FROM delivered WHERE event_id IN ({','.join('?' * len(chunk))})"
                seen.update(r[0] for r in self._conn.execute(q, chunk))
        return set(event_ids) - seen

    def mark(self, event_ids: list[str]) -> None:
        now = utc_now()
        with self._lock:
            self._conn.executemany("INSERT OR IGNORE INTO delivered VALUES (?, ?)", [(e, now) for e in event_ids])

    def close(self) -> None:
        with self._lock:
            self._conn.close()

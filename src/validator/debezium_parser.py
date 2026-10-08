"""Debezium envelope parser.

Handles JSON-converter output with or without embedded schemas, INSERT /
UPDATE / DELETE / snapshot-READ events, tombstones and control events. The
row image is located by envelope field name, never by hard-coded offsets.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from src.common.models import CDCEvent

ROW_OPS = {"c", "u", "d", "r"}
CONTROL_OPS = {"t", "m"}  # truncate, message
_LENGTH_TYPES = {"CHAR", "VARCHAR", "BINARY", "VARBINARY", "DECIMAL", "NUMERIC", "BIT"}


class EnvelopeError(ValueError):
    """The message is valid JSON but not a usable Debezium change event."""


def _unwrap(value: Any) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Return (payload, schema) for schema-enabled or schemaless JSON."""
    if not isinstance(value, dict):
        raise EnvelopeError(f"envelope must be a JSON object, got {type(value).__name__}")
    if "payload" in value and "schema" in value:
        payload = value["payload"]
        if payload is None:
            return {}, value.get("schema")
        if not isinstance(payload, dict):
            raise EnvelopeError("envelope 'payload' must be an object")
        return payload, value.get("schema")
    return value, None


def _column_schema(envelope_schema: dict[str, Any] | None, image: str) -> dict[str, dict[str, Any]]:
    """Extract per-column type information from the envelope schema."""
    if not envelope_schema:
        return {}
    for f in envelope_schema.get("fields", []):
        if f.get("field") != image or f.get("type") != "struct":
            continue
        out: dict[str, dict[str, Any]] = {}
        for col in f.get("fields", []):
            params = col.get("parameters") or {}
            raw_type = params.get("__debezium.source.column.type")
            out[col["field"]] = {
                "connect_type": col.get("type"),
                "logical_type": col.get("name"),
                "optional": bool(col.get("optional", False)),
                "sql_type": _sql_type(raw_type, params) if raw_type else None,
            }
        return out
    return {}


def _sql_type(raw_type: str, params: dict[str, Any]) -> str:
    base = raw_type.upper().strip()
    unsigned = base.endswith(" UNSIGNED")
    if unsigned:
        base = base[: -len(" UNSIGNED")]
    length = params.get("__debezium.source.column.length")
    scale = params.get("__debezium.source.column.scale")
    sql = base
    if base in _LENGTH_TYPES and length:
        sql = f"{base}({length},{scale})" if scale not in (None, "") and base in {"DECIMAL", "NUMERIC"} else f"{base}({length})"
    return f"{sql} UNSIGNED" if unsigned else sql


def stable_event_id(source: dict[str, Any], op: str, key: Any, ts_ms: Any) -> str:
    """Deterministic event id from CDC coordinates.

    Binlog file/pos/row identify a change uniquely; snapshot rows share one
    position, so the primary key is always included as well. Re-delivery of
    the same change therefore yields the same id (idempotency).
    """
    material = {
        "server": source.get("name"),
        "db": source.get("db"),
        "table": source.get("table"),
        "op": op,
        "file": source.get("file"),
        "pos": source.get("pos"),
        "row": source.get("row"),
        "gtid": source.get("gtid"),
        "lsn": source.get("lsn"),  # postgres
        "key": key,
    }
    if not source.get("file") and not source.get("lsn"):
        material["ts_ms"] = ts_ms  # last resort for sources without a log position
    digest = hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode()).hexdigest()
    return f"{source.get('table') or 'unknown'}-{op}-{digest[:20]}"


def parse_key(raw_key: Any) -> dict[str, Any] | None:
    if raw_key is None:
        return None
    if isinstance(raw_key, dict) and "payload" in raw_key and "schema" in raw_key:
        raw_key = raw_key["payload"]
    return raw_key if isinstance(raw_key, dict) else {"key": raw_key}


def parse(value: Any, key: Any = None, correlation_id: str = "") -> CDCEvent | None:
    """Parse a decoded message value. Returns None for tombstones.

    Raises EnvelopeError for JSON that is not a Debezium change event.
    """
    if value is None:
        return None
    payload, schema = _unwrap(value)
    if not payload:
        return None  # schema-enabled tombstone

    op = payload.get("op")
    source = payload.get("source")
    if not isinstance(op, str) or not op:
        raise EnvelopeError("missing 'op' field")
    if op not in ROW_OPS | CONTROL_OPS:
        raise EnvelopeError(f"unsupported operation '{op}'")
    if not isinstance(source, dict):
        raise EnvelopeError("missing 'source' block")
    if op in ROW_OPS and not source.get("table"):
        raise EnvelopeError("source block has no table")

    before, after = payload.get("before"), payload.get("after")
    for name, image in (("before", before), ("after", after)):
        if image is not None and not isinstance(image, dict):
            raise EnvelopeError(f"'{name}' must be an object or null")
    if op in {"c", "r", "u"} and after is None:
        raise EnvelopeError(f"op '{op}' without 'after' image")
    if op == "d" and before is None:
        raise EnvelopeError("delete without 'before' image")

    parsed_key = parse_key(key)
    image = "before" if op == "d" else "after"
    return CDCEvent(
        event_id=stable_event_id(source, op, parsed_key, payload.get("ts_ms")),
        correlation_id=correlation_id,
        source_name=source.get("name", "") or "",
        source_database=source.get("db", "") or "",
        source_table=source.get("table", "") or "",
        operation=op,
        ts_ms=payload.get("ts_ms"),
        key=parsed_key,
        before=before,
        after=after,
        source_metadata={k: source.get(k) for k in (
            "connector", "name", "db", "table", "file", "pos", "row", "gtid", "snapshot", "ts_ms", "version"
        ) if k in source},
        column_schema=_column_schema(schema, image),
    )

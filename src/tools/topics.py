"""Topic inspection helpers (never load a whole topic into memory).

    python -m src.tools.topics counts
    python -m src.tools.topics peek cdc.dlq --last 2
    python -m src.tools.topics find cdc.audit --event-id <id>
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from typing import Any, Callable

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from src.common.config import get_settings


def _consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": get_settings().redpanda_brokers,
        "group.id": f"inspect-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def watermarks(topic: str) -> dict[int, tuple[int, int]]:
    c = _consumer()
    try:
        md = c.list_topics(topic, timeout=10).topics.get(topic)
        if md is None or md.error is not None:
            return {}
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in md.partitions}
    finally:
        c.close()


class TopicWatcher:
    """Reads a topic from a fixed starting point and waits for a matching record."""

    def __init__(self, topic: str, from_beginning: bool = False):
        self.topic = topic
        self._c = _consumer()
        marks = watermarks(topic)
        self._c.assign([TopicPartition(topic, p, OFFSET_BEGINNING if from_beginning else hi)
                        for p, (_, hi) in marks.items()])

    def wait_for(self, predicate: Callable[[dict[str, Any]], bool], timeout: float) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self._c.poll(0.5)
            if msg is None or msg.error():
                continue
            try:
                value = json.loads(msg.value()) if msg.value() else None
            except (ValueError, UnicodeDecodeError):
                continue
            if isinstance(value, dict) and predicate(value):
                value["_kafka"] = {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}
                return value
        return None

    def drain(self, timeout: float = 2.0) -> list[dict[str, Any]]:
        out, deadline = [], time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self._c.poll(0.3)
            if msg is None or msg.error():
                continue
            try:
                out.append(json.loads(msg.value()))
            except (ValueError, TypeError, UnicodeDecodeError):
                out.append({"_undecodable": (msg.value() or b"")[:200].decode(errors="replace")})
        return out

    def close(self) -> None:
        self._c.close()


def peek(topic: str, last: int) -> list[Any]:
    c = _consumer()
    try:
        parts = []
        for p, (lo, hi) in watermarks(topic).items():
            parts.append(TopicPartition(topic, p, max(lo, hi - last)))
        c.assign(parts)
        out, idle = [], 0
        while idle < 4 and len(out) < last * max(1, len(parts)):
            msg = c.poll(0.5)
            if msg is None or msg.error():
                idle += 1
                continue
            try:
                out.append(json.loads(msg.value()))
            except (ValueError, TypeError, UnicodeDecodeError):
                out.append({"_undecodable": (msg.value() or b"")[:200].decode(errors="replace")})
        return out[-last:]
    finally:
        c.close()


def find(topic: str, field: str, value: str, timeout: float = 5.0) -> list[dict[str, Any]]:
    w = TopicWatcher(topic, from_beginning=True)
    try:
        return [m for m in w.drain(timeout) if isinstance(m, dict) and str(m.get(field)) == value]
    finally:
        w.close()


def main() -> None:
    s = get_settings()
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("counts")
    p = sub.add_parser("peek")
    p.add_argument("topic")
    p.add_argument("--last", type=int, default=1)
    f = sub.add_parser("find")
    f.add_argument("topic")
    f.add_argument("--event-id", required=True)
    args = parser.parse_args()
    if args.cmd == "counts":
        for t in (s.mutations_topic, s.validated_topic, s.dlq_topic, s.repaired_topic, s.audit_topic):
            marks = watermarks(t)
            total = sum(hi - lo for lo, hi in marks.values())
            print(f"  {t:<16} partitions={len(marks):<2} messages={total}")
    elif args.cmd == "peek":
        for m in peek(args.topic, args.last):
            print(json.dumps(m, indent=2, ensure_ascii=False)[:6000])
    else:
        for m in find(args.topic, "event_id", args.event_id):
            print(json.dumps(m, indent=2, ensure_ascii=False)[:6000])


if __name__ == "__main__":
    main()

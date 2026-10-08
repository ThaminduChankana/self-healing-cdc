"""Inspect or manage the AI worker's local state store (idempotency + repair cache).

    python -m src.tools.state stats
    python -m src.tools.state show <event_id>
    python -m src.tools.state clear-cache      # forget approved repairs (next drift consults the model)

Run inside the worker container: docker exec cdc-ai-worker python -m src.tools.state stats
"""

from __future__ import annotations

import json
import sqlite3
import sys

from src.common.config import get_settings


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    conn = sqlite3.connect(str(get_settings().state_db_path))
    conn.executescript("CREATE TABLE IF NOT EXISTS repair_cache (fingerprint TEXT PRIMARY KEY, repair TEXT, created_at TEXT);"
                       "CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY, repair_id TEXT, "
                       "status TEXT, detail TEXT, deferrals INTEGER DEFAULT 0, updated_at TEXT);")
    cmd = sys.argv[1]
    if cmd == "stats":
        rows = conn.execute("SELECT status, COUNT(*) FROM processed_events GROUP BY status").fetchall()
        cached = conn.execute("SELECT COUNT(*) FROM repair_cache").fetchone()[0]
        print(json.dumps({"processed": dict(rows), "cached_repairs": cached}, indent=2))
    elif cmd == "show" and len(sys.argv) == 3:
        row = conn.execute("SELECT * FROM processed_events WHERE event_id=?", (sys.argv[2],)).fetchone()
        print(json.dumps(row))
    elif cmd == "clear-cache":
        n = conn.execute("DELETE FROM repair_cache").rowcount
        conn.commit()
        print(f"cleared {n} cached repair(s)")
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()

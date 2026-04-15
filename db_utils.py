from __future__ import annotations

import os
import sqlite3


def resolve_sqlite_path(db_path: str | None = None) -> str:
    raw_path = str(db_path or "").strip()
    if raw_path:
        return os.path.abspath(raw_path)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "dbmonitor.sqlite3")


def get_sqlite_conn(
    db_path: str | None = None,
    timeout: float = 10.0,
    row_factory=None,
) -> sqlite3.Connection:
    """Return a SQLite connection configured for concurrent DBMonitor processes."""

    resolved_path = resolve_sqlite_path(db_path)
    conn = sqlite3.connect(resolved_path, timeout=float(timeout))

    if row_factory is not None:
        conn.row_factory = row_factory

    busy_timeout_ms = max(1000, int(float(timeout) * 1000))
    conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    return conn

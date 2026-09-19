#!/usr/bin/env python3
"""Maintain one append-only SQLite backup of the collector database.

The first run creates a consistent full copy. Later runs append rows whose
autoincrement IDs are newer than the last row in the backup. This is intended
for append-only snapshot tables; it is not a point-in-time replication system.
"""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import sys


TABLES = ("spx_market_snapshots", "spx_option_snapshots")
BATCH_SIZE = 10_000


def sqlite_path_from_env(repo_dir: Path) -> Path:
    raw = os.environ.get("DB_URL", f"sqlite:///{repo_dir / 'spx_options.db'}")
    if not raw.startswith("sqlite:///"):
        raise ValueError(f"backup_sqlite.py only supports sqlite DB_URL values. Got: {raw!r}")
    path = raw[len("sqlite:///") :]
    if raw.startswith("sqlite:////"):
        path = "/" + raw[len("sqlite:////") :]
    return Path(path or repo_dir / "spx_options.db").expanduser()


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    if not rows:
        raise ValueError(f"Missing expected SQLite table: {table}")
    return [str(row[1]) for row in rows]


def validate_schema(source: sqlite3.Connection, backup: sqlite3.Connection, table: str) -> list[str]:
    source_columns = table_columns(source, table)
    backup_columns = table_columns(backup, table)
    if source_columns != backup_columns:
        raise ValueError(
            f"Schema mismatch for {table}; source={source_columns!r}, backup={backup_columns!r}"
        )
    if "id" not in source_columns or "snapshot_ts" not in source_columns:
        raise ValueError(f"{table} must contain id and snapshot_ts for incremental backup")
    return source_columns


def append_new_rows(source: sqlite3.Connection, backup: sqlite3.Connection, table: str, columns: list[str]) -> int:
    last_id = backup.execute(f'SELECT COALESCE(MAX("id"), 0) FROM "{table}"').fetchone()[0]
    quoted = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    query = f'SELECT {quoted} FROM "{table}" WHERE "id" > ? ORDER BY "id"'
    insert = f'INSERT INTO "{table}" ({quoted}) VALUES ({placeholders})'
    inserted = 0
    cursor = source.execute(query, (last_id,))
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        backup.executemany(insert, rows)
        inserted += len(rows)
    return inserted


def run(source_path: Path, backup_path: Path) -> dict[str, object]:
    if not source_path.is_file():
        raise FileNotFoundError(f"SQLite DB not found at {source_path}")
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    try:
        if not backup_path.exists() or backup_path.stat().st_size == 0:
            # SQLite's online backup API creates a consistent initial copy.
            backup = sqlite3.connect(backup_path)
            try:
                source.backup(backup)
            finally:
                backup.close()
            return {"mode": "initial", "inserted": 0, "path": str(backup_path)}

        backup = sqlite3.connect(backup_path)
        try:
            counts = {}
            with backup:
                for table in TABLES:
                    columns = validate_schema(source, backup, table)
                    counts[table] = append_new_rows(source, backup, table, columns)
            return {"mode": "incremental", "inserted": counts, "path": str(backup_path)}
        finally:
            backup.close()
    finally:
        source.close()


def main() -> int:
    repo_dir = Path(__file__).resolve().parent.parent
    source_path = sqlite_path_from_env(repo_dir)
    backup_path = Path(os.environ.get("BACKUP_PATH", repo_dir / "backups/sqlite/spx_options.db.backup"))
    result = run(source_path, backup_path)
    print(result)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"backup_sqlite.py: {exc}", file=sys.stderr)
        raise SystemExit(1)

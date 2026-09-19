import sqlite3
import tempfile
from pathlib import Path
import unittest

from scripts import backup_sqlite


SCHEMA = """
CREATE TABLE spx_market_snapshots (
  id INTEGER PRIMARY KEY,
  snapshot_ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  spot_price REAL
);
CREATE TABLE spx_option_snapshots (
  id INTEGER PRIMARY KEY,
  snapshot_ts TEXT NOT NULL,
  symbol TEXT NOT NULL,
  expiration_date TEXT,
  strike_price REAL
);
"""


class BackupSqliteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.source = root / "source.db"
        self.backup = root / "backups" / "spx_options.db.backup"
        db = sqlite3.connect(self.source)
        db.executescript(SCHEMA)
        db.executemany("INSERT INTO spx_market_snapshots VALUES (?, ?, ?, ?)", [
            (1, "2026-05-07T20:45:00Z", "SPX", 5000),
            (2, "2026-05-08T20:45:00Z", "SPX", 5010),
        ])
        db.executemany("INSERT INTO spx_option_snapshots VALUES (?, ?, ?, ?, ?)", [
            (1, "2026-05-07T20:45:00Z", "SPXW", "2026-05-15", 5000),
            (2, "2026-05-08T20:45:00Z", "SPXW", "2026-05-15", 5010),
        ])
        db.commit()
        db.close()

    def tearDown(self):
        self.temp.cleanup()

    def test_initial_backup_then_incremental_rows(self):
        self.assertEqual(backup_sqlite.run(self.source, self.backup)["mode"], "initial")
        db = sqlite3.connect(self.source)
        db.execute("INSERT INTO spx_market_snapshots VALUES (?, ?, ?, ?)", (3, "2026-05-09T20:45:00Z", "SPX", 5020))
        db.execute("INSERT INTO spx_option_snapshots VALUES (?, ?, ?, ?, ?)", (3, "2026-05-09T20:45:00Z", "SPXW", "2026-05-15", 5020))
        db.commit()
        db.close()

        result = backup_sqlite.run(self.source, self.backup)
        self.assertEqual(result["mode"], "incremental")
        self.assertEqual(result["inserted"], {"spx_market_snapshots": 1, "spx_option_snapshots": 1})
        check = sqlite3.connect(self.backup)
        self.assertEqual(check.execute("SELECT COUNT(*) FROM spx_market_snapshots").fetchone()[0], 3)
        self.assertEqual(check.execute("SELECT MAX(snapshot_ts) FROM spx_option_snapshots").fetchone()[0], "2026-05-09T20:45:00Z")
        check.close()

    def test_second_run_is_idempotent(self):
        backup_sqlite.run(self.source, self.backup)
        result = backup_sqlite.run(self.source, self.backup)
        self.assertEqual(result["inserted"], {"spx_market_snapshots": 0, "spx_option_snapshots": 0})


if __name__ == "__main__":
    unittest.main()

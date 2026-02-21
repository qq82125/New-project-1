from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.services.db_migration import migrate_sqlite_to_target, verify_sqlite_vs_target


def _create_source_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE email_rules_versions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          profile TEXT NOT NULL,
          version TEXT NOT NULL,
          config_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE content_rules_versions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          profile TEXT NOT NULL,
          version TEXT NOT NULL,
          config_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE qc_rules_versions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          profile TEXT NOT NULL,
          version TEXT NOT NULL,
          config_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE output_rules_versions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          profile TEXT NOT NULL,
          version TEXT NOT NULL,
          config_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE scheduler_rules_versions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          profile TEXT NOT NULL,
          version TEXT NOT NULL,
          config_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE rules_drafts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ruleset TEXT NOT NULL,
          profile TEXT NOT NULL,
          config_json TEXT NOT NULL,
          validation_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT NOT NULL
        );
        CREATE TABLE sources (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          connector TEXT NOT NULL,
          url TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          priority INTEGER NOT NULL DEFAULT 0,
          trust_tier TEXT NOT NULL,
          tags_json TEXT NOT NULL DEFAULT '[]',
          rate_limit_json TEXT NOT NULL DEFAULT '{}',
          fetch_json TEXT NOT NULL DEFAULT '{}',
          parsing_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          last_fetched_at TEXT,
          last_fetch_status TEXT,
          last_fetch_http_status INTEGER,
          last_fetch_error TEXT,
          last_success_at TEXT,
          last_http_status INTEGER,
          last_error TEXT
        );
        """
    )
    payload = '{"k":"v"}'
    for table in (
        "email_rules_versions",
        "content_rules_versions",
        "qc_rules_versions",
        "output_rules_versions",
        "scheduler_rules_versions",
    ):
        conn.execute(
            f"INSERT INTO {table}(profile,version,config_json,created_at,created_by,is_active) VALUES(?,?,?,?,?,?)",
            ("legacy", "v1", payload, "2026-01-01T00:00:00Z", "test", 1),
        )
    conn.execute(
        "INSERT INTO rules_drafts(ruleset,profile,config_json,validation_json,created_at,created_by) VALUES(?,?,?,?,?,?)",
        ("email_rules", "legacy", payload, "[]", "2026-01-01T00:00:00Z", "test"),
    )
    conn.execute(
        "INSERT INTO sources(id,name,connector,url,enabled,priority,trust_tier,tags_json,rate_limit_json,fetch_json,parsing_json,created_at,updated_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "s1",
            "Source1",
            "rss",
            "https://example.com/feed.xml",
            1,
            10,
            "A",
            "[]",
            "{}",
            "{}",
            "{}",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    conn.commit()
    conn.close()


class DBMigrationTests(unittest.TestCase):
    def test_migrate_with_checkpoint_resume(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "src.db"
            tgt = root / "tgt.db"
            cp = root / "checkpoint.json"
            _create_source_db(src)

            out1 = migrate_sqlite_to_target(
                project_root=Path.cwd(),
                target_url=f"sqlite:///{tgt.as_posix()}",
                source_sqlite_url_or_path=f"sqlite:///{src.as_posix()}",
                batch_size=1,
                resume=True,
                checkpoint_path=cp,
            )
            self.assertTrue(out1["ok"])
            self.assertTrue(cp.exists())

            out2 = migrate_sqlite_to_target(
                project_root=Path.cwd(),
                target_url=f"sqlite:///{tgt.as_posix()}",
                source_sqlite_url_or_path=f"sqlite:///{src.as_posix()}",
                batch_size=1,
                resume=True,
                checkpoint_path=cp,
            )
            self.assertTrue(out2["ok"])
            self.assertEqual(sum(out2["moved"].values()), 0)

    def test_verify_with_tables_and_sample(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "src.db"
            tgt = root / "tgt.db"
            _create_source_db(src)
            migrate_sqlite_to_target(
                project_root=Path.cwd(),
                target_url=f"sqlite:///{tgt.as_posix()}",
                source_sqlite_url_or_path=f"sqlite:///{src.as_posix()}",
                batch_size=100,
                resume=False,
                checkpoint_path=root / "checkpoint.json",
            )
            out = verify_sqlite_vs_target(
                project_root=Path.cwd(),
                target_url=f"sqlite:///{tgt.as_posix()}",
                source_sqlite_url_or_path=f"sqlite:///{src.as_posix()}",
                tables=["email_rules_versions", "sources"],
                sample_rate=0.05,
            )
            self.assertTrue(out["ok"])
            self.assertEqual(set(out["tables"]), {"email_rules_versions", "sources"})


if __name__ == "__main__":
    unittest.main()

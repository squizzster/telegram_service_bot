from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bot_libs.sql import SQLiteQueueStore, SQLiteSettings
from tests.test_sql import make_job


class SQLBotInitCLITests(unittest.TestCase):
    def test_init_preserves_existing_queue_rows(self) -> None:
        with _repo_temp_dir() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            result = _run_sql_bot_init("--db", str(db_path), "--init")

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(_queue_row_count(db_path), 1)

    def test_drop_delete_current_db_requires_force_and_drops_rows(self) -> None:
        with _repo_temp_dir() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            blocked = _run_sql_bot_init(
                "--db",
                str(db_path),
                "--drop-delete-current-db",
            )
            self.assertEqual(blocked.returncode, 1)
            self.assertIn("--drop-delete-current-db requires --force", blocked.stderr)
            self.assertEqual(_queue_row_count(db_path), 1)

            dropped = _run_sql_bot_init(
                "--db",
                str(db_path),
                "--drop-delete-current-db",
                "--force",
            )

            self.assertEqual(dropped.returncode, 0, dropped.stderr)
            self.assertEqual(_queue_row_count(db_path), 0)


def _run_sql_bot_init(*args: str) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    return subprocess.run(
        [sys.executable, "-m", "bot_libs.sql_bot_init", *args],
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _repo_temp_dir() -> TemporaryDirectory[str]:
    tmp_root = Path(__file__).resolve().parents[1] / ".tmp" / "tests"
    tmp_root.mkdir(parents=True, exist_ok=True)
    return TemporaryDirectory(dir=tmp_root)


def _queue_row_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM telegram_queue").fetchone()[0])

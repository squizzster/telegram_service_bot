from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from bot_libs.queue_models import (
    CONTENT_TYPE_AUDIO,
    CONTENT_TYPE_TEXT,
    STATUS_DEAD,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    QueueJobData,
)
from bot_libs.stage_names import STAGE_MESSAGE_REMOVED, STAGE_RETRY_WAITING
from bot_libs.sql import (
    QueueStoreError,
    SQLiteQueueStore,
    SQLiteSettings,
    SchemaVerificationError,
)


def make_job(
    *,
    update_id: int = 1,
    chat_id: int = 123,
    message_id: int = 456,
    max_attempts: int = 3,
    content_type: str = CONTENT_TYPE_TEXT,
) -> QueueJobData:
    return QueueJobData(
        update_id=update_id,
        chat_id=chat_id,
        message_id=message_id,
        telegram_date="2026-04-23T18:42:37+00:00",
        content_type=content_type,
        is_supported=True,
        text="hello",
        payload_json="{}",
        raw_update_json="{}",
        max_attempts=max_attempts,
    )


def fetch_row(db_path: Path, job_id: int) -> sqlite3.Row:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM telegram_queue WHERE id = ?",
            (job_id,),
        ).fetchone()
    assert row is not None
    return row


class SQLiteQueueStoreTests(unittest.TestCase):
    def test_sqlite_settings_rejects_memory_database(self) -> None:
        with self.assertRaisesRegex(ValueError, "temporary file DB"):
            SQLiteSettings(db_path=":memory:")

    def test_insert_queue_job_rejects_missing_database_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)

            db_path.unlink()

            with self.assertRaises(SchemaVerificationError) as exc_info:
                store.insert_queue_job(make_job())

        self.assertIn("does not exist", str(exc_info.exception))

    def test_insert_queue_job_reports_missing_schema(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            db_path.touch()
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))

            with self.assertRaises(SchemaVerificationError) as exc_info:
                store.insert_queue_job(make_job())

        error_text = str(exc_info.exception)
        self.assertIn("missing table telegram_queue", error_text)
        self.assertIn("schema version mismatch", error_text)

    def test_create_schema_is_idempotent_and_preserves_existing_jobs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            result = store.create_schema(create_parent_dir=True)
            check = store.verify_schema()

            self.assertFalse(result.created_file)
            self.assertTrue(check.ok)
            with sqlite3.connect(db_path) as conn:
                row_count = conn.execute("SELECT COUNT(*) FROM telegram_queue").fetchone()[0]
            self.assertEqual(row_count, 1)

    def test_reset_database_recreates_empty_schema(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            result = store.reset_database(create_parent_dir=True)
            check = store.verify_schema()

            self.assertTrue(result.created_file)
            self.assertTrue(check.ok)
            with sqlite3.connect(db_path) as conn:
                row_count = conn.execute("SELECT COUNT(*) FROM telegram_queue").fetchone()[0]
            self.assertEqual(row_count, 0)

    def test_create_schema_does_not_drop_wrong_existing_schema(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE telegram_queue (id INTEGER PRIMARY KEY)")
                conn.execute("INSERT INTO telegram_queue (id) VALUES (42)")
                conn.execute("PRAGMA user_version = 1")
                conn.commit()

            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))

            with self.assertRaises(QueueStoreError):
                store.create_schema(create_parent_dir=True)

            with sqlite3.connect(db_path) as conn:
                columns = [
                    row[1]
                    for row in conn.execute("PRAGMA table_info(telegram_queue)")
                ]
                row_count = conn.execute("SELECT COUNT(*) FROM telegram_queue").fetchone()[0]
            self.assertEqual(columns, ["id"])
            self.assertEqual(row_count, 1)

    def test_claim_next_job_rejects_missing_database_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            db_path.unlink()

            with self.assertRaises(SchemaVerificationError) as exc_info:
                store.claim_next_job(worker_name="worker-1")

        self.assertIn("does not exist", str(exc_info.exception))

    def test_claim_next_job_picks_lowest_due_id_first(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            first_insert = store.insert_queue_job(make_job(update_id=1))
            second_insert = store.insert_queue_job(make_job(update_id=2, message_id=457))

            self.assertIsNotNone(first_insert.queue_id)
            self.assertIsNotNone(second_insert.queue_id)

            first_claim = store.claim_next_job(worker_name="worker-1")
            second_claim = store.claim_next_job(worker_name="worker-1")

            self.assertIsNotNone(first_claim)
            self.assertIsNotNone(second_claim)
            self.assertEqual(first_claim["id"], first_insert.queue_id)
            self.assertEqual(second_claim["id"], second_insert.queue_id)
            self.assertEqual(first_claim["status"], STATUS_PROCESSING)
            self.assertEqual(second_claim["status"], STATUS_PROCESSING)

    def test_claim_next_job_does_not_recover_stale_rows_implicitly(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            claimed = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(claimed)

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "UPDATE telegram_queue SET locked_at = ? WHERE id = ?",
                    ("2000-01-01 00:00:00", claimed["id"]),
                )
                conn.commit()

            next_job = store.claim_next_job(worker_name="worker-2")
            row = fetch_row(db_path, claimed["id"])

            self.assertIsNone(next_job)
            self.assertEqual(row["status"], STATUS_PROCESSING)
            self.assertEqual(row["locked_by"], "worker-1")
            self.assertEqual(row["attempts"], 1)

    def test_get_next_available_at_returns_earliest_queued_timestamp(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            first_insert = store.insert_queue_job(make_job(update_id=1))
            second_insert = store.insert_queue_job(make_job(update_id=2, message_id=457))

            self.assertIsNotNone(first_insert.queue_id)
            self.assertIsNotNone(second_insert.queue_id)

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    UPDATE telegram_queue
                    SET available_at = ?
                    WHERE id = ?
                    """,
                    ("2026-04-23 18:45:00", first_insert.queue_id),
                )
                conn.execute(
                    """
                    UPDATE telegram_queue
                    SET available_at = ?
                    WHERE id = ?
                    """,
                    ("2026-04-23 18:44:00", second_insert.queue_id),
                )
                conn.commit()

            next_available_at = store.get_next_available_at()

            self.assertEqual(
                next_available_at,
                datetime(2026, 4, 23, 18, 44, 0, tzinfo=timezone.utc),
            )

    def test_requeue_stale_processing_jobs_requeues_old_locks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            claimed = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(claimed)

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    UPDATE telegram_queue
                    SET locked_at = ?
                    WHERE id = ?
                    """,
                    ("2000-01-01 00:00:00", claimed["id"]),
                )
                conn.commit()

            recovered = store.requeue_stale_processing_jobs(
                older_than_seconds=30,
                worker_name="worker-2",
            )
            row = fetch_row(db_path, claimed["id"])

            self.assertEqual(recovered, 1)
            self.assertEqual(row["status"], STATUS_QUEUED)
            self.assertEqual(row["attempts"], 1)
            self.assertEqual(row["last_error"], "stale_lock_recovered")
            self.assertIsNone(row["locked_at"])
            self.assertIsNone(row["locked_by"])
            self.assertIsNone(row["finished_at"])

    def test_mark_job_for_retry_marks_exhausted_jobs_dead(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job(max_attempts=1))
            self.assertIsNotNone(insert_result.queue_id)

            claimed = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(claimed)

            store.mark_job_for_retry(
                claimed["id"],
                delay_seconds=60,
                last_error="worker failed",
            )
            row = fetch_row(db_path, claimed["id"])

            self.assertEqual(row["status"], STATUS_DEAD)
            self.assertEqual(row["last_error"], "worker failed")
            self.assertIsNone(row["locked_at"])
            self.assertIsNone(row["locked_by"])
            self.assertIsNotNone(row["finished_at"])

    def test_mark_job_for_retry_requeues_with_future_available_at(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            claimed = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(claimed)

            before_retry = datetime.now(timezone.utc)
            store.mark_job_for_retry(
                claimed["id"],
                delay_seconds=60,
                last_error="worker failed",
            )
            row = fetch_row(db_path, claimed["id"])
            available_at = datetime.strptime(
                row["available_at"],
                "%Y-%m-%d %H:%M:%S",
            ).replace(tzinfo=timezone.utc)

            self.assertEqual(row["status"], STATUS_QUEUED)
            self.assertEqual(row["last_error"], "worker failed")
            self.assertIsNone(row["locked_at"])
            self.assertIsNone(row["locked_by"])
            self.assertGreaterEqual(
                available_at,
                before_retry + timedelta(seconds=55),
            )

    def test_mark_job_dead_can_store_message_removed_stage(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            claimed = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(claimed)

            store.mark_job_dead(
                claimed["id"],
                last_error="source message deleted",
                result_json={"failure_class": "deleted_message"},
                stage=STAGE_MESSAGE_REMOVED,
            )
            row = fetch_row(db_path, claimed["id"])

            self.assertEqual(row["status"], STATUS_DEAD)
            self.assertEqual(row["stage"], STAGE_MESSAGE_REMOVED)
            self.assertEqual(row["last_error"], "source message deleted")

    def test_make_retry_waiting_jobs_due_releases_future_retries_only(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            retry_insert = store.insert_queue_job(make_job(update_id=1))
            fresh_insert = store.insert_queue_job(make_job(update_id=2, message_id=457))
            self.assertIsNotNone(retry_insert.queue_id)
            self.assertIsNotNone(fresh_insert.queue_id)

            claimed = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(claimed)
            store.mark_job_for_retry(
                claimed["id"],
                delay_seconds=3600,
                last_error="temporary failure",
            )

            before_release = datetime.now(timezone.utc)
            released = store.make_retry_waiting_jobs_due(exclude_job_id=fresh_insert.queue_id)
            retry_row = fetch_row(db_path, claimed["id"])
            fresh_row = fetch_row(db_path, fresh_insert.queue_id)
            retry_available_at = datetime.strptime(
                retry_row["available_at"],
                "%Y-%m-%d %H:%M:%S",
            ).replace(tzinfo=timezone.utc)

            self.assertEqual(released, 1)
            self.assertEqual(retry_row["status"], STATUS_QUEUED)
            self.assertEqual(retry_row["stage"], STAGE_RETRY_WAITING)
            self.assertEqual(retry_row["stage_detail"], "fast_retry_after_success")
            self.assertLessEqual(retry_available_at, before_release + timedelta(seconds=1))
            self.assertEqual(fresh_row["attempts"], 0)

    def test_make_retry_waiting_jobs_due_can_filter_by_content_type_delay_and_limit(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)

            text_insert_1 = store.insert_queue_job(
                make_job(update_id=1, content_type=CONTENT_TYPE_TEXT)
            )
            text_insert_2 = store.insert_queue_job(
                make_job(update_id=2, message_id=457, content_type=CONTENT_TYPE_TEXT)
            )
            audio_insert = store.insert_queue_job(
                make_job(update_id=3, message_id=458, content_type=CONTENT_TYPE_AUDIO)
            )
            self.assertIsNotNone(text_insert_1.queue_id)
            self.assertIsNotNone(text_insert_2.queue_id)
            self.assertIsNotNone(audio_insert.queue_id)

            first_text = store.claim_next_job(worker_name="worker-1")
            second_text = store.claim_next_job(worker_name="worker-1")
            audio = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(first_text)
            self.assertIsNotNone(second_text)
            self.assertIsNotNone(audio)

            store.mark_job_for_retry(
                first_text["id"],
                delay_seconds=60,
                last_error="short text failure",
            )
            store.mark_job_for_retry(
                second_text["id"],
                delay_seconds=3600,
                last_error="long text failure",
            )
            store.mark_job_for_retry(
                audio["id"],
                delay_seconds=60,
                last_error="short audio failure",
            )

            released = store.make_retry_waiting_jobs_due(
                content_type=CONTENT_TYPE_TEXT,
                max_delay_seconds=15 * 60,
                limit=1,
            )

            first_text_row = fetch_row(db_path, first_text["id"])
            second_text_row = fetch_row(db_path, second_text["id"])
            audio_row = fetch_row(db_path, audio["id"])

            self.assertEqual(released, 1)
            self.assertEqual(first_text_row["stage_detail"], "fast_retry_after_success")
            self.assertEqual(second_text_row["last_error"], "long text failure")
            self.assertEqual(audio_row["last_error"], "short audio failure")

    def test_set_job_outbound_json_persists_progress(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            store.set_job_outbound_json(
                insert_result.queue_id,
                outbound_json={"transcript": {"messages": [{"index": 0, "message_id": 7}]}},
                stage=STAGE_RETRY_WAITING,
                stage_detail="testing",
            )

            row = fetch_row(db_path, insert_result.queue_id)

            self.assertEqual(
                row["outbound_json"],
                '{"transcript":{"messages":[{"index":0,"message_id":7}]}}',
            )
            self.assertEqual(row["stage"], STAGE_RETRY_WAITING)
            self.assertEqual(row["stage_detail"], "testing")

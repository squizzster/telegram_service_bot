from __future__ import annotations

import sqlite3
import unittest
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from bot_libs.queue_models import (
    CONTENT_TYPE_AUDIO,
    CONTENT_TYPE_PHOTO,
    CONTENT_TYPE_TEXT,
    STATUS_DEAD,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    QueueJobData,
)
from bot_libs.action_models import (
    ACTION_ANSWER_QUESTION,
    ACTION_CALCULATE_INCOME_EXPENSES,
    ACTION_DETECTION_COMPLETE,
    ACTION_DETECTION_PENDING,
    ACTION_DETECTION_PROCESSING,
    ACTION_LOG_EXPENSES,
    ACTION_LOG_INCOME,
    ACTION_SHOW_ALL_DETAILED,
    ACTION_SHOW_EXPENSES,
    ACTION_PROVIDER_ASKING_A_QUESTION,
    ACTION_PROVIDER_LOG_EXPENSES,
    ACTION_PROVIDER_LOG_INCOME,
    ACTION_STATUS_DEAD,
    ACTION_STATUS_DONE,
    ACTION_STATUS_PROCESSING,
    ACTION_STATUS_QUEUED,
)
from bot_libs.stage_names import (
    STAGE_DONE,
    STAGE_FAILED,
    STAGE_MESSAGE_REMOVED,
    STAGE_PROCESSING_ACTION,
    STAGE_RETRY_WAITING,
)
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
    is_supported: bool = True,
    processing_text: str | None = None,
) -> QueueJobData:
    return QueueJobData(
        update_id=update_id,
        chat_id=chat_id,
        message_id=message_id,
        telegram_date="2026-04-23T18:42:37+00:00",
        content_type=content_type,
        is_supported=is_supported,
        text="hello",
        processing_text=processing_text,
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

    def test_schema_creates_seeded_action_catalog(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)

            catalog_by_label = store.get_action_catalog_by_provider_label()
            catalog_by_code = store.get_action_catalog_by_code()
            check = store.verify_schema()

            self.assertTrue(check.ok)
            self.assertIn(ACTION_PROVIDER_LOG_EXPENSES, catalog_by_label)
            self.assertIn(ACTION_PROVIDER_ASKING_A_QUESTION, catalog_by_label)
            self.assertIn(ACTION_LOG_EXPENSES, catalog_by_code)
            self.assertIn(ACTION_ANSWER_QUESTION, catalog_by_code)
            self.assertIn(ACTION_CALCULATE_INCOME_EXPENSES, catalog_by_code)
            self.assertIn(ACTION_SHOW_EXPENSES, catalog_by_code)
            self.assertIn(ACTION_SHOW_ALL_DETAILED, catalog_by_code)
            self.assertFalse(catalog_by_code["NONE"].is_executable)
            self.assertTrue(catalog_by_code[ACTION_LOG_INCOME].is_enabled)
            self.assertFalse(catalog_by_code[ACTION_ANSWER_QUESTION].is_enabled)
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM sqlite_master
                        WHERE type = 'table'
                          AND name = 'incoming_outgoing_expenses'
                        """
                    ).fetchone()[0],
                    1,
                )

    def test_insert_queue_job_sets_initial_action_detection_status(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            text_insert = store.insert_queue_job(
                make_job(update_id=1, content_type=CONTENT_TYPE_TEXT)
            )
            photo_insert = store.insert_queue_job(
                make_job(update_id=2, message_id=457, content_type=CONTENT_TYPE_PHOTO)
            )
            unsupported_insert = store.insert_queue_job(
                make_job(
                    update_id=3,
                    message_id=458,
                    content_type=CONTENT_TYPE_TEXT,
                    is_supported=False,
                )
            )

            text_row = fetch_row(db_path, text_insert.queue_id)
            photo_row = fetch_row(db_path, photo_insert.queue_id)
            unsupported_row = fetch_row(db_path, unsupported_insert.queue_id)

            self.assertEqual(text_row["action_detection_status"], "pending")
            self.assertEqual(photo_row["action_detection_status"], "not_applicable")
            self.assertEqual(
                unsupported_row["action_detection_status"],
                "not_applicable",
            )

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

    def test_complete_action_detection_inserts_child_actions_idempotently(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(
                make_job(processing_text="I spent 30 and earned 250")
            )
            self.assertIsNotNone(insert_result.queue_id)
            queue_id = int(insert_result.queue_id)

            first_run_id = store.start_action_detection_run(
                queue_id=queue_id,
                provider="openai",
                prompt_id="prompt",
                prompt_version="3",
                incoming_text_chars=29,
                incoming_text_sha256="abc",
            )
            first_result = store.complete_action_detection(
                queue_id=queue_id,
                detection_run_id=first_run_id,
                raw_response_json={"output_text": '["reporting_expenses"]'},
                normalized_actions_json={
                    "status": ACTION_DETECTION_COMPLETE,
                    "provider_labels": [
                        ACTION_PROVIDER_LOG_EXPENSES,
                        ACTION_PROVIDER_LOG_INCOME,
                    ],
                    "action_codes": [ACTION_LOG_EXPENSES, ACTION_LOG_INCOME],
                    "none": False,
                },
                action_codes=(ACTION_LOG_EXPENSES, ACTION_LOG_INCOME),
            )
            second_run_id = store.start_action_detection_run(
                queue_id=queue_id,
                provider="openai",
                prompt_id="prompt",
                prompt_version="3",
                incoming_text_chars=29,
                incoming_text_sha256="abc",
            )
            second_result = store.complete_action_detection(
                queue_id=queue_id,
                detection_run_id=second_run_id,
                raw_response_json={"output_text": '["reporting_expenses"]'},
                normalized_actions_json={
                    "status": ACTION_DETECTION_COMPLETE,
                    "provider_labels": [ACTION_PROVIDER_LOG_EXPENSES],
                    "action_codes": [ACTION_LOG_EXPENSES],
                    "none": False,
                },
                action_codes=(ACTION_LOG_EXPENSES,),
            )

            row = fetch_row(db_path, queue_id)
            actions = store.get_actions_for_queue_job(queue_id)
            stored_result = json.loads(row["action_detection_result_json"])

            self.assertEqual(first_result["created_action_count"], 2)
            self.assertEqual(second_result["created_action_count"], 0)
            self.assertEqual(row["action_detection_status"], ACTION_DETECTION_COMPLETE)
            self.assertEqual(stored_result["detection_run_id"], second_run_id)
            self.assertEqual(
                [action["action_code"] for action in actions],
                [ACTION_LOG_EXPENSES, ACTION_LOG_INCOME],
            )

    def test_complete_action_detection_none_creates_no_child_actions(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job(processing_text="thanks"))
            self.assertIsNotNone(insert_result.queue_id)
            queue_id = int(insert_result.queue_id)
            run_id = store.start_action_detection_run(
                queue_id=queue_id,
                provider="openai",
                prompt_id="prompt",
                prompt_version="3",
                incoming_text_chars=6,
                incoming_text_sha256="abc",
            )

            result = store.complete_action_detection(
                queue_id=queue_id,
                detection_run_id=run_id,
                raw_response_json={"output_text": '["none"]'},
                normalized_actions_json={
                    "status": ACTION_DETECTION_COMPLETE,
                    "provider_labels": ["none"],
                    "action_codes": [],
                    "none": True,
                },
                action_codes=(),
            )

            self.assertEqual(result["created_action_count"], 0)
            self.assertEqual(store.get_actions_for_queue_job(queue_id), ())

    def test_requeue_stale_processing_jobs_resets_action_detection_processing(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)

            claimed = store.claim_next_job(worker_name="worker-1")
            self.assertIsNotNone(claimed)
            store.mark_action_detection_processing(claimed["id"])
            run_id = store.start_action_detection_run(
                queue_id=claimed["id"],
                provider="openai",
                prompt_id="prompt",
                prompt_version="3",
                incoming_text_chars=5,
                incoming_text_sha256="abc",
            )

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
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                run_row = conn.execute(
                    "SELECT * FROM action_detection_runs WHERE id = ?",
                    (run_id,),
                ).fetchone()

            self.assertEqual(recovered, 1)
            self.assertEqual(row["status"], STATUS_QUEUED)
            self.assertEqual(row["action_detection_status"], ACTION_DETECTION_PENDING)
            self.assertEqual(run_row["status"], "failed")
            self.assertEqual(run_row["error"], "stale_processing_recovered")

    def test_claim_next_action_waits_until_source_job_is_done(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(
                make_job(processing_text="I spent 30 on petrol")
            )
            queue_id = int(insert_result.queue_id)
            run_id = store.start_action_detection_run(
                queue_id=queue_id,
                provider="openai",
                prompt_id="prompt",
                prompt_version="1",
                incoming_text_chars=20,
                incoming_text_sha256="abc",
            )
            store.complete_action_detection(
                queue_id=queue_id,
                detection_run_id=run_id,
                raw_response_json={"output_text": '["reporting_expenses"]'},
                normalized_actions_json={
                    "status": ACTION_DETECTION_COMPLETE,
                    "provider_labels": ["reporting_expenses"],
                    "action_codes": [ACTION_LOG_EXPENSES],
                    "none": False,
                },
                action_codes=(ACTION_LOG_EXPENSES,),
            )

            self.assertIsNone(store.get_next_action_available_at())
            self.assertIsNone(store.claim_next_action(worker_name="action-worker"))

            store.mark_job_done(queue_id, result_json={"ok": True})
            self.assertIsNotNone(store.get_next_action_available_at())
            claim = store.claim_next_action(worker_name="action-worker")

            self.assertIsNotNone(claim)
            assert claim is not None
            self.assertEqual(claim["queue_id"], queue_id)
            self.assertEqual(claim["source_status"], "done")
            self.assertEqual(claim["action_code"], ACTION_LOG_EXPENSES)
            self.assertEqual(claim["status"], ACTION_STATUS_PROCESSING)
            self.assertEqual(claim["stage"], STAGE_PROCESSING_ACTION)
            self.assertEqual(claim["attempts"], 1)
            self.assertEqual(claim["locked_by"], "action-worker")
            self.assertEqual(claim["chat_id"], 123)
            self.assertEqual(claim["message_id"], 456)
            self.assertEqual(claim["processing_text"], "I spent 30 on petrol")

    def test_mark_action_for_retry_persists_backoff_state(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(
                make_job(processing_text="I spent 30 on petrol")
            )
            queue_id = int(insert_result.queue_id)
            store.insert_incoming_message_actions(
                queue_id=queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_EXPENSES,),
            )
            store.mark_job_done(queue_id, result_json={"ok": True})
            claim = store.claim_next_action(worker_name="action-worker")
            assert claim is not None

            store.mark_action_for_retry(
                int(claim["id"]),
                delay_seconds=3,
                last_error="temporary failure",
            )
            action = store.get_action_job(int(claim["id"]))

            self.assertIsNotNone(action)
            assert action is not None
            self.assertEqual(action["status"], ACTION_STATUS_QUEUED)
            self.assertEqual(action["stage"], STAGE_RETRY_WAITING)
            self.assertEqual(action["last_error"], "temporary failure")
            self.assertIsNone(action["locked_at"])
            self.assertIsNone(action["locked_by"])
            self.assertIsNone(action["finished_at"])
            self.assertGreater(
                datetime.strptime(action["available_at"], "%Y-%m-%d %H:%M:%S"),
                datetime.now() - timedelta(seconds=1),
            )

    def test_mark_action_done_and_dead_are_terminal(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job(processing_text="hello"))
            queue_id = int(insert_result.queue_id)
            store.insert_incoming_message_actions(
                queue_id=queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_EXPENSES, ACTION_LOG_INCOME),
            )
            store.mark_job_done(queue_id, result_json={"ok": True})
            first = store.claim_next_action(worker_name="action-worker")
            second = store.claim_next_action(worker_name="action-worker")
            assert first is not None
            assert second is not None

            store.mark_action_done(
                int(first["id"]),
                result_json={"done": True},
                stage=STAGE_DONE,
            )
            store.mark_action_dead(
                int(second["id"]),
                last_error="permanent failure",
                result_json={"dead": True},
                stage=STAGE_FAILED,
            )

            done_action = store.get_action_job(int(first["id"]))
            dead_action = store.get_action_job(int(second["id"]))
            assert done_action is not None
            assert dead_action is not None
            self.assertEqual(done_action["status"], ACTION_STATUS_DONE)
            self.assertEqual(done_action["stage"], STAGE_DONE)
            self.assertEqual(json.loads(done_action["result_json"]), {"done": True})
            self.assertIsNone(done_action["locked_at"])
            self.assertIsNotNone(done_action["finished_at"])
            self.assertEqual(dead_action["status"], ACTION_STATUS_DEAD)
            self.assertEqual(dead_action["stage"], STAGE_FAILED)
            self.assertEqual(dead_action["last_error"], "permanent failure")
            self.assertEqual(json.loads(dead_action["result_json"]), {"dead": True})
            self.assertIsNotNone(dead_action["finished_at"])

    def test_income_expense_calculation_records_rows_and_marks_sources_processed(
        self,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            first = store.insert_queue_job(
                make_job(
                    update_id=1,
                    message_id=456,
                    processing_text="I spent 20 pound on fish.",
                )
            )
            second = store.insert_queue_job(
                make_job(
                    update_id=2,
                    message_id=457,
                    processing_text="Actually, it was thirty pounds.",
                )
            )
            command = store.insert_queue_job(
                make_job(
                    update_id=3,
                    message_id=458,
                    processing_text="/calculate",
                )
            )
            assert first.queue_id is not None
            assert second.queue_id is not None
            assert command.queue_id is not None
            store.insert_incoming_message_actions(
                queue_id=first.queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_EXPENSES,),
            )
            store.insert_incoming_message_actions(
                queue_id=second.queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_EXPENSES,),
            )
            store.insert_incoming_message_actions(
                queue_id=command.queue_id,
                detection_run_id=None,
                action_codes=(ACTION_CALCULATE_INCOME_EXPENSES,),
            )
            store.mark_job_done(first.queue_id, result_json={"ok": True})
            store.mark_job_done(second.queue_id, result_json={"ok": True})
            store.mark_job_done(command.queue_id, result_json={"ok": True})
            source_one = store.claim_next_action(worker_name="worker")
            source_two = store.claim_next_action(worker_name="worker")
            calculation = store.claim_next_action(worker_name="worker")
            assert source_one is not None
            assert source_two is not None
            assert calculation is not None
            store.mark_action_done(int(source_one["id"]), result_json={"ok": True})
            store.mark_action_done(int(source_two["id"]), result_json={"ok": True})

            sources = store.get_unprocessed_income_expense_source_actions()
            inserted = store.record_income_expense_calculation(
                calculation_action_id=int(calculation["id"]),
                source_action_ids=tuple(int(source["action_id"]) for source in sources),
                entries=(
                    {
                        "entry_id": second.queue_id,
                        "date_time_utc": "2026-04-26 11:01:30",
                        "direction": "outgoing",
                        "description": "Fish purchase",
                        "notes": "Corrected from entry 1.",
                        "price_value": "30",
                    },
                ),
            )

            self.assertEqual(len(sources), 2)
            self.assertEqual(len(inserted), 1)
            self.assertEqual(inserted[0]["entry_id"], second.queue_id)
            self.assertEqual(inserted[0]["week_year"], 2026)
            self.assertEqual(inserted[0]["week_number"], 17)
            self.assertEqual(inserted[0]["price_value"], "30.00")
            self.assertEqual(
                store.get_unprocessed_income_expense_source_actions(),
                (),
            )
            with sqlite3.connect(db_path) as conn:
                processed_count = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM incoming_message_actions
                    WHERE action_code = ?
                      AND income_expense_processed_at IS NOT NULL
                    """,
                    (ACTION_LOG_EXPENSES,),
                ).fetchone()[0]
            self.assertEqual(processed_count, 2)

    def test_get_income_expense_rows_for_week_filters_latest_by_direction(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            source = store.insert_queue_job(
                make_job(
                    update_id=1,
                    message_id=456,
                    processing_text="Earned 160 putting up TVs, spent 56 on aquarium.",
                )
            )
            command = store.insert_queue_job(
                make_job(update_id=2, message_id=457, processing_text="/calculate")
            )
            assert source.queue_id is not None
            assert command.queue_id is not None
            store.insert_incoming_message_actions(
                queue_id=source.queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_INCOME, ACTION_LOG_EXPENSES),
            )
            store.insert_incoming_message_actions(
                queue_id=command.queue_id,
                detection_run_id=None,
                action_codes=(ACTION_CALCULATE_INCOME_EXPENSES,),
            )
            store.mark_job_done(source.queue_id, result_json={"ok": True})
            store.mark_job_done(command.queue_id, result_json={"ok": True})
            income_source = store.claim_next_action(worker_name="worker")
            expense_source = store.claim_next_action(worker_name="worker")
            calculation = store.claim_next_action(worker_name="worker")
            assert income_source is not None
            assert expense_source is not None
            assert calculation is not None
            store.mark_action_done(int(income_source["id"]), result_json={"ok": True})
            store.mark_action_done(int(expense_source["id"]), result_json={"ok": True})
            sources = store.get_unprocessed_income_expense_source_actions()
            store.record_income_expense_calculation(
                calculation_action_id=int(calculation["id"]),
                source_action_ids=tuple(int(source["action_id"]) for source in sources),
                entries=(
                    {
                        "entry_id": source.queue_id,
                        "date_time_utc": "2026-04-26 14:43:23",
                        "direction": "incoming",
                        "description": "Putting up TVs",
                        "notes": "",
                        "price_value": "160",
                    },
                    {
                        "entry_id": source.queue_id,
                        "date_time_utc": "2026-04-26 14:43:23",
                        "direction": "outgoing",
                        "description": "Aquarium",
                        "notes": "",
                        "price_value": "56",
                    },
                ),
            )

            income_rows = store.get_income_expense_rows_for_week(direction="incoming")
            expense_rows = store.get_income_expense_rows_for_week(direction="outgoing")
            all_rows = store.get_income_expense_rows_for_week()

            self.assertEqual([row["direction"] for row in income_rows], ["incoming"])
            self.assertEqual([row["direction"] for row in expense_rows], ["outgoing"])
            self.assertEqual(len(all_rows), 2)

    def test_mark_action_for_retry_exhausted_marks_dead(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job(processing_text="hello"))
            queue_id = int(insert_result.queue_id)
            store.insert_incoming_message_actions(
                queue_id=queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_EXPENSES,),
            )
            with sqlite3.connect(db_path) as conn:
                conn.execute("UPDATE incoming_message_actions SET max_attempts = 1")
                conn.commit()
            store.mark_job_done(queue_id, result_json={"ok": True})
            claim = store.claim_next_action(worker_name="action-worker")
            assert claim is not None

            store.mark_action_for_retry(
                int(claim["id"]),
                delay_seconds=1,
                last_error="temporary failure",
            )
            action = store.get_action_job(int(claim["id"]))

            self.assertIsNotNone(action)
            assert action is not None
            self.assertEqual(action["status"], ACTION_STATUS_DEAD)
            self.assertEqual(action["stage"], STAGE_FAILED)
            self.assertEqual(action["last_error"], "temporary failure")
            self.assertIsNotNone(action["finished_at"])

    def test_requeue_stale_processing_actions_resets_to_retry_waiting(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job(processing_text="hello"))
            queue_id = int(insert_result.queue_id)
            store.insert_incoming_message_actions(
                queue_id=queue_id,
                detection_run_id=None,
                action_codes=(ACTION_LOG_EXPENSES,),
            )
            store.mark_job_done(queue_id, result_json={"ok": True})
            claim = store.claim_next_action(worker_name="action-worker")
            assert claim is not None
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    UPDATE incoming_message_actions
                    SET locked_at = ?
                    WHERE id = ?
                    """,
                    ("2000-01-01 00:00:00", int(claim["id"])),
                )
                conn.commit()

            recovered = store.requeue_stale_processing_actions(older_than_seconds=1)
            action = store.get_action_job(int(claim["id"]))

            self.assertEqual(recovered, 1)
            self.assertIsNotNone(action)
            assert action is not None
            self.assertEqual(action["status"], ACTION_STATUS_QUEUED)
            self.assertEqual(action["stage"], STAGE_RETRY_WAITING)
            self.assertEqual(action["last_error"], "stale_action_lock_recovered")
            self.assertIsNone(action["locked_at"])
            self.assertIsNone(action["locked_by"])

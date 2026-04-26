from __future__ import annotations

import sqlite3
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from bot_libs.action_detection import ActionDetectionService
from bot_libs.action_models import (
    ACTION_CALCULATE_INCOME_EXPENSES,
    ACTION_DETECTION_COMPLETE,
    ACTION_DETECTION_FAILED,
    ACTION_DETECTION_PENDING,
    ACTION_LOG_EXPENSES,
    ACTION_SHOW_ALL_DETAILED,
    ACTION_SHOW_EXPENSES,
    ProviderActionDetectionResult,
)
from bot_libs.queue_models import CONTENT_TYPE_TEXT, QueueJobData
from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError
from bot_libs.sql import SQLiteQueueStore, SQLiteSettings


def make_job() -> QueueJobData:
    return QueueJobData(
        update_id=1,
        chat_id=123,
        message_id=456,
        telegram_date="2026-04-23T18:42:37+00:00",
        content_type=CONTENT_TYPE_TEXT,
        is_supported=True,
        text="I spent 30 on petrol",
        processing_text="I spent 30 on petrol",
        payload_json="{}",
        raw_update_json="{}",
        max_attempts=3,
    )


class FakeProvider:
    def __init__(self, raw_actions_text: str) -> None:
        self.raw_actions_text = raw_actions_text
        self.calls: list[str] = []

    async def __call__(self, *, incoming_text: str) -> ProviderActionDetectionResult:
        self.calls.append(incoming_text)
        return ProviderActionDetectionResult(
            provider="fake",
            prompt_id="prompt",
            prompt_version="3",
            raw_actions_text=self.raw_actions_text,
            raw_response={"output_text": self.raw_actions_text},
        )


class ExplodingProvider:
    async def __call__(self, *, incoming_text: str) -> ProviderActionDetectionResult:
        del incoming_text
        raise AssertionError("provider should not be called")


class ActionDetectionServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_detect_actions_creates_child_jobs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)
            row = store.get_queue_job(int(insert_result.queue_id))
            self.assertIsNotNone(row)
            provider = FakeProvider('["reporting_expenses"]')
            service = ActionDetectionService(
                queue_store=store,
                provider=provider,
                provider_name="fake",
                prompt_id="prompt",
                prompt_version="3",
            )

            result = await service.detect_actions(row)

            self.assertEqual(provider.calls, ["I spent 30 on petrol"])
            self.assertEqual(result["status"], ACTION_DETECTION_COMPLETE)
            self.assertEqual(result["action_codes"], [ACTION_LOG_EXPENSES])
            self.assertEqual(result["created_action_count"], 1)
            actions = store.get_actions_for_queue_job(int(insert_result.queue_id))
            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0]["action_code"], ACTION_LOG_EXPENSES)

    async def test_detect_actions_maps_direct_commands_without_provider(self) -> None:
        cases = (
            ("/calculate", ACTION_CALCULATE_INCOME_EXPENSES),
            ("/calculate_force", ACTION_CALCULATE_INCOME_EXPENSES),
            ("/calc extra text", ACTION_CALCULATE_INCOME_EXPENSES),
            ("/show_expenses@my_bot", ACTION_SHOW_EXPENSES),
            ("/show_all_detailed", ACTION_SHOW_ALL_DETAILED),
        )
        for command_text, action_code in cases:
            with self.subTest(command_text=command_text):
                with TemporaryDirectory() as tmpdir:
                    db_path = Path(tmpdir) / "queue.sqlite"
                    store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
                    store.create_schema(create_parent_dir=True)
                    job = replace(
                        make_job(),
                        text=command_text,
                        processing_text=command_text,
                    )
                    insert_result = store.insert_queue_job(job)
                    self.assertIsNotNone(insert_result.queue_id)
                    row = store.get_queue_job(int(insert_result.queue_id))
                    self.assertIsNotNone(row)
                    service = ActionDetectionService(
                        queue_store=store,
                        provider=ExplodingProvider(),
                        provider_name="fake",
                        prompt_id="prompt",
                    )

                    result = await service.detect_actions(row)

                    self.assertEqual(result["status"], ACTION_DETECTION_COMPLETE)
                    self.assertEqual(result["action_codes"], [action_code])
                    self.assertEqual(result["created_action_count"], 1)
                    actions = store.get_actions_for_queue_job(int(insert_result.queue_id))
                    self.assertEqual(len(actions), 1)
                    self.assertEqual(actions[0]["action_code"], action_code)

    async def test_detect_actions_fails_disabled_question_action_without_child_job(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)
            row = store.get_queue_job(int(insert_result.queue_id))
            self.assertIsNotNone(row)
            provider = FakeProvider('["asking_a_question"]')
            service = ActionDetectionService(
                queue_store=store,
                provider=provider,
                provider_name="fake",
                prompt_id="prompt",
            )

            with self.assertRaises(PermanentJobError):
                await service.detect_actions(row)

            failed_row = store.get_queue_job(int(insert_result.queue_id))
            self.assertIsNotNone(failed_row)
            self.assertEqual(
                failed_row["action_detection_status"],
                ACTION_DETECTION_FAILED,
            )
            actions = store.get_actions_for_queue_job(int(insert_result.queue_id))
            self.assertEqual(actions, ())

    async def test_complete_detection_skips_provider(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)
            queue_id = int(insert_result.queue_id)
            run_id = store.start_action_detection_run(
                queue_id=queue_id,
                provider="fake",
                prompt_id="prompt",
                prompt_version="3",
                incoming_text_chars=19,
                incoming_text_sha256="abc",
            )
            store.complete_action_detection(
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
            row = store.get_queue_job(queue_id)
            self.assertIsNotNone(row)
            service = ActionDetectionService(
                queue_store=store,
                provider=ExplodingProvider(),
                provider_name="fake",
                prompt_id="prompt",
                prompt_version="3",
            )

            result = await service.detect_actions(row)

            self.assertEqual(result["status"], ACTION_DETECTION_COMPLETE)
            self.assertTrue(result["none"])

    async def test_invalid_provider_output_marks_detection_pending(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "queue.sqlite"
            store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
            store.create_schema(create_parent_dir=True)
            insert_result = store.insert_queue_job(make_job())
            self.assertIsNotNone(insert_result.queue_id)
            queue_id = int(insert_result.queue_id)
            row = store.get_queue_job(queue_id)
            self.assertIsNotNone(row)
            service = ActionDetectionService(
                queue_store=store,
                provider=FakeProvider("not JSON"),
                provider_name="fake",
                prompt_id="prompt",
                prompt_version="3",
            )

            with self.assertRaises(RetryableJobError):
                await service.detect_actions(row)

            latest_row = store.get_queue_job(queue_id)
            self.assertIsNotNone(latest_row)
            self.assertEqual(
                latest_row["action_detection_status"],
                ACTION_DETECTION_PENDING,
            )
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                run_row = conn.execute(
                    """
                    SELECT *
                    FROM action_detection_runs
                    WHERE queue_id = ?
                    """,
                    (queue_id,),
                ).fetchone()
            self.assertEqual(run_row["status"], "failed")
            self.assertIn("invalid JSON", run_row["error"])

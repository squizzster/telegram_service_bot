from __future__ import annotations

import json
import signal
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot_libs.sql import SQLiteSettings
from bot_libs.stage_names import (
    STAGE_CALLING_STT_API,
    STAGE_FAILED,
    STAGE_MESSAGE_REMOVED,
    STAGE_SENDING_RESPONSE,
    STAGE_UNSUPPORTED,
)
import telegram_queue_daemon as daemon


async def immediate_to_thread(func, *args, **kwargs):
    return func(*args, **kwargs)


def telegram_sent_message(
    message_id: int,
    *,
    reply_to_message_id: int | None = None,
) -> SimpleNamespace:
    reply_to_message = (
        SimpleNamespace(message_id=reply_to_message_id)
        if reply_to_message_id is not None
        else None
    )
    return SimpleNamespace(message_id=message_id, reply_to_message=reply_to_message)


class FakeQueueStore:
    def __init__(self) -> None:
        self.done_calls: list[dict[str, object]] = []
        self.retry_calls: list[dict[str, object]] = []
        self.dead_calls: list[dict[str, object]] = []
        self.fast_retry_release_calls: list[dict[str, object]] = []
        self.stage_calls: list[dict[str, object]] = []
        self.outbound_calls: list[dict[str, object]] = []
        self.fail_mark_done = False
        self.rows: dict[int, dict[str, object]] = {}

    def mark_job_done(
        self,
        job_id: int,
        *,
        result_json: object = None,
        stage: str = "DONE",
    ) -> None:
        if self.fail_mark_done:
            raise daemon.QueueStoreError("mark done failed")
        self.done_calls.append(
            {
                "job_id": job_id,
                "result_json": result_json,
                "stage": stage,
            }
        )
        self.rows[job_id] = {
            "id": job_id,
            "status": "done",
            "stage": stage,
            "is_supported": 1 if stage != STAGE_UNSUPPORTED else 0,
            "chat_id": -1003986727769,
            "chat_type": "supergroup",
            "message_id": 41,
        }

    def mark_job_for_retry(
        self,
        job_id: int,
        *,
        delay_seconds: int,
        last_error: str,
    ) -> None:
        self.retry_calls.append(
            {
                "job_id": job_id,
                "delay_seconds": delay_seconds,
                "last_error": last_error,
            }
        )
        self.rows[job_id] = {
            "id": job_id,
            "status": "queued",
            "stage": "RETRY_WAITING",
            "available_at": "2026-04-25 16:00:00",
            "is_supported": 1,
            "chat_id": -1003986727769,
            "chat_type": "supergroup",
            "message_id": 41,
        }

    def mark_job_dead(
        self,
        job_id: int,
        *,
        last_error: str,
        result_json: object = None,
        stage: str = STAGE_FAILED,
    ) -> None:
        self.dead_calls.append(
            {
                "job_id": job_id,
                "last_error": last_error,
                "result_json": result_json,
                "stage": stage,
            }
        )
        self.rows[job_id] = {
            "id": job_id,
            "status": "dead",
            "stage": stage,
            "is_supported": 1,
            "chat_id": -1003986727769,
            "chat_type": "supergroup",
            "message_id": 41,
        }

    def set_job_stage(
        self,
        job_id: int,
        *,
        stage: str,
        stage_detail: str | None = None,
    ) -> None:
        self.stage_calls.append(
            {
                "job_id": job_id,
                "stage": stage,
                "stage_detail": stage_detail,
            }
        )

    def set_job_processing_text(
        self,
        job_id: int,
        *,
        processing_text: str,
        stage: str | None = None,
        stage_detail: str | None = None,
    ) -> None:
        row = self.rows.setdefault(
            job_id,
            {
                "id": job_id,
                "status": "processing",
                "stage": "READY_TO_PROCESS",
                "is_supported": 1,
                "chat_id": -1003986727769,
                "chat_type": "supergroup",
                "message_id": 41,
                "action_detection_status": "not_applicable",
            },
        )
        row["processing_text"] = processing_text
        if stage is not None:
            row["stage"] = stage
        row["stage_detail"] = stage_detail

    def set_job_outbound_json(
        self,
        job_id: int,
        *,
        outbound_json: object,
        stage: str | None = None,
        stage_detail: str | None = None,
    ) -> None:
        self.outbound_calls.append(
            {
                "job_id": job_id,
                "outbound_json": outbound_json,
                "stage": stage,
                "stage_detail": stage_detail,
            }
        )
        row = self.rows.setdefault(
            job_id,
            {
                "id": job_id,
                "status": "processing",
                "stage": "READY_TO_PROCESS",
                "is_supported": 1,
                "chat_id": -1003986727769,
                "chat_type": "supergroup",
                "message_id": 41,
                "processing_text": "hello world",
                "action_detection_status": "pending",
            },
        )
        row["outbound_json"] = json.dumps(outbound_json)
        if stage is not None:
            row["stage"] = stage
        row["stage_detail"] = stage_detail

    def make_retry_waiting_jobs_due(
        self,
        *,
        exclude_job_id: int | None = None,
        content_type: str | None = None,
        max_delay_seconds: int | None = None,
        limit: int = 10,
    ) -> int:
        self.fast_retry_release_calls.append(
            {
                "exclude_job_id": exclude_job_id,
                "content_type": content_type,
                "max_delay_seconds": max_delay_seconds,
                "limit": limit,
            }
        )
        return 2

    def get_queue_job(self, job_id: int) -> dict[str, object] | None:
        if job_id not in self.rows:
            self.rows[job_id] = {
                "id": job_id,
                "status": "processing",
                "stage": "READY_TO_PROCESS",
                "is_supported": 1,
                "chat_id": -1003986727769,
                "chat_type": "supergroup",
                "message_id": 41,
                "processing_text": "hello world",
                "action_detection_status": "not_applicable",
            }
        return self.rows.get(job_id)


class RetryProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise daemon.RetryableJobError("temporary_failure")


class RetryAfterProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise daemon.RetryableJobError(
            "telegram rate limited",
            retry_after_seconds=25,
        )


class PermanentProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise daemon.PermanentJobError("invalid_payload")


class ExplodingProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise RuntimeError("boom")


class OriginalMessageUnavailableProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise daemon.OriginalMessageUnavailableError("source message deleted")


class ActivityProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row
        async with context.activity(STAGE_CALLING_STT_API, None):
            return {
                "outcome": "processed",
                "processor": "activity",
            }


class SaveProcessingTextProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row
        await context.set_processing_text("fresh canonical text", None, None)
        return {
            "outcome": "processed",
            "processor": "save-text",
        }


class DeferredTranscriptProcessor(daemon.QueueJobProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row
        await context.set_processing_text(
            "voice transcript",
            STAGE_SENDING_RESPONSE,
            None,
        )
        return {
            "outcome": "processed",
            "processor": "voice",
            "processing_text": "voice transcript",
            "transcript_message_ids": [],
            "transcript_reply_deferred": True,
        }


class RecordingActionDetector:
    def __init__(self, result: dict[str, object] | None = None) -> None:
        self.rows: list[dict[str, object]] = []
        self.result = result or {
            "status": "complete",
            "provider_labels": ["reporting_expenses"],
            "action_codes": ["LOG_EXPENSES"],
            "created_action_count": 1,
            "none": False,
        }

    async def detect_actions(self, row: dict[str, object]) -> dict[str, object]:
        self.rows.append(dict(row))
        return dict(self.result)


class RetryableActionDetector:
    async def detect_actions(self, row: dict[str, object]) -> dict[str, object]:
        del row
        raise daemon.RetryableJobError("classifier temporary failure")


class TelegramQueueDaemonTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_args_accepts_uppercase_debug_alias(self) -> None:
        args = daemon.parse_args(["--DEBUG"])

        self.assertTrue(args.debug)

    def test_parse_args_defaults_to_long_idle_poll(self) -> None:
        args = daemon.parse_args([])

        self.assertEqual(args.poll_seconds, 120)

    def make_config(self) -> daemon.Config:
        return daemon.Config(
            token="token",
            sqlite_settings=SQLiteSettings(db_path="/tmp/test-telegram-queue-daemon.sqlite"),
            worker_name="queue-daemon",
            poll_seconds=300,
            stale_lock_seconds=1800,
            queue_max_attempts=daemon.DEFAULT_MAX_ATTEMPTS,
            pidfile_path="/tmp/test-telegram-queue-daemon.pid",
            action_daemon_pidfile_path="/tmp/test-telegram-action-daemon.pid",
        )

    def make_row(
        self,
        *,
        attempts: int = 1,
        max_attempts: int = daemon.DEFAULT_MAX_ATTEMPTS,
        is_supported: int = 1,
        content_type: str = "text",
        payload_json: str | None = None,
        text: str | None = "hello world",
        action_detection_status: str | None = None,
    ) -> dict[str, object]:
        row = {
            "id": 42,
            "attempts": attempts,
            "max_attempts": max_attempts,
            "is_supported": is_supported,
            "content_type": content_type,
            "payload_json": payload_json
            if payload_json is not None
            else json.dumps({"content": {"type": content_type}, "extra": {}}),
            "text": text,
            "processing_text": text,
            "chat_id": -1003986727769,
            "chat_type": "supergroup",
            "message_id": 41,
        }
        if action_detection_status is not None:
            row["action_detection_status"] = action_detection_status
        return row

    def make_bot(self) -> SimpleNamespace:
        return SimpleNamespace(set_message_reaction=AsyncMock())

    def test_next_retry_delay_seconds_matches_locked_schedule(self) -> None:
        self.assertEqual(
            [
                daemon.next_retry_delay_seconds(
                    attempts=n,
                    max_attempts=daemon.DEFAULT_MAX_ATTEMPTS,
                )
                for n in range(1, daemon.DEFAULT_MAX_ATTEMPTS)
            ],
            [1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049],
        )
        self.assertIsNone(
            daemon.next_retry_delay_seconds(
                attempts=daemon.DEFAULT_MAX_ATTEMPTS,
                max_attempts=daemon.DEFAULT_MAX_ATTEMPTS,
            )
        )

    async def test_wait_for_wakeup_consumes_sighup_received_before_idle(self) -> None:
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=FakeQueueStore(),
        )

        worker._handle_signal(signal.SIGHUP)
        worker._handle_signal(signal.SIGHUP)
        self.assertEqual(worker._pending_wakeups, 2)

        await worker.wait_for_wakeup()

        self.assertEqual(worker._pending_wakeups, 0)
        self.assertFalse(worker._wake_event.is_set())

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_marks_success_done_and_sets_success_reaction(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(config=self.make_config(), queue_store=store)

        await worker.process_one_row(bot, self.make_row())

        self.assertEqual(
            store.done_calls,
            [
                {
                    "job_id": 42,
                    "result_json": {
                        "outcome": "processed",
                        "attempts": 1,
                        "content_type": "text",
                        "processor": "text",
                        "text_length": 11,
                        "has_text": True,
                        "action_detection": {"status": "not_applicable"},
                    },
                    "stage": "DONE",
                }
            ],
        )
        self.assertEqual(
            store.fast_retry_release_calls,
            [
                {
                    "exclude_job_id": 42,
                    "content_type": "text",
                    "max_delay_seconds": daemon.RETRY_SHORT_MAX_SECONDS,
                    "limit": 10,
                }
            ],
        )
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "👌")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_detects_actions_before_marking_done(self) -> None:
        store = FakeQueueStore()
        action_detector = RecordingActionDetector()
        bot = self.make_bot()
        row = self.make_row(action_detection_status="pending")
        store.rows[42] = dict(row)
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            action_detector=action_detector,
        )

        await worker.process_one_row(bot, row)

        self.assertEqual(len(action_detector.rows), 1)
        self.assertEqual(action_detector.rows[0]["action_detection_status"], "pending")
        self.assertEqual(
            store.done_calls[0]["result_json"]["action_detection"],
            {
                "status": "complete",
                "provider_labels": ["reporting_expenses"],
                "action_codes": ["LOG_EXPENSES"],
                "created_action_count": 1,
                "none": False,
            },
        )

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_reloads_latest_row_before_action_detection(self) -> None:
        store = FakeQueueStore()
        action_detector = RecordingActionDetector()
        bot = self.make_bot()
        row = self.make_row(
            content_type="voice",
            text=None,
            action_detection_status="pending",
        )
        store.rows[42] = dict(row)
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=SaveProcessingTextProcessor(),
            action_detector=action_detector,
        )

        await worker.process_one_row(bot, row)

        self.assertEqual(
            action_detector.rows[0]["processing_text"],
            "fresh canonical text",
        )
        self.assertEqual(store.done_calls[0]["result_json"]["processor"], "save-text")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_sends_deferred_transcript_with_action_labels(self) -> None:
        store = FakeQueueStore()
        action_detector = RecordingActionDetector(
            {
                "status": "complete",
                "provider_labels": ["reporting_expenses"],
                "action_codes": ["LOG_EXPENSES"],
                "created_action_count": 1,
                "none": False,
            }
        )
        bot = SimpleNamespace(
            send_message=AsyncMock(
                return_value=telegram_sent_message(9001, reply_to_message_id=41)
            ),
            set_message_reaction=AsyncMock(),
        )
        row = self.make_row(
            content_type="voice",
            text=None,
            action_detection_status="pending",
        )
        store.rows[42] = dict(row)
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=DeferredTranscriptProcessor(),
            action_detector=action_detector,
        )

        await worker.process_one_row(bot, row)

        bot.send_message.assert_awaited_once_with(
            chat_id=-1003986727769,
            text="voice transcript\n[reporting_expenses]",
            reply_to_message_id=41,
            allow_sending_without_reply=False,
        )
        self.assertEqual(
            store.done_calls[0]["result_json"]["transcript_message_ids"],
            [9001],
        )
        self.assertIn(
            {
                "job_id": 42,
                "stage": STAGE_SENDING_RESPONSE,
                "stage_detail": None,
            },
            store.stage_calls,
        )

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_retries_when_action_detection_is_retryable(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        row = self.make_row(action_detection_status="pending")
        store.rows[42] = dict(row)
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            action_detector=RetryableActionDetector(),
        )

        await worker.process_one_row(bot, row)

        self.assertEqual(store.done_calls, [])
        self.assertEqual(
            store.retry_calls,
            [
                {
                    "job_id": 42,
                    "delay_seconds": 1,
                    "last_error": "classifier temporary failure",
                }
            ],
        )
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "🤔")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_does_not_set_success_reaction_when_mark_done_fails(self) -> None:
        store = FakeQueueStore()
        store.fail_mark_done = True
        bot = self.make_bot()
        worker = daemon.QueueDaemon(config=self.make_config(), queue_store=store)

        await worker.process_one_row(bot, self.make_row())

        self.assertEqual(store.done_calls, [])
        self.assertEqual(store.fast_retry_release_calls, [])
        bot.set_message_reaction.assert_not_awaited()

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_supplies_stage_activity_context(self) -> None:
        store = FakeQueueStore()
        reactions: list[str] = []

        async def record_reaction(**kwargs: object) -> bool:
            reactions.append(str(kwargs["reaction"]))
            return True

        bot = SimpleNamespace(set_message_reaction=AsyncMock(side_effect=record_reaction))
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=ActivityProcessor(),
        )

        await worker.process_one_row(bot, self.make_row())

        self.assertEqual(
            store.stage_calls,
            [
                {
                    "job_id": 42,
                    "stage": STAGE_CALLING_STT_API,
                    "stage_detail": None,
                }
            ],
        )
        self.assertEqual(reactions, ["✍", "👌"])

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_marks_unsupported_rows_done(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(config=self.make_config(), queue_store=store)

        await worker.process_one_row(
            bot,
            self.make_row(is_supported=0, content_type="video"),
        )

        self.assertEqual(
            store.done_calls,
            [
                {
                    "job_id": 42,
                    "result_json": {
                        "outcome": "unsupported_content",
                        "action": "no_processing",
                        "content_type": "video",
                    },
                    "stage": STAGE_UNSUPPORTED,
                }
            ],
        )
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "🤷")

    @patch("telegram_queue_daemon.asyncio.sleep", new_callable=AsyncMock)
    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_reaction_reconcile_reasserts_latest_done_reaction(
        self,
        sleep_mock,
    ) -> None:
        store = FakeQueueStore()
        store.rows[42] = {
            "id": 42,
            "status": "done",
            "stage": "DONE",
            "is_supported": 1,
            "chat_id": -1003986727769,
            "chat_type": "supergroup",
            "message_id": 41,
        }
        bot = self.make_bot()
        worker = daemon.QueueDaemon(config=self.make_config(), queue_store=store)

        await worker._reconcile_reaction_after_delay(
            bot,
            self.make_row(),
            reason="processed_done_confirm",
        )

        sleep_mock.assert_awaited_once_with(daemon.REACTION_RECONCILE_DELAY_SECONDS)
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "👌")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_schedules_retry_and_sets_retry_reaction(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=RetryProcessor(),
        )

        await worker.process_one_row(bot, self.make_row(attempts=1))

        self.assertEqual(
            store.retry_calls,
            [
                {
                    "job_id": 42,
                    "delay_seconds": 1,
                    "last_error": "temporary_failure",
                }
            ],
        )
        bot.set_message_reaction.assert_awaited_once()
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "🤔")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_honors_retry_after_over_short_policy_delay(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=RetryAfterProcessor(),
        )

        await worker.process_one_row(bot, self.make_row(attempts=1))

        self.assertEqual(
            store.retry_calls,
            [
                {
                    "job_id": 42,
                    "delay_seconds": 25,
                    "last_error": "telegram rate limited",
                }
            ],
        )
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "🤔")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_marks_retry_exhausted_dead(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=RetryProcessor(),
        )

        await worker.process_one_row(
            bot,
            self.make_row(attempts=daemon.DEFAULT_MAX_ATTEMPTS),
        )

        self.assertEqual(
            store.dead_calls,
            [
                {
                    "job_id": 42,
                    "last_error": "temporary_failure",
                    "result_json": {
                        "outcome": "dead",
                        "failure_class": "retry_exhausted",
                        "attempts": daemon.DEFAULT_MAX_ATTEMPTS,
                    },
                    "stage": STAGE_FAILED,
                }
            ],
        )
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "💔")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_marks_permanent_failure_dead(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=PermanentProcessor(),
        )

        await worker.process_one_row(bot, self.make_row())

        self.assertEqual(
            store.dead_calls,
            [
                {
                    "job_id": 42,
                    "last_error": "invalid_payload",
                    "result_json": {
                        "outcome": "dead",
                        "failure_class": "permanent_failure",
                        "attempts": 1,
                    },
                    "stage": STAGE_FAILED,
                }
            ],
        )
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "💔")

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_marks_original_message_unavailable_dead(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=OriginalMessageUnavailableProcessor(),
        )

        await worker.process_one_row(bot, self.make_row())

        self.assertEqual(
            store.dead_calls,
            [
                {
                    "job_id": 42,
                    "last_error": "source message deleted",
                    "result_json": {
                        "outcome": "dead",
                        "failure_class": "deleted_message",
                        "attempts": 1,
                    },
                    "stage": STAGE_MESSAGE_REMOVED,
                }
            ],
        )
        bot.set_message_reaction.assert_not_awaited()

    @patch("telegram_queue_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_row_treats_unknown_exceptions_as_retryable(self) -> None:
        store = FakeQueueStore()
        bot = self.make_bot()
        worker = daemon.QueueDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=ExplodingProcessor(),
        )

        await worker.process_one_row(bot, self.make_row(attempts=2))

        self.assertEqual(
            store.retry_calls,
            [
                {
                    "job_id": 42,
                    "delay_seconds": 3,
                    "last_error": "boom",
                }
            ],
        )
        reaction = bot.set_message_reaction.await_args.kwargs["reaction"]
        self.assertEqual(reaction, "🤔")

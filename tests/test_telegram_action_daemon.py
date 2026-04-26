from __future__ import annotations

import asyncio
import signal
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot_libs.action_models import (
    ACTION_CALCULATE_INCOME_EXPENSES,
    ACTION_SHOW_ALL,
    ACTION_STATUS_PROCESSING,
    ACTION_STATUS_QUEUED,
)
from bot_libs.sql import SQLiteSettings
from bot_libs.stage_names import (
    STAGE_ANSWERING_QUESTION,
    STAGE_FAILED,
    STAGE_PROCESSING_ACTION,
)
import telegram_action_daemon as daemon


async def immediate_to_thread(func, *args, **kwargs):
    return func(*args, **kwargs)


class FakeActionStore:
    def __init__(self) -> None:
        self.done_calls: list[dict[str, object]] = []
        self.retry_calls: list[dict[str, object]] = []
        self.dead_calls: list[dict[str, object]] = []
        self.stage_calls: list[dict[str, object]] = []
        self.outbound_calls: list[dict[str, object]] = []
        self.recovered = 0
        self.next_action_available_at = None
        self.action_rows: list[dict[str, object]] = []

    def claim_next_action(
        self,
        *,
        worker_name: str,
        action_codes: tuple[str, ...] | None = None,
    ) -> dict[str, object] | None:
        for row in self.action_rows:
            if row.get("status") != ACTION_STATUS_QUEUED:
                continue
            if action_codes is not None and row.get("action_code") not in action_codes:
                continue

            row["status"] = ACTION_STATUS_PROCESSING
            row["stage"] = STAGE_PROCESSING_ACTION
            row["locked_by"] = worker_name
            row["attempts"] = int(row.get("attempts") or 0) + 1
            return dict(row)
        return None

    def mark_action_done(
        self,
        action_job_id: int,
        *,
        result_json: object = None,
        stage: str = "DONE",
    ) -> None:
        self.done_calls.append(
            {
                "action_job_id": action_job_id,
                "result_json": result_json,
                "stage": stage,
            }
        )

    def mark_action_for_retry(
        self,
        action_job_id: int,
        *,
        delay_seconds: int,
        last_error: str,
    ) -> None:
        self.retry_calls.append(
            {
                "action_job_id": action_job_id,
                "delay_seconds": delay_seconds,
                "last_error": last_error,
            }
        )

    def mark_action_dead(
        self,
        action_job_id: int,
        *,
        last_error: str,
        result_json: object = None,
        stage: str = STAGE_FAILED,
    ) -> None:
        self.dead_calls.append(
            {
                "action_job_id": action_job_id,
                "last_error": last_error,
                "result_json": result_json,
                "stage": stage,
            }
        )

    def set_action_stage(self, action_job_id: int, *, stage: str) -> None:
        self.stage_calls.append({"action_job_id": action_job_id, "stage": stage})

    def set_action_outbound_json(
        self,
        action_job_id: int,
        *,
        outbound_json: object,
        stage: str | None = None,
    ) -> None:
        self.outbound_calls.append(
            {
                "action_job_id": action_job_id,
                "outbound_json": outbound_json,
                "stage": stage,
            }
        )

    def requeue_stale_processing_actions(
        self,
        *,
        older_than_seconds: int,
        worker_name: str | None = None,
    ) -> int:
        del older_than_seconds, worker_name
        return self.recovered

    def get_next_action_available_at(self):
        return self.next_action_available_at


class SuccessProcessor(daemon.ActionProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        return {"processor": "success"}


class BlockingCalculateProcessor(daemon.ActionProcessor):
    def __init__(self) -> None:
        self.blocking_started = asyncio.Event()
        self.release_blocking = asyncio.Event()
        self.report_processed = asyncio.Event()
        self.processed_action_codes: list[str] = []

    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, context
        action_code = str(row.get("action_code") or "")
        self.processed_action_codes.append(action_code)
        if action_code == ACTION_CALCULATE_INCOME_EXPENSES:
            self.blocking_started.set()
            await self.release_blocking.wait()
        if action_code == ACTION_SHOW_ALL:
            self.report_processed.set()
        return {"processor": "blocking-calculate"}


class ContextProcessor(daemon.ActionProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row
        await context.set_stage(STAGE_PROCESSING_ACTION)
        await context.set_outbound_json({"message_ids": [1]}, STAGE_PROCESSING_ACTION)
        return {"processor": "context"}


class AiPromptStageProcessor(daemon.ActionProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row
        await context.set_stage(STAGE_ANSWERING_QUESTION)
        return {"processor": "ai-prompt-stage"}


class RetryProcessor(daemon.ActionProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise daemon.RetryableJobError("temporary action failure")


class RetryAfterProcessor(daemon.ActionProcessor):
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


class PermanentProcessor(daemon.ActionProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise daemon.PermanentJobError("bad action payload")


class ExplodingProcessor(daemon.ActionProcessor):
    async def process(
        self,
        bot: object,
        row: dict[str, object],
        *,
        context: object = None,
    ) -> dict[str, object]:
        del bot, row, context
        raise RuntimeError("boom")


class TelegramActionDaemonTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_args_accepts_uppercase_debug_alias(self) -> None:
        args = daemon.parse_args(["--DEBUG"])

        self.assertTrue(args.debug)

    def test_parse_args_defaults_to_long_idle_poll(self) -> None:
        args = daemon.parse_args([])

        self.assertEqual(args.poll_seconds, 120)

    def make_config(self) -> daemon.Config:
        repo_root = Path(__file__).resolve().parents[1]
        return daemon.Config(
            token="token",
            sqlite_settings=SQLiteSettings(
                db_path=str(repo_root / ".test-telegram-action-daemon.sqlite")
            ),
            worker_name="action-daemon",
            poll_seconds=300,
            stale_lock_seconds=1800,
            queue_max_attempts=daemon.DEFAULT_MAX_ATTEMPTS,
            pidfile_path=str(repo_root / ".test-telegram-action-daemon.pid"),
        )

    def make_row(
        self,
        *,
        id: int = 7,
        queue_id: int = 42,
        attempts: int = 1,
        max_attempts: int = daemon.DEFAULT_MAX_ATTEMPTS,
        action_code: str = "LOG_EXPENSES",
        status: str | None = None,
    ) -> dict[str, object]:
        row = {
            "id": id,
            "queue_id": queue_id,
            "action_code": action_code,
            "attempts": attempts,
            "max_attempts": max_attempts,
            "chat_id": -1003986727769,
            "chat_type": "supergroup",
            "message_id": 41,
        }
        if status is not None:
            row["status"] = status
        return row

    def make_bot(self) -> SimpleNamespace:
        reactions: list[str] = []

        async def record_reaction(**kwargs: object) -> bool:
            reactions.append(str(kwargs["reaction"]))
            return True

        return SimpleNamespace(
            reactions=reactions,
            set_message_reaction=AsyncMock(side_effect=record_reaction),
        )

    async def test_wait_for_wakeup_consumes_sighup_received_before_idle(self) -> None:
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=FakeActionStore(),
        )

        worker._handle_signal(signal.SIGHUP)
        worker._handle_signal(signal.SIGHUP)
        self.assertEqual(worker._pending_wakeups, 2)

        await worker.wait_for_wakeup()

        self.assertEqual(worker._pending_wakeups, 0)
        self.assertFalse(worker._wake_event.is_set())

    async def test_usr1_requests_restart_and_wakes_loop(self) -> None:
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=FakeActionStore(),
        )

        worker._handle_signal(signal.SIGUSR1)

        self.assertTrue(worker._restart_requested)
        self.assertTrue(worker._stop_requested)
        self.assertTrue(worker._wake_event.is_set())

    async def test_show_all_runs_while_calculate_action_is_blocked(self) -> None:
        store = FakeActionStore()
        store.action_rows.append(
            self.make_row(
                id=1,
                queue_id=10,
                action_code=ACTION_CALCULATE_INCOME_EXPENSES,
                attempts=0,
                status=ACTION_STATUS_QUEUED,
            )
        )
        bot = self.make_bot()
        processor = BlockingCalculateProcessor()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=processor,
        )

        drain_task = asyncio.create_task(worker.drain_due_actions(bot))
        await asyncio.wait_for(processor.blocking_started.wait(), timeout=1)

        store.action_rows.append(
            self.make_row(
                id=2,
                queue_id=11,
                action_code=ACTION_SHOW_ALL,
                attempts=0,
                status=ACTION_STATUS_QUEUED,
            )
        )
        worker._handle_signal(signal.SIGHUP)

        await asyncio.wait_for(processor.report_processed.wait(), timeout=1)
        processor.release_blocking.set()
        processed = await asyncio.wait_for(drain_task, timeout=1)

        self.assertEqual(processed, 2)
        self.assertEqual(
            processor.processed_action_codes,
            [ACTION_CALCULATE_INCOME_EXPENSES, ACTION_SHOW_ALL],
        )
        self.assertEqual(
            [call["action_job_id"] for call in store.done_calls],
            [2, 1],
        )

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_marks_success_done_and_sets_reactions(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=SuccessProcessor(),
        )

        await worker.process_one_action(bot, self.make_row())

        self.assertEqual(
            store.done_calls,
            [
                {
                    "action_job_id": 7,
                    "result_json": {
                        "processor": "success",
                        "outcome": "processed",
                        "attempts": 1,
                        "action_code": "LOG_EXPENSES",
                    },
                    "stage": "DONE",
                }
            ],
        )
        self.assertEqual(bot.reactions, ["✍", "👌"])

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_supplies_processing_context(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=ContextProcessor(),
        )

        await worker.process_one_action(bot, self.make_row())

        self.assertEqual(
            store.stage_calls,
            [{"action_job_id": 7, "stage": STAGE_PROCESSING_ACTION}],
        )
        self.assertEqual(
            store.outbound_calls,
            [
                {
                    "action_job_id": 7,
                    "outbound_json": {"message_ids": [1]},
                    "stage": STAGE_PROCESSING_ACTION,
                }
            ],
        )

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_sets_lightning_for_ai_prompt_stage(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=AiPromptStageProcessor(),
        )

        await worker.process_one_action(
            bot,
            self.make_row(action_code="ANSWER_QUESTION"),
        )

        self.assertEqual(
            store.stage_calls,
            [{"action_job_id": 7, "stage": STAGE_ANSWERING_QUESTION}],
        )
        self.assertEqual(bot.reactions, ["✍", "⚡", "👌"])

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_schedules_retry_and_sets_retry_reaction(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=RetryProcessor(),
        )

        await worker.process_one_action(bot, self.make_row(attempts=1))

        self.assertEqual(
            store.retry_calls,
            [
                {
                    "action_job_id": 7,
                    "delay_seconds": 1,
                    "last_error": "temporary action failure",
                }
            ],
        )
        self.assertEqual(bot.reactions, ["✍", "🤔"])

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_honors_retry_after(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=RetryAfterProcessor(),
        )

        await worker.process_one_action(bot, self.make_row(attempts=1))

        self.assertEqual(store.retry_calls[0]["delay_seconds"], 25)
        self.assertEqual(bot.reactions, ["✍", "🤔"])

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_marks_retry_exhausted_dead(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=RetryProcessor(),
        )

        await worker.process_one_action(
            bot,
            self.make_row(attempts=daemon.DEFAULT_MAX_ATTEMPTS),
        )

        self.assertEqual(
            store.dead_calls,
            [
                {
                    "action_job_id": 7,
                    "last_error": "temporary action failure",
                    "result_json": {
                        "outcome": "dead",
                        "failure_class": "retry_exhausted",
                        "attempts": daemon.DEFAULT_MAX_ATTEMPTS,
                        "action_code": "LOG_EXPENSES",
                    },
                    "stage": STAGE_FAILED,
                }
            ],
        )
        self.assertEqual(bot.reactions, ["✍", "💔"])

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_marks_permanent_failure_dead(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=PermanentProcessor(),
        )

        await worker.process_one_action(bot, self.make_row())

        self.assertEqual(store.dead_calls[0]["last_error"], "bad action payload")
        self.assertEqual(store.dead_calls[0]["stage"], STAGE_FAILED)
        self.assertEqual(bot.reactions, ["✍", "💔"])

    @patch("telegram_action_daemon.asyncio.to_thread", new=immediate_to_thread)
    async def test_process_one_action_treats_unknown_exceptions_as_retryable(self) -> None:
        store = FakeActionStore()
        bot = self.make_bot()
        worker = daemon.ActionDaemon(
            config=self.make_config(),
            queue_store=store,
            processor=ExplodingProcessor(),
        )

        await worker.process_one_action(bot, self.make_row(attempts=2))

        self.assertEqual(
            store.retry_calls,
            [
                {
                    "action_job_id": 7,
                    "delay_seconds": 3,
                    "last_error": "boom",
                }
            ],
        )
        self.assertEqual(bot.reactions, ["✍", "🤔"])

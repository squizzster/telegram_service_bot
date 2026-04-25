from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from bot import TelegramBotRunner, WebhookHTTPServer
from bot_libs.queue_enqueue import QueuePersistResult
from bot_libs.queue_models import CONTENT_TYPE_TEXT, QueueInsertResult, QueueJobData


def make_job() -> QueueJobData:
    return QueueJobData(
        update_id=449012319,
        chat_id=-1003986727769,
        message_id=41,
        telegram_date="2026-04-23T18:42:37+00:00",
        content_type=CONTENT_TYPE_TEXT,
        is_supported=True,
        text="hello",
        payload_json="{}",
        raw_update_json="{}",
        max_attempts=3,
        processing_text="hello",
    )


class CreateTaskRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def __call__(
        self,
        coroutine,
        update: object | None = None,
        *,
        name: str | None = None,
    ) -> None:
        self.calls.append(
            {
                "coroutine": coroutine,
                "update": update,
                "name": name,
            }
        )
        coroutine.close()


class TelegramBotRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_persist_and_dispatch_update_schedules_feedback_after_insert(self) -> None:
        runner = TelegramBotRunner.__new__(TelegramBotRunner)
        update = SimpleNamespace(update_id=449012319)
        persist_result = QueuePersistResult(
            job=make_job(),
            insert_result=QueueInsertResult(queue_id=99),
        )
        task_recorder = CreateTaskRecorder()
        runner.queue_enqueuer = SimpleNamespace(
            persist_update=AsyncMock(return_value=persist_result),
            apply_post_persist_feedback=AsyncMock(),
        )
        runner.ptb_app = SimpleNamespace(
            create_task=task_recorder,
            process_update=AsyncMock(),
        )
        runner.wake_queue_daemon = AsyncMock()

        result = await TelegramBotRunner.persist_and_dispatch_update(
            runner,
            update,
            emit_status_feedback=True,
        )

        self.assertIs(result, persist_result)
        runner.queue_enqueuer.persist_update.assert_awaited_once_with(update)
        runner.wake_queue_daemon.assert_awaited_once_with(449012319)
        runner.queue_enqueuer.apply_post_persist_feedback.assert_called_once_with(
            update,
            persist_result,
        )
        runner.ptb_app.process_update.assert_called_once_with(update)
        self.assertEqual(
            [call["name"] for call in task_recorder.calls],
            [
                "post-persist-feedback:449012319",
                "process-update:449012319",
            ],
        )

    async def test_persist_and_dispatch_update_skips_side_effects_for_duplicates(self) -> None:
        runner = TelegramBotRunner.__new__(TelegramBotRunner)
        update = SimpleNamespace(update_id=449012319)
        persist_result = QueuePersistResult(
            job=make_job(),
            insert_result=QueueInsertResult(queue_id=None, duplicate=True),
        )
        task_recorder = CreateTaskRecorder()
        runner.queue_enqueuer = SimpleNamespace(
            persist_update=AsyncMock(return_value=persist_result),
            apply_post_persist_feedback=AsyncMock(),
        )
        runner.ptb_app = SimpleNamespace(
            create_task=task_recorder,
            process_update=AsyncMock(),
        )
        runner.wake_queue_daemon = AsyncMock()

        result = await TelegramBotRunner.persist_and_dispatch_update(
            runner,
            update,
            emit_status_feedback=True,
        )

        self.assertIs(result, persist_result)
        runner.queue_enqueuer.persist_update.assert_awaited_once_with(update)
        runner.wake_queue_daemon.assert_not_awaited()
        runner.queue_enqueuer.apply_post_persist_feedback.assert_not_called()
        runner.ptb_app.process_update.assert_not_called()
        self.assertEqual(task_recorder.calls, [])

    async def test_persist_and_dispatch_update_treats_wake_failure_as_best_effort(self) -> None:
        runner = TelegramBotRunner.__new__(TelegramBotRunner)
        update = SimpleNamespace(update_id=449012319)
        persist_result = QueuePersistResult(
            job=make_job(),
            insert_result=QueueInsertResult(queue_id=99),
        )
        task_recorder = CreateTaskRecorder()
        runner.queue_enqueuer = SimpleNamespace(
            persist_update=AsyncMock(return_value=persist_result),
            apply_post_persist_feedback=AsyncMock(),
        )
        runner.ptb_app = SimpleNamespace(
            create_task=task_recorder,
            process_update=AsyncMock(),
        )
        runner.wake_queue_daemon = AsyncMock(side_effect=RuntimeError("wake failed"))

        result = await TelegramBotRunner.persist_and_dispatch_update(
            runner,
            update,
            emit_status_feedback=True,
        )

        self.assertIs(result, persist_result)
        runner.wake_queue_daemon.assert_awaited_once_with(449012319)
        runner.ptb_app.process_update.assert_called_once_with(update)

    async def test_persist_and_dispatch_update_treats_process_task_failure_as_best_effort(self) -> None:
        runner = TelegramBotRunner.__new__(TelegramBotRunner)
        update = SimpleNamespace(update_id=449012319)
        persist_result = QueuePersistResult(
            job=make_job(),
            insert_result=QueueInsertResult(queue_id=99),
        )
        runner.queue_enqueuer = SimpleNamespace(
            persist_update=AsyncMock(return_value=persist_result),
            apply_post_persist_feedback=AsyncMock(),
        )
        runner.ptb_app = SimpleNamespace(
            create_task=Mock(side_effect=RuntimeError("schedule failed")),
            process_update=AsyncMock(),
        )
        runner.wake_queue_daemon = AsyncMock()

        result = await TelegramBotRunner.persist_and_dispatch_update(
            runner,
            update,
            emit_status_feedback=True,
        )

        self.assertIs(result, persist_result)
        runner.wake_queue_daemon.assert_awaited_once_with(449012319)
        runner.ptb_app.process_update.assert_called_once_with(update)

    async def test_recover_pending_updates_restores_webhook_on_failure(self) -> None:
        runner = TelegramBotRunner.__new__(TelegramBotRunner)
        recovered_update = SimpleNamespace(
            update_id=449012319,
            effective_message=None,
            effective_chat=None,
        )
        bot = SimpleNamespace(
            delete_webhook=AsyncMock(),
            get_updates=AsyncMock(return_value=[recovered_update]),
        )
        runner.ptb_app = SimpleNamespace(bot=bot)
        runner.log_webhook_info = AsyncMock()
        runner.install_webhook = AsyncMock()
        runner.persist_and_dispatch_update = AsyncMock(
            side_effect=RuntimeError("queue persistence failed")
        )

        with self.assertRaises(RuntimeError):
            await TelegramBotRunner.recover_pending_updates_from_telegram(runner)

        bot.delete_webhook.assert_awaited_once_with(drop_pending_updates=False)
        runner.persist_and_dispatch_update.assert_awaited_once_with(
            recovered_update,
            emit_status_feedback=False,
        )
        runner.install_webhook.assert_awaited_once()


class WebhookHTTPServerTests(unittest.TestCase):
    def test_uvicorn_access_log_is_disabled(self) -> None:
        config = SimpleNamespace(
            webhook_path="/telegram",
            service_dump_dir=None,
            listen_host="127.0.0.1",
            listen_port=0,
        )

        server = WebhookHTTPServer(
            config=config,
            ptb_app=SimpleNamespace(),
            accept_update=AsyncMock(),
            debug=False,
        )

        self.assertFalse(server.server.config.access_log)

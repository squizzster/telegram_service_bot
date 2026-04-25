from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telegram.constants import ChatType
from telegram.error import RetryAfter, TelegramError

from bot_libs.queue_enqueue import (
    QueueEnqueueError,
    QueueEnqueuer,
    QueuePersistResult,
    try_set_status_reaction,
)
from bot_libs.queue_models import (
    CONTENT_TYPE_TEXT,
    QueueInsertResult,
    QueueJobData,
    STATUS_DONE,
    STATUS_QUEUED,
)
from bot_libs.reaction_policy import REACTION_ACCEPTED, REACTION_FAILURE
from bot_libs.reaction_policy import _REACTION_RATE_LIMIT_SUPPRESSED_UNTIL
from bot_libs.stage_names import STAGE_DONE, STAGE_QUEUED
from bot_libs.sql import SchemaVerificationError


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


class BrokenQueueStore:
    def insert_queue_job(self, job: QueueJobData) -> None:
        del job
        raise SchemaVerificationError(
            "SQLite database file does not exist: /tmp/telegram.sqlite"
        )


class FeedbackQueueStore:
    def __init__(self, row: dict[str, object] | None = None) -> None:
        self.row = row or {
            "id": 99,
            "status": STATUS_QUEUED,
            "stage": STAGE_QUEUED,
            "is_supported": 1,
        }

    def get_queue_job(self, job_id: int) -> dict[str, object] | None:
        if job_id != 99:
            return None
        return dict(self.row)


async def immediate_to_thread(func, *args, **kwargs):
    return func(*args, **kwargs)


class QueueEnqueuerTests(unittest.IsolatedAsyncioTestCase):
    @patch("bot_libs.queue_enqueue.asyncio.to_thread", new=immediate_to_thread)
    @patch("bot_libs.queue_enqueue.extract_queue_job")
    async def test_persist_update_wraps_store_errors(
        self,
        extract_queue_job_mock,
    ) -> None:
        extract_queue_job_mock.return_value = make_job()
        update = SimpleNamespace(
            update_id=449012319,
            effective_message=SimpleNamespace(message_id=41),
            effective_chat=SimpleNamespace(id=-1003986727769),
        )
        enqueuer = QueueEnqueuer(queue_store=BrokenQueueStore(), max_attempts=3)

        with self.assertRaises(QueueEnqueueError) as exc_info:
            await enqueuer.persist_update(update)

        self.assertEqual(exc_info.exception.reason, "queue_store_unavailable")
        self.assertEqual(exc_info.exception.retry_delay_seconds, 1.0)

    @patch("bot_libs.queue_enqueue.try_set_status_reaction", new_callable=AsyncMock)
    async def test_apply_post_persist_feedback_marks_processing_after_insert(
        self,
        try_set_status_reaction_mock,
    ) -> None:
        update = SimpleNamespace(
            effective_message=SimpleNamespace(message_id=41),
            effective_chat=SimpleNamespace(id=-1003986727769, type=ChatType.SUPERGROUP),
        )
        persist_result = QueuePersistResult(
            job=make_job(),
            insert_result=QueueInsertResult(queue_id=99),
        )
        enqueuer = QueueEnqueuer(queue_store=FeedbackQueueStore(), max_attempts=3)

        await enqueuer.apply_post_persist_feedback(update, persist_result)

        try_set_status_reaction_mock.assert_awaited_once_with(
            update,
            REACTION_ACCEPTED,
            reason="queue_persisted",
        )

    @patch("bot_libs.queue_enqueue.asyncio.to_thread", new=immediate_to_thread)
    @patch("bot_libs.queue_enqueue.try_set_status_reaction", new_callable=AsyncMock)
    async def test_apply_post_persist_feedback_skips_stale_accepted_and_reconciles_done(
        self,
        try_set_status_reaction_mock,
    ) -> None:
        update = SimpleNamespace(
            effective_message=SimpleNamespace(message_id=41),
            effective_chat=SimpleNamespace(id=-1003986727769, type=ChatType.SUPERGROUP),
        )
        persist_result = QueuePersistResult(
            job=make_job(),
            insert_result=QueueInsertResult(queue_id=99),
        )
        enqueuer = QueueEnqueuer(
            queue_store=FeedbackQueueStore(
                {
                    "id": 99,
                    "status": STATUS_DONE,
                    "stage": STAGE_DONE,
                    "is_supported": 1,
                }
            ),
            max_attempts=3,
        )

        await enqueuer.apply_post_persist_feedback(update, persist_result)

        try_set_status_reaction_mock.assert_awaited_once_with(
            update,
            "👌",
            reason="post_persist_reconcile",
        )

    @patch("bot_libs.queue_enqueue.asyncio.to_thread", new=immediate_to_thread)
    async def test_apply_post_persist_feedback_reasserts_done_after_late_accepted(
        self,
    ) -> None:
        store = FeedbackQueueStore()
        reactions: list[str] = []

        async def set_reaction(emoji: str) -> bool:
            reactions.append(emoji)
            if emoji == REACTION_ACCEPTED:
                store.row = {
                    "id": 99,
                    "status": STATUS_DONE,
                    "stage": STAGE_DONE,
                    "is_supported": 1,
                }
            return True

        update = SimpleNamespace(
            effective_message=SimpleNamespace(
                message_id=41,
                set_reaction=AsyncMock(side_effect=set_reaction),
            ),
            effective_chat=SimpleNamespace(id=-1003986727769, type=ChatType.SUPERGROUP),
        )
        persist_result = QueuePersistResult(
            job=make_job(),
            insert_result=QueueInsertResult(queue_id=99),
        )
        enqueuer = QueueEnqueuer(queue_store=store, max_attempts=3)

        await enqueuer.apply_post_persist_feedback(update, persist_result)

        self.assertEqual(reactions, [REACTION_ACCEPTED, "👌"])

    @patch("bot_libs.queue_enqueue.asyncio.sleep", new_callable=AsyncMock)
    async def test_try_set_status_reaction_retries_once_after_telegram_error(
        self,
        sleep_mock,
    ) -> None:
        message = SimpleNamespace(
            message_id=41,
            set_reaction=AsyncMock(side_effect=[TelegramError("temporary"), True]),
        )
        update = SimpleNamespace(
            effective_message=message,
            effective_chat=SimpleNamespace(id=-1003986727769, type=ChatType.SUPERGROUP),
        )

        await try_set_status_reaction(
            update,
            REACTION_FAILURE,
            reason="queue_store_unavailable",
            retry_delay_seconds=1.0,
        )

        self.assertEqual(message.set_reaction.await_count, 2)
        sleep_mock.assert_awaited_once_with(1.0)

    async def test_try_set_status_reaction_suppresses_repeated_retry_after(self) -> None:
        _REACTION_RATE_LIMIT_SUPPRESSED_UNTIL.clear()
        message = SimpleNamespace(
            message_id=41,
            set_reaction=AsyncMock(side_effect=RetryAfter(12)),
        )
        update = SimpleNamespace(
            effective_message=message,
            effective_chat=SimpleNamespace(id=-1003986727769, type=ChatType.SUPERGROUP),
        )

        try:
            first = await try_set_status_reaction(
                update,
                REACTION_ACCEPTED,
                reason="queue_persisted",
            )
            second = await try_set_status_reaction(
                update,
                REACTION_ACCEPTED,
                reason="queue_persisted",
            )
        finally:
            _REACTION_RATE_LIMIT_SUPPRESSED_UNTIL.clear()

        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(message.set_reaction.await_count, 1)

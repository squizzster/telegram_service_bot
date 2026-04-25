from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from telegram import Update

from bot_libs.reaction_policy import (
    REACTION_ACCEPTED,
    REACTION_UNSUPPORTED,
    can_use_status_reactions,
    reaction_for_row_state,
    reaction_is_terminal_for_row,
    set_update_reaction,
)
from bot_libs.queue_extract import extract_queue_job
from bot_libs.queue_models import (
    QueueInsertResult,
    QueueJobData,
)
from bot_libs.sql import QueueStoreError, SQLiteQueueStore

log = logging.getLogger(__name__)


async def try_set_status_reaction(
    update: Update,
    emoji: str,
    *,
    reason: str,
    retry_delay_seconds: float = 0.0,
) -> None:
    await set_update_reaction(
        update,
        emoji,
        reason=reason,
        retry_delay_seconds=retry_delay_seconds,
    )


@dataclass(frozen=True, slots=True)
class QueuePersistResult:
    job: QueueJobData | None = None
    insert_result: QueueInsertResult | None = None

    @property
    def skipped(self) -> bool:
        return self.job is None

    @property
    def duplicate(self) -> bool:
        return bool(self.insert_result and self.insert_result.duplicate)

    @property
    def inserted(self) -> bool:
        return self.job is not None and self.insert_result is not None and not self.insert_result.duplicate


class QueueEnqueueError(RuntimeError):
    def __init__(self, *, reason: str, retry_delay_seconds: float = 0.0) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retry_delay_seconds = retry_delay_seconds


class QueueEnqueuer:
    def __init__(self, *, queue_store: SQLiteQueueStore, max_attempts: int) -> None:
        self.queue_store = queue_store
        self.max_attempts = max_attempts

    async def persist_update(self, update: Update) -> QueuePersistResult:
        message = update.effective_message
        chat = update.effective_chat
        if message is None or chat is None:
            log.debug(
                "Skipping queue persistence for non-message update update_id=%s",
                update.update_id,
            )
            return QueuePersistResult()

        try:
            # Commands are intentionally persisted like any other inbound message.
            job = extract_queue_job(update, max_attempts=self.max_attempts)
        except Exception as exc:
            log.exception(
                "Failed to extract queue job update_id=%s chat_id=%s message_id=%s",
                update.update_id,
                chat.id,
                message.message_id,
            )
            raise QueueEnqueueError(reason="queue_extract_failed") from exc

        if job is None:
            return QueuePersistResult()

        try:
            insert_result = await asyncio.to_thread(self.queue_store.insert_queue_job, job)
        except QueueStoreError as exc:
            log.error(
                "Queue store unavailable for update_id=%s chat_id=%s message_id=%s content_type=%s: %s",
                job.update_id,
                job.chat_id,
                job.message_id,
                job.content_type,
                exc,
            )
            raise QueueEnqueueError(
                reason="queue_store_unavailable",
                retry_delay_seconds=1.0,
            ) from exc
        except Exception as exc:
            log.exception(
                "Failed to enqueue update_id=%s chat_id=%s message_id=%s content_type=%s",
                job.update_id,
                job.chat_id,
                job.message_id,
                job.content_type,
            )
            raise QueueEnqueueError(reason="queue_insert_failed") from exc

        if insert_result.duplicate:
            log.info(
                "Duplicate update skipped update_id=%s chat_id=%s message_id=%s",
                job.update_id,
                job.chat_id,
                job.message_id,
            )
            return QueuePersistResult(job=job, insert_result=insert_result)

        log.info(
            "Enqueued message queue_id=%s update_id=%s chat_id=%s message_id=%s content_type=%s supported=%s",
            insert_result.queue_id,
            job.update_id,
            job.chat_id,
            job.message_id,
            job.content_type,
            job.is_supported,
        )
        return QueuePersistResult(job=job, insert_result=insert_result)

    async def apply_post_persist_feedback(
        self,
        update: Update,
        persist_result: QueuePersistResult,
    ) -> None:
        if not persist_result.inserted or persist_result.job is None:
            return
        if persist_result.insert_result is None or persist_result.insert_result.queue_id is None:
            return

        emoji = REACTION_ACCEPTED
        reason = "queue_persisted"
        if not persist_result.job.is_supported:
            emoji = REACTION_UNSUPPORTED
            reason = "unsupported_content_type"

        await self._send_guarded_post_persist_reaction(
            update,
            queue_id=persist_result.insert_result.queue_id,
            emoji=emoji,
            reason=reason,
        )

    async def _send_guarded_post_persist_reaction(
        self,
        update: Update,
        *,
        queue_id: int,
        emoji: str,
        reason: str,
    ) -> None:
        row = await self._load_queue_job_for_feedback(queue_id)
        if row is None:
            return

        current_emoji = reaction_for_row_state(row)
        if current_emoji != emoji:
            await self._reconcile_post_persist_reaction(
                update,
                queue_id=queue_id,
                previous_emoji=emoji,
                row=row,
            )
            return

        await try_set_status_reaction(
            update,
            emoji,
            reason=reason,
        )
        await self._reconcile_post_persist_reaction(
            update,
            queue_id=queue_id,
            previous_emoji=emoji,
        )

    async def _reconcile_post_persist_reaction(
        self,
        update: Update,
        *,
        queue_id: int,
        previous_emoji: str,
        row: dict[str, object] | None = None,
    ) -> None:
        current_row = row or await self._load_queue_job_for_feedback(queue_id)
        if current_row is None:
            return

        current_emoji = reaction_for_row_state(current_row)
        if current_emoji is None or current_emoji == previous_emoji:
            return
        if not reaction_is_terminal_for_row(current_row) and current_emoji == REACTION_ACCEPTED:
            return

        await try_set_status_reaction(
            update,
            current_emoji,
            reason="post_persist_reconcile",
        )

    async def _load_queue_job_for_feedback(self, queue_id: int) -> dict[str, object] | None:
        get_queue_job = getattr(self.queue_store, "get_queue_job", None)
        if get_queue_job is None:
            return None
        try:
            return await asyncio.to_thread(get_queue_job, queue_id)
        except QueueStoreError:
            log.debug(
                "post_persist_feedback_skipped reason=queue_state_unavailable queue_id=%s",
                queue_id,
                exc_info=True,
            )
            return None

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from telegram import Bot  # noqa: E402

from bot_libs.action_commands import direct_action_command_for_text  # noqa: E402
from bot_libs.action_detection import ActionDetectionService  # noqa: E402
from bot_libs.action_models import (  # noqa: E402
    ACTION_DETECTION_PENDING,
    ACTION_DETECTION_PROCESSING,
)
from bot_libs.daemon_signal import (  # noqa: E402
    DAEMON_PROCESS_NAME,
    resolve_daemon_pidfile,
    resolve_action_daemon_pidfile,
    wake_action_daemon,
)
from bot_libs.logging_utils import configure_app_logging  # noqa: E402
from bot_libs.process_guard import (  # noqa: E402
    ProcessAlreadyRunningError,
    acquire_process_guard,
)
from bot_libs.pipeline_status import PipelineStatusController  # noqa: E402
from bot_libs.queue_processing_context import QueueProcessingContext  # noqa: E402
from bot_libs.queue_models import (  # noqa: E402
    DEFAULT_QUEUE_MAX_ATTEMPTS,
)
from bot_libs.queue_processor_errors import (  # noqa: E402
    OriginalMessageUnavailableError,
    PermanentJobError,
    RetryableJobError,
)
from bot_libs.queue_processors.dispatch import (  # noqa: E402
    DispatchingQueueJobProcessor,
)
from bot_libs.queue_processors.voice import (  # noqa: E402
    send_deferred_transcript_reply,
)
from bot_libs.sql import (  # noqa: E402
    QueueStoreError,
    SQLiteQueueStore,
    SQLiteSettings,
    SchemaVerificationError,
    build_sqlite_settings_from_env,
)
from bot_libs.reaction_policy import (  # noqa: E402
    REACTION_CALLING_STT_API,
    REACTION_FAILURE,
    REACTION_SUCCESS,
    REACTION_UNSUPPORTED,
    reaction_for_stage,
    reaction_for_row_state,
    retry_reaction_for_delay,
    set_row_reaction,
)
from bot_libs.retry_policy import (  # noqa: E402
    RETRY_SHORT_MAX_SECONDS,
    next_retry_delay_seconds,
)
from bot_libs.runtime_checks import (  # noqa: E402
    RuntimeCheckError,
    run_runtime_system_checks,
)
from bot_libs.stage_names import (  # noqa: E402
    STAGE_DETECTING_ACTIONS,
    STAGE_FAILED,
    STAGE_MESSAGE_REMOVED,
    STAGE_UNSUPPORTED,
)
from bot_libs.status_feedback import ReactionFeedbackClient  # noqa: E402

log = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = DEFAULT_QUEUE_MAX_ATTEMPTS
DEFAULT_POLL_SECONDS = 120
REACTION_RECONCILE_DELAY_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class Config:
    token: str
    sqlite_settings: SQLiteSettings
    worker_name: str
    poll_seconds: int
    stale_lock_seconds: int
    queue_max_attempts: int
    pidfile_path: str
    action_daemon_pidfile_path: str

    @classmethod
    def from_env(
        cls,
        *,
        worker_name: str,
        poll_seconds: int,
        stale_lock_seconds: int,
        pidfile_path: str | None = None,
    ) -> "Config":
        normalized_worker_name = worker_name.strip()
        if not normalized_worker_name:
            raise ValueError("worker_name must not be empty")
        if poll_seconds < 1:
            raise ValueError("poll_seconds must be >= 1")
        if stale_lock_seconds < 1:
            raise ValueError("stale_lock_seconds must be >= 1")

        token = os.environ["TELEGRAM_BOT_KEY"]
        queue_max_attempts = int(
            os.getenv("QUEUE_MAX_ATTEMPTS", str(DEFAULT_MAX_ATTEMPTS))
        )
        if not 1 <= queue_max_attempts <= DEFAULT_MAX_ATTEMPTS:
            raise ValueError(
                "QUEUE_MAX_ATTEMPTS must be between 1 and "
                f"{DEFAULT_MAX_ATTEMPTS}"
            )

        return cls(
            token=token,
            sqlite_settings=build_sqlite_settings_from_env(),
            worker_name=normalized_worker_name,
            poll_seconds=poll_seconds,
            stale_lock_seconds=stale_lock_seconds,
            queue_max_attempts=queue_max_attempts,
            pidfile_path=resolve_daemon_pidfile(pidfile_path),
            action_daemon_pidfile_path=resolve_action_daemon_pidfile(),
        )


class QueueJobProcessor:
    async def process(
        self,
        bot: Bot,
        row: dict[str, object],
        *,
        context: QueueProcessingContext | None = None,
    ) -> Mapping[str, object]:
        raise NotImplementedError


class StubQueueJobProcessor(QueueJobProcessor):
    async def process(
        self,
        bot: Bot,
        row: dict[str, object],
        *,
        context: QueueProcessingContext | None = None,
    ) -> Mapping[str, object]:
        del bot, row, context
        await asyncio.sleep(0)
        return {
            "outcome": "processed",
            "processor": "stub",
        }


class QueueDaemon:
    def __init__(
        self,
        *,
        config: Config,
        queue_store: SQLiteQueueStore,
        processor: QueueJobProcessor | None = None,
        action_detector: ActionDetectionService | None = None,
    ) -> None:
        self.config = config
        self.queue_store = queue_store
        self.processor = processor or DispatchingQueueJobProcessor()
        self.action_detector = action_detector or ActionDetectionService(
            queue_store=queue_store
        )
        self._wake_event = asyncio.Event()
        self._pending_wakeups = 0
        self._stop_requested = False
        self._reaction_reconcile_tasks: set[asyncio.Task[None]] = set()

    async def run(self, *, once: bool = False) -> None:
        log.debug(
            "Queue daemon starting worker=%s db_path=%s poll_seconds=%s "
            "stale_lock_seconds=%s max_attempts=%s pidfile=%s once=%s",
            self.config.worker_name,
            self.config.sqlite_settings.db_path,
            self.config.poll_seconds,
            self.config.stale_lock_seconds,
            self.config.queue_max_attempts,
            self.config.pidfile_path,
            once,
        )
        log.info(
            "queue daemon started worker=%s poll_seconds=%s max_attempts=%s",
            self.config.worker_name,
            self.config.poll_seconds,
            self.config.queue_max_attempts,
        )
        await asyncio.to_thread(self.queue_store.verify_schema)
        log.debug("Queue schema verification complete")

        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)
        log.debug("Signal handlers installed for SIGHUP/SIGTERM/SIGINT")

        async with Bot(self.config.token) as bot:
            await self._recover_stale_processing_jobs()
            while True:
                await self._recover_stale_processing_jobs()
                processed = await self.drain_due_jobs(bot)
                if processed > 0:
                    log.debug("Drain pass complete processed_count=%s", processed)

                if once or self._stop_requested:
                    log.debug(
                        "Queue daemon exiting once=%s stop_requested=%s",
                        once,
                        self._stop_requested,
                    )
                    await self._drain_reaction_reconcile_tasks()
                    return

                await self.wait_for_wakeup()

    async def drain_due_jobs(self, bot: Bot) -> int:
        processed = 0

        while not self._stop_requested:
            try:
                row = await asyncio.to_thread(
                    self.queue_store.claim_next_job,
                    worker_name=self.config.worker_name,
                )
            except (QueueStoreError, SchemaVerificationError):
                log.exception("Failed to claim next queue job")
                return processed
            if row is None:
                if processed > 0:
                    log.debug(
                        "No more due queue jobs claimed worker=%s processed_count=%s",
                        self.config.worker_name,
                        processed,
                    )
                return processed

            processed += 1
            log.debug(
                "job=%s claimed update_id=%s chat_id=%s message_id=%s "
                "type=%s attempts=%s max_attempts=%s",
                row.get("id"),
                row.get("update_id"),
                row.get("chat_id"),
                row.get("message_id"),
                row.get("content_type"),
                row.get("attempts"),
                row.get("max_attempts"),
            )
            await self.process_one_row(bot, row)

        return processed

    async def process_one_row(self, bot: Bot, row: dict[str, object]) -> None:
        job_id = _row_int(row, "id")
        attempts = _row_int(row, "attempts")
        max_attempts = _row_int(
            row,
            "max_attempts",
            default=self.config.queue_max_attempts,
        )
        content_type = _row_text(row, "content_type", default="unknown")
        log.debug(
            "Processing queue job id=%s content_type=%s attempts=%s max_attempts=%s supported=%s",
            job_id,
            content_type,
            attempts,
            max_attempts,
            _row_is_supported(row),
        )

        if not _row_is_supported(row):
            result_json = {
                "outcome": "unsupported_content",
                "action": "no_processing",
                "content_type": content_type,
            }
            try:
                await asyncio.to_thread(
                    self.queue_store.mark_job_done,
                    job_id,
                    result_json=result_json,
                    stage=STAGE_UNSUPPORTED,
                )
            except (QueueStoreError, SchemaVerificationError):
                log.exception("Failed to close unsupported queue job id=%s", job_id)
                return
            log.info(
                "Closed unsupported queue job id=%s content_type=%s",
                job_id,
                result_json["content_type"],
            )
            reaction_set = await set_row_reaction(
                bot,
                row,
                REACTION_UNSUPPORTED,
                reason="unsupported_done",
            )
            if not reaction_set:
                log.debug(
                    "Unsupported reaction was not updated for queue job id=%s emoji=%s",
                    job_id,
                    REACTION_UNSUPPORTED,
                )
            self._schedule_reaction_reconcile(bot, row, reason="unsupported_confirm")
            return

        try:
            processor_result = await self.processor.process(
                bot,
                row,
                context=self._build_processing_context(job_id, bot=bot, row=row),
            )
            latest_row = await asyncio.to_thread(self.queue_store.get_queue_job, job_id)
            if latest_row is None:
                raise RetryableJobError(
                    f"queue job disappeared after processing id={job_id}"
                )
            action_result = await self._maybe_detect_actions(
                bot,
                latest_row,
                processor_result=processor_result,
            )
        except OriginalMessageUnavailableError as exc:
            log.debug(
                "Source message unavailable queue_job_id=%s error=%s",
                job_id,
                _error_text(exc),
            )
            await self._mark_dead(
                bot,
                row,
                failure_class="deleted_message",
                last_error=_error_text(exc),
                attempts=attempts,
                stage=STAGE_MESSAGE_REMOVED,
                send_failure_reaction=False,
            )
            return
        except PermanentJobError as exc:
            log.debug(
                "Queue job permanent failure queue_job_id=%s error_class=%s error=%s",
                job_id,
                exc.__class__.__name__,
                _error_text(exc),
            )
            await self._mark_dead(
                bot,
                row,
                failure_class="permanent_failure",
                last_error=_error_text(exc),
                attempts=attempts,
            )
            return
        except RetryableJobError as exc:
            log.debug(
                "Queue job retryable failure queue_job_id=%s error_class=%s error=%s",
                job_id,
                exc.__class__.__name__,
                _error_text(exc),
            )
            await self._handle_retryable_failure(
                bot,
                row,
                last_error=_error_text(exc),
                attempts=attempts,
                max_attempts=max_attempts,
                retry_after_seconds=exc.retry_after_seconds,
            )
            return
        except Exception as exc:
            log.exception(
                "Unexpected processor error for queue job id=%s attempts=%s",
                job_id,
                attempts,
            )
            await self._handle_retryable_failure(
                bot,
                row,
                last_error=_error_text(exc),
                attempts=attempts,
                max_attempts=max_attempts,
            )
            return

        try:
            result_json = _normalize_success_result(processor_result, attempts=attempts)
            result_json["action_detection"] = action_result
            transcript_message_ids = await self._maybe_send_deferred_transcript_reply(
                bot,
                job_id=job_id,
                processor_result=processor_result,
                action_result=action_result,
            )
            if transcript_message_ids is not None:
                result_json["transcript_message_ids"] = transcript_message_ids
        except OriginalMessageUnavailableError as exc:
            log.debug(
                "Source message unavailable during deferred transcript send "
                "queue_job_id=%s error=%s",
                job_id,
                _error_text(exc),
            )
            await self._mark_dead(
                bot,
                row,
                failure_class="deleted_message",
                last_error=_error_text(exc),
                attempts=attempts,
                stage=STAGE_MESSAGE_REMOVED,
                send_failure_reaction=False,
            )
            return
        except PermanentJobError as exc:
            log.debug(
                "Deferred transcript send permanent failure queue_job_id=%s "
                "error_class=%s error=%s",
                job_id,
                exc.__class__.__name__,
                _error_text(exc),
            )
            await self._mark_dead(
                bot,
                row,
                failure_class="permanent_failure",
                last_error=_error_text(exc),
                attempts=attempts,
            )
            return
        except RetryableJobError as exc:
            log.debug(
                "Deferred transcript send retryable failure queue_job_id=%s "
                "error_class=%s error=%s",
                job_id,
                exc.__class__.__name__,
                _error_text(exc),
            )
            await self._handle_retryable_failure(
                bot,
                row,
                last_error=_error_text(exc),
                attempts=attempts,
                max_attempts=max_attempts,
                retry_after_seconds=exc.retry_after_seconds,
            )
            return
        try:
            await asyncio.to_thread(
                self.queue_store.mark_job_done,
                job_id,
                result_json=result_json,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception(
                "Failed to mark queue job done after processing id=%s; "
                "leaving locked for stale recovery",
                job_id,
            )
            return
        if _created_child_actions(action_result):
            reaction_set = await set_row_reaction(
                bot,
                latest_row,
                REACTION_CALLING_STT_API,
                reason="actions_queued",
            )
            if not reaction_set:
                log.debug(
                    "Action-queued reaction was not updated for queue job id=%s emoji=%s",
                    job_id,
                    REACTION_CALLING_STT_API,
                )
            await self._wake_action_daemon(job_id, action_result=action_result)
        else:
            reaction_set = await set_row_reaction(
                bot,
                latest_row,
                REACTION_SUCCESS,
                reason="processed_done",
            )
            if not reaction_set:
                log.debug(
                    "Success reaction was not updated for queue job id=%s emoji=%s",
                    job_id,
                    REACTION_SUCCESS,
                )
            self._schedule_reaction_reconcile(
                bot,
                latest_row,
                reason="processed_done_confirm",
            )
        log.info(
            "Marked queue job done id=%s attempts=%s processor=%s",
            job_id,
            attempts,
            result_json.get("processor"),
        )
        await self._make_retry_waiting_jobs_due_after_success(
            job_id,
            content_type=content_type,
        )

    async def _maybe_detect_actions(
        self,
        bot: Bot,
        row: dict[str, object],
        *,
        processor_result: Mapping[str, object],
    ) -> dict[str, object]:
        del processor_result
        if _should_show_action_detection_prompt_reaction(row):
            emoji = reaction_for_stage(STAGE_DETECTING_ACTIONS)
            if emoji is not None:
                reaction_set = await set_row_reaction(
                    bot,
                    row,
                    emoji,
                    reason="action_detection_prompt",
                )
                if not reaction_set:
                    log.debug(
                        "Action detection prompt reaction was not updated for "
                        "queue job id=%s emoji=%s",
                        row.get("id"),
                        emoji,
                    )
        return await self.action_detector.detect_actions(row)

    async def _maybe_send_deferred_transcript_reply(
        self,
        bot: Bot,
        *,
        job_id: int,
        processor_result: Mapping[str, object],
        action_result: Mapping[str, object],
    ) -> list[int] | None:
        if processor_result.get("transcript_reply_deferred") is not True:
            return None

        latest_row = await asyncio.to_thread(self.queue_store.get_queue_job, job_id)
        if latest_row is None:
            raise RetryableJobError(
                f"queue job disappeared before deferred transcript send id={job_id}"
            )

        return await send_deferred_transcript_reply(
            bot,
            latest_row,
            action_detection_result=action_result,
            context=self._build_processing_context(job_id, bot=bot, row=latest_row),
        )

    async def wait_for_wakeup(self) -> None:
        if self._wake_event.is_set():
            pending_wakeups = self._consume_pending_wakeups()
            self._wake_event.clear()
            if self._stop_requested:
                return
            if pending_wakeups > 0:
                log.debug(
                    "Consuming pending SIGHUP wake request count=%s; continuing drain immediately",
                    pending_wakeups,
                )
            return

        timeout_seconds = await self._next_wait_timeout_seconds()
        if timeout_seconds <= 0:
            log.debug("Next retry is already due; continuing drain immediately")
            return

        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return

        pending_wakeups = self._consume_pending_wakeups()
        self._wake_event.clear()
        if self._stop_requested:
            return
        if pending_wakeups > 0:
            log.debug(
                "Queue daemon woke from SIGHUP count=%s",
                pending_wakeups,
            )

    async def _recover_stale_processing_jobs(self) -> int:
        try:
            recovered = await asyncio.to_thread(
                self.queue_store.requeue_stale_processing_jobs,
                older_than_seconds=self.config.stale_lock_seconds,
                worker_name=self.config.worker_name,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception("Failed to recover stale processing jobs")
            return 0
        if recovered > 0:
            log.warning(
                "Recovered stale queue jobs count=%s worker=%s",
                recovered,
                self.config.worker_name,
            )
        return recovered

    async def _next_wait_timeout_seconds(self) -> float:
        try:
            next_due_at = await asyncio.to_thread(self.queue_store.get_next_available_at)
        except (QueueStoreError, SchemaVerificationError):
            log.exception("Failed to inspect next queued retry time")
            return float(self.config.poll_seconds)
        timeout_seconds = float(self.config.poll_seconds)
        if next_due_at is None:
            return timeout_seconds

        now = datetime.now(timezone.utc)
        wait_until_due = (next_due_at - now).total_seconds()
        resolved_timeout = max(0.0, min(timeout_seconds, wait_until_due))
        log.debug(
            "Next queued retry due_at=%s wait_until_due=%.3f resolved_timeout=%.3f",
            next_due_at.isoformat(),
            wait_until_due,
            resolved_timeout,
        )
        return resolved_timeout

    async def _handle_retryable_failure(
        self,
        bot: Bot,
        row: dict[str, object],
        *,
        last_error: str,
        attempts: int,
        max_attempts: int,
        retry_after_seconds: int | None = None,
    ) -> None:
        job_id = _row_int(row, "id")

        try:
            delay_seconds = next_retry_delay_seconds(
                attempts=attempts,
                max_attempts=max_attempts,
            )
        except ValueError as exc:
            combined_error = f"{last_error}; {exc}"
            await self._mark_dead(
                bot,
                row,
                failure_class="invalid_retry_policy",
                last_error=combined_error,
                attempts=attempts,
            )
            return

        if delay_seconds is None:
            log.debug(
                "Retry policy exhausted queue_job_id=%s attempts=%s max_attempts=%s",
                job_id,
                attempts,
                max_attempts,
            )
            await self._mark_dead(
                bot,
                row,
                failure_class="retry_exhausted",
                last_error=last_error,
                attempts=attempts,
            )
            return

        if retry_after_seconds is not None:
            delay_seconds = max(delay_seconds, retry_after_seconds)

        retry_emoji = retry_reaction_for_delay(delay_seconds)
        log.debug(
            "Retry policy selected queue_job_id=%s attempts=%s max_attempts=%s "
            "delay_seconds=%s retry_emoji=%s",
            job_id,
            attempts,
            max_attempts,
            delay_seconds,
            retry_emoji,
        )
        try:
            await asyncio.to_thread(
                self.queue_store.mark_job_for_retry,
                job_id,
                delay_seconds=delay_seconds,
                last_error=last_error,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception(
                "Failed to persist retry for queue job id=%s; "
                "leaving locked for stale recovery",
                job_id,
            )
            return
        reaction_set = await set_row_reaction(
            bot,
            row,
            retry_emoji,
            reason="retry_scheduled",
        )
        if not reaction_set:
            log.debug(
                "Retry reaction was not updated for queue job id=%s "
                "attempts=%s delay_seconds=%s emoji=%s",
                job_id,
                attempts,
                delay_seconds,
                retry_emoji,
            )
        self._schedule_reaction_reconcile(bot, row, reason="retry_scheduled_confirm")
        log.warning(
            "Scheduled retry for queue job id=%s attempts=%s delay_seconds=%s error=%s",
            job_id,
            attempts,
            delay_seconds,
            last_error,
        )

    async def _mark_dead(
        self,
        bot: Bot,
        row: dict[str, object],
        *,
        failure_class: str,
        last_error: str,
        attempts: int,
        stage: str = STAGE_FAILED,
        send_failure_reaction: bool = True,
    ) -> None:
        job_id = _row_int(row, "id")
        result_json = {
            "outcome": "dead",
            "failure_class": failure_class,
            "attempts": attempts,
        }
        try:
            await asyncio.to_thread(
                self.queue_store.mark_job_dead,
                job_id,
                last_error=last_error,
                result_json=result_json,
                stage=stage,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception(
                "Failed to persist dead state for queue job id=%s; "
                "leaving locked for stale recovery",
                job_id,
            )
            return
        if send_failure_reaction:
            reaction_set = await set_row_reaction(
                bot,
                row,
                REACTION_FAILURE,
                reason=failure_class,
            )
            if not reaction_set:
                log.debug(
                    "Failure reaction was not updated for queue job id=%s emoji=%s",
                    job_id,
                    REACTION_FAILURE,
                )
            self._schedule_reaction_reconcile(bot, row, reason="dead_confirm")
        log.warning(
            "Marked queue job dead id=%s attempts=%s failure_class=%s error=%s",
            job_id,
            attempts,
            failure_class,
            last_error,
        )

    def _build_processing_context(
        self,
        job_id: int,
        *,
        bot: Bot,
        row: Mapping[str, object],
    ) -> QueueProcessingContext:
        async def set_stage(stage: str, stage_detail: str | None = None) -> None:
            log.debug("job=%s stage=%s detail=%s", job_id, stage, stage_detail)
            await asyncio.to_thread(
                self.queue_store.set_job_stage,
                job_id,
                stage=stage,
                stage_detail=stage_detail,
            )

        async def set_processing_text(
            processing_text: str,
            stage: str | None = None,
            stage_detail: str | None = None,
        ) -> None:
            log.debug(
                "job=%s processing_text_saved stage=%s detail=%s chars=%s",
                job_id,
                stage,
                stage_detail,
                len(processing_text),
            )
            await asyncio.to_thread(
                self.queue_store.set_job_processing_text,
                job_id,
                processing_text=processing_text,
                stage=stage,
                stage_detail=stage_detail,
            )

        async def set_outbound_json(
            outbound_json: dict[str, Any],
            stage: str | None = None,
            stage_detail: str | None = None,
        ) -> None:
            log.debug(
                "job=%s outbound_json_saved stage=%s detail=%s keys=%s",
                job_id,
                stage,
                stage_detail,
                ",".join(sorted(outbound_json.keys())),
            )
            await asyncio.to_thread(
                self.queue_store.set_job_outbound_json,
                job_id,
                outbound_json=outbound_json,
                stage=stage,
                stage_detail=stage_detail,
            )

        status_controller = PipelineStatusController(
            set_stage=set_stage,
            feedback_client=ReactionFeedbackClient(bot=bot, row=row),
        )

        return QueueProcessingContext(
            set_stage=set_stage,
            set_processing_text=set_processing_text,
            set_outbound_json=set_outbound_json,
            activity=status_controller.activity,
        )

    def _schedule_reaction_reconcile(
        self,
        bot: Bot,
        row: Mapping[str, object],
        *,
        reason: str,
    ) -> None:
        task = asyncio.create_task(
            self._reconcile_reaction_after_delay(bot, dict(row), reason=reason),
            name=f"reaction-reconcile:{row.get('id')}:{reason}",
        )
        self._reaction_reconcile_tasks.add(task)
        task.add_done_callback(self._reaction_reconcile_tasks.discard)

    async def _reconcile_reaction_after_delay(
        self,
        bot: Bot,
        row: dict[str, object],
        *,
        reason: str,
    ) -> None:
        await asyncio.sleep(REACTION_RECONCILE_DELAY_SECONDS)

        latest_row = row
        get_queue_job = getattr(self.queue_store, "get_queue_job", None)
        if get_queue_job is not None:
            try:
                loaded = await asyncio.to_thread(get_queue_job, _row_int(row, "id"))
            except (QueueStoreError, SchemaVerificationError):
                log.debug(
                    "reaction_reconcile_skipped reason=queue_state_unavailable job=%s",
                    row.get("id"),
                    exc_info=True,
                )
                return
            if loaded is None:
                return
            latest_row = loaded

        emoji = reaction_for_row_state(latest_row)
        if emoji is None:
            return

        reaction_set = await set_row_reaction(
            bot,
            latest_row,
            emoji,
            reason=reason,
        )
        if not reaction_set:
            log.debug(
                "Reaction reconcile did not update queue job id=%s emoji=%s reason=%s",
                latest_row.get("id"),
                emoji,
                reason,
            )

    async def _drain_reaction_reconcile_tasks(self) -> None:
        if not self._reaction_reconcile_tasks:
            return
        await asyncio.gather(
            *tuple(self._reaction_reconcile_tasks),
            return_exceptions=True,
        )

    async def _make_retry_waiting_jobs_due_after_success(
        self,
        job_id: int,
        *,
        content_type: str,
    ) -> None:
        try:
            released_count = await asyncio.to_thread(
                self.queue_store.make_retry_waiting_jobs_due,
                exclude_job_id=job_id,
                content_type=content_type,
                max_delay_seconds=RETRY_SHORT_MAX_SECONDS,
                limit=10,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception(
                "Failed to release retry-waiting jobs after success id=%s",
                job_id,
            )
            return
        if released_count > 0:
            log.info(
                "Released retry-waiting queue jobs for fast retry after success "
                "success_job_id=%s released_count=%s",
                job_id,
                released_count,
            )

    async def _wake_action_daemon(
        self,
        job_id: int,
        *,
        action_result: Mapping[str, object],
    ) -> None:
        try:
            wake_result = await asyncio.to_thread(
                wake_action_daemon,
                self.config.action_daemon_pidfile_path,
            )
        except Exception:
            log.exception(
                "Failed to wake action daemon after source job done id=%s",
                job_id,
            )
            return

        if wake_result.signaled:
            log.debug(
                "Woke action daemon after source job done id=%s pid=%s created_actions=%s",
                job_id,
                wake_result.pid,
                action_result.get("created_action_count"),
            )
            return

        log.debug(
            "Action daemon wake skipped after source job done id=%s reason=%s pid=%s",
            job_id,
            wake_result.reason,
            wake_result.pid,
        )

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        loop.add_signal_handler(signal.SIGHUP, self._handle_signal, signal.SIGHUP)
        loop.add_signal_handler(signal.SIGTERM, self._handle_signal, signal.SIGTERM)
        loop.add_signal_handler(signal.SIGINT, self._handle_signal, signal.SIGINT)

    def _handle_signal(self, sig: signal.Signals) -> None:
        if sig is signal.SIGHUP:
            self._pending_wakeups += 1
            log.debug(
                "wake requested signal=%s pending=%s",
                sig.name,
                self._pending_wakeups,
            )
            self._wake_event.set()
            return

        log.info("queue daemon stopping reason=%s", sig.name)
        self._stop_requested = True
        self._wake_event.set()

    def _consume_pending_wakeups(self) -> int:
        pending_wakeups = self._pending_wakeups
        self._pending_wakeups = 0
        return pending_wakeups


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drain queued Telegram jobs from SQLite")
    parser.add_argument(
        "--debug",
        "--DEBUG",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--trace",
        "--TRACE",
        action="store_true",
        help="Enable trace logging for selected low-level libraries",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one drain pass and exit",
    )
    parser.add_argument(
        "--worker-name",
        default="queue-daemon",
        help="Identifier written into telegram_queue.locked_by",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=DEFAULT_POLL_SECONDS,
        help="Maximum idle wait before housekeeping wakeup",
    )
    parser.add_argument(
        "--stale-lock-seconds",
        type=int,
        default=1800,
        help="Age threshold for recycling stuck processing rows",
    )
    parser.add_argument(
        "--pidfile",
        help="Path to the daemon pidfile used for startup guard and SIGHUP wakeups",
    )
    return parser.parse_args(argv)


def configure_logging(debug: bool, *, trace: bool = False) -> None:
    configure_app_logging(debug=debug, trace=trace)


async def run_daemon(args: argparse.Namespace) -> None:
    await run_runtime_system_checks(component="queue-daemon")
    config = Config.from_env(
        worker_name=args.worker_name,
        poll_seconds=args.poll_seconds,
        stale_lock_seconds=args.stale_lock_seconds,
        pidfile_path=args.pidfile,
    )
    queue_store = SQLiteQueueStore(config.sqlite_settings)
    daemon = QueueDaemon(config=config, queue_store=queue_store)
    with acquire_process_guard(
        pidfile_path=config.pidfile_path,
        process_name=DAEMON_PROCESS_NAME,
    ):
        await daemon.run(once=args.once)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.debug, trace=args.trace)

    try:
        asyncio.run(run_daemon(args))
    except (KeyError, ValueError, ProcessAlreadyRunningError, RuntimeCheckError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1
    except (QueueStoreError, SchemaVerificationError) as exc:
        print(f"Queue error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130

    return 0


def _row_int(
    row: Mapping[str, object],
    key: str,
    *,
    default: int | None = None,
) -> int:
    value = row.get(key, default)
    if value is None:
        raise ValueError(f"Queue row field {key!r} is missing")
    return int(value)


def _row_text(
    row: Mapping[str, object],
    key: str,
    *,
    default: str | None = None,
) -> str:
    value = row.get(key, default)
    if value is None:
        raise ValueError(f"Queue row field {key!r} is missing")
    return str(value)


def _should_show_action_detection_prompt_reaction(row: Mapping[str, object]) -> bool:
    status = row.get("action_detection_status")
    if status not in {ACTION_DETECTION_PENDING, ACTION_DETECTION_PROCESSING}:
        return False

    processing_text = row.get("processing_text")
    if not isinstance(processing_text, str) or not processing_text.strip():
        return False

    return direct_action_command_for_text(processing_text) is None


def _row_is_supported(row: Mapping[str, object]) -> bool:
    return bool(int(row.get("is_supported", 0)))


def _error_text(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def _normalize_success_result(
    processor_result: Mapping[str, object],
    *,
    attempts: int,
) -> dict[str, object]:
    result_json = dict(processor_result)
    result_json.setdefault("outcome", "processed")
    result_json.setdefault("attempts", attempts)
    return result_json


def _created_child_actions(action_result: Mapping[str, object]) -> bool:
    try:
        return int(action_result.get("created_action_count", 0)) > 0
    except (TypeError, ValueError):
        return False


if __name__ == "__main__":
    raise SystemExit(main())

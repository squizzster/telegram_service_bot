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

from bot_libs.action_dispatch import DispatchingActionProcessor  # noqa: E402
from bot_libs.action_models import (  # noqa: E402
    ACTION_SHOW_ALL,
    ACTION_SHOW_ALL_DETAILED,
    ACTION_SHOW_EXPENSES,
    ACTION_SHOW_INCOME,
)
from bot_libs.action_processing_context import ActionProcessingContext  # noqa: E402
from bot_libs.daemon_signal import (  # noqa: E402
    ACTION_DAEMON_PROCESS_NAME,
    resolve_action_daemon_pidfile,
)
from bot_libs.dev_restart import RESTART_SIGNAL, restart_current_process  # noqa: E402
from bot_libs.logging_utils import configure_app_logging  # noqa: E402
from bot_libs.process_guard import (  # noqa: E402
    ProcessAlreadyRunningError,
    acquire_process_guard,
)
from bot_libs.queue_models import DEFAULT_QUEUE_MAX_ATTEMPTS  # noqa: E402
from bot_libs.queue_processor_errors import (  # noqa: E402
    OriginalMessageUnavailableError,
    PermanentJobError,
    RetryableJobError,
)
from bot_libs.reaction_policy import (  # noqa: E402
    REACTION_CALLING_STT_API,
    REACTION_FAILURE,
    REACTION_SUCCESS,
    reaction_for_stage,
    retry_reaction_for_delay,
    set_row_reaction,
)
from bot_libs.retry_policy import next_retry_delay_seconds  # noqa: E402
from bot_libs.runtime_checks import (  # noqa: E402
    RuntimeCheckError,
    run_runtime_system_checks,
)
from bot_libs.sql import (  # noqa: E402
    QueueStoreError,
    SQLiteQueueStore,
    SQLiteSettings,
    SchemaVerificationError,
    build_sqlite_settings_from_env,
)
from bot_libs.stage_names import (  # noqa: E402
    STAGE_DONE,
    STAGE_FAILED,
    STAGE_MESSAGE_REMOVED,
    STAGE_PROCESSING_ACTION,
)

log = logging.getLogger(__name__)

DEFAULT_MAX_ATTEMPTS = DEFAULT_QUEUE_MAX_ATTEMPTS
DEFAULT_POLL_SECONDS = 120
REPORT_ACTION_CODES = (
    ACTION_SHOW_EXPENSES,
    ACTION_SHOW_INCOME,
    ACTION_SHOW_ALL,
    ACTION_SHOW_ALL_DETAILED,
)


@dataclass(frozen=True, slots=True)
class Config:
    token: str
    sqlite_settings: SQLiteSettings
    worker_name: str
    poll_seconds: int
    stale_lock_seconds: int
    queue_max_attempts: int
    pidfile_path: str

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

        queue_max_attempts = int(
            os.getenv("QUEUE_MAX_ATTEMPTS", str(DEFAULT_MAX_ATTEMPTS))
        )
        if not 1 <= queue_max_attempts <= DEFAULT_MAX_ATTEMPTS:
            raise ValueError(
                "QUEUE_MAX_ATTEMPTS must be between 1 and "
                f"{DEFAULT_MAX_ATTEMPTS}"
            )

        return cls(
            token=os.environ["TELEGRAM_BOT_KEY"],
            sqlite_settings=build_sqlite_settings_from_env(),
            worker_name=normalized_worker_name,
            poll_seconds=poll_seconds,
            stale_lock_seconds=stale_lock_seconds,
            queue_max_attempts=queue_max_attempts,
            pidfile_path=resolve_action_daemon_pidfile(pidfile_path),
        )


class ActionProcessor:
    async def process(
        self,
        bot: Bot,
        row: Mapping[str, object],
        *,
        context: ActionProcessingContext | None = None,
    ) -> Mapping[str, object]:
        raise NotImplementedError


class ActionDaemon:
    def __init__(
        self,
        *,
        config: Config,
        queue_store: SQLiteQueueStore,
        processor: ActionProcessor | None = None,
    ) -> None:
        self.config = config
        self.queue_store = queue_store
        self.processor = processor or DispatchingActionProcessor()
        self._wake_event = asyncio.Event()
        self._pending_wakeups = 0
        self._stop_requested = False
        self._restart_requested = False

    async def run(self, *, once: bool = False) -> bool:
        log.debug(
            "Action daemon starting worker=%s db_path=%s poll_seconds=%s "
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
            "action daemon started worker=%s poll_seconds=%s max_attempts=%s",
            self.config.worker_name,
            self.config.poll_seconds,
            self.config.queue_max_attempts,
        )
        await asyncio.to_thread(self.queue_store.verify_schema)
        log.debug("Queue schema verification complete")

        loop = asyncio.get_running_loop()
        self._install_signal_handlers(loop)
        log.debug("Signal handlers installed for SIGHUP/SIGTERM/SIGINT/SIGUSR1")

        async with Bot(self.config.token) as bot:
            await self._recover_stale_processing_actions()
            while True:
                await self._recover_stale_processing_actions()
                processed = await self.drain_due_actions(bot)
                if processed > 0:
                    log.debug("Action drain pass complete processed_count=%s", processed)

                if once or self._stop_requested:
                    log.debug(
                        "Action daemon exiting once=%s stop_requested=%s",
                        once,
                        self._stop_requested,
                    )
                    return self._restart_requested

                await self.wait_for_wakeup()

    async def drain_due_actions(self, bot: Bot) -> int:
        processed = 0

        while not self._stop_requested:
            try:
                row = await asyncio.to_thread(
                    self.queue_store.claim_next_action,
                    worker_name=self.config.worker_name,
                )
            except (QueueStoreError, SchemaVerificationError):
                log.exception("Failed to claim next action job")
                return processed

            if row is None:
                if processed > 0:
                    log.debug(
                        "No more due action jobs claimed worker=%s processed_count=%s",
                        self.config.worker_name,
                        processed,
                    )
                return processed

            processed += 1
            log.debug(
                "action_job=%s claimed queue_id=%s action_code=%s attempts=%s "
                "max_attempts=%s",
                row.get("id"),
                row.get("queue_id"),
                row.get("action_code"),
                row.get("attempts"),
                row.get("max_attempts"),
            )
            if _is_report_action(row):
                await self.process_one_action(bot, row)
                continue

            action_task = asyncio.create_task(self.process_one_action(bot, row))
            processed += await self._drain_report_actions_while_blocked(
                bot,
                blocking_task=action_task,
            )
            await action_task

        return processed

    async def _drain_report_actions_while_blocked(
        self,
        bot: Bot,
        *,
        blocking_task: asyncio.Task[None],
    ) -> int:
        processed = 0

        while not self._stop_requested and not blocking_task.done():
            try:
                row = await asyncio.to_thread(
                    self.queue_store.claim_next_action,
                    worker_name=self.config.worker_name,
                    action_codes=REPORT_ACTION_CODES,
                )
            except (QueueStoreError, SchemaVerificationError):
                log.exception("Failed to claim report action job")
                return processed

            if row is not None:
                processed += 1
                log.debug(
                    "report action_job=%s claimed while blocking action runs "
                    "queue_id=%s action_code=%s attempts=%s max_attempts=%s",
                    row.get("id"),
                    row.get("queue_id"),
                    row.get("action_code"),
                    row.get("attempts"),
                    row.get("max_attempts"),
                )
                await self.process_one_action(bot, row)
                continue

            wake_task = asyncio.create_task(self._wake_event.wait())
            done, pending = await asyncio.wait(
                {blocking_task, wake_task},
                timeout=1,
                return_when=asyncio.FIRST_COMPLETED,
            )
            del pending
            if not wake_task.done():
                wake_task.cancel()
                await asyncio.gather(wake_task, return_exceptions=True)
            if wake_task in done and self._wake_event.is_set():
                pending_wakeups = self._consume_pending_wakeups()
                self._wake_event.clear()
                if pending_wakeups > 0:
                    log.debug(
                        "Consuming wake request while blocking action runs count=%s",
                        pending_wakeups,
                    )

        return processed

    async def process_one_action(self, bot: Bot, row: dict[str, object]) -> None:
        action_job_id = _row_int(row, "id")
        attempts = _row_int(row, "attempts")
        max_attempts = _row_int(
            row,
            "max_attempts",
            default=self.config.queue_max_attempts,
        )
        action_code = _row_text(row, "action_code", default="unknown")

        await self._set_processing_reaction(bot, row)
        try:
            result = await self.processor.process(
                bot,
                row,
                context=self._build_processing_context(action_job_id, bot=bot, row=row),
            )
        except OriginalMessageUnavailableError as exc:
            log.debug(
                "Action source message unavailable action_job_id=%s action_code=%s "
                "error=%s",
                action_job_id,
                action_code,
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
                "Action job permanent failure action_job_id=%s action_code=%s "
                "error_class=%s error=%s",
                action_job_id,
                action_code,
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
                "Action job retryable failure action_job_id=%s action_code=%s "
                "error_class=%s error=%s",
                action_job_id,
                action_code,
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
                "Unexpected action processor error action_job_id=%s action_code=%s",
                action_job_id,
                action_code,
            )
            await self._handle_retryable_failure(
                bot,
                row,
                last_error=_error_text(exc),
                attempts=attempts,
                max_attempts=max_attempts,
            )
            return

        result_json = _normalize_success_result(
            result,
            attempts=attempts,
            action_code=action_code,
        )
        try:
            await asyncio.to_thread(
                self.queue_store.mark_action_done,
                action_job_id,
                result_json=result_json,
                stage=STAGE_DONE,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception(
                "Failed to mark action job done id=%s; leaving locked for stale recovery",
                action_job_id,
            )
            return

        reaction_set = await set_row_reaction(
            bot,
            row,
            REACTION_SUCCESS,
            reason="action_done",
        )
        if not reaction_set:
            log.debug(
                "Success reaction was not updated for action job id=%s emoji=%s",
                action_job_id,
                REACTION_SUCCESS,
            )
        log.info(
            "Marked action job done id=%s queue_id=%s action_code=%s attempts=%s",
            action_job_id,
            row.get("queue_id"),
            action_code,
            attempts,
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
            log.debug("Next action retry is already due; continuing drain immediately")
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
            log.debug("Action daemon woke from SIGHUP count=%s", pending_wakeups)

    async def _recover_stale_processing_actions(self) -> int:
        try:
            recovered = await asyncio.to_thread(
                self.queue_store.requeue_stale_processing_actions,
                older_than_seconds=self.config.stale_lock_seconds,
                worker_name=self.config.worker_name,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception("Failed to recover stale processing action jobs")
            return 0
        if recovered > 0:
            log.warning(
                "Recovered stale action jobs count=%s worker=%s",
                recovered,
                self.config.worker_name,
            )
        return recovered

    async def _next_wait_timeout_seconds(self) -> float:
        try:
            next_due_at = await asyncio.to_thread(
                self.queue_store.get_next_action_available_at
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception("Failed to inspect next queued action retry time")
            return float(self.config.poll_seconds)
        timeout_seconds = float(self.config.poll_seconds)
        if next_due_at is None:
            return timeout_seconds

        now = datetime.now(timezone.utc)
        wait_until_due = (next_due_at - now).total_seconds()
        resolved_timeout = max(0.0, min(timeout_seconds, wait_until_due))
        log.debug(
            "Next action retry due_at=%s wait_until_due=%.3f resolved_timeout=%.3f",
            next_due_at.isoformat(),
            wait_until_due,
            resolved_timeout,
        )
        return resolved_timeout

    async def _handle_retryable_failure(
        self,
        bot: Bot,
        row: Mapping[str, object],
        *,
        last_error: str,
        attempts: int,
        max_attempts: int,
        retry_after_seconds: int | None = None,
    ) -> None:
        action_job_id = _row_int(row, "id")

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
        try:
            await asyncio.to_thread(
                self.queue_store.mark_action_for_retry,
                action_job_id,
                delay_seconds=delay_seconds,
                last_error=last_error,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception(
                "Failed to persist retry for action job id=%s; leaving locked for stale recovery",
                action_job_id,
            )
            return

        reaction_set = await set_row_reaction(
            bot,
            row,
            retry_emoji,
            reason="action_retry_scheduled",
        )
        if not reaction_set:
            log.debug(
                "Retry reaction was not updated for action job id=%s attempts=%s "
                "delay_seconds=%s emoji=%s",
                action_job_id,
                attempts,
                delay_seconds,
                retry_emoji,
            )
        log.warning(
            "Scheduled retry for action job id=%s attempts=%s delay_seconds=%s error=%s",
            action_job_id,
            attempts,
            delay_seconds,
            last_error,
        )

    async def _mark_dead(
        self,
        bot: Bot,
        row: Mapping[str, object],
        *,
        failure_class: str,
        last_error: str,
        attempts: int,
        stage: str = STAGE_FAILED,
        send_failure_reaction: bool = True,
    ) -> None:
        action_job_id = _row_int(row, "id")
        result_json = {
            "outcome": "dead",
            "failure_class": failure_class,
            "attempts": attempts,
            "action_code": row.get("action_code"),
        }
        try:
            await asyncio.to_thread(
                self.queue_store.mark_action_dead,
                action_job_id,
                last_error=last_error,
                result_json=result_json,
                stage=stage,
            )
        except (QueueStoreError, SchemaVerificationError):
            log.exception(
                "Failed to persist dead state for action job id=%s; "
                "leaving locked for stale recovery",
                action_job_id,
            )
            return

        if send_failure_reaction:
            reaction_set = await set_row_reaction(
                bot,
                row,
                REACTION_FAILURE,
                reason=f"action_{failure_class}",
            )
            if not reaction_set:
                log.debug(
                    "Failure reaction was not updated for action job id=%s emoji=%s",
                    action_job_id,
                    REACTION_FAILURE,
                )
        log.warning(
            "Marked action job dead id=%s attempts=%s failure_class=%s error=%s",
            action_job_id,
            attempts,
            failure_class,
            last_error,
        )

    async def _set_processing_reaction(
        self,
        bot: Bot,
        row: Mapping[str, object],
    ) -> None:
        reaction_set = await set_row_reaction(
            bot,
            row,
            REACTION_CALLING_STT_API,
            reason="action_processing",
        )
        if not reaction_set:
            log.debug(
                "Processing reaction was not updated for action job id=%s emoji=%s",
                row.get("id"),
                REACTION_CALLING_STT_API,
            )

    def _build_processing_context(
        self,
        action_job_id: int,
        *,
        bot: Bot,
        row: Mapping[str, object],
    ) -> ActionProcessingContext:
        async def set_stage(stage: str) -> None:
            log.debug("action_job=%s stage=%s", action_job_id, stage)
            await asyncio.to_thread(
                self.queue_store.set_action_stage,
                action_job_id,
                stage=stage,
            )
            stage_reaction = reaction_for_stage(stage)
            if stage_reaction is not None:
                reaction_set = await set_row_reaction(
                    bot,
                    row,
                    stage_reaction,
                    reason=f"action_stage:{stage}",
                )
                if not reaction_set:
                    log.debug(
                        "Stage reaction was not updated for action job id=%s "
                        "stage=%s emoji=%s",
                        action_job_id,
                        stage,
                        stage_reaction,
                    )

        async def set_outbound_json(
            outbound_json: dict[str, Any],
            stage: str | None = None,
        ) -> None:
            log.debug(
                "action_job=%s outbound_json_saved stage=%s keys=%s",
                action_job_id,
                stage,
                ",".join(sorted(outbound_json.keys())),
            )
            await asyncio.to_thread(
                self.queue_store.set_action_outbound_json,
                action_job_id,
                outbound_json=outbound_json,
                stage=stage,
            )

        return ActionProcessingContext(
            set_stage=set_stage,
            set_outbound_json=set_outbound_json,
            queue_store=self.queue_store,
        )

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        loop.add_signal_handler(signal.SIGHUP, self._handle_signal, signal.SIGHUP)
        loop.add_signal_handler(signal.SIGTERM, self._handle_signal, signal.SIGTERM)
        loop.add_signal_handler(signal.SIGINT, self._handle_signal, signal.SIGINT)
        loop.add_signal_handler(RESTART_SIGNAL, self._handle_signal, RESTART_SIGNAL)

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
        if sig is RESTART_SIGNAL:
            self._restart_requested = True
            log.info("action daemon restart requested by %s", sig.name)
            self._stop_requested = True
            self._wake_event.set()
            return

        log.info("action daemon stopping reason=%s", sig.name)
        self._stop_requested = True
        self._wake_event.set()

    def _consume_pending_wakeups(self) -> int:
        pending_wakeups = self._pending_wakeups
        self._pending_wakeups = 0
        return pending_wakeups


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drain queued Telegram action jobs")
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
        default="action-daemon",
        help="Identifier written into incoming_message_actions.locked_by",
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
        help="Age threshold for recycling stuck processing action rows",
    )
    parser.add_argument(
        "--pidfile",
        help="Path to the action daemon pidfile used for startup guard and SIGHUP wakeups",
    )
    return parser.parse_args(argv)


def configure_logging(debug: bool, *, trace: bool = False) -> None:
    configure_app_logging(debug=debug, trace=trace)


async def run_daemon(args: argparse.Namespace) -> bool:
    await run_runtime_system_checks(component="action-daemon")
    config = Config.from_env(
        worker_name=args.worker_name,
        poll_seconds=args.poll_seconds,
        stale_lock_seconds=args.stale_lock_seconds,
        pidfile_path=args.pidfile,
    )
    queue_store = SQLiteQueueStore(config.sqlite_settings)
    daemon = ActionDaemon(config=config, queue_store=queue_store)
    with acquire_process_guard(
        pidfile_path=config.pidfile_path,
        process_name=ACTION_DAEMON_PROCESS_NAME,
    ):
        return await daemon.run(once=args.once)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.debug, trace=args.trace)

    try:
        restart_requested = asyncio.run(run_daemon(args))
        if restart_requested:
            restart_current_process()
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
        raise ValueError(f"Action row field {key!r} is missing")
    return int(value)


def _row_text(
    row: Mapping[str, object],
    key: str,
    *,
    default: str | None = None,
) -> str:
    value = row.get(key, default)
    if value is None:
        raise ValueError(f"Action row field {key!r} is missing")
    return str(value)


def _error_text(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def _is_report_action(row: Mapping[str, object]) -> bool:
    return str(row.get("action_code") or "") in REPORT_ACTION_CODES


def _normalize_success_result(
    processor_result: Mapping[str, object],
    *,
    attempts: int,
    action_code: str,
) -> dict[str, object]:
    result_json = dict(processor_result)
    result_json.setdefault("outcome", "processed")
    result_json.setdefault("attempts", attempts)
    result_json.setdefault("action_code", action_code)
    return result_json


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# bot.py
# also READ/ANALYZE: ./bot_py.readme.md
# bot_py.readme.md

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus

import uvicorn
from bot_libs.daemon_signal import resolve_daemon_pidfile, wake_queue_daemon
from bot_libs.dev_restart import (
    BOT_PROCESS_NAME,
    RESTART_SIGNAL,
    resolve_bot_pidfile,
    restart_current_process,
)
from bot_libs.logging_utils import configure_app_logging
from bot_libs.process_guard import ProcessAlreadyRunningError, acquire_process_guard
from bot_libs.queue_enqueue import (
    QueueEnqueueError,
    QueueEnqueuer,
    QueuePersistResult,
    try_set_status_reaction,
)
from bot_libs.queue_models import DEFAULT_QUEUE_MAX_ATTEMPTS
from bot_libs.reaction_policy import REACTION_FAILURE
from bot_libs.runtime_checks import RuntimeCheckError, run_runtime_system_checks
from bot_libs.service_dump import TelegramServiceDumper
from bot_libs.sql import (
    QueueStoreError,
    SchemaVerificationError,
    SQLiteQueueStore,
    SQLiteSettings,
    build_sqlite_settings_from_env,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

log = logging.getLogger(__name__)

ALL_HTTP_METHODS = ["GET", "POST", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"]
TELEGRAM_SECRET_HEADER = "x-telegram-bot-api-secret-token"
SECRET_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")
SERVICE_DUMP_DIR_ENV = "TELEGRAM_SERVICE_DUMP_DIR"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Config:
    token: str
    webhook_secret: str
    public_base_url: str
    listen_host: str
    listen_port: int
    webhook_path: str
    service_dump_dir: str | None
    sqlite_settings: SQLiteSettings
    queue_max_attempts: int
    bot_pidfile_path: str
    daemon_pidfile_path: str

    @classmethod
    def from_env(cls, *, save_dump_dir: bool = False) -> "Config":
        token = os.environ["TELEGRAM_BOT_KEY"]
        webhook_secret = os.environ["TELEGRAM_WEBHOOK_SECRET"]
        queue_max_attempts = int(
            os.getenv("QUEUE_MAX_ATTEMPTS", str(DEFAULT_QUEUE_MAX_ATTEMPTS))
        )
        service_dump_dir: str | None = None

        if not SECRET_TOKEN_RE.fullmatch(webhook_secret):
            raise ValueError(
                "TELEGRAM_WEBHOOK_SECRET is invalid. "
                "Expected 1-256 chars using only A-Z, a-z, 0-9, _ and -"
            )
        if not 1 <= queue_max_attempts <= DEFAULT_QUEUE_MAX_ATTEMPTS:
            raise ValueError(
                "QUEUE_MAX_ATTEMPTS must be between 1 and "
                f"{DEFAULT_QUEUE_MAX_ATTEMPTS}"
            )
        if save_dump_dir:
            service_dump_dir = os.getenv(SERVICE_DUMP_DIR_ENV, "").strip()
            if not service_dump_dir:
                raise ValueError(
                    f"--save-dump-dir requires {SERVICE_DUMP_DIR_ENV} to be set"
                )

        return cls(
            token=token,
            webhook_secret=webhook_secret,
            public_base_url=os.getenv(
                "PUBLIC_BASE_URL", "https://service-bot.mktvman.com"
            ).rstrip("/"),
            listen_host=os.getenv("LISTEN_HOST", "127.0.0.1"),
            listen_port=int(os.getenv("LISTEN_PORT", "3002")),
            webhook_path=os.getenv("WEBHOOK_PATH", "/telegram"),
            service_dump_dir=service_dump_dir,
            sqlite_settings=build_sqlite_settings_from_env(),
            queue_max_attempts=queue_max_attempts,
            bot_pidfile_path=resolve_bot_pidfile(),
            daemon_pidfile_path=resolve_daemon_pidfile(),
        )

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram webhook bot")
    parser.add_argument(
        "--debug",
        "--DEBUG",
        dest="debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--trace",
        "--TRACE",
        dest="trace",
        action="store_true",
        help="Enable trace logging for selected low-level libraries",
    )
    parser.add_argument(
        "--save-dump-dir",
        action="store_true",
        help=f"Save each incoming Telegram webhook JSON under ${SERVICE_DUMP_DIR_ENV}",
    )
    return parser.parse_args(argv)


def configure_logging(debug: bool, *, trace: bool = False) -> None:
    configure_app_logging(debug=debug, trace=trace)


def log_received_json(data: object) -> None:
    if log.isEnabledFor(logging.DEBUG):
        pretty_json = json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True)
        log.debug("Received JSON:\n%s", pretty_json)
        return

    if not log.isEnabledFor(logging.INFO):
        return

    update_id = None
    chat_id = None
    message_id = None
    if isinstance(data, dict):
        update_id = data.get("update_id")
        message = data.get("message")
        if isinstance(message, dict):
            message_id = message.get("message_id")
            chat = message.get("chat")
            if isinstance(chat, dict):
                chat_id = chat.get("id")

    log.info(
        "Received Telegram update update_id=%s chat_id=%s message_id=%s",
        update_id,
        chat_id,
        message_id,
    )


def misconfigured_response(
    status_code: HTTPStatus = HTTPStatus.INTERNAL_SERVER_ERROR,
) -> Response:
    return Response(status_code=status_code)


def format_telegram_datetime(value: datetime | int | float | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    return str(value)


def summarize_update(update: Update) -> str:
    parts: list[str] = [f"update_id={update.update_id}"]

    message = update.effective_message
    chat = update.effective_chat
    if chat is not None:
        parts.append(f"chat_id={chat.id}")
        if getattr(chat, "type", None):
            parts.append(f"chat_type={chat.type}")
    if message is not None:
        parts.append(f"message_id={message.message_id}")
        text = (message.text or message.caption or "").strip().replace("\n", " ")
        if text:
            parts.append(f"text={text[:120]!r}")
    else:
        update_type = update.effective_message or update.effective_chat or None
        if update_type is None:
            parts.append("kind=non-message-update")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Telegram handlers
# ---------------------------------------------------------------------------

class BotHandlers:
    async def start_cmd(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        del context

        if update.message is not None:
            await update.message.reply_text("Welcome. Say hello.")

    async def on_error(
        self, update: object, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        log.exception(
            "Unhandled error while processing update %r",
            update,
            exc_info=context.error,
        )


def register_handlers(
    app: Application,
    handlers: BotHandlers,
) -> None:
    app.add_handler(CommandHandler("start", handlers.start_cmd), group=0)
    app.add_error_handler(handlers.on_error)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class WebhookHTTPServer:
    def __init__(
        self,
        *,
        config: Config,
        ptb_app: Application,
        accept_update: Callable[[Update], Awaitable[QueuePersistResult]],
        debug: bool,
    ) -> None:
        self.config = config
        self.ptb_app = ptb_app
        self.accept_update = accept_update
        self.service_dumper = (
            TelegramServiceDumper(config.service_dump_dir)
            if config.service_dump_dir
            else None
        )

        self.starlette_app = Starlette(
            routes=[
                Route(config.webhook_path, self.telegram_webhook, methods=ALL_HTTP_METHODS),
                Route("/{path:path}", self.sinkhole, methods=ALL_HTTP_METHODS),
            ]
        )
        self.starlette_app.state.ptb_app = ptb_app

        self.server = uvicorn.Server(
            uvicorn.Config(
                app=self.starlette_app,
                host=config.listen_host,
                port=config.listen_port,
                use_colors=False,
                log_level="debug" if debug else "info",
                access_log=False,
            )
        )

    async def sinkhole(self, request: Request) -> Response:
        path = request.url.path
        query = request.scope.get("query_string", b"").decode("ascii", errors="ignore")
        log.warning("Sinkholed request path=%r method=%s query=%r", path, request.method, query)
        return misconfigured_response()

    async def telegram_webhook(self, request: Request) -> Response:
        if request.method != "POST":
            log.warning(
                "Rejected non-POST request to webhook path: method=%s",
                request.method,
            )
            return misconfigured_response(HTTPStatus.METHOD_NOT_ALLOWED)

        supplied_secret = request.headers.get(TELEGRAM_SECRET_HEADER, "")
        if not hmac.compare_digest(supplied_secret, self.config.webhook_secret):
            log.warning("Rejected webhook request with invalid secret header")
            return misconfigured_response(HTTPStatus.FORBIDDEN)

        try:
            raw_body = await request.body()
            data = json.loads(raw_body)
        except Exception:
            log.warning("Rejected webhook request with invalid JSON")
            return misconfigured_response(HTTPStatus.BAD_REQUEST)

        if self.service_dumper is not None:
            try:
                dump_path = await asyncio.to_thread(
                    self.service_dumper.dump_update,
                    data,
                    raw_json_bytes=raw_body,
                )
                log.info("Saved Telegram service dump: %s", dump_path)
            except Exception:
                update_id = data.get("update_id") if isinstance(data, dict) else None
                log.exception(
                    "Failed to save Telegram service dump update_id=%s",
                    update_id,
                )

        log_received_json(data)

        try:
            update = Update.de_json(data=data, bot=self.ptb_app.bot)
        except Exception:
            log.warning("Rejected webhook request with unparseable Telegram update")
            return misconfigured_response(HTTPStatus.BAD_REQUEST)

        try:
            await self.accept_update(update)
        except QueueEnqueueError as exc:
            self.ptb_app.create_task(
                try_set_status_reaction(
                    update,
                    REACTION_FAILURE,
                    reason=exc.reason,
                    retry_delay_seconds=exc.retry_delay_seconds,
                ),
                update=update,
                name=f"webhook-failure-feedback:{update.update_id}",
            )
            status_code = (
                HTTPStatus.SERVICE_UNAVAILABLE
                if exc.reason == "queue_store_unavailable"
                else HTTPStatus.INTERNAL_SERVER_ERROR
            )
            return Response(status_code=status_code)

        return Response(status_code=HTTPStatus.OK)

    async def serve(self) -> None:
        await self.server.serve()

    async def wait_until_started(self, server_task: asyncio.Task[None]) -> None:
        while not self.server.started:
            if server_task.done():
                await server_task
                raise RuntimeError("Webhook HTTP server exited before startup completed")
            await asyncio.sleep(0.05)

    def request_shutdown(self) -> None:
        self.server.should_exit = True


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class TelegramBotRunner:
    def __init__(
        self,
        config: Config,
        debug: bool,
        queue_store: SQLiteQueueStore,
    ) -> None:
        self.config = config
        self.debug = debug
        self.queue_store = queue_store

        self.ptb_app = Application.builder().token(config.token).updater(None).build()
        self.queue_enqueuer = QueueEnqueuer(
            queue_store=queue_store,
            max_attempts=config.queue_max_attempts,
        )
        self.daemon_pidfile_path = config.daemon_pidfile_path
        register_handlers(self.ptb_app, BotHandlers())

        self.http_server = WebhookHTTPServer(
            config=config,
            ptb_app=self.ptb_app,
            accept_update=self.accept_live_update,
            debug=debug,
        )

        self._started = False
        self._restart_requested = False

    @property
    def webhook_url(self) -> str:
        return f"{self.config.public_base_url}{self.config.webhook_path}"

    async def log_webhook_info(self, label: str) -> None:
        info = await self.ptb_app.bot.get_webhook_info()
        log.info(
            "%s url=%r pending_update_count=%s last_error_date=%s last_error_message=%r "
            "last_sync_error_date=%s max_connections=%s ip_address=%r",
            label,
            info.url,
            info.pending_update_count,
            format_telegram_datetime(info.last_error_date),
            info.last_error_message,
            format_telegram_datetime(info.last_synchronization_error_date),
            info.max_connections,
            info.ip_address,
        )

    async def accept_live_update(self, update: Update) -> QueuePersistResult:
        return await self.persist_and_dispatch_update(
            update,
            emit_status_feedback=True,
        )

    async def persist_and_dispatch_update(
        self,
        update: Update,
        *,
        emit_status_feedback: bool,
    ) -> QueuePersistResult:
        persist_result = await self.queue_enqueuer.persist_update(update)
        if not persist_result.inserted:
            return persist_result

        if emit_status_feedback:
            feedback_coro = self.queue_enqueuer.apply_post_persist_feedback(
                update,
                persist_result,
            )
            try:
                self.ptb_app.create_task(
                    feedback_coro,
                    update=update,
                    name=f"post-persist-feedback:{update.update_id}",
                )
            except Exception:
                feedback_coro.close()
                log.exception(
                    "Failed to schedule post-persist feedback update_id=%s",
                    update.update_id,
                )

        try:
            await self.wake_queue_daemon(update.update_id)
        except Exception:
            log.exception(
                "Failed to wake queue daemon after persisted update_id=%s",
                update.update_id,
            )

        process_update_coro = self.ptb_app.process_update(update)
        try:
            self.ptb_app.create_task(
                process_update_coro,
                update=update,
                name=f"process-update:{update.update_id}",
            )
        except Exception:
            process_update_coro.close()
            log.exception(
                "Failed to schedule PTB processing after persisted update_id=%s",
                update.update_id,
            )
        return persist_result

    async def wake_queue_daemon(self, update_id: int | None) -> None:
        result = await asyncio.to_thread(wake_queue_daemon, self.daemon_pidfile_path)
        if result.signaled:
            log.debug(
                "Woke queue daemon pid=%s after update_id=%s",
                result.pid,
                update_id,
            )
            return
        log.info(
            "Queue daemon wake skipped after update_id=%s reason=%s pid=%s",
            update_id,
            result.reason,
            result.pid,
        )

    async def recover_pending_updates_from_telegram(self) -> int:
        """
        Drain Telegram's central update queue on startup by temporarily switching
        from webhook mode to getUpdates mode. Recovered updates are persisted
        through the same SQLite acceptance path as live webhooks before webhook
        delivery is restored.
        """
        await self.log_webhook_info("Telegram webhook status before startup recovery")

        total_recovered = 0
        duplicate_updates = 0
        next_offset: int | None = None
        webhook_removed = False

        try:
            await self.ptb_app.bot.delete_webhook(drop_pending_updates=False)
            webhook_removed = True
            log.info("Webhook temporarily removed for startup backlog recovery")

            while True:
                updates = await self.ptb_app.bot.get_updates(
                    offset=next_offset,
                    limit=100,
                    timeout=0,
                )
                if not updates:
                    break

                log.info(
                    "Recovered %s pending update(s) from Telegram in startup batch",
                    len(updates),
                )

                for update in updates:
                    log.info("Startup backlog replay: %s", summarize_update(update))
                    persist_result = await self.persist_and_dispatch_update(
                        update,
                        emit_status_feedback=False,
                    )
                    if persist_result.duplicate:
                        duplicate_updates += 1
                        continue
                    if persist_result.inserted:
                        total_recovered += 1

                # Telegram confirms older updates when getUpdates is called with an
                # offset higher than their update_id. We only advance the offset after
                # this batch has been durably persisted.
                next_offset = updates[-1].update_id + 1

            log.info(
                "Startup backlog replay complete: queued=%s duplicate=%s",
                total_recovered,
                duplicate_updates,
            )
            return total_recovered
        finally:
            if webhook_removed:
                try:
                    await self.install_webhook()
                except Exception:
                    log.exception("Failed to restore webhook after startup recovery")
                    raise

    async def install_webhook(self) -> None:
        await self.ptb_app.bot.set_webhook(
            url=self.webhook_url,
            secret_token=self.config.webhook_secret,
            allowed_updates=["message"],
            drop_pending_updates=False,
        )

        log.info("Webhook registered: %s", self.webhook_url)
        log.info(
            "Local listener: http://%s:%s%s",
            self.config.listen_host,
            self.config.listen_port,
            self.config.webhook_path,
        )
        await self.log_webhook_info("Telegram webhook status after install")

    async def start(self) -> None:
        await self.ptb_app.start()
        self._started = True

    async def stop(self) -> None:
        if self._started:
            try:
                await self.ptb_app.stop()
            except Exception:
                log.exception("Error while stopping telegram application")
            finally:
                self._started = False

        log.info("Telegram application stopped; webhook left registered for future delivery")

    async def run(self) -> None:
        async with self.ptb_app:
            server_task: asyncio.Task[None] | None = None
            try:
                self._install_signal_handlers()
                await self.start()
                server_task = asyncio.create_task(
                    self.http_server.serve(),
                    name="telegram-webhook-http-server",
                )
                await self.http_server.wait_until_started(server_task)
                await self.recover_pending_updates_from_telegram()
                await server_task
            finally:
                if server_task is not None and not server_task.done():
                    self.http_server.request_shutdown()
                    try:
                        await server_task
                    except Exception:
                        log.exception("Error while stopping webhook HTTP server")
                await self.stop()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(RESTART_SIGNAL, self._request_restart)
        log.debug("Signal handler installed for %s", RESTART_SIGNAL.name)

    def _request_restart(self) -> None:
        self._restart_requested = True
        log.info("Bot restart requested by %s", RESTART_SIGNAL.name)
        self.http_server.request_shutdown()

    @property
    def restart_requested(self) -> bool:
        return self._restart_requested


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def main(debug: bool, *, save_dump_dir: bool = False) -> bool:
    await run_runtime_system_checks(component="bot")
    config = Config.from_env(save_dump_dir=save_dump_dir)
    with acquire_process_guard(
        pidfile_path=config.bot_pidfile_path,
        process_name=BOT_PROCESS_NAME,
    ):
        queue_store = SQLiteQueueStore(config.sqlite_settings)
        queue_store.verify_schema()
        log.info("SQLite queue schema verified: %s", config.sqlite_settings.db_path)
        if config.service_dump_dir is not None:
            log.info("Telegram service dump capture enabled: %s", config.service_dump_dir)

        runner = TelegramBotRunner(
            config=config,
            debug=debug,
            queue_store=queue_store,
        )
        await runner.run()
        return runner.restart_requested


if __name__ == "__main__":
    args = parse_args()
    configure_logging(args.debug, trace=args.trace)

    try:
        restart_requested = asyncio.run(
            main(debug=args.debug, save_dump_dir=args.save_dump_dir)
        )
        if restart_requested:
            restart_current_process()
    except (
        KeyError,
        ValueError,
        ProcessAlreadyRunningError,
        QueueStoreError,
        RuntimeCheckError,
        SchemaVerificationError,
    ) as exc:
        log.error("Startup failed: %s", exc)
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        log.info("Shutdown requested by Ctrl-C")

from __future__ import annotations

import logging
import os
import signal
import sys
from dataclasses import dataclass
from pathlib import Path

from bot_libs.daemon_signal import (
    ACTION_DAEMON_ARGV_MARKER,
    ACTION_DAEMON_PROCESS_NAME,
    DAEMON_ARGV_MARKER,
    DAEMON_PROCESS_NAME,
    resolve_action_daemon_pidfile,
    resolve_daemon_pidfile,
)
from bot_libs.process_guard import (
    is_expected_live_process,
    read_process_metadata,
)

BOT_ARGV_MARKER = "bot.py"
BOT_PROCESS_NAME = "telegram-bot"
BOT_PIDFILE_ENV = "TELEGRAM_BOT_PIDFILE"
DEFAULT_BOT_PIDFILE = "/tmp/telegram_bot.pid"
RESTART_SIGNAL = signal.SIGUSR1

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RestartSignalResult:
    name: str
    signaled: bool
    reason: str
    pid: int | None = None


def resolve_bot_pidfile(value: str | None = None) -> str:
    return (value or os.getenv(BOT_PIDFILE_ENV) or DEFAULT_BOT_PIDFILE).strip()


def restart_current_process() -> None:
    argv = [sys.executable, *sys.argv]
    log.info("Restarting current process via exec: %s", " ".join(argv))
    os.execv(sys.executable, argv)


def signal_running_development_processes() -> tuple[RestartSignalResult, ...]:
    return (
        _signal_process(
            name="bot",
            pidfile_path=resolve_bot_pidfile(),
            expected_process_name=BOT_PROCESS_NAME,
            expected_argv_marker=BOT_ARGV_MARKER,
        ),
        _signal_process(
            name="queue-daemon",
            pidfile_path=resolve_daemon_pidfile(),
            expected_process_name=DAEMON_PROCESS_NAME,
            expected_argv_marker=DAEMON_ARGV_MARKER,
        ),
        _signal_process(
            name="action-daemon",
            pidfile_path=resolve_action_daemon_pidfile(),
            expected_process_name=ACTION_DAEMON_PROCESS_NAME,
            expected_argv_marker=ACTION_DAEMON_ARGV_MARKER,
        ),
    )


def _signal_process(
    *,
    name: str,
    pidfile_path: str | Path,
    expected_process_name: str,
    expected_argv_marker: str,
) -> RestartSignalResult:
    metadata = read_process_metadata(pidfile_path)
    if metadata is None:
        return RestartSignalResult(
            name=name,
            signaled=False,
            reason="pidfile_missing_or_invalid",
        )

    if not is_expected_live_process(
        metadata,
        expected_process_name=expected_process_name,
        expected_argv_marker=expected_argv_marker,
    ):
        return RestartSignalResult(
            name=name,
            signaled=False,
            reason="process_not_live_or_not_expected",
            pid=metadata.pid,
        )

    try:
        os.kill(metadata.pid, RESTART_SIGNAL)
    except ProcessLookupError:
        return RestartSignalResult(
            name=name,
            signaled=False,
            reason="process_not_found",
            pid=metadata.pid,
        )

    return RestartSignalResult(
        name=name,
        signaled=True,
        reason="signaled",
        pid=metadata.pid,
    )


def main() -> int:
    results = signal_running_development_processes()
    for result in results:
        status = "sent" if result.signaled else "skipped"
        pid_text = f" pid={result.pid}" if result.pid is not None else ""
        print(f"{result.name}: {status} SIGUSR1{pid_text} reason={result.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

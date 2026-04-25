from __future__ import annotations

import os
import signal
from dataclasses import dataclass
from pathlib import Path

from bot_libs.process_guard import (
    is_expected_live_process,
    read_process_metadata,
)

DAEMON_PIDFILE_ENV = "TELEGRAM_QUEUE_DAEMON_PIDFILE"
DEFAULT_DAEMON_PIDFILE = "/tmp/telegram_queue_daemon.pid"
DAEMON_PROCESS_NAME = "telegram-queue-daemon"
DAEMON_ARGV_MARKER = "telegram_queue_daemon.py"


@dataclass(frozen=True, slots=True)
class DaemonSignalResult:
    signaled: bool
    reason: str
    pid: int | None = None


def resolve_daemon_pidfile(value: str | None = None) -> str:
    return (value or os.getenv(DAEMON_PIDFILE_ENV) or DEFAULT_DAEMON_PIDFILE).strip()


def wake_queue_daemon(pidfile_path: str | Path | None = None) -> DaemonSignalResult:
    resolved_pidfile = Path(pidfile_path or resolve_daemon_pidfile())
    metadata = read_process_metadata(resolved_pidfile)
    if metadata is None:
        return DaemonSignalResult(signaled=False, reason="pidfile_missing_or_invalid")

    if not is_expected_live_process(
        metadata,
        expected_process_name=DAEMON_PROCESS_NAME,
        expected_argv_marker=DAEMON_ARGV_MARKER,
    ):
        return DaemonSignalResult(
            signaled=False,
            reason="daemon_process_not_live_or_not_expected",
            pid=metadata.pid,
        )

    try:
        os.kill(metadata.pid, signal.SIGHUP)
    except ProcessLookupError:
        return DaemonSignalResult(
            signaled=False,
            reason="daemon_process_not_found",
            pid=metadata.pid,
        )

    return DaemonSignalResult(signaled=True, reason="signaled", pid=metadata.pid)

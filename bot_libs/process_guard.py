from __future__ import annotations

import fcntl
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any


class ProcessAlreadyRunningError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ProcessMetadata:
    pid: int
    process_name: str
    argv: tuple[str, ...]
    started_at: str
    start_time_ticks: int | None

    def as_json(self) -> str:
        return json.dumps(
            {
                "pid": self.pid,
                "process_name": self.process_name,
                "argv": list(self.argv),
                "started_at": self.started_at,
                "start_time_ticks": self.start_time_ticks,
            },
            sort_keys=True,
        )


class ProcessGuard:
    def __init__(self, *, pidfile_path: str | Path, process_name: str) -> None:
        self.pidfile_path = Path(pidfile_path)
        self.process_name = process_name
        self._fd: int | None = None

    def acquire(self) -> "ProcessGuard":
        self.pidfile_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.pidfile_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            metadata = read_process_metadata(self.pidfile_path)
            raise ProcessAlreadyRunningError(
                _running_error_message(self.pidfile_path, metadata)
            ) from exc

        metadata = ProcessMetadata(
            pid=os.getpid(),
            process_name=self.process_name,
            argv=tuple(sys.argv),
            started_at=datetime.now(timezone.utc).isoformat(),
            start_time_ticks=get_process_start_time_ticks(os.getpid()),
        )
        os.ftruncate(fd, 0)
        os.write(fd, (metadata.as_json() + "\n").encode("utf-8"))
        os.fsync(fd)
        self._fd = fd
        return self

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "ProcessGuard":
        if self._fd is not None:
            return self
        return self.acquire()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        self.release()


def acquire_process_guard(
    *,
    pidfile_path: str | Path,
    process_name: str,
) -> ProcessGuard:
    return ProcessGuard(pidfile_path=pidfile_path, process_name=process_name).acquire()


def read_process_metadata(pidfile_path: str | Path) -> ProcessMetadata | None:
    try:
        raw_text = Path(pidfile_path).read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw_text:
        return None

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None

    try:
        return ProcessMetadata(
            pid=int(data["pid"]),
            process_name=str(data.get("process_name") or ""),
            argv=tuple(str(item) for item in data.get("argv") or ()),
            started_at=str(data.get("started_at") or ""),
            start_time_ticks=_optional_int(data.get("start_time_ticks")),
        )
    except (KeyError, TypeError, ValueError):
        return None


def is_expected_live_process(
    metadata: ProcessMetadata,
    *,
    expected_process_name: str,
    expected_argv_marker: str,
) -> bool:
    if metadata.process_name != expected_process_name:
        return False
    if metadata.pid < 1:
        return False
    if not _pid_exists(metadata.pid):
        return False

    current_start_time = get_process_start_time_ticks(metadata.pid)
    if (
        metadata.start_time_ticks is not None
        and current_start_time is not None
        and metadata.start_time_ticks != current_start_time
    ):
        return False

    cmdline = get_process_cmdline(metadata.pid)
    if cmdline is not None and expected_argv_marker not in cmdline:
        return False

    return True


def get_process_cmdline(pid: int) -> str | None:
    try:
        data = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    text = data.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()
    return text or None


def get_process_start_time_ticks(pid: int) -> int | None:
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return None

    try:
        after_comm = stat_text.rsplit(") ", 1)[1]
        fields = after_comm.split()
        return int(fields[19])
    except (IndexError, ValueError):
        return None


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _running_error_message(
    pidfile_path: Path,
    metadata: ProcessMetadata | None,
) -> str:
    if metadata is None:
        return f"process lock is already held: {pidfile_path}"
    return (
        f"{metadata.process_name or 'process'} already running "
        f"pid={metadata.pid} pidfile={pidfile_path}"
    )

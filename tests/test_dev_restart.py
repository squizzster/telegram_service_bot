from __future__ import annotations

import io
import signal
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from bot_libs import dev_restart
from bot_libs.process_guard import ProcessMetadata


class DevRestartTests(unittest.TestCase):
    def test_signal_process_skips_missing_pidfile(self) -> None:
        result = dev_restart._signal_process(
            name="bot",
            pidfile_path="/tmp/missing.pid",
            expected_process_name=dev_restart.BOT_PROCESS_NAME,
            expected_argv_marker=dev_restart.BOT_ARGV_MARKER,
        )

        self.assertFalse(result.signaled)
        self.assertEqual(result.reason, "pidfile_missing_or_invalid")
        self.assertIsNone(result.pid)

    def test_signal_process_skips_unexpected_live_process(self) -> None:
        metadata = _metadata(process_name="not-the-bot")

        with patch("bot_libs.dev_restart.read_process_metadata", return_value=metadata):
            result = dev_restart._signal_process(
                name="bot",
                pidfile_path="/tmp/bot.pid",
                expected_process_name=dev_restart.BOT_PROCESS_NAME,
                expected_argv_marker=dev_restart.BOT_ARGV_MARKER,
            )

        self.assertFalse(result.signaled)
        self.assertEqual(result.reason, "process_not_live_or_not_expected")
        self.assertEqual(result.pid, 12345)

    def test_signal_process_sends_usr1_to_expected_process(self) -> None:
        metadata = _metadata()

        with patch("bot_libs.dev_restart.read_process_metadata", return_value=metadata):
            with patch(
                "bot_libs.dev_restart.is_expected_live_process",
                return_value=True,
            ) as expected_live:
                with patch("bot_libs.dev_restart.os.kill") as kill:
                    result = dev_restart._signal_process(
                        name="bot",
                        pidfile_path="/tmp/bot.pid",
                        expected_process_name=dev_restart.BOT_PROCESS_NAME,
                        expected_argv_marker=dev_restart.BOT_ARGV_MARKER,
                    )

        expected_live.assert_called_once_with(
            metadata,
            expected_process_name=dev_restart.BOT_PROCESS_NAME,
            expected_argv_marker=dev_restart.BOT_ARGV_MARKER,
        )
        kill.assert_called_once_with(12345, signal.SIGUSR1)
        self.assertTrue(result.signaled)
        self.assertEqual(result.reason, "signaled")
        self.assertEqual(result.pid, 12345)

    def test_signal_process_reports_pid_disappearing_before_signal(self) -> None:
        metadata = _metadata()

        with patch("bot_libs.dev_restart.read_process_metadata", return_value=metadata):
            with patch("bot_libs.dev_restart.is_expected_live_process", return_value=True):
                with patch(
                    "bot_libs.dev_restart.os.kill",
                    side_effect=ProcessLookupError,
                ):
                    result = dev_restart._signal_process(
                        name="bot",
                        pidfile_path="/tmp/bot.pid",
                        expected_process_name=dev_restart.BOT_PROCESS_NAME,
                        expected_argv_marker=dev_restart.BOT_ARGV_MARKER,
                    )

        self.assertFalse(result.signaled)
        self.assertEqual(result.reason, "process_not_found")
        self.assertEqual(result.pid, 12345)

    def test_main_prints_each_restart_signal_result(self) -> None:
        results = (
            dev_restart.RestartSignalResult(
                name="bot",
                signaled=True,
                reason="signaled",
                pid=111,
            ),
            dev_restart.RestartSignalResult(
                name="queue-daemon",
                signaled=False,
                reason="pidfile_missing_or_invalid",
            ),
        )

        output = io.StringIO()
        with patch(
            "bot_libs.dev_restart.signal_running_development_processes",
            return_value=results,
        ):
            with redirect_stdout(output):
                exit_code = dev_restart.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            output.getvalue().splitlines(),
            [
                "bot: sent SIGUSR1 pid=111 reason=signaled",
                "queue-daemon: skipped SIGUSR1 reason=pidfile_missing_or_invalid",
            ],
        )


def _metadata(
    *,
    pid: int = 12345,
    process_name: str = dev_restart.BOT_PROCESS_NAME,
    argv: tuple[str, ...] = ("python", "bot.py"),
) -> ProcessMetadata:
    return ProcessMetadata(
        pid=pid,
        process_name=process_name,
        argv=argv,
        started_at="2026-04-26T00:00:00+00:00",
        start_time_ticks=123,
    )


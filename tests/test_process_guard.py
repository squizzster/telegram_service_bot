from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bot_libs.daemon_signal import wake_queue_daemon
from bot_libs.process_guard import (
    ProcessAlreadyRunningError,
    acquire_process_guard,
    read_process_metadata,
)


class ProcessGuardTests(unittest.TestCase):
    def test_guard_writes_metadata_and_rejects_second_holder(self) -> None:
        with TemporaryDirectory() as tmpdir:
            pidfile = Path(tmpdir) / "bot.pid"

            with acquire_process_guard(
                pidfile_path=pidfile,
                process_name="telegram-bot",
            ):
                metadata = read_process_metadata(pidfile)
                self.assertIsNotNone(metadata)
                assert metadata is not None
                self.assertEqual(metadata.process_name, "telegram-bot")
                self.assertGreater(metadata.pid, 0)

                with self.assertRaises(ProcessAlreadyRunningError):
                    acquire_process_guard(
                        pidfile_path=pidfile,
                        process_name="telegram-bot",
                    )

            with acquire_process_guard(
                pidfile_path=pidfile,
                process_name="telegram-bot",
            ):
                self.assertIsNotNone(read_process_metadata(pidfile))

    def test_wake_queue_daemon_reports_missing_pidfile(self) -> None:
        with TemporaryDirectory() as tmpdir:
            result = wake_queue_daemon(Path(tmpdir) / "missing.pid")

        self.assertFalse(result.signaled)
        self.assertEqual(result.reason, "pidfile_missing_or_invalid")

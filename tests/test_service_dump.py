from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bot import Config, parse_args
from bot_libs.service_dump import TelegramServiceDumper

BASE_ENV = {
    "TELEGRAM_BOT_KEY": "123456:ABCDEF",
    "TELEGRAM_WEBHOOK_SECRET": "secret_token",
    "SQL_TELEGRAM_FILE": "/tmp/telegram-service.sqlite",
}


class ParseArgsTests(unittest.TestCase):
    def test_parse_args_enables_service_dump_capture(self) -> None:
        args = parse_args(["--save-dump-dir"])

        self.assertTrue(args.save_dump_dir)
        self.assertFalse(args.debug)


class ConfigTests(unittest.TestCase):
    def test_from_env_requires_dump_dir_when_capture_enabled(self) -> None:
        with patch.dict(os.environ, BASE_ENV, clear=True):
            with self.assertRaises(ValueError) as exc_info:
                Config.from_env(save_dump_dir=True)

        self.assertIn("TELEGRAM_SERVICE_DUMP_DIR", str(exc_info.exception))

    def test_from_env_sets_dump_dir_when_capture_enabled(self) -> None:
        with patch.dict(
            os.environ,
            {**BASE_ENV, "TELEGRAM_SERVICE_DUMP_DIR": "/tmp/telegram-dumps"},
            clear=True,
        ):
            config = Config.from_env(save_dump_dir=True)

        self.assertEqual(config.service_dump_dir, "/tmp/telegram-dumps")

    def test_from_env_ignores_dump_dir_when_capture_disabled(self) -> None:
        with patch.dict(
            os.environ,
            {**BASE_ENV, "TELEGRAM_SERVICE_DUMP_DIR": "/tmp/telegram-dumps"},
            clear=True,
        ):
            config = Config.from_env(save_dump_dir=False)

        self.assertIsNone(config.service_dump_dir)


class TelegramServiceDumperTests(unittest.TestCase):
    def test_dump_update_writes_single_json_file(self) -> None:
        payload = {
            "update_id": 449012319,
            "message": {
                "message_id": 41,
                "chat": {"id": -1003986727769},
                "text": "hello",
            },
        }

        with TemporaryDirectory() as tmpdir:
            dumper = TelegramServiceDumper(Path(tmpdir) / "nested" / "dumps")

            dump_path = dumper.dump_update(payload)
            dump_files = list(dump_path.parent.iterdir())

            self.assertEqual(len(dump_files), 1)
            self.assertEqual(json.loads(dump_path.read_text(encoding="utf-8")), payload)
            self.assertIn("update_449012319", dump_path.name)
            self.assertIn("chat_-1003986727769", dump_path.name)
            self.assertIn("message_41", dump_path.name)

    def test_dump_update_preserves_raw_json_bytes_when_supplied(self) -> None:
        payload = {
            "update_id": 7,
            "message": {
                "message_id": 8,
                "chat": {"id": 9},
            },
        }
        raw_json = b'{"update_id":7,"message":{"message_id":8,"chat":{"id":9}}}'

        with TemporaryDirectory() as tmpdir:
            dumper = TelegramServiceDumper(tmpdir)
            dump_path = dumper.dump_update(payload, raw_json_bytes=raw_json)

            self.assertEqual(dump_path.read_bytes(), raw_json)

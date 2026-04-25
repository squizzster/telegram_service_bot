from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from bot_libs.replay_service_dumps import replay_dump_directory
from bot_libs.sql import SQLiteQueueStore, SQLiteSettings

REAL_DUMP_BODIES = {
    "20260423T192025.429549Z_update_449012333_chat_-1003986727769_message_55_57506dc3bb.json": """{"update_id":449012333,
"message":{"message_id":55,"from":{"id":1609506098,"is_bot":false,"first_name":"Mark","username":"g_booking","language_code":"en"},"chat":{"id":-1003986727769,"title":"Charles-Service-Group","type":"supergroup"},"date":1776972025,"text":"wewew"}}""",
    "20260423T192034.830347Z_update_449012334_chat_-1003986727769_message_56_8358dbf672.json": """{"update_id":449012334,
"message":{"message_id":56,"from":{"id":1609506098,"is_bot":false,"first_name":"Mark","username":"g_booking","language_code":"en"},"chat":{"id":-1003986727769,"title":"Charles-Service-Group","type":"supergroup"},"date":1776972034,"voice":{"duration":3,"mime_type":"audio/ogg","file_id":"AwACAgQAAyEFAATtoKNZAAM4aepxAh2vKjfqLHAaipHgqUxlnZYAAgYaAAIpbVlTTGxj9NGdrwE7BA","file_unique_id":"AgADBhoAAiltWVM","file_size":14663}}}""",
    "20260423T192141.277163Z_update_449012335_chat_-1003986727769_message_57_04f6a948bb.json": """{"update_id":449012335,
"message":{"message_id":57,"from":{"id":1609506098,"is_bot":false,"first_name":"Mark","username":"g_booking","language_code":"en"},"chat":{"id":-1003986727769,"title":"Charles-Service-Group","type":"supergroup"},"date":1776972101,"photo":[{"file_id":"AgACAgQAAyEFAATtoKNZAAM5aepxRUf0i0yegRIB9h5heLNBZvoAAgsMaxspbVlTMg2cypFVazQBAAMCAANzAAM7BA","file_unique_id":"AQADCwxrGyltWVN4","file_size":1354,"width":90,"height":67},{"file_id":"AgACAgQAAyEFAATtoKNZAAM5aepxRUf0i0yegRIB9h5heLNBZvoAAgsMaxspbVlTMg2cypFVazQBAAMCAANtAAM7BA","file_unique_id":"AQADCwxrGyltWVNy","file_size":13826,"width":320,"height":240},{"file_id":"AgACAgQAAyEFAATtoKNZAAM5aepxRUf0i0yegRIB9h5heLNBZvoAAgsMaxspbVlTMg2cypFVazQBAAMCAAN4AAM7BA","file_unique_id":"AQADCwxrGyltWVN9","file_size":41426,"width":800,"height":600},{"file_id":"AgACAgQAAyEFAATtoKNZAAM5aepxRUf0i0yegRIB9h5heLNBZvoAAgsMaxspbVlTMg2cypFVazQBAAMCAAN5AAM7BA","file_unique_id":"AQADCwxrGyltWVN-","file_size":63485,"width":1280,"height":960}]}}""",
    "20260423T192141.454980Z_update_449012336_chat_-1003986727769_message_58_15bac21a23.json": """{"update_id":449012336,
"message":{"message_id":58,"from":{"id":1609506098,"is_bot":false,"first_name":"Mark","username":"g_booking","language_code":"en"},"chat":{"id":-1003986727769,"title":"Charles-Service-Group","type":"supergroup"},"date":1776972101,"photo":[{"file_id":"AgACAgQAAyEFAATtoKNZAAM6aepxRQo9Uju7xRp8d7woe6ofcWkAAgwMaxspbVlTU0UxQrtPz5UBAAMCAANzAAM7BA","file_unique_id":"AQADDAxrGyltWVN4","file_size":1502,"width":51,"height":90},{"file_id":"AgACAgQAAyEFAATtoKNZAAM6aepxRQo9Uju7xRp8d7woe6ofcWkAAgwMaxspbVlTU0UxQrtPz5UBAAMCAANtAAM7BA","file_unique_id":"AQADDAxrGyltWVNy","file_size":20747,"width":180,"height":320},{"file_id":"AgACAgQAAyEFAATtoKNZAAM6aepxRQo9Uju7xRp8d7woe6ofcWkAAgwMaxspbVlTU0UxQrtPz5UBAAMCAAN4AAM7BA","file_unique_id":"AQADDAxrGyltWVN9","file_size":85340,"width":450,"height":800},{"file_id":"AgACAgQAAyEFAATtoKNZAAM6aepxRQo9Uju7xRp8d7woe6ofcWkAAgwMaxspbVlTU0UxQrtPz5UBAAMCAAN5AAM7BA","file_unique_id":"AQADDAxrGyltWVN-","file_size":151590,"width":720,"height":1280}],"caption":"OK - some mages"}}""",
    "20260423T192202.194310Z_update_449012337_chat_-1003986727769_message_59_51ea6f4b8b.json": """{"update_id":449012337,
"message":{"message_id":59,"from":{"id":1609506098,"is_bot":false,"first_name":"Mark","username":"g_booking","language_code":"en"},"chat":{"id":-1003986727769,"title":"Charles-Service-Group","type":"supergroup"},"date":1776972122,"media_group_id":"14215776976937444","document":{"file_name":"ChatGPT Image Apr 22, 2026, 10_29_57 AM.png","mime_type":"image/png","thumbnail":{"file_id":"AAMCBAADIQUABO2go1kAAztp6nFae8xDA2s7ZF2QUI8pwaU7tgACBxoAAiltWVNu0B0MfQvQFQEAB20AAzsE","file_unique_id":"AQADBxoAAiltWVNy","file_size":20158,"width":180,"height":320},"thumb":{"file_id":"AAMCBAADIQUABO2go1kAAztp6nFae8xDA2s7ZF2QUI8pwaU7tgACBxoAAiltWVNu0B0MfQvQFQEAB20AAzsE","file_unique_id":"AQADBxoAAiltWVNy","file_size":20158,"width":180,"height":320},"file_id":"BQACAgQAAyEFAATtoKNZAAM7aepxWnvMQwNrO2RdkFCPKcGlO7YAAgcaAAIpbVlTbtAdDH0L0BU7BA","file_unique_id":"AgADBxoAAiltWVM","file_size":1558709}}}""",
    "20260423T192202.219386Z_update_449012338_chat_-1003986727769_message_60_2cb574a1bb.json": """{"update_id":449012338,
"message":{"message_id":60,"from":{"id":1609506098,"is_bot":false,"first_name":"Mark","username":"g_booking","language_code":"en"},"chat":{"id":-1003986727769,"title":"Charles-Service-Group","type":"supergroup"},"date":1776972122,"media_group_id":"14215776976937444","document":{"file_name":"ChatGPT Image Apr 22, 2026, 06_37_37 AM.png","mime_type":"image/png","thumbnail":{"file_id":"AAMCBAADIQUABO2go1kAAzxp6nFaiVrW3Xk-0pOexXkzFZ0UdgACCBoAAiltWVNL9mnG85ag1QEAB20AAzsE","file_unique_id":"AQADCBoAAiltWVNy","file_size":23449,"width":180,"height":320},"thumb":{"file_id":"AAMCBAADIQUABO2go1kAAzxp6nFaiVrW3Xk-0pOexXkzFZ0UdgACCBoAAiltWVNL9mnG85ag1QEAB20AAzsE","file_unique_id":"AQADCBoAAiltWVNy","file_size":23449,"width":180,"height":320},"file_id":"BQACAgQAAyEFAATtoKNZAAM8aepxWola1t15PtKTnsV5MxWdFHYAAggaAAIpbVlTS_ZpxvOWoNU7BA","file_unique_id":"AgADCBoAAiltWVM","file_size":1745405},"caption":"rrrrrrrrrrrr"}}""",
}


class ReplayServiceDumpsTests(unittest.TestCase):
    def test_replay_dump_directory_inserts_realistic_samples(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dump_dir = self._write_dump_dir(Path(tmpdir) / "dumps")
            store = self._create_store(Path(tmpdir) / "queue.sqlite")

            summary = replay_dump_directory(
                dump_dir,
                queue_store=store,
                max_attempts=5,
                preserve_update_ids=True,
            )

            self.assertEqual(summary.total_files, 6)
            self.assertEqual(summary.inserted, 6)
            self.assertEqual(summary.duplicates, 0)
            self.assertEqual(summary.skipped, 0)
            self.assertEqual(
                [record.content_type for record in summary.records],
                ["text", "voice", "photo", "photo", "document", "document"],
            )

            with sqlite3.connect(store.settings.db_path) as conn:
                row_count = conn.execute(
                    "SELECT COUNT(*) FROM telegram_queue"
                ).fetchone()[0]
                max_attempt_values = [
                    row[0]
                    for row in conn.execute(
                        "SELECT max_attempts FROM telegram_queue ORDER BY update_id"
                    )
                ]

            self.assertEqual(row_count, 6)
            self.assertEqual(max_attempt_values, [5, 5, 5, 5, 5, 5])

    def test_replay_dump_directory_remaps_update_ids_by_default(self) -> None:
        with TemporaryDirectory() as tmpdir:
            dump_dir = self._write_dump_dir(Path(tmpdir) / "dumps")
            store = self._create_store(Path(tmpdir) / "queue.sqlite")

            first_summary = replay_dump_directory(
                dump_dir,
                queue_store=store,
                max_attempts=3,
                preserve_update_ids=True,
            )
            second_summary = replay_dump_directory(
                dump_dir,
                queue_store=store,
                max_attempts=3,
            )

            self.assertEqual(first_summary.inserted, 6)
            self.assertEqual(second_summary.inserted, 6)
            self.assertEqual(second_summary.duplicates, 0)
            self.assertTrue(
                all(
                    record.replay_update_id is not None
                    and record.original_update_id is not None
                    and record.replay_update_id > record.original_update_id
                    for record in second_summary.records
                )
            )

            with sqlite3.connect(store.settings.db_path) as conn:
                row_count = conn.execute(
                    "SELECT COUNT(*) FROM telegram_queue"
                ).fetchone()[0]

            self.assertEqual(row_count, 12)

    def _write_dump_dir(self, dump_dir: Path) -> Path:
        dump_dir.mkdir(parents=True, exist_ok=True)
        for filename, body in REAL_DUMP_BODIES.items():
            (dump_dir / filename).write_text(body, encoding="utf-8")
        return dump_dir

    def _create_store(self, db_path: Path) -> SQLiteQueueStore:
        store = SQLiteQueueStore(SQLiteSettings(db_path=str(db_path)))
        store.create_schema(create_parent_dir=True)
        return store

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot_libs.runtime_checks import (
    REQUIRED_ENV_VARS,
    RuntimeCheckError,
    _check_openai_key,
    _check_required_env,
    _format_failures,
    _normalize_gemini_model_id,
    run_runtime_system_checks,
)


class RuntimeChecksTests(unittest.IsolatedAsyncioTestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_required_env_reports_all_missing_values(self) -> None:
        failures = _check_required_env()

        for env_var in REQUIRED_ENV_VARS:
            self.assertIn(f"missing required environment variable {env_var}", failures)

    @patch.dict(
        os.environ,
        {
            "TELEGRAM_BOT_KEY": "telegram-token",
            "TELEGRAM_WEBHOOK_SECRET": "webhook-secret",
            "SQL_TELEGRAM_FILE": "/home/auto_charles/telegram/db/test.sqlite",
            "OPENAI_API_KEY": "openai-key",
            "DEEPGRAM_API_KEY": "deepgram-key",
            "GEMINI_API_KEY": "gemini-key",
        },
        clear=True,
    )
    def test_required_env_accepts_complete_values(self) -> None:
        self.assertEqual(_check_required_env(), [])

    def test_normalize_gemini_model_id_strips_models_prefix(self) -> None:
        self.assertEqual(
            _normalize_gemini_model_id("models/gemini-3-flash-preview"),
            "gemini-3-flash-preview",
        )

    @patch.dict(os.environ, {"GEMINI_API_KEY": "secret-google-key"}, clear=True)
    def test_failure_format_redacts_configured_secrets(self) -> None:
        message = _format_failures(
            ["url=https://example.test?key=secret-google-key"]
        )

        self.assertNotIn("secret-google-key", message)
        self.assertIn("<GEMINI_API_KEY:redacted>", message)

    @patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "openai-key",
            "OPENAI_TRANSCRIBE_MODEL": "custom-transcribe-model",
        },
        clear=True,
    )
    async def test_openai_credential_check_uses_configured_model(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(return_value=SimpleNamespace(status_code=200))
        )

        result = await _check_openai_key(client)

        self.assertIsNone(result)
        client.get.assert_awaited_once_with(
            "https://api.openai.com/v1/models/custom-transcribe-model",
            headers={"Authorization": "Bearer openai-key"},
        )

    @patch.dict(
        os.environ,
        {
            "TELEGRAM_BOT_KEY": "telegram-token",
            "TELEGRAM_WEBHOOK_SECRET": "webhook-secret",
            "SQL_TELEGRAM_FILE": "/home/auto_charles/telegram/db/test.sqlite",
            "OPENAI_API_KEY": "openai-key",
            "DEEPGRAM_API_KEY": "deepgram-key",
            "GEMINI_API_KEY": "gemini-key",
        },
        clear=True,
    )
    @patch("bot_libs.runtime_checks._check_credentials", new_callable=AsyncMock)
    @patch("bot_libs.runtime_checks._check_commands", return_value=[])
    @patch("bot_libs.runtime_checks._check_imports", return_value=[])
    async def test_run_runtime_system_checks_returns_report(
        self,
        check_imports: object,
        check_commands: object,
        check_credentials: AsyncMock,
    ) -> None:
        del check_imports, check_commands
        check_credentials.return_value = []

        report = await run_runtime_system_checks(component="test")

        self.assertIn("telegram", report.imports_checked)
        self.assertIn("OPENAI_API_KEY", report.env_vars_checked)
        self.assertIn("OPEN_AI", report.credentials_checked)
        check_credentials.assert_awaited_once()

    @patch.dict(
        os.environ,
        {
            "TELEGRAM_BOT_KEY": "telegram-token",
            "SQL_TELEGRAM_FILE": "/home/auto_charles/telegram/db/test.sqlite",
            "OPENAI_API_KEY": "openai-key",
        },
        clear=True,
    )
    @patch("bot_libs.runtime_checks._check_credentials", new_callable=AsyncMock)
    @patch("bot_libs.runtime_checks._check_commands", return_value=[])
    @patch("bot_libs.runtime_checks._check_imports", return_value=[])
    async def test_action_daemon_runtime_checks_use_minimal_profile(
        self,
        check_imports: object,
        check_commands: object,
        check_credentials: AsyncMock,
    ) -> None:
        del check_imports, check_commands
        check_credentials.return_value = []

        report = await run_runtime_system_checks(component="action-daemon")

        self.assertEqual(
            report.env_vars_checked,
            ("TELEGRAM_BOT_KEY", "SQL_TELEGRAM_FILE", "OPENAI_API_KEY"),
        )
        self.assertEqual(report.commands_checked, ())
        self.assertEqual(report.credentials_checked, ("telegram", "OPEN_AI"))
        check_credentials.assert_awaited_once_with(
            timeout_seconds=10.0,
            credential_labels=("telegram", "OPEN_AI"),
        )

    @patch.dict(
        os.environ,
        {
            "TELEGRAM_BOT_KEY": "telegram-token",
            "TELEGRAM_WEBHOOK_SECRET": "webhook-secret",
            "SQL_TELEGRAM_FILE": "/home/auto_charles/telegram/db/test.sqlite",
            "OPENAI_API_KEY": "openai-key",
            "DEEPGRAM_API_KEY": "deepgram-key",
            "GEMINI_API_KEY": "gemini-key",
        },
        clear=True,
    )
    @patch("bot_libs.runtime_checks._check_commands", return_value=[])
    @patch("bot_libs.runtime_checks._check_imports", return_value=["missing x"])
    async def test_run_runtime_system_checks_stops_before_network_when_local_checks_fail(
        self,
        check_imports: object,
        check_commands: object,
    ) -> None:
        del check_imports, check_commands

        with patch(
            "bot_libs.runtime_checks._check_credentials",
            new_callable=AsyncMock,
        ) as check_credentials:
            with self.assertRaisesRegex(RuntimeCheckError, "missing x"):
                await run_runtime_system_checks(component="test")

            check_credentials.assert_not_awaited()

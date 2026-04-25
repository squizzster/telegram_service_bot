from __future__ import annotations

import logging
import unittest

from bot_libs.logging_utils import (
    RedactSecretsFilter,
    configure_app_logging,
    redact_secrets,
)


class LoggingUtilsTests(unittest.TestCase):
    def test_redact_secrets_masks_bot_tokens_in_strings(self) -> None:
        self.assertEqual(
            redact_secrets("https://api.telegram.org/bot123456:ABC_def-789/getMe"),
            "https://api.telegram.org/bot<redacted>/getMe",
        )

    def test_redaction_filter_masks_message_arguments(self) -> None:
        record = logging.LogRecord(
            name="telegram.Bot",
            level=logging.DEBUG,
            pathname=__file__,
            lineno=1,
            msg="url=%s",
            args=("https://api.telegram.org/bot123456:ABC_def-789/getMe",),
            exc_info=None,
        )

        RedactSecretsFilter().filter(record)

        self.assertEqual(record.getMessage(), "url=https://api.telegram.org/bot<redacted>/getMe")

    def test_debug_logging_keeps_openai_sdk_internals_quiet(self) -> None:
        openai_logger = logging.getLogger("openai")
        openai_base_client_logger = logging.getLogger("openai._base_client")
        old_openai_level = openai_logger.level
        old_base_client_level = openai_base_client_logger.level
        try:
            configure_app_logging(debug=True)

            self.assertEqual(openai_logger.level, logging.WARNING)
            self.assertEqual(openai_base_client_logger.level, logging.WARNING)
        finally:
            openai_logger.setLevel(old_openai_level)
            openai_base_client_logger.setLevel(old_base_client_level)

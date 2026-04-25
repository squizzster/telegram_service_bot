from __future__ import annotations

import unittest

from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut

from bot_libs.queue_processor_errors import (
    OriginalMessageUnavailableError,
    PermanentJobError,
    RetryableJobError,
)
from bot_libs.telegram_error_policy import (
    is_original_message_unavailable_error,
    job_error_from_telegram_error,
)


class TelegramErrorPolicyTests(unittest.TestCase):
    def test_original_message_unavailable_markers_are_permanent_cancel(self) -> None:
        error = BadRequest("Message to be replied not found")

        self.assertTrue(is_original_message_unavailable_error(error))
        job_error = job_error_from_telegram_error(error, action="send_transcript")

        self.assertIsInstance(job_error, OriginalMessageUnavailableError)
        self.assertIn("send_transcript", str(job_error))

    def test_other_bad_request_is_permanent(self) -> None:
        error = BadRequest("can't parse entities")

        self.assertFalse(is_original_message_unavailable_error(error))
        job_error = job_error_from_telegram_error(error, action="send_transcript")

        self.assertIsInstance(job_error, PermanentJobError)
        self.assertNotIsInstance(job_error, OriginalMessageUnavailableError)

    def test_forbidden_is_permanent(self) -> None:
        error = Forbidden("bot was kicked from the group chat")

        job_error = job_error_from_telegram_error(error, action="send_transcript")

        self.assertIsInstance(job_error, PermanentJobError)

    def test_timeout_is_retryable(self) -> None:
        error = TimedOut("timed out")

        job_error = job_error_from_telegram_error(error, action="send_transcript")

        self.assertIsInstance(job_error, RetryableJobError)

    def test_retry_after_preserves_telegram_cooldown(self) -> None:
        error = RetryAfter(25)

        job_error = job_error_from_telegram_error(error, action="send_transcript")

        self.assertIsInstance(job_error, RetryableJobError)
        self.assertEqual(job_error.retry_after_seconds, 25)

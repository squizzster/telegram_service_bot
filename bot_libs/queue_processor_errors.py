from __future__ import annotations


class RetryableJobError(RuntimeError):
    """
    Raised when processing failed but may succeed later.

    Examples:
    - Telegram network/API failure
    - temporary unavailable file
    - downstream transient failure
    """

    def __init__(
        self,
        message: str = "",
        *,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class PermanentJobError(RuntimeError):
    """
    Raised when processing cannot succeed on retry.

    Examples:
    - missing file_id
    - malformed payload_json
    - unsupported internal processor state
    """


class OriginalMessageUnavailableError(PermanentJobError):
    """
    Raised when the source Telegram message disappeared while processing.

    A deleted source message is treated as a user cancellation, not a transient
    Telegram failure.
    """


class UnusableTranscriptError(PermanentJobError):
    """
    Raised when an STT provider returned successfully but produced unusable text.

    Examples:
    - no transcript text in a successful provider response
    - transcript word count exceeds the speech-rate sanity limit
    """

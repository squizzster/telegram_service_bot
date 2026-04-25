from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from bot_libs.queue_processor_errors import (
    PermanentJobError,
    RetryableJobError,
    UnusableTranscriptError,
)

DEFAULT_DEEPGRAM_MODEL_ID = "nova-3"
DEFAULT_DEEPGRAM_LANGUAGE = "en"
MAX_DEEPGRAM_AUDIO_BYTES = 100 * 1024 * 1024

log = logging.getLogger(__name__)


def resolve_deepgram_model_id() -> str:
    return (os.getenv("DEEPGRAM_MODEL") or DEFAULT_DEEPGRAM_MODEL_ID).strip()


def resolve_deepgram_language() -> str:
    return (os.getenv("DEEPGRAM_LANGUAGE") or DEFAULT_DEEPGRAM_LANGUAGE).strip()


def resolve_deepgram_api_key() -> str:
    api_key = (os.getenv("DEEPGRAM_API_KEY") or "").strip()
    if not api_key:
        raise RetryableJobError("DEEPGRAM_API_KEY missing")
    return api_key


async def transcribe_audio_bytes(
    *,
    audio_bytes: bytes,
    mime_type: str | None,
) -> dict[str, str]:
    del mime_type

    if not audio_bytes:
        raise PermanentJobError("audio payload is empty")
    if len(audio_bytes) > MAX_DEEPGRAM_AUDIO_BYTES:
        raise PermanentJobError("audio payload exceeds Deepgram inline limit")

    model_id = resolve_deepgram_model_id()
    language = resolve_deepgram_language()
    resolve_deepgram_api_key()
    log.debug(
        "Deepgram STT request starting model=%s language=%s audio_bytes=%s",
        model_id,
        language,
        len(audio_bytes),
    )

    def _transcribe_sync() -> object:
        try:
            from deepgram import DeepgramClient
        except ImportError as exc:
            raise RetryableJobError("deepgram package is not installed") from exc

        deepgram = DeepgramClient()
        return deepgram.listen.v1.media.transcribe_file(
            request=audio_bytes,
            model=model_id,
            language=language,
            smart_format=True,
            numerals=True,
        )

    try:
        response = await asyncio.to_thread(_transcribe_sync)
    except (PermanentJobError, RetryableJobError):
        raise
    except Exception as exc:
        log.debug(
            "Deepgram STT request failed error_class=%s error=%s",
            exc.__class__.__name__,
            exc,
        )
        raise RetryableJobError(f"Deepgram request failed: {exc}") from exc

    transcript = _extract_transcript_text(response)
    log.debug(
        "Deepgram STT request succeeded model=%s transcript_chars=%s",
        model_id,
        len(transcript),
    )
    return {
        "provider": "deepgram",
        "model_id": model_id,
        "transcript": transcript,
    }


def _extract_transcript_text(response: object) -> str:
    results = _get_field(response, "results")
    channels = _get_field(results, "channels")
    if isinstance(channels, list) and channels:
        alternatives = _get_field(channels[0], "alternatives")
        if isinstance(alternatives, list) and alternatives:
            transcript = _get_field(alternatives[0], "transcript")
            if isinstance(transcript, str) and transcript.strip():
                return transcript.strip()

    log.debug("Deepgram STT response contained no transcript text")
    raise UnusableTranscriptError("Deepgram response contained no transcript text")


def _get_field(value: object, field_name: str) -> Any:
    if isinstance(value, dict):
        return value.get(field_name)
    return getattr(value, field_name, None)

from __future__ import annotations

import asyncio
import io
import logging
import os
from typing import Any

from bot_libs.queue_processor_errors import (
    PermanentJobError,
    RetryableJobError,
    UnusableTranscriptError,
)
from bot_libs.queue_processors.transcript_validation import reject_prompt_echo

DEFAULT_OPENAI_MODEL_ID = "gpt-4o-transcribe"
DEFAULT_OPENAI_TRANSCRIBE_PROMPT = (
    "For earnings, spendings: amounts in \u00a3 unless noted"
)
MAX_OPENAI_AUDIO_BYTES = 25 * 1024 * 1024
MODEL_ENV_KEYS = (
    "OPENAI_TRANSCRIBE_MODEL",
    "OPENAI_AUDIO_MODEL",
    "OPENAI_MODEL",
    "VOICE_TRANSCRIBE_MODEL",
)
PROMPT_ENV_KEYS = (
    "OPENAI_TRANSCRIBE_PROMPT",
    "VOICE_TRANSCRIBE_PROMPT",
    "TELEGRAM_VOICE_TRANSCRIBE_PROMPT",
)

log = logging.getLogger(__name__)


def resolve_openai_model_id() -> str:
    for env_key in MODEL_ENV_KEYS:
        value = (os.getenv(env_key) or "").strip()
        if value:
            return value
    return DEFAULT_OPENAI_MODEL_ID


def resolve_openai_transcribe_prompt() -> str:
    for env_key in PROMPT_ENV_KEYS:
        value = (os.getenv(env_key) or "").strip()
        if value:
            return value
    return DEFAULT_OPENAI_TRANSCRIBE_PROMPT


def resolve_openai_api_key() -> str:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RetryableJobError("OPENAI_API_KEY missing")
    return api_key


async def transcribe_audio_bytes(
    *,
    audio_bytes: bytes,
    mime_type: str | None,
) -> dict[str, str]:
    if not audio_bytes:
        raise PermanentJobError("audio payload is empty")
    if len(audio_bytes) > MAX_OPENAI_AUDIO_BYTES:
        raise PermanentJobError("audio payload exceeds OpenAI upload limit")

    api_key = resolve_openai_api_key()
    model_id = resolve_openai_model_id()
    prompt = resolve_openai_transcribe_prompt()
    log.debug(
        "OpenAI STT request starting model=%s mime_type=%s audio_bytes=%s prompt_chars=%s",
        model_id,
        mime_type or "audio/ogg",
        len(audio_bytes),
        len(prompt),
    )

    try:
        response = await asyncio.to_thread(
            _transcribe_sync,
            audio_bytes,
            mime_type,
            model_id,
            prompt,
            api_key,
        )
    except (PermanentJobError, RetryableJobError):
        raise
    except Exception as exc:
        raise _job_error_from_openai_error(exc) from exc

    transcript = _extract_transcript_text(response)
    reject_prompt_echo(
        transcript=transcript,
        prompt=prompt,
        provider_name="OpenAI",
    )
    log.debug(
        "OpenAI STT request succeeded model=%s transcript_chars=%s",
        model_id,
        len(transcript),
    )
    return {
        "provider": "OPEN_AI",
        "model_id": model_id,
        "transcript": transcript,
    }


def _transcribe_sync(
    audio_bytes: bytes,
    mime_type: str | None,
    model_id: str,
    prompt: str,
    api_key: str,
) -> object:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RetryableJobError("openai package is not installed") from exc

    audio_file = io.BytesIO(audio_bytes)
    audio_file.name = _file_name_for_audio(audio_bytes, mime_type)
    client = OpenAI(api_key=api_key, timeout=90.0)
    return client.audio.transcriptions.create(
        model=model_id,
        prompt=prompt,
        file=audio_file,
    )


def _file_name_for_audio(audio_bytes: bytes, mime_type: str | None) -> str:
    magic_file_name = _file_name_for_audio_bytes(audio_bytes)
    if magic_file_name is not None:
        return magic_file_name

    return _file_name_for_mime_type(mime_type)


def _file_name_for_audio_bytes(audio_bytes: bytes) -> str | None:
    if audio_bytes.startswith(b"OggS"):
        return "audio.ogg"
    if audio_bytes.startswith(b"RIFF") and audio_bytes[8:12] == b"WAVE":
        return "audio.wav"
    if audio_bytes.startswith(b"fLaC"):
        return "audio.flac"
    if audio_bytes.startswith(b"ID3") or _looks_like_mp3_frame(audio_bytes):
        return "audio.mp3"
    if audio_bytes.startswith(b"\x1a\x45\xdf\xa3"):
        return "audio.webm"
    if len(audio_bytes) >= 12 and audio_bytes[4:8] == b"ftyp":
        return "audio.mp4"
    return None


def _file_name_for_mime_type(mime_type: str | None) -> str:
    match (mime_type or "").split(";", 1)[0].strip().lower():
        case "audio/mpeg" | "audio/mp3":
            return "audio.mp3"
        case "audio/mpga":
            return "audio.mpga"
        case "audio/mp4" | "audio/m4a" | "audio/x-m4a":
            return "audio.m4a"
        case "audio/wav" | "audio/x-wav":
            return "audio.wav"
        case "audio/webm":
            return "audio.webm"
        case "audio/flac":
            return "audio.flac"
        case (
            "audio/ogg"
            | "audio/oga"
            | "audio/opus"
            | "audio/vorbis"
            | "application/ogg"
            | ""
        ):
            return "audio.ogg"
    return "audio.bin"


def _looks_like_mp3_frame(audio_bytes: bytes) -> bool:
    if len(audio_bytes) < 2:
        return False
    return audio_bytes[0] == 0xFF and (audio_bytes[1] & 0xE0) == 0xE0


def _extract_transcript_text(response: object) -> str:
    if isinstance(response, str):
        transcript = response.strip()
    else:
        transcript = _get_field(response, "text")
        if isinstance(transcript, str):
            transcript = transcript.strip()
        else:
            transcript = ""

    if transcript:
        return transcript

    log.debug("OpenAI STT response contained no transcript text")
    raise UnusableTranscriptError("OpenAI response contained no transcript text")


def _job_error_from_openai_error(exc: Exception) -> Exception:
    try:
        import openai
    except ImportError:
        return RetryableJobError(f"OpenAI request failed: {exc}")

    if isinstance(
        exc,
        (
            openai.APIConnectionError,
            openai.APITimeoutError,
            openai.RateLimitError,
            openai.InternalServerError,
        ),
    ):
        log.debug(
            "OpenAI STT request retryable failure error_class=%s error=%s",
            exc.__class__.__name__,
            exc,
        )
        return RetryableJobError(f"OpenAI request failed: {exc}")

    if isinstance(exc, openai.APIStatusError):
        status_code = exc.status_code
        log.debug(
            "OpenAI STT request status failure status_code=%s error_class=%s error=%s",
            status_code,
            exc.__class__.__name__,
            exc,
        )
        message = f"OpenAI API error status={status_code}: {exc}"
        if status_code in {408, 409, 429} or status_code >= 500:
            return RetryableJobError(message)
        return PermanentJobError(message)

    log.debug(
        "OpenAI STT request unexpected failure error_class=%s error=%s",
        exc.__class__.__name__,
        exc,
    )
    return RetryableJobError(f"OpenAI request failed: {exc}")


def _get_field(value: object, field_name: str) -> Any:
    if isinstance(value, dict):
        return value.get(field_name)
    return getattr(value, field_name, None)

from __future__ import annotations

import base64
import logging
import os
from typing import Any

import httpx

from bot_libs.queue_processor_errors import (
    PermanentJobError,
    RetryableJobError,
    UnusableTranscriptError,
)
from bot_libs.queue_processors.transcript_validation import reject_prompt_echo

DEFAULT_MODEL_ID = "gemini-3-flash-preview"
DEFAULT_TRANSCRIBE_PROMPT = """Listen to the audio,

TASK:
Now, then listen again,
really use top-down processing,
allow contextual inference guide you whilst intuition, semantic priming, lexical probability and your sheer intelligence enhance your auditory senses beyond that even of the greater wax moth allowing you to extract what you hear ensuring that you never make anything up!!

FINAL_TASK:
Transcribe to English, output only the final text."""
GEMINI_API_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model_id}:generateContent"
)
MAX_INLINE_AUDIO_BYTES = 19 * 1024 * 1024
MODEL_ENV_KEYS = (
    "MODEL_ID",
    "GEMINI_MODEL",
    "GEMINI_AUDIO_MODEL",
    "VOICE_TRANSCRIBE_MODEL",
)
PROMPT_ENV_KEYS = (
    "VOICE_TRANSCRIBE_PROMPT",
    "TELEGRAM_VOICE_TRANSCRIBE_PROMPT",
    "GEMINI_TRANSCRIBE_PROMPT",
)

log = logging.getLogger(__name__)


def resolve_model_id() -> str:
    for env_key in MODEL_ENV_KEYS:
        value = (os.getenv(env_key) or "").strip()
        if value:
            return value
    return DEFAULT_MODEL_ID


def resolve_transcribe_prompt() -> str:
    for env_key in PROMPT_ENV_KEYS:
        value = (os.getenv(env_key) or "").strip()
        if value:
            return value
    return DEFAULT_TRANSCRIBE_PROMPT


def resolve_api_key() -> str:
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        raise PermanentJobError("GEMINI_API_KEY missing")
    return api_key


async def transcribe_audio_bytes(
    *,
    audio_bytes: bytes,
    mime_type: str | None,
) -> dict[str, str]:
    if not audio_bytes:
        raise PermanentJobError("audio payload is empty")
    if len(audio_bytes) > MAX_INLINE_AUDIO_BYTES:
        raise PermanentJobError("audio payload exceeds inline Gemini limit")

    api_key = resolve_api_key()
    model_id = resolve_model_id()
    prompt = resolve_transcribe_prompt()
    log.debug(
        "Gemini STT request starting model=%s mime_type=%s audio_bytes=%s prompt_chars=%s",
        model_id,
        mime_type or "audio/ogg",
        len(audio_bytes),
        len(prompt),
    )

    request_json = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": prompt},
                    {
                        "inline_data": {
                            "mime_type": mime_type or "audio/ogg",
                            "data": base64.b64encode(audio_bytes).decode("ascii"),
                        }
                    },
                ],
            }
        ]
    }

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    }

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(90.0, connect=20.0),
        ) as client:
            response = await client.post(
                GEMINI_API_URL.format(model_id=model_id),
                headers=headers,
                json=request_json,
            )
    except httpx.HTTPError as exc:
        log.debug(
            "Gemini STT request transport failure model=%s error_class=%s error=%s",
            model_id,
            exc.__class__.__name__,
            exc,
        )
        raise RetryableJobError(f"Gemini request failed: {exc}") from exc

    log.debug(
        "Gemini STT response received model=%s status_code=%s response_bytes=%s",
        model_id,
        response.status_code,
        len(response.content),
    )
    if response.status_code in {408, 429} or response.status_code >= 500:
        log.debug(
            "Gemini STT response classified retryable status_code=%s error=%s",
            response.status_code,
            _google_error_text(response),
        )
        raise RetryableJobError(_google_error_text(response))
    if response.status_code >= 400:
        log.debug(
            "Gemini STT response classified permanent status_code=%s error=%s",
            response.status_code,
            _google_error_text(response),
        )
        raise PermanentJobError(_google_error_text(response))

    try:
        response_json = response.json()
    except ValueError as exc:
        log.debug("Gemini STT response invalid JSON status_code=%s", response.status_code)
        raise RetryableJobError("Gemini returned invalid JSON") from exc

    transcript = _extract_transcript_text(response_json)
    reject_prompt_echo(
        transcript=transcript,
        prompt=prompt,
        provider_name="Gemini",
    )
    log.debug(
        "Gemini STT response extracted transcript model=%s transcript_chars=%s",
        model_id,
        len(transcript),
    )
    return {
        "model_id": model_id,
        "transcript": transcript,
    }


def _google_error_text(response: httpx.Response) -> str:
    prefix = f"Gemini API error status={response.status_code}"
    try:
        response_json = response.json()
    except ValueError:
        text = response.text.strip()
        return f"{prefix}: {text or 'empty response body'}"

    error = response_json.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message.strip():
            return f"{prefix}: {message.strip()}"

    text = response.text.strip()
    return f"{prefix}: {text or 'empty response body'}"


def _extract_transcript_text(response_json: dict[str, Any]) -> str:
    candidates = response_json.get("candidates")
    if isinstance(candidates, list):
        text_parts: list[str] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            content = candidate.get("content")
            if not isinstance(content, dict):
                continue
            parts = content.get("parts")
            if not isinstance(parts, list):
                continue
            for part in parts:
                if not isinstance(part, dict):
                    continue
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    text_parts.append(text.strip())
        transcript = "\n".join(text_parts).strip()
        if transcript:
            return transcript

    prompt_feedback = response_json.get("promptFeedback")
    if isinstance(prompt_feedback, dict):
        block_reason = prompt_feedback.get("blockReason")
        if isinstance(block_reason, str) and block_reason.strip():
            raise PermanentJobError(
                f"Gemini response blocked: {block_reason.strip()}"
            )

    log.debug("Gemini STT response contained no transcript text")
    raise UnusableTranscriptError("Gemini response contained no transcript text")

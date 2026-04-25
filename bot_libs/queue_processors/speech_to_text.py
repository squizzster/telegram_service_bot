from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Awaitable, Callable

from bot_libs.queue_processor_errors import (
    PermanentJobError,
    RetryableJobError,
    UnusableTranscriptError,
)
from bot_libs.queue_processors.audio_validation import (
    transcript_exceeds_speech_rate,
    transcript_word_count,
)
from bot_libs.queue_processors import deepgram_audio, gemini_audio, openai_audio
from bot_libs.queue_processors.transcript_validation import reject_prompt_echo

log = logging.getLogger(__name__)

TranscribeCallable = Callable[
    [bytes, str | None],
    Awaitable[dict[str, str]],
]


@dataclass(frozen=True, slots=True)
class SpeechToTextProvider:
    name: str
    transcribe: TranscribeCallable


def default_speech_to_text_providers() -> tuple[SpeechToTextProvider, ...]:
    return (
        SpeechToTextProvider("google", _transcribe_with_google),
        SpeechToTextProvider("OPEN_AI", _transcribe_with_openai),
        SpeechToTextProvider("deepgram", _transcribe_with_deepgram),
    )


async def transcribe_audio_bytes(
    *,
    audio_bytes: bytes,
    mime_type: str | None,
    duration_seconds: int | None = None,
    providers: tuple[SpeechToTextProvider, ...] | None = None,
) -> dict[str, str]:
    if not audio_bytes:
        raise PermanentJobError("audio payload is empty")

    attempted_providers = providers or default_speech_to_text_providers()
    if not attempted_providers:
        raise RetryableJobError("no speech-to-text providers configured")

    failures: list[str] = []
    retryable_failures = 0
    unusable_transcript_failures = 0
    for provider in attempted_providers:
        log.debug(
            "STT provider attempt provider=%s audio_bytes=%s mime_type=%s duration_seconds=%s",
            provider.name,
            len(audio_bytes),
            mime_type,
            duration_seconds,
        )
        try:
            result = await provider.transcribe(audio_bytes, mime_type)
        except UnusableTranscriptError as exc:
            unusable_transcript_failures += 1
            failures.append(f"{provider.name}: {exc}")
            log.debug(
                "STT provider returned unusable transcript provider=%s error=%s",
                provider.name,
                exc,
            )
            continue
        except (PermanentJobError, RetryableJobError) as exc:
            if isinstance(exc, RetryableJobError):
                retryable_failures += 1
            failures.append(f"{provider.name}: {exc}")
            log.debug(
                "STT provider failed provider=%s error_class=%s retryable=%s error=%s",
                provider.name,
                exc.__class__.__name__,
                isinstance(exc, RetryableJobError),
                exc,
            )
            continue
        except Exception as exc:
            retryable_failures += 1
            failures.append(f"{provider.name}: {exc}")
            log.debug(
                "STT provider raised unexpected error provider=%s error_class=%s error=%s",
                provider.name,
                exc.__class__.__name__,
                exc,
            )
            continue

        transcript = result.get("transcript", "").strip()
        if transcript:
            prompt = result.get("prompt") or result.get("transcribe_prompt")
            if isinstance(prompt, str):
                try:
                    reject_prompt_echo(
                        transcript=transcript,
                        prompt=prompt,
                        provider_name=provider.name,
                    )
                except UnusableTranscriptError as exc:
                    unusable_transcript_failures += 1
                    failures.append(f"{provider.name}: {exc}")
                    log.debug(
                        "STT provider transcript rejected as prompt echo "
                        "provider=%s transcript_chars=%s prompt_chars=%s",
                        provider.name,
                        len(transcript),
                        len(prompt),
                    )
                    continue
            word_count = transcript_word_count(transcript)
            if duration_seconds is not None and transcript_exceeds_speech_rate(
                transcript=transcript,
                duration_seconds=duration_seconds,
            ):
                unusable_transcript_failures += 1
                failures.append(
                    f"{provider.name}: transcript word count "
                    f"{word_count} exceeds duration limit"
                )
                log.debug(
                    "STT provider transcript rejected by speech-rate check "
                    "provider=%s word_count=%s duration_seconds=%s",
                    provider.name,
                    word_count,
                    duration_seconds,
                )
                continue
            log.debug(
                "STT provider accepted provider=%s model=%s transcript_chars=%s word_count=%s",
                result.get("provider", provider.name),
                result.get("model_id"),
                len(transcript),
                word_count,
            )
            return {
                **result,
                "provider": result.get("provider", provider.name),
                "transcript": transcript,
            }
        unusable_transcript_failures += 1
        failures.append(f"{provider.name}: empty transcript")
        log.debug("STT provider returned empty transcript provider=%s", provider.name)

    if (
        unusable_transcript_failures == len(attempted_providers)
        and retryable_failures == 0
    ):
        log.debug(
            "STT aggregate permanent unusable result providers=%s failures=%s",
            len(attempted_providers),
            "; ".join(failures),
        )
        raise UnusableTranscriptError(
            "all speech-to-text providers returned unusable transcripts: "
            + "; ".join(failures)
        )

    log.debug(
        "STT aggregate retryable failure providers=%s retryable_failures=%s "
        "unusable_transcript_failures=%s failures=%s",
        len(attempted_providers),
        retryable_failures,
        unusable_transcript_failures,
        "; ".join(failures),
    )
    raise RetryableJobError(
        "all speech-to-text providers failed: " + "; ".join(failures)
    )


async def _transcribe_with_deepgram(
    audio_bytes: bytes,
    mime_type: str | None,
) -> dict[str, str]:
    return await deepgram_audio.transcribe_audio_bytes(
        audio_bytes=audio_bytes,
        mime_type=mime_type,
    )


async def _transcribe_with_openai(
    audio_bytes: bytes,
    mime_type: str | None,
) -> dict[str, str]:
    return await openai_audio.transcribe_audio_bytes(
        audio_bytes=audio_bytes,
        mime_type=mime_type,
    )


async def _transcribe_with_google(
    audio_bytes: bytes,
    mime_type: str | None,
) -> dict[str, str]:
    result = await gemini_audio.transcribe_audio_bytes(
        audio_bytes=audio_bytes,
        mime_type=mime_type,
    )
    return {
        **result,
        "provider": "google",
    }

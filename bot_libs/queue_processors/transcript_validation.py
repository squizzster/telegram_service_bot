from __future__ import annotations

from bot_libs.queue_processor_errors import UnusableTranscriptError


def reject_prompt_echo(
    *,
    transcript: str,
    prompt: str | None,
    provider_name: str,
) -> None:
    if not prompt or not prompt.strip():
        return
    if _normalize_text(transcript) != _normalize_text(prompt):
        return
    raise UnusableTranscriptError(
        f"{provider_name} response matched the transcription prompt"
    )


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().split())

from __future__ import annotations

import json
import math
import subprocess
import tempfile

from bot_libs.queue_processor_errors import PermanentJobError, RetryableJobError

MIN_AUDIO_DURATION_SECONDS = 3
MAX_AUDIO_DURATION_SECONDS = 60
MAX_SPEECH_WORDS_PER_MINUTE = 300


def probe_audio_duration_seconds(
    audio_bytes: bytes,
    *,
    suffix: str = ".audio",
) -> int:
    if not audio_bytes:
        raise PermanentJobError("audio payload is empty")

    with tempfile.NamedTemporaryFile(suffix=suffix) as handle:
        handle.write(audio_bytes)
        handle.flush()

        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-print_format",
                    "json",
                    "-show_entries",
                    "format=duration:stream=codec_type",
                    handle.name,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=False,
            )
        except OSError as exc:
            raise RetryableJobError("ffprobe is not available") from exc

    if result.returncode != 0:
        raise PermanentJobError("audio payload is corrupt or unreadable")

    try:
        data = json.loads(result.stdout)
        streams = data.get("streams", [])
        has_audio = any(s.get("codec_type") == "audio" for s in streams)
        if not has_audio:
            raise ValueError("no audio stream")

        duration_text = data.get("format", {}).get("duration")
        if duration_text is None:
            raise ValueError("missing duration")

        duration = float(duration_text)
        if not math.isfinite(duration) or duration < 0:
            raise ValueError("invalid duration")
    except Exception as exc:
        raise PermanentJobError("audio payload has no valid duration") from exc

    return int(round(duration))


def validate_audio_duration_limit(duration_seconds: int) -> None:
    if duration_seconds < MIN_AUDIO_DURATION_SECONDS:
        raise PermanentJobError(
            f"audio duration {duration_seconds}s is below {MIN_AUDIO_DURATION_SECONDS}s minimum"
        )
    if duration_seconds >= MAX_AUDIO_DURATION_SECONDS:
        raise PermanentJobError(
            f"audio duration {duration_seconds}s exceeds {MAX_AUDIO_DURATION_SECONDS}s limit"
        )


def transcript_word_count(transcript: str) -> int:
    return len([part for part in transcript.strip().split(" ") if part])


def max_words_for_duration(duration_seconds: int) -> int:
    return math.ceil(duration_seconds * MAX_SPEECH_WORDS_PER_MINUTE / 60)


def transcript_exceeds_speech_rate(
    *,
    transcript: str,
    duration_seconds: int,
) -> bool:
    return transcript_word_count(transcript) > max_words_for_duration(duration_seconds)

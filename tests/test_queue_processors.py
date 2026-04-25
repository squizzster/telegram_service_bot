from __future__ import annotations

import os
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from telegram.error import BadRequest, TelegramError, TimedOut

from bot_libs.queue_processor_errors import (
    OriginalMessageUnavailableError,
    PermanentJobError,
    RetryableJobError,
    UnusableTranscriptError,
)
from bot_libs.queue_processing_context import QueueProcessingContext
from bot_libs.queue_processors.audio_validation import (
    MIN_AUDIO_DURATION_SECONDS,
    max_words_for_duration,
    transcript_exceeds_speech_rate,
    transcript_word_count,
    validate_audio_duration_limit,
)
from bot_libs.queue_processors.dispatch import DispatchingQueueJobProcessor
from bot_libs.queue_processors.file_common import (
    get_telegram_file_info,
    get_telegram_file_and_info,
)
from bot_libs.queue_processors.gemini_audio import (
    DEFAULT_MODEL_ID,
    DEFAULT_TRANSCRIBE_PROMPT,
    resolve_model_id,
    resolve_transcribe_prompt,
    transcribe_audio_bytes,
)
from bot_libs.queue_processors.deepgram_audio import (
    DEFAULT_DEEPGRAM_LANGUAGE,
    DEFAULT_DEEPGRAM_MODEL_ID,
    resolve_deepgram_language,
    resolve_deepgram_model_id,
    transcribe_audio_bytes as transcribe_deepgram_audio_bytes,
)
from bot_libs.queue_processors.openai_audio import (
    DEFAULT_OPENAI_MODEL_ID,
    DEFAULT_OPENAI_TRANSCRIBE_PROMPT,
    resolve_openai_model_id,
    resolve_openai_transcribe_prompt,
    transcribe_audio_bytes as transcribe_openai_audio_bytes,
)
from bot_libs.queue_processors.speech_to_text import (
    SpeechToTextProvider,
    default_speech_to_text_providers,
    transcribe_audio_bytes as transcribe_speech_to_text_audio_bytes,
)
from bot_libs.queue_processors.voice import (
    _record_outbound_message,
    process as process_voice,
)
from bot_libs.stage_names import STAGE_SENDING_RESPONSE


def telegram_sent_message(
    message_id: int,
    *,
    reply_to_message_id: int | None = None,
) -> SimpleNamespace:
    reply_to_message = (
        SimpleNamespace(message_id=reply_to_message_id)
        if reply_to_message_id is not None
        else None
    )
    return SimpleNamespace(message_id=message_id, reply_to_message=reply_to_message)


class QueueProcessorDispatchTests(unittest.IsolatedAsyncioTestCase):
    def make_bot(self) -> SimpleNamespace:
        return SimpleNamespace(get_file=AsyncMock())

    async def test_dispatches_text_rows(self) -> None:
        processor = DispatchingQueueJobProcessor()
        bot = self.make_bot()

        result = await processor.process(
            bot,
            {
                "content_type": "text",
                "payload_json": '{"content":{"type":"text"},"extra":{}}',
                "text": "hello",
            },
        )

        self.assertEqual(
            result,
            {
                "outcome": "processed",
                "content_type": "text",
                "processor": "text",
                "text_length": 5,
                "has_text": True,
            },
        )
        bot.get_file.assert_not_awaited()

    async def test_invalid_payload_json_is_permanent(self) -> None:
        processor = DispatchingQueueJobProcessor()

        with self.assertRaisesRegex(PermanentJobError, "payload_json is invalid JSON"):
            await processor.process(
                self.make_bot(),
                {
                    "content_type": "text",
                    "payload_json": "{not-json",
                    "text": "hello",
                },
            )

    async def test_non_object_payload_json_is_permanent(self) -> None:
        processor = DispatchingQueueJobProcessor()

        with self.assertRaisesRegex(
            PermanentJobError,
            "payload_json must decode to an object",
        ):
            await processor.process(
                self.make_bot(),
                {
                    "content_type": "text",
                    "payload_json": '["not","an","object"]',
                    "text": "hello",
                },
            )

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_processor_uses_row_file_data_and_payload_extra(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 17
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "gemini-3-flash-preview",
            "transcript": "hello there",
        }
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(901, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(),
        )
        processor = DispatchingQueueJobProcessor()

        result = await processor.process(
            bot,
            {
                "content_type": "voice",
                "payload_json": (
                    '{"content":{"type":"voice"},'
                    '"extra":{"duration_seconds":17}}'
                ),
                "file_id": "voice-file-id",
                "file_unique_id": "voice-unique-id",
                "mime_type": "audio/ogg",
                "file_size": 321,
                "chat_id": 123,
                "message_id": 456,
                "chat_type": "supergroup",
            },
        )

        self.assertEqual(
            result,
            {
                "outcome": "processed",
                "content_type": "voice",
                "processor": "voice",
                "file_id": "voice-file-id",
                "file_unique_id": "voice-unique-id",
                "file_name": None,
                "mime_type": "audio/ogg",
                "file_size": 321,
                "telegram_file_path": "voice/file_1.ogg",
                "duration_seconds": 17,
                "audio_duration_seconds": 17,
                "transcript_length": 11,
                "transcript_preview": "hello there",
                "processing_text": "hello there",
                "transcript_provider": "deepgram",
                "transcript_model": "gemini-3-flash-preview",
                "transcript_message_ids": [901],
            },
        )
        bot.get_file.assert_awaited_once_with("voice-file-id")

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_audio_processor_uses_voice_transcription_pipeline(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 12
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "nova-3",
            "transcript": "audio transcript",
        }
        tg_file = SimpleNamespace(
            file_path="audio/file_1.mp3",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"mp3-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(902, reply_to_message_id=457)
            ),
            set_message_reaction=AsyncMock(),
        )
        processor = DispatchingQueueJobProcessor()

        result = await processor.process(
            bot,
            {
                "content_type": "audio",
                "payload_json": (
                    '{"content":{"type":"audio"},'
                    '"extra":{"duration_seconds":12,"performer":"A","title":"B"}}'
                ),
                "file_id": "audio-file-id",
                "file_unique_id": "audio-unique-id",
                "file_name": "clip.mp3",
                "mime_type": "audio/mpeg",
                "file_size": 654,
                "chat_id": 123,
                "message_id": 457,
                "chat_type": "supergroup",
            },
        )

        self.assertEqual(result["outcome"], "processed")
        self.assertEqual(result["content_type"], "audio")
        self.assertEqual(result["processor"], "audio")
        self.assertEqual(result["telegram_file_path"], "audio/file_1.mp3")
        self.assertEqual(result["duration_seconds"], 12)
        self.assertEqual(result["audio_duration_seconds"], 12)
        self.assertEqual(result["processing_text"], "audio transcript")
        self.assertEqual(result["transcript_provider"], "deepgram")
        self.assertEqual(result["transcript_model"], "nova-3")
        self.assertEqual(result["transcript_message_ids"], [902])
        bot.get_file.assert_awaited_once_with("audio-file-id")
        probe_duration.assert_called_once_with(b"mp3-bytes", suffix=".mpeg")
        transcribe.assert_awaited_once_with(
            audio_bytes=b"mp3-bytes",
            mime_type="audio/mpeg",
            duration_seconds=12,
        )
        bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text="Transcript:\n\naudio transcript",
            reply_to_message_id=457,
            allow_sending_without_reply=False,
        )


class QueueProcessorFileCommonTests(unittest.IsolatedAsyncioTestCase):
    def make_bot(self) -> SimpleNamespace:
        return SimpleNamespace(get_file=AsyncMock())

    async def test_missing_file_id_is_permanent(self) -> None:
        with self.assertRaisesRegex(PermanentJobError, "file_id missing"):
            await get_telegram_file_info(self.make_bot(), {})

    async def test_telegram_error_is_retryable(self) -> None:
        bot = self.make_bot()
        bot.get_file.side_effect = TelegramError("temporary telegram failure")

        with self.assertRaisesRegex(RetryableJobError, "temporary telegram failure"):
            await get_telegram_file_info(bot, {"file_id": "voice-file-id"})

    async def test_bad_request_file_error_is_permanent(self) -> None:
        bot = self.make_bot()
        bot.get_file.side_effect = BadRequest("file not found")

        with self.assertRaisesRegex(PermanentJobError, "file not found"):
            await get_telegram_file_info(bot, {"file_id": "voice-file-id"})

    async def test_file_path_is_sanitized_when_ptb_returns_full_url(self) -> None:
        bot = self.make_bot()
        bot.get_file.return_value = SimpleNamespace(
            file_path=(
                "https://api.telegram.org/file/bot123456:ABCDEF/"
                "voice/file_0.oga"
            )
        )

        _, file_info = await get_telegram_file_and_info(
            bot,
            {"file_id": "voice-file-id"},
        )

        self.assertEqual(file_info["telegram_file_path"], "voice/file_0.oga")


class GeminiAudioTests(unittest.IsolatedAsyncioTestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_model_and_prompt_defaults(self) -> None:
        self.assertEqual(resolve_model_id(), DEFAULT_MODEL_ID)
        self.assertEqual(resolve_transcribe_prompt(), DEFAULT_TRANSCRIBE_PROMPT)
        self.assertIn("Listen to the audio,", DEFAULT_TRANSCRIBE_PROMPT)
        self.assertIn(
            "Transcribe to English, output only the final text.",
            DEFAULT_TRANSCRIBE_PROMPT,
        )

    @patch.dict(
        os.environ,
        {
            "MODEL_ID": "gemini-3-flash-preview",
            "VOICE_TRANSCRIBE_PROMPT": "Return only transcript text.",
        },
        clear=True,
    )
    def test_model_and_prompt_use_env_overrides(self) -> None:
        self.assertEqual(resolve_model_id(), "gemini-3-flash-preview")
        self.assertEqual(resolve_transcribe_prompt(), "Return only transcript text.")

    @patch("bot_libs.queue_processors.gemini_audio.httpx.AsyncClient")
    @patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}, clear=True)
    async def test_transcribe_audio_bytes_uses_google_rest_api(self, client_cls: object) -> None:
        response = httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": "hello from transcript",
                                }
                            ]
                        }
                    }
                ]
            },
        )
        client = AsyncMock()
        client.post.return_value = response
        client_cm = AsyncMock()
        client_cm.__aenter__.return_value = client
        client_cm.__aexit__.return_value = False
        client_cls.return_value = client_cm

        result = await transcribe_audio_bytes(
            audio_bytes=b"abc",
            mime_type="audio/ogg",
        )

        self.assertEqual(
            result,
            {
                "model_id": DEFAULT_MODEL_ID,
                "transcript": "hello from transcript",
            },
        )
        request_json = client.post.await_args.kwargs["json"]
        self.assertEqual(
            request_json["contents"][0]["parts"][0]["text"],
            DEFAULT_TRANSCRIBE_PROMPT,
        )
        self.assertEqual(
            client.post.await_args.kwargs["headers"]["x-goog-api-key"],
            "test-key",
        )

    @patch("bot_libs.queue_processors.gemini_audio.httpx.AsyncClient")
    @patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}, clear=True)
    async def test_google_prompt_echo_is_unusable(self, client_cls: object) -> None:
        response = httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": DEFAULT_TRANSCRIBE_PROMPT,
                                }
                            ]
                        }
                    }
                ]
            },
        )
        client = AsyncMock()
        client.post.return_value = response
        client_cm = AsyncMock()
        client_cm.__aenter__.return_value = client
        client_cm.__aexit__.return_value = False
        client_cls.return_value = client_cm

        with self.assertRaisesRegex(
            UnusableTranscriptError,
            "matched the transcription prompt",
        ):
            await transcribe_audio_bytes(
                audio_bytes=b"abc",
                mime_type="audio/ogg",
            )


class AudioValidationTests(unittest.TestCase):
    def test_validate_audio_duration_rejects_under_three_seconds(self) -> None:
        with self.assertRaisesRegex(PermanentJobError, "below 3s minimum"):
            validate_audio_duration_limit(MIN_AUDIO_DURATION_SECONDS - 1)

    def test_transcript_word_count_uses_spaces(self) -> None:
        self.assertEqual(transcript_word_count("one two  three"), 3)

    def test_max_words_for_duration_uses_300_words_per_minute(self) -> None:
        self.assertEqual(max_words_for_duration(60), 300)
        self.assertEqual(max_words_for_duration(1), 5)

    def test_transcript_exceeds_speech_rate(self) -> None:
        self.assertFalse(
            transcript_exceeds_speech_rate(
                transcript="one two three four five",
                duration_seconds=1,
            )
        )
        self.assertTrue(
            transcript_exceeds_speech_rate(
                transcript="one two three four five six",
                duration_seconds=1,
            )
        )


class DeepgramAudioTests(unittest.IsolatedAsyncioTestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_deepgram_defaults(self) -> None:
        self.assertEqual(resolve_deepgram_model_id(), DEFAULT_DEEPGRAM_MODEL_ID)
        self.assertEqual(resolve_deepgram_language(), DEFAULT_DEEPGRAM_LANGUAGE)

    @patch.dict(
        os.environ,
        {
            "DEEPGRAM_MODEL": "nova-3",
            "DEEPGRAM_LANGUAGE": "en-US",
        },
        clear=True,
    )
    def test_deepgram_uses_env_overrides(self) -> None:
        self.assertEqual(resolve_deepgram_model_id(), "nova-3")
        self.assertEqual(resolve_deepgram_language(), "en-US")

    @patch("bot_libs.queue_processors.deepgram_audio.asyncio.to_thread")
    @patch.dict(os.environ, {"DEEPGRAM_API_KEY": "deepgram-key"}, clear=True)
    async def test_deepgram_transcribe_audio_bytes_uses_sdk(self, to_thread: object) -> None:
        to_thread.return_value = SimpleNamespace(
            results=SimpleNamespace(
                channels=[
                    SimpleNamespace(
                        alternatives=[
                            SimpleNamespace(transcript="hello from deepgram")
                        ]
                    )
                ]
            )
        )

        result = await transcribe_deepgram_audio_bytes(
            audio_bytes=b"abc",
            mime_type="audio/ogg",
        )

        self.assertEqual(
            result,
            {
                "provider": "deepgram",
                "model_id": DEFAULT_DEEPGRAM_MODEL_ID,
                "transcript": "hello from deepgram",
            },
        )
        to_thread.assert_awaited_once()


class OpenAIAudioTests(unittest.IsolatedAsyncioTestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_openai_defaults(self) -> None:
        self.assertEqual(resolve_openai_model_id(), DEFAULT_OPENAI_MODEL_ID)
        self.assertEqual(
            resolve_openai_transcribe_prompt(),
            DEFAULT_OPENAI_TRANSCRIBE_PROMPT,
        )

    @patch.dict(
        os.environ,
        {
            "OPENAI_TRANSCRIBE_MODEL": "custom-transcribe-model",
            "OPENAI_TRANSCRIBE_PROMPT": "Use ledger vocabulary.",
        },
        clear=True,
    )
    def test_openai_uses_env_overrides(self) -> None:
        self.assertEqual(resolve_openai_model_id(), "custom-transcribe-model")
        self.assertEqual(
            resolve_openai_transcribe_prompt(),
            "Use ledger vocabulary.",
        )

    @patch.dict(os.environ, {}, clear=True)
    async def test_openai_missing_api_key_is_retryable(self) -> None:
        with self.assertRaisesRegex(RetryableJobError, "OPENAI_API_KEY missing"):
            await transcribe_openai_audio_bytes(
                audio_bytes=b"abc",
                mime_type="audio/ogg",
            )

    @patch("bot_libs.queue_processors.openai_audio.asyncio.to_thread")
    @patch("openai.OpenAI")
    @patch.dict(os.environ, {"OPENAI_API_KEY": "openai-key"}, clear=True)
    async def test_openai_transcribe_audio_bytes_uses_sdk(
        self,
        openai_cls: object,
        to_thread: object,
    ) -> None:
        async def run_sync(func: object, *args: object) -> object:
            return func(*args)

        to_thread.side_effect = run_sync
        client = openai_cls.return_value
        client.audio.transcriptions.create.return_value = SimpleNamespace(
            text="hello from openai",
        )

        result = await transcribe_openai_audio_bytes(
            audio_bytes=b"abc",
            mime_type="audio/ogg",
        )

        self.assertEqual(
            result,
            {
                "provider": "OPEN_AI",
                "model_id": DEFAULT_OPENAI_MODEL_ID,
                "transcript": "hello from openai",
            },
        )
        openai_cls.assert_called_once_with(api_key="openai-key", timeout=90.0)
        create_kwargs = client.audio.transcriptions.create.call_args.kwargs
        self.assertEqual(create_kwargs["model"], DEFAULT_OPENAI_MODEL_ID)
        self.assertEqual(create_kwargs["prompt"], DEFAULT_OPENAI_TRANSCRIBE_PROMPT)
        self.assertEqual(create_kwargs["file"].name, "audio.ogg")
        self.assertEqual(create_kwargs["file"].read(), b"abc")

    @patch("bot_libs.queue_processors.openai_audio.asyncio.to_thread")
    @patch("openai.OpenAI")
    @patch.dict(os.environ, {"OPENAI_API_KEY": "openai-key"}, clear=True)
    async def test_openai_transcribe_audio_bytes_names_vorbis_as_ogg(
        self,
        openai_cls: object,
        to_thread: object,
    ) -> None:
        async def run_sync(func: object, *args: object) -> object:
            return func(*args)

        to_thread.side_effect = run_sync
        client = openai_cls.return_value
        client.audio.transcriptions.create.return_value = SimpleNamespace(
            text="hello from openai",
        )

        await transcribe_openai_audio_bytes(
            audio_bytes=b"OggS\x00vorbis-bytes",
            mime_type="audio/vorbis",
        )

        create_kwargs = client.audio.transcriptions.create.call_args.kwargs
        self.assertEqual(create_kwargs["file"].name, "audio.ogg")

    @patch("bot_libs.queue_processors.openai_audio.asyncio.to_thread")
    @patch("openai.OpenAI")
    @patch.dict(os.environ, {"OPENAI_API_KEY": "openai-key"}, clear=True)
    async def test_openai_transcribe_audio_bytes_uses_real_file_type_before_mime(
        self,
        openai_cls: object,
        to_thread: object,
    ) -> None:
        async def run_sync(func: object, *args: object) -> object:
            return func(*args)

        to_thread.side_effect = run_sync
        client = openai_cls.return_value
        client.audio.transcriptions.create.return_value = SimpleNamespace(
            text="hello from openai",
        )

        await transcribe_openai_audio_bytes(
            audio_bytes=b"RIFF\x24\x00\x00\x00WAVEfmt ",
            mime_type="audio/ogg",
        )

        create_kwargs = client.audio.transcriptions.create.call_args.kwargs
        self.assertEqual(create_kwargs["file"].name, "audio.wav")

    @patch("bot_libs.queue_processors.openai_audio.asyncio.to_thread")
    @patch("openai.OpenAI")
    @patch.dict(os.environ, {"OPENAI_API_KEY": "openai-key"}, clear=True)
    async def test_openai_transcribe_audio_bytes_falls_back_to_mime_when_unknown(
        self,
        openai_cls: object,
        to_thread: object,
    ) -> None:
        async def run_sync(func: object, *args: object) -> object:
            return func(*args)

        to_thread.side_effect = run_sync
        client = openai_cls.return_value
        client.audio.transcriptions.create.return_value = SimpleNamespace(
            text="hello from openai",
        )

        await transcribe_openai_audio_bytes(
            audio_bytes=b"unknown-audio-bytes",
            mime_type="audio/wav",
        )

        create_kwargs = client.audio.transcriptions.create.call_args.kwargs
        self.assertEqual(create_kwargs["file"].name, "audio.wav")

    @patch("bot_libs.queue_processors.openai_audio.asyncio.to_thread")
    @patch("openai.OpenAI")
    @patch.dict(os.environ, {"OPENAI_API_KEY": "openai-key"}, clear=True)
    async def test_openai_empty_transcript_is_unusable(
        self,
        openai_cls: object,
        to_thread: object,
    ) -> None:
        async def run_sync(func: object, *args: object) -> object:
            return func(*args)

        to_thread.side_effect = run_sync
        openai_cls.return_value.audio.transcriptions.create.return_value = (
            SimpleNamespace(text=" ")
        )

        with self.assertRaisesRegex(
            UnusableTranscriptError,
            "OpenAI response contained no transcript text",
        ):
            await transcribe_openai_audio_bytes(
                audio_bytes=b"abc",
                mime_type="audio/ogg",
            )

    @patch("bot_libs.queue_processors.openai_audio.asyncio.to_thread")
    @patch("openai.OpenAI")
    @patch.dict(os.environ, {"OPENAI_API_KEY": "openai-key"}, clear=True)
    async def test_openai_prompt_echo_is_unusable(
        self,
        openai_cls: object,
        to_thread: object,
    ) -> None:
        async def run_sync(func: object, *args: object) -> object:
            return func(*args)

        to_thread.side_effect = run_sync
        openai_cls.return_value.audio.transcriptions.create.return_value = (
            SimpleNamespace(text=DEFAULT_OPENAI_TRANSCRIBE_PROMPT)
        )

        with self.assertRaisesRegex(
            UnusableTranscriptError,
            "matched the transcription prompt",
        ):
            await transcribe_openai_audio_bytes(
                audio_bytes=b"abc",
                mime_type="audio/ogg",
            )


class SpeechToTextProviderTests(unittest.IsolatedAsyncioTestCase):
    def test_default_provider_order_starts_with_google(self) -> None:
        self.assertEqual(
            [provider.name for provider in default_speech_to_text_providers()],
            ["google", "OPEN_AI", "deepgram"],
        )

    async def test_uses_deepgram_first(self) -> None:
        deepgram = AsyncMock(
            return_value={
                "provider": "deepgram",
                "model_id": "nova-3",
                "transcript": "deepgram transcript",
            }
        )
        google = AsyncMock(
            return_value={
                "provider": "google",
                "model_id": "gemini-3-flash-preview",
                "transcript": "google transcript",
            }
        )

        result = await transcribe_speech_to_text_audio_bytes(
            audio_bytes=b"abc",
            mime_type="audio/ogg",
            providers=(
                SpeechToTextProvider("deepgram", deepgram),
                SpeechToTextProvider("google", google),
            ),
        )

        self.assertEqual(result["provider"], "deepgram")
        self.assertEqual(result["transcript"], "deepgram transcript")
        deepgram.assert_awaited_once_with(b"abc", "audio/ogg")
        google.assert_not_awaited()

    async def test_falls_back_to_google_when_deepgram_fails(self) -> None:
        deepgram = AsyncMock(side_effect=RetryableJobError("deepgram unavailable"))
        google = AsyncMock(
            return_value={
                "provider": "google",
                "model_id": "gemini-3-flash-preview",
                "transcript": "google transcript",
            }
        )

        result = await transcribe_speech_to_text_audio_bytes(
            audio_bytes=b"abc",
            mime_type="audio/ogg",
            providers=(
                SpeechToTextProvider("deepgram", deepgram),
                SpeechToTextProvider("google", google),
            ),
        )

        self.assertEqual(result["provider"], "google")
        self.assertEqual(result["transcript"], "google transcript")
        deepgram.assert_awaited_once_with(b"abc", "audio/ogg")
        google.assert_awaited_once_with(b"abc", "audio/ogg")

    async def test_prompt_echo_provider_result_falls_back_to_next_provider(self) -> None:
        prompt = "Transcribe to English, output only the final text."
        google = AsyncMock(
            return_value={
                "provider": "google",
                "model_id": "gemini-3-flash-preview",
                "transcript": prompt,
                "prompt": prompt,
            }
        )
        openai = AsyncMock(
            return_value={
                "provider": "OPEN_AI",
                "model_id": "gpt-4o-transcribe",
                "transcript": "openai transcript",
            }
        )

        result = await transcribe_speech_to_text_audio_bytes(
            audio_bytes=b"abc",
            mime_type="audio/ogg",
            providers=(
                SpeechToTextProvider("google", google),
                SpeechToTextProvider("OPEN_AI", openai),
            ),
        )

        self.assertEqual(result["provider"], "OPEN_AI")
        self.assertEqual(result["transcript"], "openai transcript")
        google.assert_awaited_once_with(b"abc", "audio/ogg")
        openai.assert_awaited_once_with(b"abc", "audio/ogg")

    async def test_falls_back_to_google_when_deepgram_transcript_is_too_dense(self) -> None:
        deepgram = AsyncMock(
            return_value={
                "provider": "deepgram",
                "model_id": "nova-3",
                "transcript": "one two three four five six",
            }
        )
        google = AsyncMock(
            return_value={
                "provider": "google",
                "model_id": "gemini-3-flash-preview",
                "transcript": "one two three",
            }
        )

        result = await transcribe_speech_to_text_audio_bytes(
            audio_bytes=b"abc",
            mime_type="audio/ogg",
            duration_seconds=1,
            providers=(
                SpeechToTextProvider("deepgram", deepgram),
                SpeechToTextProvider("google", google),
            ),
        )

        self.assertEqual(result["provider"], "google")
        self.assertEqual(result["transcript"], "one two three")
        deepgram.assert_awaited_once_with(b"abc", "audio/ogg")
        google.assert_awaited_once_with(b"abc", "audio/ogg")

    async def test_all_too_dense_provider_results_are_permanent(self) -> None:
        deepgram = AsyncMock(
            return_value={
                "provider": "deepgram",
                "model_id": "nova-3",
                "transcript": "one two three four five six",
            }
        )
        google = AsyncMock(
            return_value={
                "provider": "google",
                "model_id": "gemini-3-flash-preview",
                "transcript": "one two three four five six seven",
            }
        )

        with self.assertRaisesRegex(
            PermanentJobError,
            "unusable transcripts",
        ):
            await transcribe_speech_to_text_audio_bytes(
                audio_bytes=b"abc",
                mime_type="audio/ogg",
                duration_seconds=1,
                providers=(
                    SpeechToTextProvider("deepgram", deepgram),
                    SpeechToTextProvider("google", google),
                ),
            )

        deepgram.assert_awaited_once_with(b"abc", "audio/ogg")
        google.assert_awaited_once_with(b"abc", "audio/ogg")

    async def test_no_text_and_too_dense_provider_results_are_permanent(self) -> None:
        deepgram = AsyncMock(
            return_value={
                "provider": "deepgram",
                "model_id": "nova-3",
                "transcript": "",
            }
        )
        google = AsyncMock(
            return_value={
                "provider": "google",
                "model_id": "gemini-3-flash-preview",
                "transcript": "one two three four five six seven",
            }
        )

        with self.assertRaisesRegex(
            PermanentJobError,
            "unusable transcripts",
        ):
            await transcribe_speech_to_text_audio_bytes(
                audio_bytes=b"abc",
                mime_type="audio/ogg",
                duration_seconds=1,
                providers=(
                    SpeechToTextProvider("deepgram", deepgram),
                    SpeechToTextProvider("google", google),
                ),
            )

        deepgram.assert_awaited_once_with(b"abc", "audio/ogg")
        google.assert_awaited_once_with(b"abc", "audio/ogg")

    async def test_all_provider_failures_are_retryable(self) -> None:
        deepgram = AsyncMock(side_effect=RetryableJobError("deepgram unavailable"))
        google = AsyncMock(side_effect=PermanentJobError("google rejected"))

        with self.assertRaisesRegex(
            RetryableJobError,
            "all speech-to-text providers failed",
        ):
            await transcribe_speech_to_text_audio_bytes(
                audio_bytes=b"abc",
                mime_type="audio/ogg",
                providers=(
                    SpeechToTextProvider("deepgram", deepgram),
                    SpeechToTextProvider("google", google),
                ),
            )

        deepgram.assert_awaited_once_with(b"abc", "audio/ogg")
        google.assert_awaited_once_with(b"abc", "audio/ogg")


class VoiceProcessorTests(unittest.IsolatedAsyncioTestCase):
    def test_record_outbound_message_ignores_malformed_existing_messages(self) -> None:
        result = _record_outbound_message(
            {
                "messages": [
                    {"index": "2", "message_id": "9002", "chars": "12"},
                    {"index": "bad", "message_id": 9003},
                    {"message_id": 9004},
                    {"index": 5, "message_id": "bad"},
                    "not-a-message",
                ],
                "provider": "stored",
            },
            index=1,
            message_id=9001,
            chars=20,
        )

        self.assertEqual(
            result,
            {
                "messages": [
                    {"index": 1, "message_id": 9001, "chars": 20},
                    {"index": 2, "message_id": 9002, "chars": 12},
                ],
                "provider": "stored",
            },
        )

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_transcribes_and_replies(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 7
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "gemini-3-flash-preview",
            "transcript": "hello world",
        }
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9001, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(),
        )

        result = await process_voice(
            bot,
            {
                "chat_id": 123,
                "message_id": 456,
                "message_thread_id": 789,
                "chat_type": "supergroup",
                "file_id": "voice-file-id",
                "file_unique_id": "voice-unique-id",
                "mime_type": "audio/ogg",
                "file_size": 321,
            },
            {"extra": {"duration_seconds": 7}},
        )

        self.assertEqual(result["processor"], "voice")
        self.assertEqual(result["duration_seconds"], 7)
        self.assertEqual(result["audio_duration_seconds"], 7)
        self.assertEqual(result["transcript_length"], 11)
        self.assertEqual(result["processing_text"], "hello world")
        self.assertEqual(result["transcript_provider"], "deepgram")
        self.assertEqual(result["transcript_message_ids"], [9001])
        bot.get_file.assert_awaited_once_with("voice-file-id")
        tg_file.download_as_bytearray.assert_awaited_once()
        probe_duration.assert_called_once_with(b"ogg-bytes", suffix=".ogg")
        transcribe.assert_awaited_once_with(
            audio_bytes=b"ogg-bytes",
            mime_type="audio/ogg",
            duration_seconds=7,
        )
        bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text="Transcript:\n\nhello world",
            message_thread_id=789,
            reply_to_message_id=456,
            allow_sending_without_reply=False,
        )
        self.assertEqual(bot.set_message_reaction.await_count, 2)

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_stops_before_stt_when_reaction_says_source_is_gone(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 7
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "nova-3",
            "transcript": "hello world",
        }
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9001, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(
                side_effect=BadRequest("Message to react not found")
            ),
        )

        with self.assertRaisesRegex(
            OriginalMessageUnavailableError,
            "Message to react not found",
        ):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {"duration_seconds": 7}},
            )

        transcribe.assert_not_awaited()
        bot.get_file.assert_not_awaited()
        bot.send_message.assert_not_awaited()

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_continues_when_reaction_times_out(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 7
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "nova-3",
            "transcript": "hello world",
        }
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9001, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(side_effect=TimedOut("timed out")),
        )

        result = await process_voice(
            bot,
            {
                "chat_id": 123,
                "message_id": 456,
                "chat_type": "supergroup",
                "file_id": "voice-file-id",
                "mime_type": "audio/ogg",
            },
            {"extra": {"duration_seconds": 7}},
        )

        self.assertEqual(result["processing_text"], "hello world")
        transcribe.assert_awaited_once()
        bot.get_file.assert_awaited_once()
        bot.send_message.assert_awaited_once()

    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_download_bad_request_is_permanent(
        self,
        probe_duration: object,
    ) -> None:
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(side_effect=BadRequest("file not found")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(PermanentJobError, "file not found"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {"duration_seconds": 7}},
            )

        probe_duration.assert_not_called()
        bot.send_message.assert_not_awaited()

    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_download_timeout_is_retryable(
        self,
        probe_duration: object,
    ) -> None:
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(side_effect=TimedOut("timed out")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(RetryableJobError, "timed out"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {"duration_seconds": 7}},
            )

        probe_duration.assert_not_called()
        bot.send_message.assert_not_awaited()

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_fails_if_transcript_reply_target_is_gone(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 7
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "nova-3",
            "transcript": "hello world",
        }
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                side_effect=BadRequest("Message to be replied not found")
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(
            OriginalMessageUnavailableError,
            "Message to be replied not found",
        ):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {"duration_seconds": 7}},
            )

        transcribe.assert_awaited_once()
        bot.send_message.assert_awaited_once()

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_deletes_unthreaded_transcript_and_marks_deleted(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 7
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "nova-3",
            "transcript": "hello world",
        }
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(return_value=telegram_sent_message(9001)),
            delete_message=AsyncMock(return_value=True),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(
            OriginalMessageUnavailableError,
            "reply target no longer attached",
        ):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {"duration_seconds": 7}},
            )

        transcribe.assert_awaited_once()
        bot.send_message.assert_awaited_once()
        bot.delete_message.assert_awaited_once_with(chat_id=123, message_id=9001)

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_retries_transient_transcript_send_failure(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 7
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "nova-3",
            "transcript": "hello world",
        }
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(side_effect=TimedOut("timed out")),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(RetryableJobError, "timed out"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {"duration_seconds": 7}},
            )
        bot.send_message.assert_awaited_once()

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_saves_transcript_before_attempting_send(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 7
        transcribe.return_value = {
            "provider": "deepgram",
            "model_id": "nova-3",
            "transcript": "hello world",
        }
        events: list[tuple[str, object, object, object]] = []

        async def set_stage(stage: str, stage_detail: str | None) -> None:
            events.append(("stage", stage, stage_detail, None))

        async def set_processing_text(
            processing_text: str,
            stage: str | None,
            stage_detail: str | None,
        ) -> None:
            events.append(("processing_text", processing_text, stage, stage_detail))

        async def set_outbound_json(
            outbound_json: dict[str, object],
            stage: str | None,
            stage_detail: str | None,
        ) -> None:
            events.append(("outbound_json", outbound_json, stage, stage_detail))

        context = QueueProcessingContext(
            set_stage=set_stage,
            set_processing_text=set_processing_text,
            set_outbound_json=set_outbound_json,
        )
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(side_effect=TimedOut("timed out")),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(RetryableJobError, "timed out"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {"duration_seconds": 7}},
                context=context,
            )

        self.assertIn(
            ("processing_text", "hello world", STAGE_SENDING_RESPONSE, None),
            events,
        )
        bot.send_message.assert_awaited_once()

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_resumes_from_stored_processing_text(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        bot = SimpleNamespace(
            get_file=AsyncMock(),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9008, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        result = await process_voice(
            bot,
            {
                "chat_id": 123,
                "message_id": 456,
                "chat_type": "supergroup",
                "file_id": "voice-file-id",
                "file_unique_id": "voice-unique-id",
                "mime_type": "audio/ogg",
                "file_size": 321,
                "processing_text": "stored transcript",
            },
            {"extra": {"duration_seconds": 7}},
        )

        self.assertEqual(result["processing_text"], "stored transcript")
        self.assertEqual(result["transcript_provider"], "stored")
        self.assertEqual(result["audio_duration_seconds"], None)
        self.assertEqual(result["transcript_message_ids"], [9008])
        bot.get_file.assert_not_awaited()
        probe_duration.assert_not_called()
        transcribe.assert_not_awaited()
        bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text="Transcript:\n\nstored transcript",
            reply_to_message_id=456,
            allow_sending_without_reply=False,
        )

    async def test_voice_process_skips_persisted_transcript_messages(self) -> None:
        bot = SimpleNamespace(
            get_file=AsyncMock(),
            send_message=AsyncMock(),
            set_message_reaction=AsyncMock(return_value=True),
        )

        result = await process_voice(
            bot,
            {
                "chat_id": 123,
                "message_id": 456,
                "chat_type": "supergroup",
                "file_id": "voice-file-id",
                "file_unique_id": "voice-unique-id",
                "mime_type": "audio/ogg",
                "file_size": 321,
                "processing_text": "stored transcript",
                "outbound_json": json.dumps(
                    {
                        "transcript": {
                            "messages": [
                                {
                                    "index": 0,
                                    "message_id": 9008,
                                    "chars": 29,
                                }
                            ]
                        }
                    }
                ),
            },
            {"extra": {"duration_seconds": 7}},
        )

        self.assertEqual(result["transcript_message_ids"], [9008])
        bot.send_message.assert_not_awaited()

    async def test_voice_process_persists_transcript_message_progress(self) -> None:
        events: list[tuple[str, object, object, object]] = []

        async def set_stage(stage: str, stage_detail: str | None) -> None:
            events.append(("stage", stage, stage_detail, None))

        async def set_processing_text(
            processing_text: str,
            stage: str | None,
            stage_detail: str | None,
        ) -> None:
            events.append(("processing_text", processing_text, stage, stage_detail))

        async def set_outbound_json(
            outbound_json: dict[str, object],
            stage: str | None,
            stage_detail: str | None,
        ) -> None:
            events.append(("outbound_json", outbound_json, stage, stage_detail))

        context = QueueProcessingContext(
            set_stage=set_stage,
            set_processing_text=set_processing_text,
            set_outbound_json=set_outbound_json,
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9008, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        result = await process_voice(
            bot,
            {
                "chat_id": 123,
                "message_id": 456,
                "chat_type": "supergroup",
                "file_id": "voice-file-id",
                "file_unique_id": "voice-unique-id",
                "mime_type": "audio/ogg",
                "file_size": 321,
                "processing_text": "stored transcript",
            },
            {"extra": {"duration_seconds": 7}},
            context=context,
        )

        self.assertEqual(result["transcript_message_ids"], [9008])
        self.assertIn(
            (
                "outbound_json",
                {
                    "transcript": {
                        "messages": [
                            {
                                "index": 0,
                                "message_id": 9008,
                                "chars": len("Transcript:\n\nstored transcript"),
                            }
                        ]
                    }
                },
                STAGE_SENDING_RESPONSE,
                None,
            ),
            events,
        )

    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_rejects_too_long_audio(self, probe_duration: object) -> None:
        probe_duration.return_value = 60
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9002, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(PermanentJobError, "exceeds 60s limit"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "message_thread_id": 789,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {}},
            )

        bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text="Sorry, we support only AUDIO which is 60 seconds or less, please re-send that...",
            message_thread_id=789,
            reply_to_message_id=456,
            allow_sending_without_reply=False,
        )
        self.assertEqual(bot.set_message_reaction.await_count, 2)

    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_rejection_fails_if_reply_target_is_gone(
        self,
        probe_duration: object,
    ) -> None:
        probe_duration.return_value = 60
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                side_effect=BadRequest("Message to be replied not found")
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(
            OriginalMessageUnavailableError,
            "Message to be replied not found",
        ):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {}},
            )

        bot.send_message.assert_awaited_once()

    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_rejection_retries_if_reply_send_times_out(
        self,
        probe_duration: object,
    ) -> None:
        probe_duration.return_value = 60
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(side_effect=TimedOut("timed out")),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(RetryableJobError, "timed out"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {}},
            )

        bot.send_message.assert_awaited_once()

    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_rejects_too_short_audio(self, probe_duration: object) -> None:
        probe_duration.return_value = 2
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9005, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(PermanentJobError, "below 3s minimum"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "message_thread_id": 789,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {}},
            )

        bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text="Sorry, we support only AUDIO which is at least 3 seconds, please re-send that...",
            message_thread_id=789,
            reply_to_message_id=456,
            allow_sending_without_reply=False,
        )

    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_rejects_corrupt_audio(self, probe_duration: object) -> None:
        probe_duration.side_effect = PermanentJobError("audio payload is corrupt or unreadable")
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"not-audio")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9003, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(PermanentJobError, "corrupt"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "message_thread_id": 789,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {}},
            )

        bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text="We apologise, however, we couldn't understand that, please re-send.",
            message_thread_id=789,
            reply_to_message_id=456,
            allow_sending_without_reply=False,
        )

    @patch("bot_libs.queue_processors.voice.transcribe_audio_bytes")
    @patch("bot_libs.queue_processors.voice.probe_audio_duration_seconds")
    async def test_voice_process_rejects_unusable_stt_result(
        self,
        probe_duration: object,
        transcribe: object,
    ) -> None:
        probe_duration.return_value = 3
        transcribe.side_effect = PermanentJobError(
            "all speech-to-text provider transcripts exceeded the maximum speech rate"
        )
        tg_file = SimpleNamespace(
            file_path="voice/file_1.ogg",
            download_as_bytearray=AsyncMock(return_value=bytearray(b"ogg-bytes")),
        )
        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=tg_file),
            send_message=AsyncMock(
                return_value=telegram_sent_message(9004, reply_to_message_id=456)
            ),
            set_message_reaction=AsyncMock(return_value=True),
        )

        with self.assertRaisesRegex(PermanentJobError, "maximum speech rate"):
            await process_voice(
                bot,
                {
                    "chat_id": 123,
                    "message_id": 456,
                    "message_thread_id": 789,
                    "chat_type": "supergroup",
                    "file_id": "voice-file-id",
                    "mime_type": "audio/ogg",
                },
                {"extra": {}},
            )

        bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text="Sorry, I couldn't understand that speech.",
            message_thread_id=789,
            reply_to_message_id=456,
            allow_sending_without_reply=False,
        )

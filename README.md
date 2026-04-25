# telegram_service_bot

### 1/ GENERAL OVERVIEW OF THE ENTIRE SYSTEM

**Architecture:** Async, decoupled, dual-process Telegram bot. 
**Flow:** Webhook Receiver → SQLite Queue → SIGHUP IPC → Background Daemon → Media Processing / STT APIs → Telegram Reply.
**Key Concepts:**
* **Resiliency:** SQLite WAL mode queue, exponential backoff, transient vs. permanent error handling.
* **Concurrency Control:** Strict POSIX file locking (`fcntl` PID files) prevents duplicate processes.
* **UX/State Feedback:** Real-time state mapping to Telegram message reactions (pulsing emojis for long tasks).
* **Extensibility:** Pluggable media processors and STT (Speech-to-Text) failover cascade (Gemini → OpenAI → Deepgram).

---

### 2/ OVERVIEW OF EACH COMPONENT

#### 2a/ `./bot.py`
**Role:** Ingestion Point (Webhook Server).
* **Stack:** Starlette + Uvicorn + `python-telegram-bot` (PTB).
* **Action:** Rapidly accepts webhooks, validates secrets, normalizes payloads, persists to SQLite queue, sends SIGHUP to daemon, returns `200 OK`. 
* **Features:** Startup backlog recovery (fetches missed updates via polling before opening webhook), raw payload dumping.

#### 2b/ `./telegram_queue_daemon.py`
**Role:** Async Background Worker.
* **Stack:** Asyncio event loop.
* **Action:** Idles until SIGHUP or timeout. Claims unlocked DB jobs, dispatches to specific media processors, manages API failovers, applies retry/backoff policies, marks jobs done/dead.
* **Features:** Stale lock recovery, rate-limit aware Telegram API calls, multi-stage status pulsing.

---

### 3/ FILE-BY-FILE CONCEPTUAL OVERVIEW

**Core Extraction & Routing (`bot_libs/`)**
* `__init__.py`: Package root.
* `text_message.py` ... `sticker_message.py` (9 files): Protocol mappers. Translate raw Telegram media payloads into normalized `MessageContent`. Flags supported vs. unsupported types.
* `queue_extract.py`: Orchestrates extractors to build the unified `QueueJobData` struct.
* `queue_enqueue.py`: DB insertion handler. Drops duplicates, applies initial acceptance UI reactions.

**Database & Models**
* `sql.py`: SQLite interface. Handles DDL, schema checks, WAL mode config, atomic locking (`BEGIN IMMEDIATE`), and state transitions.
* `sql_bot_init.py`: CLI utility for DB bootstrapping/resetting.
* `queue_models.py`: Core structs (`QueueJobData`, `MessageContent`) and status constants.

**Job Processing Orchestration**
* `queue_processors/dispatch.py`: Router mapping content types to specific worker modules.
* `queue_processors/file_common.py`: Telegram file path metadata resolution.
* `queue_processors/text.py` -> `document.py` (4 files): Fast-path metadata extractors for non-audio media.
* `queue_processing_context.py`: Context manager abstraction allowing processors to update DB state mid-execution.

**Audio & STT Pipeline**
* `queue_processors/voice.py` & `audio.py`: Audio entrypoints. Downloads file → validates → sends to STT → replies with chunked text.
* `queue_processors/speech_to_text.py`: STT orchestration. Implements the failover cascade loop.
* `queue_processors/audio_validation.py`: `ffprobe` subprocess wrapper. Duration extraction, speech-rate anomaly detection.
* `queue_processors/transcript_validation.py`: Post-processing validation (rejects AI prompt-echoing/hallucinations).
* `queue_processors/gemini_audio.py` / `openai_audio.py` / `deepgram_audio.py`: Specific STT vendor API clients.

**State, UI & Error Policies**
* `stage_names.py`: Pipeline taxonomy (`DOWNLOADING_FILE`, `CALLING_STT_API`).
* `reaction_policy.py`: Translates system state to Telegram emojis. Implements FloodControl rate-limiting logic.
* `pipeline_definitions.py` & `pipeline_status.py`: Real-time UI engine. Manages async "pulsing" reactions during long network calls.
* `status_feedback.py`: Wrapper client for the pipeline status updates.
* `retry_policy.py`: Exponential backoff math/arrays.
* `queue_processor_errors.py`: Exception taxonomy (`RetryableJobError`, `PermanentJobError`, `UnusableTranscriptError`).
* `telegram_error_policy.py`: Maps Telegram HTTP/API errors to system retry logic (e.g., detecting deleted source messages).

**System Utilities & Diagnostics**
* `process_guard.py`: Process singleton enforcer (PID files, `fcntl` locks, `/proc` checks).
* `daemon_signal.py`: IPC sender. Locates daemon PID and fires `SIGHUP`.
* `logging_utils.py`: Custom trace levels, auto-redaction of Telegram API tokens.
* `runtime_checks.py`: Pre-flight boot validation (API keys, `ffprobe` binary, imports).
* `service_dump.py`: Diagnostic writer for raw Telegram JSON webhooks.
* `replay_service_dumps.py`: CLI tool to inject dumped JSONs back into the SQLite queue.

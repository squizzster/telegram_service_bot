# telegram_service_bot

# Telegram Queue Bot — Definitive Professional Architecture Guide

## Executive summary

The Telegram Queue Bot is a **durable, asynchronous Telegram ingestion and processing system**.

It should not be understood as a conventional Telegram bot that receives a message and handles it inline. Its real architecture is now a **layered local-first processing pipeline**:

```text
Telegram
  → bot.py webhook ingress
  → SQLite telegram_queue
  → telegram_queue_daemon.py worker
  → canonical text
  → action detection
  → SQLite incoming_message_actions
  → telegram_action_daemon.py worker
  → action processors
  → Telegram reactions / replies / terminal state
```

The governing principle is:

> **Accept quickly, persist durably, process asynchronously, and expose progress visibly.**

`bot.py` is the ingress edge. It faces Telegram, validates requests, normalizes updates, persists them into SQLite, gives immediate best-effort feedback, wakes the daemon, and returns quickly.

`telegram_queue_daemon.py` is the source-message processing engine. It owns queue claiming, retries, source-message stage transitions, content processor dispatch, transcript delivery, action detection, terminal source state, and final reaction reconciliation.

`telegram_action_daemon.py` is the derived-work processing engine. It owns durable child action claiming, action-specific retries, action stage transitions, action processor dispatch, action outbound memory, and action terminal state.

SQLite is the compact operational core of the system. It acts as:

```text
queue
+ workflow state machine
+ retry scheduler
+ lock ledger
+ outbox ledger
+ idempotency layer
+ audit trail
+ replay/debug substrate
+ recovery checkpoint
```

The most important object in the architecture is not the bot, the daemon, or any single processor.

It is the **queue row**.

The queue row is the durable memory of the system.

Telegram reactions are the visible heartbeat.

The daemons are the workers.

`bot.py` is the door.

Processors are the specialized hands.

---

## Current Implementation Snapshot

The current implemented runtime is:

```text
Telegram update
  → bot.py
  → telegram_queue
  → telegram_queue_daemon.py
  → processing_text
  → OpenAI action classifier
  → incoming_message_actions
  → telegram_action_daemon.py
  → action-specific processor
```

The current action catalog is:

| Internal code       | Provider label        | Current processor                          |
| ------------------- | --------------------- | ------------------------------------------ |
| `NONE`              | `none`                | No child action row is created             |
| `LOG_EXPENSES`      | `reporting_expenses`  | Temporary retry/success test processor     |
| `LOG_INCOME`        | `reporting_income`    | Temporary retry/success test processor     |
| `SET_REMINDER`      | `set_a_reminder`      | Temporary retry/success test processor     |
| `ANSWER_QUESTION`   | `asking_a_question`   | OpenAI-backed question answering processor |

The action detection prompt is:

```python
prompt={
    "id": "pmpt_69ed0e37aab08193aef354f523d55a240171a57891089e80",
    "variables": {
        "incoming_text": incoming_text,
    },
}
```

The code intentionally does not pass a prompt version here. OpenAI should use the prompt's default version.

The question-answering prompt is:

```python
prompt={
    "id": "pmpt_69ed2980b0988195b26594cc5467f26f07bc6d43e5ee5db1",
    "variables": {
        "question_to_answer": question,
    },
}
```

The question-answering response must contain:

````text
```answer
answer text
```
````

Missing, empty, or unclosed `answer` blocks are treated as retryable provider failures.

During development, schema compatibility is not preserved. The current schema version is `5`. Recreate the local SQLite database when the schema changes:

```bash
./bot_libs/sql_bot_init.py --drop-delete-current-db --force
./bot_libs/sql_bot_init.py --check
```

Run the workers separately:

```bash
./bot.py
./telegram_queue_daemon.py --DEBUG
./telegram_action_daemon.py --DEBUG
```

The action daemon uses the same 1, 3, 9, ... retry schedule as the source queue.

---

# 1. Architectural identity

This project implements a **durable Telegram message-processing pipeline** optimized for media workflows, especially short voice/audio transcription.

Its shape is closer to a local production job system than to a simple chatbot.

A traditional inline bot does this:

```text
Telegram message
  → bot handler
  → process immediately
  → reply
```

This system does this instead:

```text
Telegram message
  → validate
  → normalize
  → persist durable job
  → acknowledge Telegram
  → process later from queue
  → update visible state
  → reply / retry / finalize
```

That difference is the entire architecture.

The webhook path is deliberately kept thin. Heavy work such as file download, audio probing, STT calls, transcript chunking, retry decisions, and terminal reactions happens outside the HTTP request lifecycle.

This gives the system its most important property:

> Telegram delivery is volatile.
> SQLite becomes the durable continuation point.

Once a Telegram update has been inserted into SQLite, the job is no longer dependent on the webhook request, Telegram retry timing, process memory, or one specific daemon run.

---

# 2. System topology

At runtime, the system is composed of four main zones.

```text
┌─────────────────────────────────────────────────────────────┐
│                         Telegram                            │
│                                                             │
│  Webhook delivery, pending updates, Bot API, reactions       │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                    Ingress edge: bot.py                     │
│                                                             │
│  - HTTP webhook receiver                                    │
│  - Telegram secret-token validation                         │
│  - JSON parsing                                             │
│  - Update normalization                                     │
│  - SQLite queue insertion                                   │
│  - initial reaction scheduling                              │
│  - daemon wake signal                                       │
│  - PTB command dispatch after persistence                   │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 Durable core: SQLite workflow               │
│                                                             │
│  - telegram_queue source rows                               │
│  - action_detection_runs audit rows                         │
│  - action_catalog vocabulary                                │
│  - incoming_message_actions child jobs                      │
│  - retry / lock / outbox / result state                     │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│          Source processing engine: queue daemon             │
│                                                             │
│  - claim due rows                                           │
│  - recover stale processing jobs                            │
│  - dispatch processors                                      │
│  - mutate stages                                            │
│  - handle retries                                           │
│  - produce canonical processing_text                        │
│  - detect actions                                           │
│  - create durable child action jobs                         │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│         Derived work engine: action daemon                  │
│                                                             │
│  - claim due incoming_message_actions rows                  │
│  - dispatch by action_code                                  │
│  - run action-specific workflows                            │
│  - persist action outbound/result state                     │
│  - retry or dead-letter failed action jobs                  │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                   Telegram visible output                   │
│                                                             │
│  - 👀 accepted                                               │
│  - ⚡ downloading                                            │
│  - ✍ transcribing / sending                                 │
│  - 👌 complete                                               │
│  - 🤷 unsupported                                            │
│  - 🤔 / 🥱 / 😴 retry waiting                                │
│  - 💔 failed                                                 │
│  - transcript replies                                       │
│  - action replies, including question answers               │
└─────────────────────────────────────────────────────────────┘
```

The system is intentionally split around operational responsibility.

| Runtime part                | Architectural role          | Owns                                                                    |
| --------------------------- | --------------------------- | ----------------------------------------------------------------------- |
| `bot.py`                    | Ingress edge                | Validation, normalization, queue insertion, immediate acknowledgement   |
| SQLite                      | Durable workflow core       | Source rows, action rows, retry timing, locks, idempotency, audit data  |
| `telegram_queue_daemon.py`  | Source-message engine       | Source claiming, content processing, canonical text, action detection   |
| `telegram_action_daemon.py` | Derived-action engine       | Action claiming, action workflows, action retries, action terminal state |
| Processors                  | Specialized work units      | Content-specific and action-specific behavior                           |
| Reactions                   | UX projection               | Best-effort visible state, never source of truth                        |

---

# 3. The core architectural thesis

The system works because it separates four concerns cleanly.

```text
Ingress layer
  Receives Telegram updates safely and quickly.

Normalization layer
  Converts Telegram-specific message structures into canonical queue data.

Durable state layer
  Stores everything needed to continue, retry, audit, and avoid duplication.

Worker layer
  Performs heavy work and mutates durable state until terminal completion.
```

The system does not trust process memory as the source of truth.

It externalizes progress into SQLite.

That is why a job can survive:

```text
daemon crash
bot restart
duplicate Telegram delivery
STT provider outage
partial transcript sending
Telegram rate limit
deleted original message
network interruption
```

The architecture is not merely “queue-based.”

It is **stateful queue processing**.

Every meaningful step is either persisted, recoverable, or deliberately best-effort.

---

# 4. The queue row as the system’s memory

The `telegram_queue` row is the central architectural object.

It is not just a pending job.

It is simultaneously:

```text
job record
retry ledger
workflow state machine
input snapshot
idempotency checkpoint
outbox ledger
audit trail
diagnostic bundle
terminal result record
```

Conceptually, each row contains these categories of information:

| Category             | Purpose                                                               |
| -------------------- | --------------------------------------------------------------------- |
| Identity             | Uniquely ties the job to a Telegram update/message                    |
| Sender metadata      | Preserves who sent the message and from where                         |
| Content metadata     | Captures normalized type, text, file ID, MIME type, caption, duration |
| Queue mechanics      | Controls claiming, retries, locking, availability, terminal state     |
| Processing semantics | Tracks the current human/process-visible stage                        |
| Reusable work        | Stores completed semantic work such as transcript text                |
| Outbound memory      | Records Telegram replies already sent                                 |
| Raw evidence         | Preserves original Telegram payload for replay/debugging              |
| Result data          | Stores terminal processor output or failure metadata                  |

A simplified conceptual model:

```text
telegram_queue row
├── identity
│   ├── update_id
│   ├── chat_id
│   ├── message_id
│   ├── thread_id
│   └── media_group_id
│
├── content
│   ├── content_type
│   ├── text
│   ├── caption
│   ├── file_id
│   ├── file_unique_id
│   ├── mime_type
│   └── file_size
│
├── queue state
│   ├── status
│   ├── attempts
│   ├── max_attempts
│   ├── available_at
│   ├── locked_at
│   └── locked_by
│
├── processing state
│   ├── stage
│   ├── stage_detail
│   └── processing_text
│
├── memory / replay
│   ├── payload_json
│   ├── raw_update_json
│   ├── outbound_json
│   └── result_json
│
└── diagnostics
    ├── last_error
    ├── unsupported_reason
    └── recognized_type
```

The most important persisted fields are:

| Field             | Architectural meaning                                                                         |
| ----------------- | --------------------------------------------------------------------------------------------- |
| `status`          | Mechanical queue state: should the daemon claim this row, ignore it, or treat it as terminal? |
| `stage`           | Semantic processing phase: what is happening from a workflow/UX point of view?                |
| `processing_text` | Reusable semantic output, especially completed transcripts                                    |
| `outbound_json`   | Ledger of Telegram messages already sent                                                      |
| `payload_json`    | Normalized content payload used by processors                                                 |
| `raw_update_json` | Original Telegram evidence for replay/debugging                                               |
| `attempts`        | Retry progress                                                                                |
| `available_at`    | Retry scheduling timestamp                                                                    |
| `result_json`     | Terminal processor result                                                                     |

This row gives the system a stable brain.

---

# 5. Status, stage, and reaction: three separate layers

One of the strongest design choices is the separation of queue mechanics, workflow meaning, and user-visible feedback.

## 5.1 Status

`status` answers:

> What should the queue engine do with this row?

Typical values:

```text
queued
processing
done
dead
```

This is the daemon’s mechanical control plane.

```text
queued      → can be claimed when available_at is due
processing  → currently owned by a worker
done        → terminal success or clean non-error closure
dead        → terminal failure, removal, or exhausted condition
```

## 5.2 Stage

`stage` answers:

> What phase is this job in?

Typical values:

```text
QUEUED
READY_TO_PROCESS
DOWNLOADING_FILE
CALLING_STT_API
SENDING_RESPONSE
RETRY_WAITING
DONE
FAILED
MESSAGE_REMOVED
UNSUPPORTED
```

This is the semantic workflow layer.

A row can be mechanically `queued` while semantically `RETRY_WAITING`.

That distinction matters.

```text
status = queued
stage  = RETRY_WAITING
```

means:

> The daemon should not treat this as new work yet. It is waiting for its retry time.

## 5.3 Reaction

Reaction is the visible Telegram projection of state.

```text
👀 accepted
⚡ downloading
✍ transcribing / sending
👌 success
🤷 unsupported
🤔 short retry
🥱 medium retry
😴 long retry
💔 failed
```

Reactions are deliberately non-authoritative.

They are a UX layer, not a correctness layer.

The source of truth remains SQLite.

This allows reaction failures, Telegram rate limits, or deleted messages to be handled gracefully without corrupting the actual processing state.

---

# 6. End-to-end runtime lifecycle

## 6.1 Startup hardening

Runtime processes begin by validating that the system is capable of running.

```text
load config
  → validate environment
  → verify Python dependencies
  → verify system commands
  → verify credentials
  → acquire pidfile lock
  → verify SQLite schema
  → start runtime loop
```

The startup model is strict by design.

The system assumes a complete configured environment and fails fast when core dependencies are unavailable.

Important dependency classes include:

| Dependency area | Examples                                                 |
| --------------- | -------------------------------------------------------- |
| Python packages | Telegram/PTB, Starlette, Uvicorn, HTTP clients, STT SDKs |
| System tools    | `ffprobe`                                                |
| Credentials     | Telegram, OpenAI, Deepgram, Gemini, depending on process |
| Local storage   | SQLite database path and expected schema                 |
| Process control | pidfile lock paths                                       |

This is a production-style posture: do not run ambiguously.

If the runtime cannot satisfy its contract, it should fail before accepting work.

---

## 6.2 Telegram backlog recovery

When `bot.py` starts, it performs an important recovery maneuver.

```text
start HTTP server
  → temporarily delete Telegram webhook
  → call getUpdates()
  → persist pending Telegram updates into SQLite
  → advance offset only after persistence
  → reinstall webhook
```

This bridges Telegram’s pending update queue into the local durable queue.

The principle is:

> If Telegram held updates while the bot was offline, import them into local durable state before resuming normal webhook delivery.

This avoids relying on Telegram as the long-term source of pending work.

Once an update is written to SQLite, local durability takes over.

---

## 6.3 Live webhook intake

For live traffic, the webhook path is intentionally short and defensive.

```text
POST /telegram
  → validate method
  → validate Telegram secret token
  → parse JSON
  → optionally write raw service dump
  → convert payload to Telegram Update
  → normalize into QueueJobData
  → insert queue row
  → schedule initial reaction
  → wake daemon
  → schedule PTB command processing
  → return HTTP 200
```

The ordering is essential:

```text
persist first
feedback second
wake daemon third
PTB command processing after persistence
```

The system should never perform expensive work before the update is durably recorded.

Even command messages are queued before command handlers run.

This gives the operator a durable record of inbound activity regardless of whether the message is later handled through PTB command logic or queue processing.

---

## 6.4 Queue insertion

Queue insertion is where Telegram’s external data model becomes the system’s internal data model.

```text
Telegram Update
  → effective message/chat
  → extractor selection
  → MessageContent
  → QueueJobData
  → SQLite row
```

The normalization step decides:

```text
content_type
support status
initial stage
text / caption
file metadata
sender metadata
chat/thread metadata
payload_json
raw_update_json
unsupported diagnostics
```

The `update_id` uniqueness constraint gives idempotency at the ingestion boundary.

If Telegram delivers the same update twice, the database protects the system from duplicate processing.

Duplicate delivery becomes:

```text
already inserted
  → skip safely
  → return success path to Telegram
```

---

## 6.5 Daemon wakeup

After a successful insert, `bot.py` attempts to wake the daemon:

```text
read daemon pidfile
  → validate process identity
  → send SIGHUP
```

This is a latency optimization.

It is not the durability mechanism.

If the signal fails, the row is already safe in SQLite. The daemon can still discover it during polling or on next startup.

This is a clean separation:

```text
SQLite row = durable work handoff
SIGHUP      = fast notification
```

---

## 6.6 Daemon queue loop

The daemon repeatedly drains due work.

```text
recover stale processing rows
  → claim next due queued row
  → increment attempts
  → dispatch processor
  → handle result
  → mark done / retry / dead
  → reconcile reaction
  → repeat until no due work
  → sleep until signal, poll interval, or next retry
```

Claiming is transactional.

```text
BEGIN IMMEDIATE
  SELECT next due queued row
  UPDATE row:
    status = processing
    locked_at = now
    locked_by = worker
    attempts = attempts + 1
COMMIT
```

This gives the daemon exclusive ownership of the row for that attempt.

Even if the intended deployment is singleton, the database claim pattern is still robust and explicit.

---

# 7. Processing outcome model

Every claimed job eventually lands in one of a small set of outcomes.

| Outcome                              | Queue transition      | Stage             |
| ------------------------------------ | --------------------- | ----------------- |
| Processor success                    | `processing → done`   | `DONE`            |
| Unsupported content                  | `processing → done`   | `UNSUPPORTED`     |
| Original message deleted             | `processing → dead`   | `MESSAGE_REMOVED` |
| Permanent failure                    | `processing → dead`   | `FAILED`          |
| Retryable failure with attempts left | `processing → queued` | `RETRY_WAITING`   |
| Retryable failure exhausted          | `processing → dead`   | `FAILED`          |

This taxonomy keeps the daemon simple.

Processors do not directly decide the final database transition. They return normally or raise a known error type. The daemon translates that result into persistent queue state.

The important error language is:

```text
RetryableJobError
  temporary failure, try later

PermanentJobError
  non-recoverable failure, terminal dead state

OriginalMessageUnavailableError
  source Telegram message disappeared, treat as user cancellation/removal

UnusableTranscriptError
  STT output exists but is not acceptable
```

This gives the system a clean boundary between content-specific failures and queue-level decisions.

---

# 8. Content model and dispatcher

The system recognizes Telegram content through extractor modules and maps supported content to processors.

## Supported content

| Content type | Current behavior                                   |
| ------------ | -------------------------------------------------- |
| `text`       | Queued and processed as text metadata              |
| `photo`      | Selects largest photo, fetches Telegram file info  |
| `document`   | Captures file metadata, fetches Telegram file info |
| `voice`      | Downloads, validates, transcribes, replies         |
| `audio`      | Uses shared audio-like transcription pipeline      |
| `animation`  | Captures animation metadata and fetches file info  |

## Recognized unsupported content

| Content type     | Behavior                                       |
| ---------------- | ---------------------------------------------- |
| `video`          | Metadata captured, closed as unsupported       |
| `video_note`     | Metadata captured, closed as unsupported       |
| `sticker`        | Metadata captured, closed as unsupported       |
| unknown payloads | Diagnostically captured, closed as unsupported |

Unsupported does not mean ignored.

Unsupported means:

```text
recognized
  → persisted
  → classified
  → terminally closed
  → visible unsupported reaction
```

This is operationally useful because the system preserves evidence about what users are sending, even when the product does not yet process that content type.

The processor registry is conceptually:

```text
text       → text.process
photo      → photo.process
document   → document.process
animation  → animation.process
voice      → voice.process
audio      → audio.process
```

Unknown registered behavior is treated as a permanent processing configuration problem.

---

# 9. Voice/audio transcription pipeline

The voice/audio path is the richest part of the system.

It is where durability, progress visibility, provider fallback, idempotency, and Telegram reply semantics all meet.

## 9.1 Fresh successful path

```text
row claimed
  → stage DOWNLOADING_FILE
  → reaction ⚡
  → fetch Telegram file metadata
  → download audio bytes
  → validate with ffprobe
  → enforce conservative duration policy
  → stage CALLING_STT_API
  → reaction ✍
  → optional pulsing feedback
  → try Gemini
  → try OpenAI if needed
  → try Deepgram if needed
  → validate transcript
  → persist transcript in processing_text
  → stage SENDING_RESPONSE
  → split transcript into Telegram-safe chunks
  → send transcript replies
  → persist each sent message in outbound_json
  → mark done
  → reaction 👌
```

This path is deliberately checkpointed.

The system does not wait until all output is finished before saving meaningful progress.

The transcript is saved as soon as it is known.

Outbound replies are saved as each chunk is successfully sent.

---

## 9.2 Audio validation

Before spending STT provider calls, the audio is validated.

The validation layer checks:

```text
file is probeable
audio stream exists
duration is finite
duration is within conservative bounds
transcript length is plausible for duration
```

The practical duration policy keeps audio short enough for predictable latency, cost, and UX.

The exact cutoff around one minute is an implementation detail of a conservative product boundary, not a structural concern.

The more important architectural idea is:

> Validate cheap local facts before invoking expensive remote STT services.

---

## 9.3 STT provider cascade

The transcription layer uses a provider cascade.

```text
Gemini
  → OpenAI
  → Deepgram
```

Each provider adapter converts provider-specific responses and errors into system-level results.

The aggregate STT layer distinguishes between:

| Failure class              | Meaning                                                          |
| -------------------------- | ---------------------------------------------------------------- |
| Retryable provider failure | Temporary network, timeout, rate-limit, or provider outage       |
| Permanent provider failure | Bad request, invalid configuration, non-retryable provider error |
| Unusable transcript        | Provider returned text, but it failed quality checks             |

A provider can fail without failing the job immediately.

The system can fall through to the next provider.

The job fails only after the aggregate provider strategy concludes that no acceptable transcript can be produced.

---

## 9.4 Transcript validation

Returned text is not accepted blindly.

The system rejects transcripts that are:

```text
empty
prompt echoes
implausibly dense for the audio duration
```

This protects against common STT/LLM failure modes.

The prompt-echo guard is particularly important for LLM-backed transcription systems, where a model may return the instruction text rather than the spoken content.

---

# 10. Idempotency and partial-failure recovery

The audio pipeline has a strong retry-resume design.

Two fields matter most:

```text
processing_text
outbound_json
```

## 10.1 `processing_text` as semantic checkpoint

For text jobs:

```text
processing_text = original text
```

For voice/audio jobs:

```text
processing_text = completed transcript
```

Once transcription succeeds, the transcript is persisted before sending replies.

That means a later retry can avoid redoing STT:

```text
processing_text exists
  → transcript already known
  → skip download/transcribe if appropriate
  → continue from response-sending phase
```

This saves cost, time, and avoids duplicate provider calls.

---

## 10.2 `outbound_json` as outbox ledger

Transcript replies may be split across multiple Telegram messages.

After each successful send, the daemon records the sent message.

Conceptually:

```json
{
  "transcript": {
    "messages": [
      {
        "index": 0,
        "message_id": 123
      },
      {
        "index": 1,
        "message_id": 124
      }
    ]
  }
}
```

On retry:

```text
chunk already recorded
  → skip it

chunk missing
  → send it
```

This prevents duplicate transcript replies after partial failure.

The design also handles a subtle failure mode:

```text
send succeeds
  → persist outbound_json fails
```

In that case, the processor attempts to delete the just-sent Telegram message.

That is a strong outbox discipline: do not leave external side effects that the database cannot remember.

---

## 10.3 Reply-target verification

The first transcript reply is intended to be attached to the original Telegram message.

The system verifies that reply relationship.

If the original message disappeared or Telegram cannot attach the reply target, the system treats that as source removal/cancellation rather than silently sending contextless output into the chat.

Conceptually:

```text
send reply
  → verify reply target
  → if target missing:
       delete sent reply best-effort
       raise OriginalMessageUnavailableError
       mark job MESSAGE_REMOVED
```

This is a thoughtful UX and correctness boundary.

The system avoids producing orphaned transcripts.

---

# 11. Retry model

The retry schedule uses powers of three:

```text
1s
3s
9s
27s
81s
243s
729s
2187s
6561s
19683s
59049s
```

The default maximum attempt count is:

```text
len(delays) + 1 = 12
```

Attempts increment when a job is claimed.

On retryable failure:

```text
if attempts remain:
  status = queued
  stage = RETRY_WAITING
  available_at = now + computed delay

else:
  status = dead
  stage = FAILED
```

Telegram `RetryAfter` can stretch the delay.

Retry visibility becomes more passive as delay grows:

```text
short retry   → 🤔
medium retry  → 🥱
long retry    → 😴
```

A useful optimization exists after success:

```text
one job succeeds
  → nearby retry-waiting jobs of same content type may be released early
```

That treats success as a signal that a transient provider or network condition may have cleared.

This is a practical local queue heuristic.

---

# 12. Reactions as visible state

Telegram reactions are the user-facing heartbeat of the pipeline.

They make asynchronous processing feel alive.

The approximate reaction language is:

| State                  | Reaction |
| ---------------------- | -------- |
| Accepted               | 👀       |
| Downloading            | ⚡        |
| Transcribing / sending | ✍        |
| Done                   | 👌       |
| Unsupported            | 🤷       |
| Short retry            | 🤔       |
| Medium retry           | 🥱       |
| Long retry             | 😴       |
| Failed                 | 💔       |

The reaction model has several important properties:

## Reactions are best-effort

A reaction failure should generally not fail the job.

The database remains truth.

## Reactions are state-derived

The system does not set arbitrary reactions. It maps queue state and processing stage to emoji feedback.

## Reactions are reconciled

There is a race between ingress feedback and fast daemon completion.

Example:

```text
bot.py schedules 👀
daemon quickly processes job
daemon sets 👌
late 👀 task tries to run
```

The system reconciles against persisted row state so a late initial reaction does not overwrite a terminal reaction.

## Reactions handle Telegram constraints

The reaction layer accounts for:

```text
group/supergroup eligibility
valid emoji constraints
Telegram RetryAfter
deleted or unavailable source messages
non-fatal reaction failures
```

The user sees a heartbeat.

The system keeps the heartbeat subordinate to the durable row.

---

# 13. `bot.py`: the ingress edge

`bot.py` is the front-of-house runtime.

Its job is to safely receive external events and turn them into durable internal work.

It owns:

```text
HTTP webhook server
Telegram secret-token validation
JSON parsing
raw service dumps
Telegram Update conversion
queue insertion
initial feedback scheduling
daemon wakeup
startup backlog recovery
PTB command dispatch
```

It does not own:

```text
file downloads
STT calls
transcript sending
retry scheduling
terminal queue outcomes
dead-letter decisions
```

Conceptually:

```text
bot.py = doorman + recorder + notifier
```

Its live webhook flow is:

```text
POST /telegram
  → validate request
  → parse payload
  → optionally dump raw JSON
  → build Telegram Update
  → normalize and insert queue row
  → schedule initial reaction
  → wake daemon
  → schedule PTB process_update
  → return HTTP 200
```

Its most important guarantee is:

> Telegram receives success only after the update has been durably accepted.

Queue persistence failures produce non-200 responses, allowing Telegram to retry.

This keeps Telegram’s delivery retry mechanism aligned with local durability.

---

# 14. `telegram_queue_daemon.py`: the processing engine

The daemon is the back-of-house runtime.

It owns the lifecycle after persistence.

It owns:

```text
stale job recovery
due job claiming
attempt incrementing
processor dispatch
stage mutation
retry/dead/done transitions
reply sending
terminal reaction reconciliation
sleep/wake timing
```

Conceptually:

```text
daemon = queue mechanic + retry brain + processor supervisor
```

Its loop is:

```text
start
  → verify schema
  → install signal handlers
  → open Telegram Bot session
  → recover stale processing rows
  → drain due jobs
  → wait for SIGHUP, retry due time, or poll interval
```

Its central function is outcome translation.

```text
processor returns normally
  → mark done

row unsupported
  → mark done as unsupported

processor raises RetryableJobError
  → schedule retry or fail exhausted

processor raises PermanentJobError
  → mark failed

processor raises OriginalMessageUnavailableError
  → mark message removed

unexpected exception
  → treat as retryable according to daemon policy
```

Processors receive a controlled context rather than direct database ownership.

That context gives them capabilities such as:

```text
set_stage()
set_processing_text()
set_outbound_json()
activity()
```

This preserves the daemon as the owner of persistence while still letting processors express progress.

---

# 15. SQLite as compact operational core

SQLite is intentionally carrying multiple operational roles.

For this local-first architecture, that is coherent and powerful.

SQLite is acting as:

| Role                   | How it appears                                         |
| ---------------------- | ------------------------------------------------------ |
| Queue                  | `status='queued'`, `available_at`, due claiming        |
| Workflow state machine | `status`, `stage`, terminal transitions                |
| Retry scheduler        | `attempts`, `max_attempts`, retry delays               |
| Lock ledger            | `locked_at`, `locked_by`, transactional claims         |
| Idempotency layer      | unique `update_id`, `processing_text`, `outbound_json` |
| Outbox ledger          | sent Telegram message IDs                              |
| Audit store            | payloads, raw updates, timestamps, results             |
| Diagnostic store       | errors, unsupported reasons, stage details             |
| Recovery checkpoint    | stale processing recovery and resume semantics         |

This is a good fit for a single-host bot.

It avoids unnecessary distributed infrastructure while retaining durable behavior.

The responsibility that comes with this design is operational discipline:

```text
clear schema ownership
stable migrations when production data must survive upgrades
useful indexes
WAL configuration
busy timeouts
retention / cleanup policy
backup strategy
queue inspection tooling
dead-letter visibility
stale lock recovery
```

The database is not just storage.

It is the system’s local control plane.

---

# 16. Raw dumps and replay

The system includes a practical production-debugging loop.

```text
incoming Telegram payload
  → optional raw service dump
  → saved as JSON
  → replay tool can reinsert later
  → extraction and queue behavior can be tested again
```

This matters because Telegram message shapes can be varied and surprising.

Raw service dumps allow an operator to capture the exact external input that caused an issue.

Replay tooling can then:

```text
load dump
  → optionally rewrite update_id
  → parse as Telegram Update
  → run same extraction path
  → insert queue row
```

This is strong because replay uses the same normalization path as live traffic.

It is not a separate synthetic test model.

It is production evidence reused as test input.

---

# 17. Runtime safety mechanisms

The system includes several operational safety mechanisms.

## 17.1 Process guards

The main runtimes use pidfile locking.

```text
bot pidfile
queue daemon pidfile
action daemon pidfile
fcntl lock
process metadata validation
```

This prevents accidental duplicate runtimes.

That matters because duplicate bot processes could compete over webhook handling, and duplicate daemon processes could increase queue contention or external side effects.

## 17.2 Stale processing recovery

If the daemon dies mid-job, rows may remain in `processing`.

Startup or daemon loop recovery can return stale rows to `queued`.

```text
processing row locked too long
  → reset to queued
  → retry later
```

This converts process death into ordinary retryable work.

## 17.3 Signal wakeup

`SIGHUP` lets `bot.py` wake the daemon immediately after insert.

This reduces latency without coupling correctness to IPC.

## 17.4 Runtime checks

Startup checks validate the environment before accepting work.

The current operational posture expects all configured providers and dependencies to be present.

That is a strong fail-fast model.

As the system matures, this could be made more flexible by distinguishing required infrastructure from optional fallback providers.

---

# 18. Component map by architectural role

## Ingress and acceptance

| Component                 | Role                                                                   |
| ------------------------- | ---------------------------------------------------------------------- |
| `bot.py`                  | HTTP ingress, Telegram validation, queue insertion, daemon wake        |
| `queue_enqueue.py`        | Extract-and-insert orchestration, duplicate handling, initial feedback |
| `queue_extract.py`        | Telegram Update → QueueJobData normalization                           |
| `*_message.py` extractors | Content-specific Telegram message normalization                        |

## Durable state

| Component          | Role                                                                         |
| ------------------ | ---------------------------------------------------------------------------- |
| `queue_models.py`  | Shared source-message data contracts, content/status constants, stable JSON  |
| `action_models.py` | Action catalog constants, provider labels, detection status contracts        |
| `sql.py`           | SQLite schema, source/action transitions, claiming, retries, stage updates   |
| `stage_names.py`   | Central source and action stage vocabulary                                   |
| `retry_policy.py`  | Retry schedule and retry delay classification                                |

## Processing

| Component                      | Role                                                                  |
| ------------------------------ | --------------------------------------------------------------------- |
| `telegram_queue_daemon.py`     | Source queue worker, retry engine, action detection handoff           |
| `telegram_action_daemon.py`    | Child action worker, action retry engine, action terminal state owner |
| `queue_processors/dispatch.py` | Content-type processor routing                                        |
| `action_dispatch.py`           | Action-code processor routing                                         |
| `queue_processing_context.py`  | Controlled source processor stage/text/outbound mutations             |
| `action_processing_context.py` | Controlled action processor stage/outbound mutations                  |
| `queue_processor_errors.py`    | Processor outcome/error vocabulary                                    |

## Content processors

| Component        | Role                                        |
| ---------------- | ------------------------------------------- |
| `text.py`        | Text metadata processing                    |
| `photo.py`       | Photo file info processing                  |
| `document.py`    | Document file info processing               |
| `animation.py`   | Animation file info and metadata processing |
| `voice.py`       | Main audio-like transcription workflow      |
| `audio.py`       | Adapter into shared audio-like pipeline     |
| `file_common.py` | Shared Telegram file metadata helper        |

## STT and validation

| Component                  | Role                                     |
| -------------------------- | ---------------------------------------- |
| `speech_to_text.py`        | Provider cascade and aggregate STT logic |
| `gemini_audio.py`          | Gemini transcription adapter             |
| `openai_audio.py`          | OpenAI transcription adapter             |
| `deepgram_audio.py`        | Deepgram transcription adapter           |
| `audio_validation.py`      | ffprobe and speech-rate validation       |
| `transcript_validation.py` | Prompt-echo and transcript sanity guard  |

## Action detection and action processors

| Component                         | Role                                                             |
| --------------------------------- | ---------------------------------------------------------------- |
| `action_detection.py`             | Source-row action detection orchestration and audit persistence  |
| `action_detection_openai.py`      | OpenAI classifier adapter for canonical text action labels       |
| `action_detection_validation.py`  | Strict validation of provider action label output                |
| `question_answering_openai.py`    | OpenAI question-answering adapter requiring an `answer` block    |
| `action_processors/questions.py`  | `ANSWER_QUESTION` workflow, reply sending, outbound idempotency  |
| `action_processors/temporary.py`  | Temporary processors for expenses, income, and reminders         |
| `telegram_message_utils.py`       | Telegram-safe text chunking helpers                              |

## UX and Telegram feedback

| Component                  | Role                                     |
| -------------------------- | ---------------------------------------- |
| `reaction_policy.py`       | Stage/status → Telegram reaction mapping |
| `pipeline_status.py`       | Async stage activity and pulse lifecycle |
| `pipeline_definitions.py`  | Active-stage reaction definitions        |
| `status_feedback.py`       | Row-bound reaction feedback adapter      |
| `telegram_error_policy.py` | Telegram API error normalization         |

## Operations

| Component                 | Role                                                        |
| ------------------------- | ----------------------------------------------------------- |
| `process_guard.py`        | Singleton pidfile locks                                     |
| `daemon_signal.py`        | bot-to-daemon SIGHUP wakeup                                 |
| `runtime_checks.py`       | Startup dependency, command, env, and credential validation |
| `logging_utils.py`        | Logging setup and sensitive token redaction                 |
| `service_dump.py`         | Raw webhook payload capture                                 |
| `replay_service_dumps.py` | Replay captured Telegram payloads into queue                |
| `sql_bot_init.py`         | DB init/check/reset/admin CLI                               |

---

# 19. Flow examples

## 19.1 Voice note success

```text
User sends voice note
  → Telegram POSTs webhook
  → bot.py validates secret
  → raw payload optionally dumped
  → update parsed
  → voice extractor captures file_id and duration
  → queue row inserted as status=queued, stage=QUEUED
  → 👀 reaction scheduled
  → daemon woken
  → HTTP 200 returned

daemon wakes
  → claims row
  → attempts=1
  → stage DOWNLOADING_FILE
  → ⚡ reaction
  → downloads audio
  → ffprobe validates
  → stage CALLING_STT_API
  → ✍ reaction / pulse
  → STT provider cascade runs
  → transcript accepted
  → processing_text saved
  → stage SENDING_RESPONSE
  → transcript chunks sent
  → outbound_json updated per chunk
  → row marked done
  → 👌 reaction
```

## 19.2 Unsupported video

```text
User sends video
  → Telegram POSTs webhook
  → video extractor recognizes payload
  → metadata captured
  → is_supported = false
  → row inserted
  → 🤷 reaction scheduled

daemon claims row
  → sees unsupported
  → marks done with stage UNSUPPORTED
  → reconciles 🤷 reaction
```

The video is not discarded.

It is recorded and closed cleanly.

## 19.3 Provider outage

```text
audio job claimed
  → download succeeds
  → validation succeeds
  → Gemini fails
  → OpenAI fails
  → Deepgram fails
  → aggregate RetryableJobError
  → daemon computes delay
  → status=queued
  → stage=RETRY_WAITING
  → available_at=future
  → reaction 🤔 / 🥱 / 😴
```

Later:

```text
available_at arrives
  → daemon claims again
  → attempts increments
  → retry continues
```

## 19.4 Partial transcript send crash

```text
transcript split into 3 chunks
  → chunk 0 sent
  → outbound_json records chunk 0
  → daemon crashes before chunk 1
```

On retry:

```text
processing_text exists
  → skip STT

outbound_json says chunk 0 already sent
  → skip chunk 0

chunk 1 and 2 missing
  → send remaining chunks
  → mark done
```

This is the system’s idempotency model in action.

## 19.5 Original message deleted

```text
job processing
  → reaction or reply fails because original message vanished
  → OriginalMessageUnavailableError
  → row marked dead
  → stage MESSAGE_REMOVED
  → no failure reaction
```

Deletion is treated as user cancellation/removal, not ordinary system failure.

---

# 20. Operational maturity path

This architecture is already coherent. The future work is not about correcting the core shape. It is about maturing the operational envelope around it.

Useful next areas of maturation are:

## 20.1 Provider availability model

The runtime STT design supports provider fallback.

The startup model currently expects all provider credentials to be available.

A future production refinement would separate:

```text
required providers
optional fallback providers
disabled providers
configured but unhealthy providers
```

This would allow graceful degraded operation.

Example:

```text
Gemini unavailable
  → OpenAI + Deepgram still usable

Deepgram key absent
  → run with Gemini/OpenAI only

all providers absent
  → fail startup
```

## 20.2 Queue observability

As usage grows, the queue deserves operational visibility.

Useful inspection surfaces:

```text
queued jobs by content type
retry-waiting jobs by delay bucket
dead jobs by error class
average processing latency
STT provider failure counts
oldest queued job
stale processing rows
outbound send failures
unsupported content frequency
```

This can begin as CLI queries and later become a dashboard.

## 20.3 Retention and cleanup

Because SQLite stores raw payloads, normalized payloads, diagnostics, results, and outbox state, retention strategy matters.

Possible policies:

```text
retain done rows for N days
retain dead rows longer
retain raw_update_json only when service dumps are enabled
compact old result_json
archive before deletion
keep unsupported diagnostics for product planning
```

The right policy depends on privacy, disk, and debugging needs.

## 20.4 Schema evolution

During active development, schema verification is reasonable.

Once production data must survive version upgrades, formal migrations become important.

A mature migration model would include:

```text
schema version table
explicit DDL migration steps
backward-compatible column additions
migration tests
backup-before-migrate workflow
operator-visible migration logs
```

This is future hardening, not a flaw in the current architectural concept.

## 20.5 Product expansion

The current product value is concentrated in voice/audio transcription plus the first real text-derived action workflow: answering questions.

Expense, income, and reminder action processors are currently temporary workflow placeholders.

That is fine if intentional.

Future expansion could add:

```text
photo understanding
document summarization
animation/video support
text queue-driven replies
media group aggregation
multi-message conversation tasks
admin commands for queue inspection
```

The existing architecture can absorb these because content processors are already separated from ingestion and queue mechanics.

---

# 21. Architectural strengths

## 21.1 Correct webhook boundary

The webhook path does not perform heavy work inline.

It persists and returns quickly.

That is the correct design for Telegram media workloads.

## 21.2 Durable acceptance

The system records work before attempting processing.

Once in SQLite, the job can survive restarts and transient failures.

## 21.3 Rich state model

The queue row stores enough information to retry, resume, diagnose, replay, and avoid duplicates.

This makes the queue a durable workflow system rather than a simple list.

## 21.4 Excellent audio idempotency

The combination of `processing_text` and `outbound_json` prevents repeated STT work and duplicate transcript replies.

This is one of the strongest production-oriented parts of the design.

## 21.5 State-driven UX

Telegram reactions reflect actual processing state while remaining best-effort.

The user sees progress without reactions becoming a correctness dependency.

## 21.6 Practical debugging loop

Raw service dumps and replay tooling make real Telegram payloads reproducible.

This is extremely useful for investigating edge cases.

## 21.7 Clean failure taxonomy

The error model lets processors communicate intent without owning queue transitions.

The daemon remains the state authority.

---

# 22. Final system mental model

This system is best understood as:

```text
a durable Telegram intake service
+ a SQLite-backed source-message daemon
+ a SQLite-backed derived-action daemon
+ a state-driven Telegram reaction UX layer
+ a voice/audio transcription pipeline
+ a canonical text action-detection pipeline
+ action-specific workflows such as question answering
+ retry, replay, and idempotent output semantics
```

The central loop is:

```text
external Telegram event
  → normalized durable row
  → source daemon claim
  → source stage mutation
  → content-specific processing
  → canonical processing_text
  → action detection
  → durable action jobs
  → action daemon claim
  → action-specific processing
  → persisted progress
  → Telegram feedback
  → terminal state
```

The architecture is held together by a small set of deep contracts:

```text
QueueJobData is the birth certificate.
payload_json is the normalized intent.
raw_update_json is the evidence.
processing_text is reusable semantic progress.
outbound_json is the external side-effect ledger.
action_catalog is the durable vocabulary of supported action families.
action_detection_runs is the classifier audit trail.
incoming_message_actions is the durable child-work queue.
status is queue mechanics.
stage is workflow meaning.
reaction is visible but non-authoritative feedback.
processor exceptions are state-transition signals.
```

The most important architectural sentence is:

> **The database rows are the system’s memory; the daemons are the workers; `bot.py` is the ingress edge; processors are specialized hands; and Telegram reactions are the visible heartbeat of the pipeline.**

This is a sound local-first architecture.

Its core is not fragile or accidental.

It is an intentionally durable, observable, resumable Telegram processing system whose main future work is operational maturation: richer provider configuration, queue visibility, retention strategy, production-grade migrations, and expanded processors.

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from bot_libs.queue_models import (
    QueueInsertResult,
    QueueJobData,
    STATUS_DEAD,
    STATUS_DONE,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    stable_json_dumps,
)
from bot_libs.stage_names import (
    STAGE_DONE,
    STAGE_FAILED,
    STAGE_RETRY_WAITING,
)

SCHEMA_VERSION = 3
QUEUE_TABLE_NAME = "telegram_queue"
EXPECTED_INDEX_NAMES = frozenset(
    {
        "idx_telegram_queue_status_available",
        "idx_telegram_queue_chat_message",
        "idx_telegram_queue_media_group",
        "idx_telegram_queue_file_unique",
        "idx_telegram_queue_supported_status",
    }
)
EXPECTED_COLUMNS = frozenset(
    {
        "id",
        "status",
        "stage",
        "stage_updated_at",
        "stage_detail",
        "job_kind",
        "content_type",
        "is_supported",
        "update_id",
        "chat_id",
        "chat_type",
        "chat_title",
        "message_id",
        "message_thread_id",
        "media_group_id",
        "telegram_date",
        "from_id",
        "from_first_name",
        "from_username",
        "from_language_code",
        "is_bot_sender",
        "sender_chat_id",
        "sender_chat_type",
        "sender_chat_title",
        "reply_to_message_id",
        "has_file",
        "file_id",
        "file_unique_id",
        "file_name",
        "mime_type",
        "file_size",
        "text",
        "processing_text",
        "caption",
        "payload_json",
        "raw_update_json",
        "attempts",
        "max_attempts",
        "available_at",
        "locked_at",
        "locked_by",
        "last_error",
        "result_json",
        "outbound_json",
        "created_at",
        "updated_at",
        "finished_at",
    }
)

DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5000
DEFAULT_SQLITE_JOURNAL_MODE = "WAL"
DEFAULT_SQLITE_SYNCHRONOUS = "NORMAL"
DEFAULT_QUEUE_STALE_LOCK_TIMEOUT_SECONDS = 300
SQLITE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

ALLOWED_JOURNAL_MODES = frozenset(
    {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}
)
ALLOWED_SYNCHRONOUS = frozenset({"OFF", "NORMAL", "FULL", "EXTRA"})

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS telegram_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'processing', 'done', 'dead')),

    stage TEXT NOT NULL DEFAULT 'QUEUED',
    stage_updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stage_detail TEXT,

    job_kind TEXT NOT NULL DEFAULT 'incoming_message',

    content_type TEXT NOT NULL
        CHECK (
            content_type IN (
                'text',
                'photo',
                'document',
                'voice',
                'audio',
                'animation',
                'video',
                'video_note',
                'sticker',
                'unknown'
            )
        ),

    is_supported INTEGER NOT NULL DEFAULT 1
        CHECK (is_supported IN (0, 1)),

    update_id INTEGER NOT NULL UNIQUE,

    chat_id INTEGER NOT NULL,
    chat_type TEXT,
    chat_title TEXT,

    message_id INTEGER,
    message_thread_id INTEGER,
    media_group_id TEXT,
    telegram_date TEXT NOT NULL,

    from_id INTEGER,
    from_first_name TEXT,
    from_username TEXT,
    from_language_code TEXT,
    is_bot_sender INTEGER NOT NULL DEFAULT 0
        CHECK (is_bot_sender IN (0, 1)),

    sender_chat_id INTEGER,
    sender_chat_type TEXT,
    sender_chat_title TEXT,

    reply_to_message_id INTEGER,

    has_file INTEGER NOT NULL DEFAULT 0
        CHECK (has_file IN (0, 1)),
    file_id TEXT,
    file_unique_id TEXT,
    file_name TEXT,
    mime_type TEXT,
    file_size INTEGER,

    text TEXT,
    processing_text TEXT,
    caption TEXT,

    payload_json TEXT NOT NULL,
    raw_update_json TEXT NOT NULL,

    attempts INTEGER NOT NULL DEFAULT 0
        CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 12
        CHECK (max_attempts >= 1),
    available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    locked_at TEXT,
    locked_by TEXT,
    last_error TEXT,
    result_json TEXT,
    outbound_json TEXT,

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_telegram_queue_status_available
ON telegram_queue(status, available_at, id);

CREATE INDEX IF NOT EXISTS idx_telegram_queue_chat_message
ON telegram_queue(chat_id, message_id);

CREATE INDEX IF NOT EXISTS idx_telegram_queue_media_group
ON telegram_queue(media_group_id);

CREATE INDEX IF NOT EXISTS idx_telegram_queue_file_unique
ON telegram_queue(file_unique_id);

CREATE INDEX IF NOT EXISTS idx_telegram_queue_supported_status
ON telegram_queue(is_supported, status, available_at, id);
""".strip()

INSERT_QUEUE_JOB_SQL = """
INSERT INTO telegram_queue (
    status,
    stage,
    stage_detail,
    job_kind,
    content_type,
    is_supported,
    update_id,
    chat_id,
    chat_type,
    chat_title,
    message_id,
    message_thread_id,
    media_group_id,
    telegram_date,
    from_id,
    from_first_name,
    from_username,
    from_language_code,
    is_bot_sender,
    sender_chat_id,
    sender_chat_type,
    sender_chat_title,
    reply_to_message_id,
    has_file,
    file_id,
    file_unique_id,
    file_name,
    mime_type,
    file_size,
    text,
    processing_text,
    caption,
    payload_json,
    raw_update_json,
    max_attempts
) VALUES (
    :status,
    :stage,
    :stage_detail,
    :job_kind,
    :content_type,
    :is_supported,
    :update_id,
    :chat_id,
    :chat_type,
    :chat_title,
    :message_id,
    :message_thread_id,
    :media_group_id,
    :telegram_date,
    :from_id,
    :from_first_name,
    :from_username,
    :from_language_code,
    :is_bot_sender,
    :sender_chat_id,
    :sender_chat_type,
    :sender_chat_title,
    :reply_to_message_id,
    :has_file,
    :file_id,
    :file_unique_id,
    :file_name,
    :mime_type,
    :file_size,
    :text,
    :processing_text,
    :caption,
    :payload_json,
    :raw_update_json,
    :max_attempts
)
""".strip()


class QueueStoreError(RuntimeError):
    pass


class SchemaVerificationError(QueueStoreError):
    pass


@dataclass(frozen=True, slots=True)
class SQLiteSettings:
    db_path: str
    busy_timeout_ms: int = DEFAULT_SQLITE_BUSY_TIMEOUT_MS
    journal_mode: str = DEFAULT_SQLITE_JOURNAL_MODE
    synchronous: str = DEFAULT_SQLITE_SYNCHRONOUS
    stale_lock_timeout_seconds: int = DEFAULT_QUEUE_STALE_LOCK_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        db_path = self.db_path.strip()
        if not db_path:
            raise ValueError("SQL_TELEGRAM_FILE must not be empty")
        if db_path == ":memory:":
            raise ValueError(
                "SQL_TELEGRAM_FILE=:memory: is not supported; use a temporary file DB"
            )
        if self.busy_timeout_ms < 0:
            raise ValueError("SQLITE_BUSY_TIMEOUT_MS must be >= 0")
        if self.stale_lock_timeout_seconds < 1:
            raise ValueError("QUEUE_STALE_LOCK_TIMEOUT_SECONDS must be >= 1")

        journal_mode = self.journal_mode.upper()
        if journal_mode not in ALLOWED_JOURNAL_MODES:
            raise ValueError(
                "SQLITE_JOURNAL_MODE must be one of "
                + ", ".join(sorted(ALLOWED_JOURNAL_MODES))
            )

        synchronous = self.synchronous.upper()
        if synchronous not in ALLOWED_SYNCHRONOUS:
            raise ValueError(
                "SQLITE_SYNCHRONOUS must be one of "
                + ", ".join(sorted(ALLOWED_SYNCHRONOUS))
            )

        object.__setattr__(self, "db_path", db_path)
        object.__setattr__(self, "journal_mode", journal_mode)
        object.__setattr__(self, "synchronous", synchronous)


@dataclass(frozen=True, slots=True)
class DatabaseInitResult:
    db_path: str
    created_file: bool
    schema_version: int


@dataclass(frozen=True, slots=True)
class SchemaCheckResult:
    db_path: str
    user_version: int
    integrity_check_result: str | None
    table_exists: bool
    missing_columns: tuple[str, ...]
    missing_indexes: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return (
            self.table_exists
            and self.user_version == SCHEMA_VERSION
            and not self.missing_columns
            and not self.missing_indexes
            and (self.integrity_check_result in {None, "ok"})
        )

    def describe(self) -> str:
        parts = [f"db_path={self.db_path}"]
        if not self.table_exists:
            parts.append(f"missing table {QUEUE_TABLE_NAME}")
        if self.user_version != SCHEMA_VERSION:
            parts.append(
                f"schema version mismatch expected={SCHEMA_VERSION} actual={self.user_version}"
            )
        if self.missing_columns:
            parts.append("missing columns=" + ",".join(self.missing_columns))
        if self.missing_indexes:
            parts.append("missing indexes=" + ",".join(self.missing_indexes))
        if self.integrity_check_result not in {None, "ok"}:
            parts.append(f"integrity_check={self.integrity_check_result}")
        return "; ".join(parts)


class SQLiteQueueStore:
    def __init__(self, settings: SQLiteSettings) -> None:
        self.settings = settings

    def create_schema(self, *, create_parent_dir: bool = False) -> DatabaseInitResult:
        if create_parent_dir:
            self._ensure_parent_dir()

        created_file = not self._db_file_exists()
        try:
            with self._connect(allow_create=True) as conn:
                conn.executescript(SCHEMA_SQL)
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                conn.commit()
        except sqlite3.Error as exc:
            raise QueueStoreError(
                f"SQLite schema creation failed for {self.settings.db_path}: {exc}"
            ) from exc

        self.verify_schema()
        return DatabaseInitResult(
            db_path=self.settings.db_path,
            created_file=created_file,
            schema_version=SCHEMA_VERSION,
        )

    def reset_database(self, *, create_parent_dir: bool = False) -> DatabaseInitResult:
        if create_parent_dir:
            self._ensure_parent_dir()

        for path in self._sidecar_paths():
            if path.exists():
                path.unlink()

        return self.create_schema(create_parent_dir=create_parent_dir)

    def inspect_schema(self, *, run_integrity_check: bool) -> SchemaCheckResult:
        self._require_existing_db_file()

        try:
            with self._connect() as conn:
                table_exists = (
                    conn.execute(
                        """
                        SELECT 1
                        FROM sqlite_master
                        WHERE type='table' AND name=?
                        """,
                        (QUEUE_TABLE_NAME,),
                    ).fetchone()
                    is not None
                )

                columns = set()
                if table_exists:
                    columns = {
                        row["name"]
                        for row in conn.execute(f"PRAGMA table_info({QUEUE_TABLE_NAME})")
                    }

                index_names = {
                    row["name"]
                    for row in conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type='index' AND tbl_name=?
                        """,
                        (QUEUE_TABLE_NAME,),
                    )
                }

                user_version = int(
                    conn.execute("PRAGMA user_version").fetchone()[0]
                )
                integrity_check_result = None
                if run_integrity_check:
                    integrity_check_result = conn.execute(
                        "PRAGMA integrity_check"
                    ).fetchone()[0]
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

        return SchemaCheckResult(
            db_path=self.settings.db_path,
            user_version=user_version,
            integrity_check_result=integrity_check_result,
            table_exists=table_exists,
            missing_columns=tuple(sorted(EXPECTED_COLUMNS - columns)),
            missing_indexes=tuple(sorted(EXPECTED_INDEX_NAMES - index_names)),
        )

    def verify_schema(self) -> SchemaCheckResult:
        result = self.inspect_schema(run_integrity_check=False)
        if not result.ok:
            raise SchemaVerificationError(result.describe())
        return result

    def migrate_schema(self) -> SchemaCheckResult:
        """
        Future hook for installed-base schema upgrades.

        During active development, runtime startup intentionally only verifies
        the current schema version and does not mutate existing databases.
        """
        return self.verify_schema()

    def check_database(self) -> SchemaCheckResult:
        result = self.inspect_schema(run_integrity_check=True)
        if not result.ok:
            raise SchemaVerificationError(result.describe())
        return result

    def insert_queue_job(self, job: QueueJobData) -> QueueInsertResult:
        try:
            with self._connect() as conn:
                cursor = conn.execute(INSERT_QUEUE_JOB_SQL, job.as_db_params())
                queue_id = int(cursor.lastrowid)
                conn.commit()
        except sqlite3.IntegrityError as exc:
            if _is_duplicate_update_error(exc):
                return QueueInsertResult(queue_id=None, duplicate=True)
            raise
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

        return QueueInsertResult(queue_id=queue_id, duplicate=False)

    def get_max_update_id(self) -> int | None:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    f"SELECT MAX(update_id) AS max_update_id FROM {QUEUE_TABLE_NAME}"
                ).fetchone()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

        if row is None:
            return None

        value = row["max_update_id"]
        return int(value) if value is not None else None

    def get_next_available_at(self) -> datetime | None:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT available_at
                    FROM telegram_queue
                    WHERE status = ?
                    ORDER BY available_at, id
                    LIMIT 1
                    """,
                    (STATUS_QUEUED,),
                ).fetchone()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

        if row is None:
            return None

        available_at = row["available_at"]
        if available_at is None:
            return None

        return _parse_sqlite_datetime(available_at)

    def get_queue_job(self, job_id: int) -> dict[str, Any] | None:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    f"SELECT * FROM {QUEUE_TABLE_NAME} WHERE id = ?",
                    (job_id,),
                ).fetchone()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

        return dict(row) if row is not None else None

    def requeue_stale_processing_jobs(
        self,
        *,
        older_than_seconds: int,
        worker_name: str | None = None,
    ) -> int:
        del worker_name

        if older_than_seconds < 1:
            raise ValueError("older_than_seconds must be >= 1")

        stale_cutoff = _sqlite_timestamp(
            datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)
        )

        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE telegram_queue
                    SET
                        status = ?,
                        available_at = CURRENT_TIMESTAMP,
                        locked_at = NULL,
                        locked_by = NULL,
                        last_error = ?,
                        result_json = NULL,
                        finished_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status = ?
                      AND (locked_at IS NULL OR locked_at <= ?)
                    """,
                    (
                        STATUS_QUEUED,
                        "stale_lock_recovered",
                        STATUS_PROCESSING,
                        stale_cutoff,
                    ),
                )
                conn.commit()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

        return int(cursor.rowcount)

    def claim_next_job(self, *, worker_name: str) -> dict[str, Any] | None:
        conn: sqlite3.Connection | None = None
        try:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """
                    SELECT *
                    FROM telegram_queue
                    WHERE status = ?
                      AND available_at <= CURRENT_TIMESTAMP
                    ORDER BY id
                    LIMIT 1
                    """,
                    (STATUS_QUEUED,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return None

                conn.execute(
                    """
                    UPDATE telegram_queue
                    SET
                        status = ?,
                        locked_at = CURRENT_TIMESTAMP,
                        locked_by = ?,
                        attempts = attempts + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (STATUS_PROCESSING, worker_name, row["id"]),
                )
                claimed_row = conn.execute(
                    "SELECT * FROM telegram_queue WHERE id = ?",
                    (row["id"],),
                ).fetchone()
                conn.commit()
        except sqlite3.Error as exc:
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            raise self._coerce_sqlite_error(exc) from exc
        except Exception:
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
            raise

        return dict(claimed_row) if claimed_row is not None else None

    def make_retry_waiting_jobs_due(
        self,
        *,
        exclude_job_id: int | None = None,
        content_type: str | None = None,
        max_delay_seconds: int | None = None,
        limit: int = 10,
    ) -> int:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        cutoff_available_at = None
        if max_delay_seconds is not None:
            if max_delay_seconds < 1:
                raise ValueError("max_delay_seconds must be >= 1")
            cutoff_available_at = _sqlite_timestamp(
                datetime.now(timezone.utc) + timedelta(seconds=max_delay_seconds)
            )

        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE telegram_queue
                    SET
                        available_at = CURRENT_TIMESTAMP,
                        stage = ?,
                        stage_detail = ?,
                        stage_updated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id IN (
                        SELECT id
                        FROM telegram_queue
                        WHERE status = ?
                          AND attempts > 0
                          AND available_at > CURRENT_TIMESTAMP
                          AND (? IS NULL OR available_at <= ?)
                          AND (? IS NULL OR content_type = ?)
                          AND (? IS NULL OR id != ?)
                        ORDER BY id
                        LIMIT ?
                    )
                    """,
                    (
                        STAGE_RETRY_WAITING,
                        "fast_retry_after_success",
                        STATUS_QUEUED,
                        cutoff_available_at,
                        cutoff_available_at,
                        content_type,
                        content_type,
                        exclude_job_id,
                        exclude_job_id,
                        limit,
                    ),
                )
                conn.commit()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

        return int(cursor.rowcount)

    def mark_job_done(
        self,
        job_id: int,
        *,
        result_json: Any = None,
        stage: str = STAGE_DONE,
    ) -> None:
        self._update_job_status(
            job_id,
            status=STATUS_DONE,
            stage=stage,
            result_json=_normalize_json_text(result_json),
            finished=True,
            last_error=None,
        )

    def mark_job_for_retry(
        self,
        job_id: int,
        *,
        delay_seconds: int,
        last_error: str,
    ) -> None:
        available_at = _sqlite_timestamp(
            datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        )
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE telegram_queue
                    SET
                        status = CASE
                            WHEN attempts >= max_attempts THEN ?
                            ELSE ?
                        END,
                        stage = CASE
                            WHEN attempts >= max_attempts THEN ?
                            ELSE ?
                        END,
                        stage_updated_at = CURRENT_TIMESTAMP,
                        available_at = CASE
                            WHEN attempts >= max_attempts THEN available_at
                            ELSE ?
                        END,
                        locked_at = NULL,
                        locked_by = NULL,
                        last_error = ?,
                        result_json = NULL,
                        finished_at = CASE
                            WHEN attempts >= max_attempts THEN CURRENT_TIMESTAMP
                            ELSE NULL
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        STATUS_DEAD,
                        STATUS_QUEUED,
                        STAGE_FAILED,
                        STAGE_RETRY_WAITING,
                        available_at,
                        last_error,
                        job_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise QueueStoreError(f"Queue job not found for retry id={job_id}")
                conn.commit()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

    def mark_job_dead(
        self,
        job_id: int,
        *,
        last_error: str,
        result_json: Any = None,
        stage: str = STAGE_FAILED,
    ) -> None:
        self._update_job_status(
            job_id,
            status=STATUS_DEAD,
            stage=stage,
            result_json=_normalize_json_text(result_json),
            finished=True,
            last_error=last_error,
        )

    def set_job_stage(
        self,
        job_id: int,
        *,
        stage: str,
        stage_detail: str | None = None,
    ) -> None:
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE telegram_queue
                    SET
                        stage = ?,
                        stage_detail = ?,
                        stage_updated_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (stage, stage_detail, job_id),
                )
                if cursor.rowcount != 1:
                    raise QueueStoreError(f"Queue job not found id={job_id}")
                conn.commit()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

    def set_job_processing_text(
        self,
        job_id: int,
        *,
        processing_text: str,
        stage: str | None = None,
        stage_detail: str | None = None,
    ) -> None:
        stage_sql = "stage = COALESCE(?, stage),"
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    f"""
                    UPDATE telegram_queue
                    SET
                        processing_text = ?,
                        {stage_sql}
                        stage_detail = ?,
                        stage_updated_at = CASE
                            WHEN ? IS NULL THEN stage_updated_at
                            ELSE CURRENT_TIMESTAMP
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (processing_text, stage, stage_detail, stage, job_id),
                )
                if cursor.rowcount != 1:
                    raise QueueStoreError(f"Queue job not found id={job_id}")
                conn.commit()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

    def set_job_outbound_json(
        self,
        job_id: int,
        *,
        outbound_json: Any,
        stage: str | None = None,
        stage_detail: str | None = None,
    ) -> None:
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    UPDATE telegram_queue
                    SET
                        outbound_json = ?,
                        stage = COALESCE(?, stage),
                        stage_detail = ?,
                        stage_updated_at = CASE
                            WHEN ? IS NULL THEN stage_updated_at
                            ELSE CURRENT_TIMESTAMP
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        _normalize_json_text(outbound_json),
                        stage,
                        stage_detail,
                        stage,
                        job_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise QueueStoreError(f"Queue job not found id={job_id}")
                conn.commit()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

    def schema_sql(self) -> str:
        return SCHEMA_SQL

    def _update_job_status(
        self,
        job_id: int,
        *,
        status: str,
        stage: str,
        result_json: str | None,
        finished: bool,
        last_error: str | None,
    ) -> None:
        finished_sql = "CURRENT_TIMESTAMP" if finished else "NULL"
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    f"""
                    UPDATE telegram_queue
                    SET
                        status = ?,
                        stage = ?,
                        stage_updated_at = CURRENT_TIMESTAMP,
                        locked_at = NULL,
                        locked_by = NULL,
                        result_json = ?,
                        last_error = ?,
                        finished_at = {finished_sql},
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (status, stage, result_json, last_error, job_id),
                )
                if cursor.rowcount != 1:
                    raise QueueStoreError(f"Queue job not found id={job_id}")
                conn.commit()
        except sqlite3.Error as exc:
            raise self._coerce_sqlite_error(exc) from exc

    def _connect(self, *, allow_create: bool = False) -> sqlite3.Connection:
        if not allow_create:
            self._require_existing_db_file()

        db_target, use_uri = self._connection_target(allow_create=allow_create)
        conn: sqlite3.Connection | None = None

        try:
            conn = sqlite3.connect(
                db_target,
                timeout=self.settings.busy_timeout_ms / 1000,
                uri=use_uri,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(f"PRAGMA journal_mode = {self.settings.journal_mode}")
            conn.execute(f"PRAGMA synchronous = {self.settings.synchronous}")
            conn.execute(f"PRAGMA busy_timeout = {self.settings.busy_timeout_ms}")
            return conn
        except sqlite3.Error:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            raise

    def _connection_target(self, *, allow_create: bool) -> tuple[str, bool]:
        if allow_create:
            return self.settings.db_path, False

        db_uri = Path(self.settings.db_path).expanduser().resolve().as_uri()
        return f"{db_uri}?mode=rw", True

    def _coerce_sqlite_error(self, exc: sqlite3.Error) -> QueueStoreError:
        if not self._db_file_exists():
            return SchemaVerificationError(
                f"SQLite database file does not exist: {self.settings.db_path}"
            )

        if _is_schema_error(exc):
            result = self.inspect_schema(run_integrity_check=False)
            return SchemaVerificationError(result.describe())

        return QueueStoreError(
            f"SQLite operation failed for {self.settings.db_path}: {exc}"
        )

    def _require_existing_db_file(self) -> None:
        if not self._db_file_exists():
            raise SchemaVerificationError(
                f"SQLite database file does not exist: {self.settings.db_path}"
            )

    def _ensure_parent_dir(self) -> None:
        db_path = Path(self.settings.db_path)
        if db_path.parent == Path("."):
            return
        db_path.parent.mkdir(parents=True, exist_ok=True)

    def _db_file_exists(self) -> bool:
        return Path(self.settings.db_path).exists()

    def _sidecar_paths(self) -> tuple[Path, Path, Path]:
        db_path = Path(self.settings.db_path)
        return (
            db_path,
            db_path.with_name(db_path.name + "-wal"),
            db_path.with_name(db_path.name + "-shm"),
        )


def build_sqlite_settings_from_env(*, db_path: str | None = None) -> SQLiteSettings:
    resolved_db_path = (db_path or os.environ.get("SQL_TELEGRAM_FILE", "")).strip()
    busy_timeout_ms = int(
        os.environ.get("SQLITE_BUSY_TIMEOUT_MS", DEFAULT_SQLITE_BUSY_TIMEOUT_MS)
    )
    journal_mode = os.environ.get(
        "SQLITE_JOURNAL_MODE", DEFAULT_SQLITE_JOURNAL_MODE
    )
    synchronous = os.environ.get(
        "SQLITE_SYNCHRONOUS", DEFAULT_SQLITE_SYNCHRONOUS
    )
    stale_lock_timeout_seconds = int(
        os.environ.get(
            "QUEUE_STALE_LOCK_TIMEOUT_SECONDS",
            DEFAULT_QUEUE_STALE_LOCK_TIMEOUT_SECONDS,
        )
    )
    return SQLiteSettings(
        db_path=resolved_db_path,
        busy_timeout_ms=busy_timeout_ms,
        journal_mode=journal_mode,
        synchronous=synchronous,
        stale_lock_timeout_seconds=stale_lock_timeout_seconds,
    )


def _is_duplicate_update_error(exc: sqlite3.IntegrityError) -> bool:
    message = str(exc)
    return "telegram_queue.update_id" in message or "update_id" in message


def _is_schema_error(exc: sqlite3.Error) -> bool:
    message = str(exc).lower()
    return "no such table" in message or "no such column" in message


def _add_column_if_missing(
    conn: sqlite3.Connection,
    existing_columns: set[str],
    column_name: str,
    column_definition: str,
) -> None:
    if column_name in existing_columns:
        return
    conn.execute(
        f"ALTER TABLE {QUEUE_TABLE_NAME} ADD COLUMN {column_name} {column_definition}"
    )
    existing_columns.add(column_name)


def _normalize_json_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return stable_json_dumps(value)


def _sqlite_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(SQLITE_TIMESTAMP_FORMAT)


def _parse_sqlite_datetime(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("timestamp text must not be empty")

    try:
        if "T" in text or text.endswith("Z") or "+" in text:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            parsed = datetime.strptime(text, SQLITE_TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise ValueError(f"Unsupported SQLite timestamp: {value!r}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)

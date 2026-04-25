#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from telegram import Update  # noqa: E402

from bot_libs.queue_extract import extract_queue_job  # noqa: E402
from bot_libs.queue_models import DEFAULT_QUEUE_MAX_ATTEMPTS  # noqa: E402
from bot_libs.sql import (  # noqa: E402
    QueueStoreError,
    SQLiteQueueStore,
    SchemaVerificationError,
    build_sqlite_settings_from_env,
)


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    dump_path: str
    original_update_id: int | None
    replay_update_id: int | None
    content_type: str | None
    queue_id: int | None
    duplicate: bool = False
    skipped: bool = False


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    total_files: int
    inserted: int
    duplicates: int
    skipped: int
    records: tuple[ReplayRecord, ...]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay Telegram service dump JSON files into the queue database"
    )
    parser.add_argument(
        "--dump-dir",
        required=True,
        help="Directory created by bot.py --save-dump-dir",
    )
    parser.add_argument(
        "--db",
        help="Override SQL_TELEGRAM_FILE for this command",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_QUEUE_MAX_ATTEMPTS,
        help="max_attempts value to store on replayed queue rows",
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Delete and recreate the queue database before replaying dumps",
    )
    parser.add_argument(
        "--preserve-update-ids",
        action="store_true",
        help="Use update_id values from the dump files instead of rewriting them",
    )
    return parser.parse_args(argv)


def replay_dump_directory(
    dump_dir: str | Path,
    *,
    queue_store: SQLiteQueueStore,
    max_attempts: int,
    preserve_update_ids: bool = False,
) -> ReplaySummary:
    if not 1 <= max_attempts <= DEFAULT_QUEUE_MAX_ATTEMPTS:
        raise ValueError(
            "max_attempts must be between 1 and "
            f"{DEFAULT_QUEUE_MAX_ATTEMPTS}"
        )

    dump_paths = _collect_dump_paths(dump_dir)
    original_payloads = [_load_dump_payload(path) for path in dump_paths]
    original_update_ids = [_extract_update_id(payload) for payload in original_payloads]
    payloads = original_payloads

    if not preserve_update_ids:
        payloads = _rewrite_update_ids(payloads, queue_store=queue_store)

    records: list[ReplayRecord] = []
    inserted = 0
    duplicates = 0
    skipped = 0

    for dump_path, original_update_id, payload in zip(
        dump_paths,
        original_update_ids,
        payloads,
        strict=True,
    ):
        replay_update_id = _extract_update_id(payload)

        update = Update.de_json(payload, bot=None)
        job = extract_queue_job(update, max_attempts=max_attempts)
        if job is None:
            skipped += 1
            records.append(
                ReplayRecord(
                    dump_path=str(dump_path),
                    original_update_id=original_update_id,
                    replay_update_id=replay_update_id,
                    content_type=None,
                    queue_id=None,
                    skipped=True,
                )
            )
            continue

        insert_result = queue_store.insert_queue_job(job)
        if insert_result.duplicate:
            duplicates += 1
        else:
            inserted += 1

        records.append(
            ReplayRecord(
                dump_path=str(dump_path),
                original_update_id=original_update_id,
                replay_update_id=replay_update_id,
                content_type=job.content_type,
                queue_id=insert_result.queue_id,
                duplicate=insert_result.duplicate,
            )
        )

    return ReplaySummary(
        total_files=len(dump_paths),
        inserted=inserted,
        duplicates=duplicates,
        skipped=skipped,
        records=tuple(records),
    )


def _collect_dump_paths(dump_dir: str | Path) -> tuple[Path, ...]:
    root = Path(dump_dir).expanduser()
    if not root.exists():
        raise ValueError(f"Dump directory does not exist: {root}")
    if not root.is_dir():
        raise ValueError(f"Dump path is not a directory: {root}")

    dump_paths = tuple(sorted(path for path in root.iterdir() if path.suffix == ".json"))
    if not dump_paths:
        raise ValueError(f"No JSON dump files found in {root}")
    return dump_paths


def _load_dump_payload(path: Path) -> dict[str, object]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Dump file does not contain a JSON object: {path}")
    return data


def _rewrite_update_ids(
    payloads: Sequence[dict[str, object]],
    *,
    queue_store: SQLiteQueueStore,
) -> list[dict[str, object]]:
    if not payloads:
        return []

    existing_max_update_id = queue_store.get_max_update_id() or 0
    payload_max_update_id = max(_extract_update_id(payload) or 0 for payload in payloads)
    next_update_id = max(existing_max_update_id, payload_max_update_id) + 1

    rewritten_payloads: list[dict[str, object]] = []
    for offset, payload in enumerate(payloads):
        rewritten = copy.deepcopy(payload)
        rewritten["update_id"] = next_update_id + offset
        rewritten_payloads.append(rewritten)

    return rewritten_payloads


def _extract_update_id(payload: dict[str, object]) -> int | None:
    update_id = payload.get("update_id")
    return int(update_id) if isinstance(update_id, int) else None


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        queue_store = SQLiteQueueStore(build_sqlite_settings_from_env(db_path=args.db))
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    try:
        if args.reset_db:
            result = queue_store.reset_database(create_parent_dir=True)
            print(
                f"Reset {result.db_path} "
                f"(created_file={result.created_file}, schema_version={result.schema_version})"
            )
        else:
            queue_store.verify_schema()

        summary = replay_dump_directory(
            args.dump_dir,
            queue_store=queue_store,
            max_attempts=args.max_attempts,
            preserve_update_ids=args.preserve_update_ids,
        )
    except (ValueError, QueueStoreError, SchemaVerificationError) as exc:
        print(f"Replay failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Replayed {summary.total_files} dump file(s): "
        f"inserted={summary.inserted} duplicates={summary.duplicates} skipped={summary.skipped}"
    )
    for record in summary.records:
        status = "skipped" if record.skipped else "duplicate" if record.duplicate else "inserted"
        print(
            f"{status} dump={record.dump_path} "
            f"original_update_id={record.original_update_id} "
            f"replay_update_id={record.replay_update_id} "
            f"content_type={record.content_type} "
            f"queue_id={record.queue_id}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

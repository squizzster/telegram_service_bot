#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bot_libs.sql import (  # noqa: E402
    SCHEMA_VERSION,
    SQLiteQueueStore,
    SchemaVerificationError,
    build_sqlite_settings_from_env,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize and inspect the bot queue DB")
    parser.add_argument(
        "--db",
        help="Override SQL_TELEGRAM_FILE for this command",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Required for destructive reset operations",
    )

    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--init", action="store_true", help="Create the DB schema")
    actions.add_argument("--check", action="store_true", help="Verify DB integrity")
    actions.add_argument(
        "--drop-delete-current-db",
        action="store_true",
        help="Delete the current DB file and sidecars, then recreate the DB schema",
    )
    actions.add_argument("--schema", action="store_true", help="Print the expected DDL")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        store = SQLiteQueueStore(build_sqlite_settings_from_env(db_path=args.db))
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    try:
        if args.schema:
            print(f"Schema version: {SCHEMA_VERSION}")
            print(store.schema_sql())
            return 0

        if args.init:
            result = store.create_schema(create_parent_dir=True)
            print(
                f"Initialized {result.db_path} "
                f"(created_file={result.created_file}, schema_version={result.schema_version})"
            )
            return 0

        if args.check:
            result = store.check_database()
            print(
                f"Check passed for {result.db_path} "
                f"(schema_version={result.user_version}, integrity_check={result.integrity_check_result})"
            )
            return 0

        if args.drop_delete_current_db:
            if not args.force:
                print("--drop-delete-current-db requires --force", file=sys.stderr)
                return 1
            result = store.reset_database(create_parent_dir=True)
            print(
                f"Dropped and recreated {result.db_path} "
                f"(created_file={result.created_file}, schema_version={result.schema_version})"
            )
            return 0
    except SchemaVerificationError as exc:
        print(f"Schema verification failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Command failed: {exc}", file=sys.stderr)
        return 1

    print("No action selected", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

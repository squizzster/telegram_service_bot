from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


class TelegramServiceDumper:
    def __init__(self, dump_dir: str | Path) -> None:
        self.dump_dir = Path(dump_dir).expanduser()
        self.dump_dir.mkdir(parents=True, exist_ok=True)

    def dump_update(
        self,
        data: object,
        *,
        raw_json_bytes: bytes | None = None,
    ) -> Path:
        destination = self._build_destination(data)
        temp_path = destination.with_name(f"{destination.name}.tmp")

        if raw_json_bytes is None:
            temp_path.write_text(_pretty_json(data), encoding="utf-8")
        else:
            temp_path.write_bytes(raw_json_bytes)

        temp_path.replace(destination)
        return destination

    def _build_destination(self, data: object) -> Path:
        update_id, chat_id, message_id = _extract_update_fields(data)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        unique_suffix = uuid4().hex[:10]
        filename = (
            f"{timestamp}_"
            f"update_{_filename_part(update_id)}_"
            f"chat_{_filename_part(chat_id)}_"
            f"message_{_filename_part(message_id)}_"
            f"{unique_suffix}.json"
        )
        return self.dump_dir / filename


def _extract_update_fields(data: object) -> tuple[object | None, object | None, object | None]:
    if not isinstance(data, dict):
        return None, None, None

    update_id = data.get("update_id")
    message_payload = _message_payload(data)
    if not isinstance(message_payload, dict):
        return update_id, None, None

    message_id = message_payload.get("message_id")
    chat_payload = message_payload.get("chat")
    chat_id = chat_payload.get("id") if isinstance(chat_payload, dict) else None
    return update_id, chat_id, message_id


def _message_payload(data: dict[str, object]) -> object | None:
    for field_name in (
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "business_message",
    ):
        payload = data.get(field_name)
        if payload is not None:
            return payload
    return None


def _filename_part(value: object | None) -> str:
    if value is None:
        return "unknown"

    text = str(value).strip()
    if not text:
        return "unknown"

    return text.replace("/", "_")


def _pretty_json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n"

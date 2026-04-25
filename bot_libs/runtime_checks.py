from __future__ import annotations

import asyncio
from dataclasses import dataclass
import importlib.util
import logging
import os
import shutil
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_OPENAI_MODEL_ID = "gpt-4o-transcribe"
DEFAULT_GEMINI_MODEL_ID = "gemini-3-flash-preview"
OPENAI_MODEL_ENV_KEYS = (
    "OPENAI_TRANSCRIBE_MODEL",
    "OPENAI_AUDIO_MODEL",
    "OPENAI_MODEL",
    "VOICE_TRANSCRIBE_MODEL",
)
GEMINI_MODEL_ENV_KEYS = (
    "MODEL_ID",
    "GEMINI_MODEL",
    "GEMINI_AUDIO_MODEL",
    "VOICE_TRANSCRIBE_MODEL",
)
REQUIRED_IMPORTS: tuple[tuple[str, str], ...] = (
    ("telegram", "telegram"),
    ("telegram.ext", "telegram.ext"),
    ("starlette", "starlette"),
    ("uvicorn", "uvicorn"),
    ("httpx", "httpx"),
    ("openai", "openai"),
    ("deepgram", "deepgram"),
    ("google", "google"),
    ("sqlite3", "sqlite3"),
)
REQUIRED_COMMANDS: tuple[tuple[str, str], ...] = (
    ("ffprobe", "ffprobe"),
)
REQUIRED_ENV_VARS: tuple[str, ...] = (
    "TELEGRAM_BOT_KEY",
    "TELEGRAM_WEBHOOK_SECRET",
    "SQL_TELEGRAM_FILE",
    "OPENAI_API_KEY",
    "DEEPGRAM_API_KEY",
    "GEMINI_API_KEY",
)
DEFAULT_CHECK_TIMEOUT_SECONDS = 10.0


class RuntimeCheckError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RuntimeCheckReport:
    imports_checked: tuple[str, ...]
    commands_checked: tuple[str, ...]
    env_vars_checked: tuple[str, ...]
    credentials_checked: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RuntimeCheckProfile:
    imports: tuple[tuple[str, str], ...]
    commands: tuple[tuple[str, str], ...]
    env_vars: tuple[str, ...]
    credentials: tuple[str, ...]


async def run_runtime_system_checks(
    *,
    component: str,
    timeout_seconds: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> RuntimeCheckReport:
    profile = _profile_for_component(component)
    failures: list[str] = []
    failures.extend(_check_imports(profile.imports))
    failures.extend(_check_commands(profile.commands))
    failures.extend(_check_required_env(profile.env_vars))
    if failures:
        raise RuntimeCheckError(_format_failures(failures))

    credential_failures = await _check_credentials(
        timeout_seconds=timeout_seconds,
        credential_labels=profile.credentials,
    )
    if credential_failures:
        raise RuntimeCheckError(_format_failures(credential_failures))

    report = RuntimeCheckReport(
        imports_checked=tuple(label for label, _ in profile.imports),
        commands_checked=tuple(label for label, _ in profile.commands),
        env_vars_checked=profile.env_vars,
        credentials_checked=profile.credentials,
    )
    log.info(
        "Runtime system checks passed component=%s imports=%s commands=%s "
        "env_vars=%s credentials=%s",
        component,
        len(report.imports_checked),
        len(report.commands_checked),
        len(report.env_vars_checked),
        len(report.credentials_checked),
    )
    return report


def _profile_for_component(component: str) -> RuntimeCheckProfile:
    if component == "action-daemon":
        return RuntimeCheckProfile(
            imports=(
                ("telegram", "telegram"),
                ("openai", "openai"),
                ("sqlite3", "sqlite3"),
            ),
            commands=(),
            env_vars=("TELEGRAM_BOT_KEY", "SQL_TELEGRAM_FILE", "OPENAI_API_KEY"),
            credentials=("telegram", "OPEN_AI"),
        )

    return RuntimeCheckProfile(
        imports=REQUIRED_IMPORTS,
        commands=REQUIRED_COMMANDS,
        env_vars=REQUIRED_ENV_VARS,
        credentials=("telegram", "OPEN_AI", "deepgram", "google"),
    )


def _check_imports(
    imports: tuple[tuple[str, str], ...] = REQUIRED_IMPORTS,
) -> list[str]:
    failures: list[str] = []
    for label, module_name in imports:
        if importlib.util.find_spec(module_name) is None:
            failures.append(f"missing Python module {label!r} ({module_name})")
    return failures


def _check_commands(
    commands: tuple[tuple[str, str], ...] = REQUIRED_COMMANDS,
) -> list[str]:
    failures: list[str] = []
    for label, command in commands:
        if shutil.which(command) is None:
            failures.append(f"missing required executable {label!r} on PATH")
    return failures


def _check_required_env(env_vars: tuple[str, ...] = REQUIRED_ENV_VARS) -> list[str]:
    failures: list[str] = []
    for env_var in env_vars:
        if not (os.getenv(env_var) or "").strip():
            failures.append(f"missing required environment variable {env_var}")
    return failures


async def _check_credentials(
    *,
    timeout_seconds: float,
    credential_labels: tuple[str, ...] = ("telegram", "OPEN_AI", "deepgram", "google"),
) -> list[str]:
    try:
        import httpx
    except ImportError as exc:
        return [f"missing Python module 'httpx' ({exc})"]

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds, connect=min(timeout_seconds, 5.0)),
    ) as client:
        checks = tuple(_credential_check(client, label) for label in credential_labels)
        results = await asyncio.gather(*checks, return_exceptions=True)

    failures: list[str] = []
    for result in results:
        if result is None:
            continue
        if isinstance(result, BaseException):
            failures.append(
                f"credential check crashed: {_sanitize_secret_text(str(result))}"
            )
            continue
        failures.append(result)
    return failures


def _credential_check(client: Any, label: str) -> Any:
    if label == "telegram":
        return _check_telegram_key(client)
    if label == "OPEN_AI":
        return _check_openai_key(client)
    if label == "deepgram":
        return _check_deepgram_key(client)
    if label == "google":
        return _check_gemini_key(client)
    raise ValueError(f"unknown credential check label {label!r}")


async def _check_telegram_key(client: Any) -> str | None:
    token = _env("TELEGRAM_BOT_KEY")
    response = await client.get(f"https://api.telegram.org/bot{token}/getMe")
    if response.status_code == 200:
        return None
    return _http_failure("Telegram bot token", response)


async def _check_openai_key(client: Any) -> str | None:
    model_id = _resolve_first_env(OPENAI_MODEL_ENV_KEYS, DEFAULT_OPENAI_MODEL_ID)
    response = await client.get(
        f"https://api.openai.com/v1/models/{model_id}",
        headers={"Authorization": f"Bearer {_env('OPENAI_API_KEY')}"},
    )
    if response.status_code == 200:
        return None
    return _http_failure(f"OpenAI API key/model {model_id!r}", response)


async def _check_deepgram_key(client: Any) -> str | None:
    response = await client.get(
        "https://api.deepgram.com/v1/projects",
        headers={"Authorization": f"Token {_env('DEEPGRAM_API_KEY')}"},
    )
    if response.status_code == 200:
        return None
    return _http_failure("Deepgram API key", response)


async def _check_gemini_key(client: Any) -> str | None:
    model_id = _normalize_gemini_model_id(
        _resolve_first_env(GEMINI_MODEL_ENV_KEYS, DEFAULT_GEMINI_MODEL_ID)
    )
    response = await client.get(
        f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}",
        params={"key": _env("GEMINI_API_KEY")},
    )
    if response.status_code == 200:
        return None
    return _http_failure(f"Google Gemini API key/model {model_id!r}", response)


def _normalize_gemini_model_id(model_id: str) -> str:
    return model_id.removeprefix("models/").strip()


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _resolve_first_env(env_keys: tuple[str, ...], default: str) -> str:
    for env_key in env_keys:
        value = _env(env_key)
        if value:
            return value
    return default


def _http_failure(label: str, response: Any) -> str:
    return f"{label} check failed status={response.status_code}: {_response_text(response)}"


def _response_text(response: Any) -> str:
    try:
        payload = response.json()
    except Exception:
        text = (getattr(response, "text", "") or "").strip()
        return text[:300] if text else "empty response body"

    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message") or error.get("type") or error.get("code")
            if isinstance(message, str) and message.strip():
                return message.strip()[:300]
        description = payload.get("description")
        if isinstance(description, str) and description.strip():
            return description.strip()[:300]

    return str(payload)[:300]


def _format_failures(failures: list[str]) -> str:
    joined = _sanitize_secret_text("; ".join(failures))
    return f"runtime system check failed: {joined}"


def _sanitize_secret_text(value: str) -> str:
    sanitized = value
    for env_var in REQUIRED_ENV_VARS:
        secret = _env(env_var)
        if secret:
            sanitized = sanitized.replace(secret, f"<{env_var}:redacted>")
    return sanitized

from __future__ import annotations

import logging
import re

TRACE_LEVEL = 5
BOT_TOKEN_RE = re.compile(r"(bot)([0-9]+:[A-Za-z0-9_-]+)")


def install_trace_level() -> None:
    if logging.getLevelName(TRACE_LEVEL) != "TRACE":
        logging.addLevelName(TRACE_LEVEL, "TRACE")


def configure_app_logging(*, debug: bool, trace: bool = False) -> None:
    install_trace_level()
    level = TRACE_LEVEL if trace else logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        level=level,
    )
    root_logger = logging.getLogger()
    redaction_filter = RedactSecretsFilter()
    root_logger.addFilter(redaction_filter)
    for handler in root_logger.handlers:
        handler.addFilter(redaction_filter)

    third_party_level = logging.DEBUG if trace else logging.WARNING
    logging.getLogger("httpcore").setLevel(third_party_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("openai._base_client").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    # PTB debug output includes bot-token-bearing URLs and raw Telegram objects.
    # Keep it out of DEBUG/TRACE; application logs provide safe summaries.
    logging.getLogger("telegram.Bot").setLevel(logging.WARNING)
    logging.getLogger("telegram._bot").setLevel(logging.WARNING)


class RedactSecretsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_secrets(record.msg)
        if record.args:
            record.args = _redact_args(record.args)
        return True


def redact_secrets(value: object) -> object:
    if not isinstance(value, str):
        return value
    return BOT_TOKEN_RE.sub(r"\1<redacted>", value)


def _redact_args(args: object) -> object:
    if isinstance(args, tuple):
        return tuple(redact_secrets(item) for item in args)
    if isinstance(args, dict):
        return {key: redact_secrets(value) for key, value in args.items()}
    return redact_secrets(args)

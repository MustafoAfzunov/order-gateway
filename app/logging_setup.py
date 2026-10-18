"""Structured key=value logging.

Every line from the gateway looks like this:
    ts=2026-10-18T12:00:00.123+00:00 level=INFO event=CLAIMED signal_id=sig-1 \
        client_order_id=CO-sig-1 ...   (one line in reality)
so `grep signal_id=sig-1` gives the full story of one instruction, and the lines are
still easy to parse by machine.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any

LOGGER_NAME = "gateway"
_FIELDS = "kv_fields"  # attribute name used to carry our fields on the LogRecord


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    signal_id: str | None,
    client_order_id: str | None,
    level: int = logging.INFO,
    exc_info: bool = False,
    **fields: Any,
) -> None:
    """The only way the gateway logs. signal_id and client_order_id are required keyword
    arguments, so no line can leave them out. Use None when they aren't known (e.g. an update
    for an order we don't have); it's rendered as '-' so the key is still there."""
    logger.log(level, event, exc_info=exc_info, extra={_FIELDS: {
        "event": event, "signal_id": signal_id, "client_order_id": client_order_id, **fields,
    }})


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value)
    # Quote anything that would break key=value parsing.
    if not text or any(c in text for c in ' ="\n'):
        return json.dumps(text)
    return text


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds")
        fields = getattr(record, _FIELDS, None) or {"event": record.getMessage()}
        line = f"ts={ts} level={record.levelname} " + " ".join(
            f"{k}={_fmt(v)}" for k, v in fields.items()
        )
        if record.exc_info:
            line += " exc=" + json.dumps(self.formatException(record.exc_info))
        return line


def configure_logging(level: int = logging.INFO) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    if not any(isinstance(h.formatter, KeyValueFormatter) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(KeyValueFormatter())
        logger.addHandler(handler)
    logger.propagate = False  # don't print every line twice through the root logger

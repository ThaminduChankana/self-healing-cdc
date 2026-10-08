"""Structured JSON logging with correlation identifiers.

One JSON object per line on stdout — directly ingestible by CloudWatch Logs,
Cloud Logging or OpenSearch. Correlation fields (event_id, repair_id,
correlation_id, source_table) are promoted to top-level keys so a single event
can be followed across the validator, the AI worker and downstream.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from src.common.config import get_settings

_CORRELATION_FIELDS = ("event_id", "repair_id", "correlation_id", "source_table")


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "message": record.getMessage(),
        }
        for name in _CORRELATION_FIELDS:
            value = getattr(record, name, None)
            if value is not None:
                entry[name] = value
        extra = getattr(record, "extra_data", None)
        if extra:
            entry["data"] = extra
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(f"cdc.{component}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(getattr(logging, get_settings().log_level, logging.INFO))
    return logger


class ComponentLogger:
    """Logger wrapper: `log.info("msg", event_id=..., any_key=...)`.

    Correlation keys become top-level JSON fields; any other keyword arguments
    are emitted under `data`. Never pass secrets as keyword arguments.
    """

    def __init__(self, component: str):
        self.component = component
        self.logger = get_logger(component)

    def _log(self, level: int, msg: str, exc_info: bool = False, **kwargs: Any) -> None:
        extra: dict[str, Any] = {"component": self.component}
        for name in _CORRELATION_FIELDS:
            extra[name] = kwargs.pop(name, None)
        extra["extra_data"] = kwargs or None
        self.logger.log(level, msg, extra=extra, exc_info=exc_info)

    def debug(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, msg, **kwargs)

    def exception(self, msg: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, msg, exc_info=True, **kwargs)

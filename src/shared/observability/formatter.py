# shared/logging/formatter.py

import json
import logging
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Convert log records into JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process_id": record.process,
            "thread_name": record.threadName,
            "message": record.getMessage(),
        }

        exception_data = getattr(
            record,
            "exception_data",
            None,
        )

        if exception_data:
            log_data["exception"] = exception_data

        if record.exc_info:
            log_data["traceback"] = self.formatException(
                record.exc_info
            )

        return json.dumps(
            log_data,
            ensure_ascii=False,
            default=str,
        )


class TextFormatter(logging.Formatter):
    """Format logs as readable text."""

    def __init__(self) -> None:
        super().__init__(
            fmt=(
                "%(asctime)s | "
                "%(levelname)-8s | "
                "%(name)s | "
                "%(filename)s:%(lineno)d | "
                "%(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
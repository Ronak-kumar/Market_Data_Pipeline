"""Clean, readable log formatters for the market data pipeline."""

import logging
from datetime import datetime


class CleanTextFormatter(logging.Formatter):
    """Format logs as clean, readable text with timestamp, level, logger, location, and message."""

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

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record, handling exceptions cleanly."""
        # Format the base message
        message = super().format(record)

        # Append exception info if present
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            message = f"{message}\n{exc_text}"

        return message


class CompactTextFormatter(logging.Formatter):
    """Compact single-line formatter for console output."""

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            message = f"{message}\n{exc_text}"
        return message
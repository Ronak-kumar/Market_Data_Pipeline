"""Logging handlers for console and file output."""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from shared.observability.formatter import CleanTextFormatter, CompactTextFormatter
from shared.observability.logging_config import logging_settings


def create_console_handler() -> logging.Handler:
    """Create a console handler with compact formatting."""
    handler = logging.StreamHandler()
    handler.setLevel(logging_settings.console.level)
    handler.setFormatter(CompactTextFormatter())
    return handler


def create_file_handler() -> logging.Handler:
    """Create a rotating file handler with detailed formatting."""
    log_dir = Path(logging_settings.file.directory)
    log_dir.mkdir(parents=True, exist_ok=True)

    handler = TimedRotatingFileHandler(
        filename=log_dir / logging_settings.file.filename,
        when=logging_settings.file.rotation.when,
        interval=logging_settings.file.rotation.interval,
        backupCount=logging_settings.file.rotation.backup_count,
        encoding="utf-8",
    )

    handler.setLevel(logging_settings.file.level)
    handler.setFormatter(CleanTextFormatter())
    return handler
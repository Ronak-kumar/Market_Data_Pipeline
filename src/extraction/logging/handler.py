import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from extraction.logging.formatter import JsonFormatter, TextFormatter
from extraction.logging.logging_config import logging_settings


def create_console_handler() -> logging.Handler:
    handler = logging.StreamHandler()
    handler.setLevel(logging_settings.console.level)

    formatter = (
        JsonFormatter()
        if logging_settings.format.type.lower() == "json"
        else TextFormatter()
    )

    handler.setFormatter(formatter)
    return handler


def create_file_handler() -> logging.Handler:
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

    formatter = (
        JsonFormatter()
        if logging_settings.format.type.lower() == "json"
        else TextFormatter()
    )

    handler.setFormatter(formatter)
    return handler
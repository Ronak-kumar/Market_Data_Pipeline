import logging

from extraction.observability.handler import (
    create_console_handler,
    create_file_handler,
)
from extraction.observability.logging_config import logging_settings


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Returns a configured logger.
    """

    logger_name = name or logging_settings.logger.name

    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging_settings.level)
    logger.propagate = logging_settings.logger.propagate

    if logging_settings.console.enabled:
        logger.addHandler(create_console_handler())

    if logging_settings.file.enabled:
        logger.addHandler(create_file_handler())

    return logger
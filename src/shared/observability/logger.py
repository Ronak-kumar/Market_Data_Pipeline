"""Centralized logger configuration with exception hook for unhandled exceptions."""

import logging
import sys
import threading
from typing import Optional

from shared.observability.handler import create_console_handler, create_file_handler
from shared.observability.logging_config import logging_settings

# Thread-safe initialization lock
_logger_lock = threading.Lock()
_initialized_loggers: set[str] = set()


def _configure_logger(logger: logging.Logger) -> None:
    """Configure a logger with standard handlers."""
    logger.setLevel(logging_settings.level)
    logger.propagate = logging_settings.logger.propagate
    logger.handlers.clear()

    if logging_settings.console.enabled:
        logger.addHandler(create_console_handler())

    if logging_settings.file.enabled:
        logger.addHandler(create_file_handler())


def _setup_exception_hook() -> None:
    """Install a global exception hook to log unhandled exceptions."""
    def exception_hook(exc_type, exc_value, exc_traceback):
        """Log unhandled exceptions with full traceback."""
        if issubclass(exc_type, KeyboardInterrupt):
            # Allow KeyboardInterrupt to exit normally
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        # Get the root logger and ensure it has handlers
        logger = logging.getLogger("market_data_pipeline")
        if not logger.handlers:
            _configure_logger(logger)
        logger.critical(
            f"Unhandled exception: {exc_type.__name__}: {exc_value}",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

    sys.excepthook = exception_hook

    # Also handle thread exceptions
    def thread_exception_hook(args: threading.ExceptHookArgs) -> None:
        if issubclass(args.exc_type, KeyboardInterrupt):
            return
        logger = logging.getLogger("market_data_pipeline")
        if not logger.handlers:
            _configure_logger(logger)
        logger.critical(
            f"Unhandled thread exception: {args.exc_type.__name__}: {args.exc_value}",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    threading.excepthook = thread_exception_hook


# Initialize exception hook on module load
_setup_exception_hook()


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Returns a configured logger with console and file handlers.

    Args:
        name: Logger name (defaults to settings logger name)

    Returns:
        Configured logging.Logger instance
    """
    logger_name = name or logging_settings.logger.name

    logger = logging.getLogger(logger_name)

    # Thread-safe check-and-initialize
    with _logger_lock:
        if logger_name in _initialized_loggers:
            return logger

        _configure_logger(logger)
        _initialized_loggers.add(logger_name)

    return logger


def configure_root_logger() -> logging.Logger:
    """
    Configure the root logger - call once at application startup.

    Returns:
        The configured root logger
    """
    return get_logger()
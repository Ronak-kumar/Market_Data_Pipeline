"""Pydantic models for logging configuration."""

from pydantic import BaseModel


class RotationConfig(BaseModel):
    enabled: bool
    when: str
    interval: int
    backup_count: int


class FileLoggingConfig(BaseModel):
    enabled: bool
    level: str
    directory: str
    filename: str
    rotation: RotationConfig


class ConsoleLoggingConfig(BaseModel):
    enabled: bool
    level: str


class LoggerConfig(BaseModel):
    name: str
    propagate: bool


class LoggingConfig(BaseModel):
    level: str
    logger: LoggerConfig
    console: ConsoleLoggingConfig
    file: FileLoggingConfig
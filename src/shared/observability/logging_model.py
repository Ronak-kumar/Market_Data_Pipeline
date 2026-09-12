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


class JsonFormatConfig(BaseModel):
    timestamp: str
    level: str
    logger: str
    module: str
    function: str
    line: str
    process: str
    thread: str
    message: str


class FormatConfig(BaseModel):
    type: str
    text: str
    json: JsonFormatConfig


class RequestLoggingConfig(BaseModel):
    enabled: bool
    log_request_body: bool
    log_response_body: bool
    log_headers: bool


class PerformanceConfig(BaseModel):
    enabled: bool
    slow_request_threshold_ms: int


class ExceptionLoggingConfig(BaseModel):
    include_traceback: bool


class AccessLogConfig(BaseModel):
    enabled: bool


class LoggingConfig(BaseModel):
    level: str
    logger: LoggerConfig
    console: ConsoleLoggingConfig
    file: FileLoggingConfig
    format: FormatConfig
    request_logging: RequestLoggingConfig
    performance: PerformanceConfig
    exception_logging: ExceptionLoggingConfig
    access_log: AccessLogConfig
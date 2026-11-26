"""Load logging configuration from YAML."""

from functools import lru_cache
from pathlib import Path
import yaml

from shared.observability.logging_model import LoggingConfig

LOGGING_PATH = Path(__file__).resolve().parent
LOGGING_YAML_PATH = LOGGING_PATH / "logging.yaml"


@lru_cache(maxsize=1)
def load_logging_config() -> LoggingConfig:
    """Load and validate logging configuration from YAML."""
    with open(LOGGING_YAML_PATH, "r") as file:
        data = yaml.safe_load(file) or {}
    return LoggingConfig.model_validate(data)


logging_settings = load_logging_config()
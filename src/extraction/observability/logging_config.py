from functools import lru_cache
from extraction.utils.yaml_loader import load_yaml_config
from pathlib import Path
from extraction.observability.logging_model import LoggingConfig

LOGGING_PATH  = Path(__file__).resolve().parent
LOGGING_YAML_PATH = LOGGING_PATH / "logging.yaml"

@lru_cache(maxsize=1)
def load_logging_config():
    yaml_data = load_yaml_config(LOGGING_YAML_PATH, LoggingConfig)
    
    return yaml_data


logging_settings = load_logging_config()

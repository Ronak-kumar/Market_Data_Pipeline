from functools import lru_cache
from pathlib import Path
from extraction.models import AppSettings
from extraction.utils import load_yaml_config

@lru_cache
def get_application_settings(YAML_PATH=None) -> AppSettings:
    if YAML_PATH is None:
        YAML_PATH = Path(__file__).parent / "settings.yaml"
    yaml_data = load_yaml_config(YAML_PATH, AppSettings)
    return yaml_data

app_settings = get_application_settings()

from functools import lru_cache
from pathlib import Path
from shared.config.settings_model import AppSettings
from shared.utils import load_config_with_env

@lru_cache
def get_application_settings(YAML_PATH=None) -> AppSettings:
    if YAML_PATH is None:
        YAML_PATH = Path(__file__).parent / "settings.yaml"
    return load_config_with_env(AppSettings, str(YAML_PATH))

app_settings = get_application_settings()

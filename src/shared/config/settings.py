from functools import lru_cache
from pathlib import Path
from shared.config.settings_model import AppSettings, BrokerConfiguration
from shared.utils import load_config_with_env

def get_application_settings(YAML_PATH=None) -> AppSettings:
    if YAML_PATH is None:
        YAML_PATH = Path(__file__).parent / "settings.yaml"
    
    # Load main settings
    settings = load_config_with_env(AppSettings, str(YAML_PATH))
    
    # Load broker config from separate file
    broker_config_path = Path(__file__).parent / "broker_config.yaml"
    if broker_config_path.exists():
        import yaml
        with open(broker_config_path, "r") as f:
            broker_data = yaml.safe_load(f) or {}
        
        # Convert broker data to BrokerConfiguration objects
        if broker_data:
            settings.broker_configuration = {
                name: BrokerConfiguration(**config) 
                for name, config in broker_data.items()
            }
    
    return settings

app_settings = get_application_settings()

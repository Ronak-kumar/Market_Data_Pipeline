from pathlib import Path
from shared.config.settings_model import AppSettings, BrokerConfiguration
from shared.utils import load_yaml_config
import yaml

def get_application_settings(YAML_PATH=None) -> AppSettings:
    if YAML_PATH is None:
        YAML_PATH = Path(__file__).parent / "settings.yaml"
    
    # Load main settings
    settings = load_yaml_config(str(YAML_PATH), AppSettings)
    
    # Load broker config from separate file
    broker_config_path = Path(__file__).parent / "broker_config.yaml"
    if broker_config_path.exists():
        with open(broker_config_path, "r") as f:
            broker_data = yaml.safe_load(f) or {}
        
        # Convert broker data to BrokerConfiguration objects
        if broker_data:
            settings.broker_configuration = {
                name: BrokerConfiguration(**{k: v for k, v in config.items() 
                    if k in ['processable_segments', 'session_bounds', 'symbol_name_mapping']})
                for name, config in broker_data.items()
            }
    
    return settings

app_settings = get_application_settings()
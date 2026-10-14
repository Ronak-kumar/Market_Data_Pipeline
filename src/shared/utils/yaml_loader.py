import yaml
from pydantic import BaseModel
import logging
from typing import Type, TypeVar

# Use basic logging to avoid circular import during config loading
logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)

def load_yaml_config(file_path: str, model: Type[T]) -> T:
    """Load config from YAML file only (no env merge)."""
    logger.debug("Loading YAML config", extra={"file_path": file_path, "model": model.__name__})
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file) or {}
        
        instance = model.model_validate(data)
        logger.info("YAML config loaded and validated", extra={"file_path": file_path, "model": model.__name__})
        return instance
    except Exception as e:
        logger.error("YAML config load/validate failed", extra={"file_path": file_path, "model": model.__name__, "error": str(e)}, exc_info=True)
        raise

def load_config_with_env(model: Type[T], yaml_path: str = None) -> T:
    """Load config from YAML (optional) and merge with environment variables.
    
    Priority: env vars > YAML > defaults
    Uses BaseSettings automatic env var loading.
    """
    logger.debug("Loading config with env", extra={"yaml_path": yaml_path, "model": model.__name__})
    
    # Load YAML data if provided
    yaml_data = {}
    if yaml_path:
        with open(yaml_path, "r") as file:
            yaml_data = yaml.safe_load(file) or {}
    
    # Use model_validate which allows BaseSettings to overlay env vars on top
    # This way: env vars > YAML > defaults
    instance = model.model_validate(yaml_data)
    logger.info("Config loaded with env override", extra={"model": model.__name__})
    return instance

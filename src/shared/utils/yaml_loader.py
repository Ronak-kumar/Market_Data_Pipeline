import yaml
from pydantic import BaseModel
import logging
from typing import Type, TypeVar

# Use basic logging to avoid circular import during config loading
logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)


def load_yaml_config(file_path: str, model: Type[T]) -> T:
    """Load config from YAML file only (no env merge)."""
    logger.debug(f"Loading YAML config: file_path={file_path}, model={model.__name__}")
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file) or {}

        instance = model.model_validate(data)
        logger.info(f"YAML config loaded and validated: file_path={file_path}, model={model.__name__}")
        return instance
    except Exception as e:
        logger.error(f"YAML config load/validate failed: file_path={file_path}, model={model.__name__}, error={e}")
        raise
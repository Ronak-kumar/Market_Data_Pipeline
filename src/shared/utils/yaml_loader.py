import yaml
from pydantic import BaseModel
import logging

# Use basic logging to avoid circular import during config loading
logger = logging.getLogger(__name__)


def load_yaml_config(file_path: str, model: BaseModel) -> BaseModel:
    logger.debug("Loading YAML config", extra={"file_path": file_path, "model": model.__name__})
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)

        result = model.model_validate(data)
        logger.info("YAML config loaded and validated", extra={"file_path": file_path, "model": model.__name__})
        return result
    except Exception as e:
        logger.error("YAML config load/validate failed", extra={"file_path": file_path, "model": model.__name__, "error": str(e)}, exc_info=True)
        raise
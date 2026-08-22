import yaml
from pydantic import BaseModel

def load_yaml_config(file_path: str, model: BaseModel) -> BaseModel:
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
        
    return model.model_validate(data)
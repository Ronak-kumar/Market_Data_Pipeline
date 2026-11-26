"""Config Editor API Routes - Edit settings.yaml, broker_config.yaml, and .env files."""
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import yaml
from pydantic import BaseModel

from shared.config import app_settings
from shared.utils import load_yaml_config
from shared.config.settings_model import AppSettings, BrokerConfiguration

router = APIRouter(prefix="/Pipelline_Dashboard", tags=["Pipelline_Dashboard"])

# Template setup
TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Config file paths
CONFIG_DIR = Path(__file__).parent.parent / "shared" / "config"
SETTINGS_YAML = CONFIG_DIR / "settings.yaml"
BROKER_CONFIG_YAML = CONFIG_DIR / "broker_config.yaml"
ACCESS_TOKEN_JSON = CONFIG_DIR / "access_token.json"
ENV_FILE = Path(__file__).parent.parent.parent / ".env"

# Available options
BROKER_TYPES = ["upstox", "groww", "dhan", "zerodha"]
CLOUD_PROVIDERS = ["aws"]
DB_TARGETS = ["clickhouse", "postgresql", "timescaledb"]

# All available segments per broker (including disabled ones shown as checkboxes)
BROKER_AVAILABLE_SEGMENTS = {
    "upstox": [
        "MCX_FO", "BCD_FO", "BSE_EQ", "NSE_INDEX", "GLOBAL_INDEX", 
        "NSE_EQ", "BSE_INDEX", "BSE_FO", "NCD_FO", "GLOBAL_INDICATOR", 
        "NSE_COM", "NSE_FO", "BSE_COM", "NSE_CURRENCY", "BSE_CURRENCY"
    ],
    "groww": [
        "NSE_EQ", "BSE_FO", "BSE_INDEX", "NSE_FO", "BSE_EQ", 
        "MCX_FO", "NSE_INDEX", "BSE_COM", "NSE_COM", "NSE_CURRENCY", 
        "BSE_CURRENCY", "NCD_FO", "BCD_FO", "GLOBAL_INDEX", "GLOBAL_INDICATOR"
    ],
    "dhan": [
        "NSE_EQ", "NSE_FNO", "BSE_EQ", "BSE_FNO", "NSE_IDX", 
        "BSE_IDX", "MCX_FNO", "MCX_COM", "NSE_CUR", "BSE_CUR", 
        "NSE_COM", "BSE_COM", "NSE_FO", "BSE_FO", "NSE_INDEX", 
        "BSE_INDEX", "MCX_FO", "NSE_CURRENCY", "BSE_CURRENCY", 
        "NCD_FO", "BCD_FO", "GLOBAL_INDEX", "GLOBAL_INDICATOR"
    ],
    "zerodha": [
        "NSE_EQ", "NSE_FNO", "BSE_EQ", "BSE_FNO", "NSE_INDEX", 
        "BSE_INDEX", "MCX_FNO", "NSE_CURRENCY", "BSE_CURRENCY",
        "NSE_FO", "BSE_FO", "MCX_FO", "NSE_CUR", "BSE_CUR", 
        "NSE_COM", "BSE_COM", "NCD_FO", "BCD_FO", "GLOBAL_INDEX", "GLOBAL_INDICATOR"
    ],
}


def read_env_file() -> Dict[str, str]:
    """Read .env file and return key-value pairs."""
    env_vars = {}
    if ENV_FILE.exists():
        with open(ENV_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip()
    return env_vars


def write_env_file(env_vars: Dict[str, str]) -> None:
    """Write .env file from key-value pairs."""
    lines = []
    for key, value in env_vars.items():
        lines.append(f"{key}={value}")
    with open(ENV_FILE, "w") as f:
        f.write("\n".join(lines) + "\n")


def read_access_token_file() -> Dict[str, str]:
    """Read access_token.json file and return key-value pairs."""
    if ACCESS_TOKEN_JSON.exists():
        with open(ACCESS_TOKEN_JSON, "r") as f:
            return json.load(f) or {}
    return {"api_key": "", "access_token": ""}


def write_access_token_file(data: Dict[str, str]) -> None:
    """Write access_token.json file from key-value pairs."""
    with open(ACCESS_TOKEN_JSON, "w") as f:
        json.dump(data, f, indent=2)


def read_yaml_file(path: Path) -> Dict[str, Any]:
    """Read YAML file."""
    if path.exists():
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    return {}


def write_yaml_file(path: Path, data: Dict[str, Any]) -> None:
    """Write YAML file preserving comments and formatting for broker_config.yaml."""
    if path.name == "broker_config.yaml":
        _write_broker_config_with_comments(path, data)
    else:
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def _write_broker_config_with_comments(path: Path, data: Dict[str, Any]) -> None:
    """Write broker_config.yaml preserving comment structure."""
    # Read the original file to preserve comments
    lines = []
    if path.exists():
        with open(path, "r") as f:
            lines = f.readlines()
    
    # Parse the original to find comment lines and structure
    # We'll rebuild the file maintaining comment positions
    output_lines = []
    
    # For each broker in data, write its section
    for broker_name in BROKER_TYPES:
        if broker_name not in data:
            continue
            
        broker_data = data[broker_name]
        
        # Find the broker section in original lines
        broker_start = None
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{broker_name}:"):
                broker_start = i
                break
        
        # Build the broker section
        output_lines.append(f"{broker_name}:\n")
        
        # Processable segments with comments
        available_segments = BROKER_AVAILABLE_SEGMENTS.get(broker_name, [])
        enabled_segments = broker_data.get("processable_segments", [])
        disabled_segments = [s for s in available_segments if s not in enabled_segments]
        
        output_lines.append(f"  # All available segments for {broker_name.capitalize()} - uncomment to enable\n")
        output_lines.append(f"  processable_segments:\n")
        
        for segment in enabled_segments:
            output_lines.append(f"  - {segment}\n")
        
        if disabled_segments:
            output_lines.append(f"  # Available but disabled segments (uncomment to enable):\n")
            for segment in disabled_segments:
                output_lines.append(f"  # - {segment}\n")
        
        # Session bounds
        session_bounds = broker_data.get("session_bounds", {})
        output_lines.append(f"  session_bounds:\n")
        if session_bounds:
            for segment, bounds in session_bounds.items():
                output_lines.append(f"    {segment}:\n")
                output_lines.append(f"      start: {bounds.get('start', '09:15')}\n")
                output_lines.append(f"      end: '{bounds.get('end', '15:40')}'\n")
        
        # Symbol name mapping
        symbol_mapping = broker_data.get("symbol_name_mapping", {})
        output_lines.append(f"  symbol_name_mapping:\n")
        if symbol_mapping:
            for key, value in symbol_mapping.items():
                output_lines.append(f"    {key}: {value}\n")
    
    with open(path, "w") as f:
        f.writelines(output_lines)


@router.get("/", response_class=HTMLResponse)
async def config_editor(request: Request):
    """Main config editor page."""
    settings = read_yaml_file(SETTINGS_YAML)
    broker_config = read_yaml_file(BROKER_CONFIG_YAML)
    env_vars = read_env_file()
    access_token_data = read_access_token_file()
    
    # Use Pydantic models to allow attribute access in templates
    from shared.config.settings_model import AppSettings
    
    # Convert settings dict to AppSettings model for attribute access
    try:
        settings_model = AppSettings.model_validate(settings)
    except Exception as e:
        # Fallback to raw dict
        settings_model = settings
    
    # Also provide JSON-serializable versions for JavaScript
    import json
    settings_json = json.dumps(settings)
    broker_config_json = json.dumps(broker_config)
    env_vars_json = json.dumps(env_vars)
    
    # Available segments for checkboxes
    broker_available_segments = BROKER_AVAILABLE_SEGMENTS
    
    # Manually render template to avoid TemplateResponse cache issues
    template = templates.get_template("config_editor.html")
    content = template.render({
        "request": request,
        "settings": settings_model,
        "broker_config": broker_config,
        "env_vars": env_vars,
        "settings_json": settings_json,
        "broker_config_json": broker_config_json,
        "env_vars_json": env_vars_json,
        "broker_types": BROKER_TYPES,
        "cloud_providers": CLOUD_PROVIDERS,
        "db_targets": DB_TARGETS,
        "broker_available_segments": broker_available_segments,
        "access_token": access_token_data.get("access_token", ""),
        "api_key": access_token_data.get("api_key", ""),
    })
    return HTMLResponse(content=content)


@router.get("/api/settings", response_class=JSONResponse)
async def get_settings():
    """Get current settings.yaml content."""
    return read_yaml_file(SETTINGS_YAML)


@router.post("/api/settings", response_class=JSONResponse)
async def save_settings(settings_data: Dict[str, Any]):
    """Save settings.yaml - supports partial updates."""
    # Read existing settings
    existing_settings = read_yaml_file(SETTINGS_YAML)
    
    # Deep merge the new data with existing
    def deep_merge(base: dict, update: dict) -> dict:
        result = base.copy()
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    merged_settings = deep_merge(existing_settings, settings_data)
    write_yaml_file(SETTINGS_YAML, merged_settings)
    
    # Reload app settings
    global app_settings
    from shared.config.settings import get_application_settings
    app_settings = get_application_settings()
    return {"success": True, "message": "Settings saved successfully"}


@router.get("/api/broker-config", response_class=JSONResponse)
async def get_broker_config():
    """Get current broker_config.yaml content."""
    return read_yaml_file(BROKER_CONFIG_YAML)


@router.post("/api/broker-config", response_class=JSONResponse)
async def save_broker_config(broker_data: Dict[str, Any]):
    """Save broker_config.yaml."""
    write_yaml_file(BROKER_CONFIG_YAML, broker_data)
    # Reload app settings
    global app_settings
    from shared.config.settings import get_application_settings
    app_settings = get_application_settings()
    return {"success": True, "message": "Broker config saved successfully"}


@router.get("/api/broker-config/{broker_name}", response_class=JSONResponse)
async def get_broker_config_by_name(broker_name: str):
    """Get specific broker configuration."""
    broker_config = read_yaml_file(BROKER_CONFIG_YAML)
    if broker_name not in broker_config:
        raise HTTPException(status_code=404, detail=f"Broker {broker_name} not found")
    return broker_config[broker_name]


@router.post("/api/broker-config/{broker_name}", response_class=JSONResponse)
async def save_broker_config_by_name(broker_name: str, broker_data: Dict[str, Any]):
    """Save/update specific broker configuration."""
    broker_config = read_yaml_file(BROKER_CONFIG_YAML)
    
    # Extract processable_segments to save as enabled list
    # Also store available_segments separately for the UI
    if broker_name in BROKER_AVAILABLE_SEGMENTS:
        broker_data["available_segments"] = BROKER_AVAILABLE_SEGMENTS[broker_name]
    
    broker_config[broker_name] = broker_data
    write_yaml_file(BROKER_CONFIG_YAML, broker_config)
    
    # Reload app settings
    global app_settings
    from shared.config.settings import get_application_settings
    app_settings = get_application_settings()
    
    return {"success": True, "message": f"Broker {broker_name} config saved successfully"}


@router.delete("/api/broker-config/{broker_name}", response_class=JSONResponse)
async def delete_broker_config(broker_name: str):
    """Delete a broker configuration."""
    broker_config = read_yaml_file(BROKER_CONFIG_YAML)
    if broker_name not in broker_config:
        raise HTTPException(status_code=404, detail=f"Broker {broker_name} not found")
    del broker_config[broker_name]
    write_yaml_file(BROKER_CONFIG_YAML, broker_config)
    
    # Reload app settings
    global app_settings
    from shared.config.settings import get_application_settings
    app_settings = get_application_settings()
    
    return {"success": True, "message": f"Broker {broker_name} config deleted successfully"}


@router.get("/api/env", response_class=JSONResponse)
async def get_env_vars():
    """Get current .env variables."""
    return read_env_file()


@router.post("/api/env", response_class=JSONResponse)
async def save_env_vars(env_data: Dict[str, str]):
    """Save .env variables."""
    current_env = read_env_file()
    current_env.update(env_data)
    write_env_file(current_env)
    return {"success": True, "message": "Environment variables saved successfully"}


# Cloud provider specific env fields
CLOUD_ENV_FIELDS = {
    "aws": [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY", 
        "AWS_REGION",
        "AWS_S3_BUCKET",
        "AWS_S3_ENDPOINT_URL",
    ],
    "gcp": [
        "GCP_PROJECT_ID",
        "GCP_CREDENTIALS_PATH",
        "GCP_BUCKET_NAME",
    ],
    "azure": [
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_STORAGE_KEY",
        "AZURE_CONTAINER_NAME",
    ],
}


# Database specific env fields
DB_ENV_FIELDS = {
    "clickhouse": [
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PORT",
        "CLICKHOUSE_DATABASE",
        "CLICKHOUSE_USERNAME",
        "CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_SECURE",
        "CLICKHOUSE_COMPRESSION",
        "CLICKHOUSE_CONNECT_TIMEOUT",
    ],
    "postgresql": [
        "POSTGRESQL_HOST",
        "POSTGRESQL_PORT",
        "POSTGRESQL_DATABASE",
        "POSTGRESQL_USERNAME",
        "POSTGRESQL_PASSWORD",
        "POSTGRESQL_SSL_MODE",
        "POSTGRESQL_SSL_CERT",
        "POSTGRESQL_SSL_KEY",
        "POSTGRESQL_SSL_CA",
        "POSTGRESQL_POOL_MIN",
        "POSTGRESQL_POOL_MAX",
        "POSTGRESQL_CONNECT_TIMEOUT",
        "POSTGRESQL_APPLICATION_NAME",
    ],
    "timescaledb": [
        "TIMESCALEDB_HOST",
        "TIMESCALEDB_PORT",
        "TIMESCALEDB_DATABASE",
        "TIMESCALEDB_USERNAME",
        "TIMESCALEDB_PASSWORD",
        "TIMESCALEDB_SSL_MODE",
        "TIMESCALEDB_SSL_CERT",
        "TIMESCALEDB_SSL_KEY",
        "TIMESCALEDB_SSL_CA",
        "TIMESCALEDB_POOL_MIN",
        "TIMESCALEDB_POOL_MAX",
        "TIMESCALEDB_CONNECT_TIMEOUT",
        "TIMESCALEDB_APPLICATION_NAME",
        "TIMESCALEDB_CHUNK_TIME_INTERVAL",
    ],
}


@router.get("/api/cloud-env-fields/{provider}", response_class=JSONResponse)
async def get_cloud_env_fields(provider: str):
    """Get env fields for a cloud provider."""
    fields = CLOUD_ENV_FIELDS.get(provider.lower(), [])
    env_vars = read_env_file()
    return {field: env_vars.get(field, "") for field in fields}


@router.get("/api/db-env-fields/{target}", response_class=JSONResponse)
async def get_db_env_fields(target: str):
    """Get env fields for a database target."""
    fields = DB_ENV_FIELDS.get(target.lower(), [])
    env_vars = read_env_file()
    return {field: env_vars.get(field, "") for field in fields}


@router.post("/api/cloud-env-fields/{provider}", response_class=JSONResponse)
async def save_cloud_env_fields(provider: str, env_data: Dict[str, str]):
    """Save cloud provider env fields."""
    env_vars = read_env_file()
    env_vars.update(env_data)
    write_env_file(env_vars)
    return {"success": True, "message": f"Cloud ({provider}) env vars saved successfully"}


@router.post("/api/db-env-fields/{target}", response_class=JSONResponse)
async def save_db_env_fields(target: str, env_data: Dict[str, str]):
    """Save database env fields."""
    env_vars = read_env_file()
    env_vars.update(env_data)
    write_env_file(env_vars)
    return {"success": True, "message": f"Database ({target}) env vars saved successfully"}


@router.get("/api/access-token", response_class=JSONResponse)
async def get_access_token():
    """Get current access_token.json content."""
    return read_access_token_file()


@router.post("/api/access-token", response_class=JSONResponse)
async def save_access_token(token_data: Dict[str, str]):
    """Save access_token.json - supports partial updates."""
    current_data = read_access_token_file()
    current_data.update(token_data)
    write_access_token_file(current_data)
    return {"success": True, "message": "Access token saved successfully"}
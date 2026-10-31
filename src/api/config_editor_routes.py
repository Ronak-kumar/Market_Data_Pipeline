"""Config editor routes - single page to edit settings.yaml"""
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from shared.observability import get_logger
from extraction.parser import parser_discovery, parser_registry
from extraction.parser.adapters import upstox, groww, dhan, zerodha
from shared.config.settings_model import BrokerConfiguration

# Ensure parsers are discovered at module load time
parser_discovery()

logger = get_logger(__name__)

router = APIRouter(prefix="/config-editor", tags=["config-editor"])

templates = Jinja2Templates(directory="src/api/templates")

SETTINGS_PATH = Path("src/shared/config/settings.yaml")
BROKER_CONFIG_PATH = Path("src/shared/config/broker_config.yaml")

# All known segments for the UI - now loaded dynamically per broker
def get_broker_segments(broker_name: str) -> List[str]:
    """Get available segments for a specific broker from their parser."""
    try:
        parser = parser_registry.get(broker_name.lower())()
        if hasattr(parser, 'AVAILABLE_SEGMENTS'):
            return parser.AVAILABLE_SEGMENTS
        else:
            logger.warning(f"Parser {broker_name} doesn't have AVAILABLE_SEGMENTS attribute")
    except Exception as e:
        logger.warning(f"Could not load segments for {broker_name}: {e}")
    return []


def load_settings() -> Dict[str, Any]:
    """Load settings.yaml and broker_config.yaml as dict."""
    settings = {}
    if SETTINGS_PATH.exists():
        with open(SETTINGS_PATH, "r") as f:
            settings = yaml.safe_load(f) or {}
    
    # Load broker config from separate file
    if BROKER_CONFIG_PATH.exists():
        with open(BROKER_CONFIG_PATH, "r") as f:
            broker_data = yaml.safe_load(f) or {}
        if broker_data:
            settings.setdefault("broker_configuration", {})
            settings["broker_configuration"].update(broker_data)
    
    # Convert broker config dicts to BrokerConfiguration objects for compatibility
    if "broker_configuration" in settings:
        converted = {}
        for name, config in settings["broker_configuration"].items():
            if isinstance(config, dict):
                converted[name] = BrokerConfiguration(**config)
            else:
                converted[name] = config
        settings["broker_configuration"] = converted
    
    # Load environment variables for database and cloud settings
    settings.setdefault("database_settings", {})
    settings.setdefault("cloud_settings", {})
    
    import os
    # Database settings from env
    settings["database_settings"]["clickhouse"] = {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "database": os.getenv("CLICKHOUSE_DATABASE", "market_data"),
        "username": os.getenv("CLICKHOUSE_USERNAME", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        "secure": os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true",
        "compression": os.getenv("CLICKHOUSE_COMPRESSION", "true").lower() == "true",
        "connect_timeout": int(os.getenv("CLICKHOUSE_CONNECT_TIMEOUT", "10")),
        "pool_min": int(os.getenv("CLICKHOUSE_POOL_MIN", "1")),
        "pool_max": int(os.getenv("CLICKHOUSE_POOL_MAX", "10")),
    }
    settings["database_settings"]["postgresql"] = {
        "host": os.getenv("POSTGRESQL_HOST", "localhost"),
        "port": int(os.getenv("POSTGRESQL_PORT", "5432")),
        "database": os.getenv("POSTGRESQL_DATABASE", "market_data"),
        "username": os.getenv("POSTGRESQL_USERNAME", "postgres"),
        "password": os.getenv("POSTGRESQL_PASSWORD", ""),
        "pool_min": int(os.getenv("POSTGRESQL_POOL_MIN", "1")),
        "pool_max": int(os.getenv("POSTGRESQL_POOL_MAX", "10")),
        "connect_timeout": int(os.getenv("POSTGRESQL_CONNECT_TIMEOUT", "10")),
        "ssl_mode": os.getenv("POSTGRESQL_SSL_MODE", "prefer"),
        "application_name": os.getenv("POSTGRESQL_APPLICATION_NAME", "market_data_pipeline"),
    }
    settings["database_settings"]["timescaledb"] = {
        "chunk_time_interval": os.getenv("TIMESCALEDB_CHUNK_TIME_INTERVAL", "1 day"),
    }
    
    # Cloud settings from env
    settings["cloud_settings"]["aws"] = {
        "access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "region": os.getenv("AWS_REGION", "us-east-1"),
        "s3_endpoint_url": os.getenv("AWS_S3_ENDPOINT_URL", ""),
    }
    settings["cloud_settings"]["azure"] = {
        "storage_account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME", ""),
        "storage_account_key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY", ""),
        "container_name": os.getenv("AZURE_STORAGE_CONTAINER_NAME", "marketdata-pipeline"),
    }
    settings["cloud_settings"]["gcp"] = {
        "project_id": os.getenv("GCP_PROJECT_ID", ""),
        "storage_bucket": os.getenv("GCP_STORAGE_BUCKET", "marketdata-pipeline"),
        "credentials_path": os.getenv("GCP_CREDENTIALS_PATH", ""),
    }
    
    return settings


def save_settings(data: Dict[str, Any]) -> None:
    """Save dict back to settings.yaml and broker_config.yaml."""
    # Save main settings
    main_settings = {k: v for k, v in data.items() if k != "broker_configuration"}
    with open(SETTINGS_PATH, "w") as f:
        yaml.dump(main_settings, f, default_flow_style=False, sort_keys=False, indent=2)
    
    # Save broker config to separate file
    broker_config = data.get("broker_configuration", {})
    if broker_config:
        with open(BROKER_CONFIG_PATH, "w") as f:
            yaml.dump(broker_config, f, default_flow_style=False, sort_keys=False, indent=2)
    
    logger.info("settings.yaml and broker_config.yaml updated")


def parse_segments(raw_list: List[str]) -> tuple[List[str], List[str]]:
    """Parse segments list into (enabled, disabled). Preserves order and comments."""
    enabled = []
    disabled = []
    for item in raw_list:
        s = str(item).strip()
        if s.startswith("#"):
            disabled.append(s[1:].strip())
        else:
            enabled.append(s)
    return enabled, disabled


def build_segments_list(enabled: List[str], disabled: List[str], all_segments: List[str] = None) -> List[str]:
    """Rebuild segments list with # prefix for disabled, preserving original order."""
    if all_segments is None:
        all_segments = []
    # Use ALL_SEGMENTS as canonical order
    result = []
    enabled_set = set(enabled)
    disabled_set = set(disabled)
    for seg in all_segments:
        if seg in enabled_set:
            result.append(seg)
        elif seg in disabled_set:
            result.append(f"# {seg}")
    # Add any extra segments not in ALL_SEGMENTS
    for seg in enabled:
        if seg not in all_segments and seg not in result:
            result.append(seg)
    for seg in disabled:
        if seg not in all_segments and f"# {seg}" not in result:
            result.append(f"# {seg}")
    return result


def _get_broker_dict(broker_config: Any) -> Dict[str, Any]:
    """Convert broker config to dict (handles both BrokerConfiguration object and dict)."""
    if hasattr(broker_config, 'processable_segments'):
        # It's a BrokerConfiguration object
        return {
            "processable_segments": broker_config.processable_segments,
            "session_bounds": broker_config.session_bounds,
            "symbol_name_mapping": broker_config.symbol_name_mapping,
        }
    elif isinstance(broker_config, dict):
        return broker_config
    else:
        return {}


@router.get("/", response_class=HTMLResponse)
async def config_form(request: Request):
    """Show the settings editor form."""
    settings = load_settings()
    
    # Get current client
    client = settings.get("extractor_settings", {}).get("client", "upstox")
    
    # Get broker-specific segments
    all_segments = get_broker_segments(client)
    
    # Parse segments for the active broker
    broker_config = settings.get("broker_configuration", {}).get(client, {})
    broker_dict = _get_broker_dict(broker_config)
    raw_segments = broker_dict.get("processable_segments", [])
    enabled_segments, disabled_segments = parse_segments(raw_segments)
    
    # Get broker-specific session bounds and symbol mappings
    session_bounds = broker_dict.get("session_bounds", {})
    symbol_mappings = broker_dict.get("symbol_name_mapping", {})

    return templates.TemplateResponse("config_editor.html", {
        "request": request,
        "settings": settings,
        "enabled_segments": enabled_segments,
        "disabled_segments": disabled_segments,
        "all_segments": all_segments,
        "session_bounds": session_bounds,
        "symbol_mappings": symbol_mappings,
        "active_client": client,
        "message": None,
        "error": None,
    })


@router.get("/run", response_class=HTMLResponse)
async def run_page(request: Request):
    """Show the pipeline execution page."""
    settings = load_settings()
    client = settings.get("extractor_settings", {}).get("client", "upstox")
    
    # Get cache directory info
    cache_info = get_cache_info(client)
    
    return templates.TemplateResponse("run.html", {
        "request": request,
        "settings": settings,
        "client": client,
        "cache_info": cache_info,
    })


def get_cache_info(client: str) -> dict:
    """Get cache directory sizes and last modified times."""
    from pathlib import Path
    import os
    from datetime import datetime
    
    base_path = Path(__file__).parent.parent.parent / "cache" / client
    result = {}
    
    for layer in ["Bronze", "Silver"]:
        layer_path = base_path / layer
        if layer_path.exists():
            total_size = 0
            file_count = 0
            latest_mtime = 0
            for f in layer_path.rglob("*"):
                if f.is_file():
                    total_size += f.stat().st_size
                    file_count += 1
                    latest_mtime = max(latest_mtime, f.stat().st_mtime)
            
            result[layer] = {
                "size_bytes": total_size,
                "size_mb": round(total_size / (1024 * 1024), 2),
                "file_count": file_count,
                "last_modified": datetime.fromtimestamp(latest_mtime).isoformat() if latest_mtime else None,
            }
        else:
            result[layer] = {
                "size_bytes": 0,
                "size_mb": 0,
                "file_count": 0,
                "last_modified": None,
            }
    
    return result


@router.get("/run/cache-info")
async def cache_info():
    """Get cache directory info as JSON for live updates."""
    settings = load_settings()
    client = settings.get("extractor_settings", {}).get("client", "upstox")
    return get_cache_info(client)


@router.get("/broker-config/{broker_name}")
async def get_broker_config(broker_name: str):
    """Get broker-specific configuration for the UI."""
    settings = load_settings()
    
    # Ensure parsers are discovered (handles reload issues)
    from extraction.parser import parser_discovery
    parser_discovery()
    
    # Get all available segments for this broker
    all_segments = get_broker_segments(broker_name)
    
    # Get saved config for this broker
    broker_config = settings.get("broker_configuration", {}).get(broker_name.lower(), {})
    broker_dict = _get_broker_dict(broker_config)
    raw_segments = broker_dict.get("processable_segments", [])
    session_bounds = broker_dict.get("session_bounds", {})
    symbol_mappings = broker_dict.get("symbol_name_mapping", {})
    
    enabled_segments, disabled_segments = parse_segments(raw_segments)
    
    return {
        "all_segments": all_segments,
        "enabled_segments": enabled_segments,
        "disabled_segments": disabled_segments,
        "session_bounds": session_bounds,
        "symbol_mappings": symbol_mappings,
    }


@router.post("/save-env", response_class=HTMLResponse)
async def save_env_config(
    request: Request,
    # Database - ClickHouse
    ch_host: str = Form("localhost"),
    ch_port: int = Form(8123),
    ch_database: str = Form("market_data"),
    ch_username: str = Form("default"),
    ch_password: str = Form(""),
    ch_secure: Optional[str] = Form(None),
    ch_compression: Optional[str] = Form(None),
    ch_connect_timeout: int = Form(10),
    ch_pool_min: int = Form(1),
    ch_pool_max: int = Form(10),
    # Database - PostgreSQL
    pg_host: str = Form("localhost"),
    pg_port: int = Form(5432),
    pg_database: str = Form("market_data"),
    pg_username: str = Form("postgres"),
    pg_password: str = Form(""),
    pg_pool_min: int = Form(1),
    pg_pool_max: int = Form(10),
    pg_connect_timeout: int = Form(10),
    pg_ssl_mode: str = Form("prefer"),
    pg_application_name: str = Form("market_data_pipeline"),
    # Database - TimescaleDB
    ts_chunk_time_interval: str = Form("1 day"),
    # Cloud - AWS
    aws_access_key_id: str = Form(""),
    aws_secret_access_key: str = Form(""),
    aws_region: str = Form("us-east-1"),
    aws_s3_endpoint_url: str = Form(""),
    # Cloud - Azure
    azure_storage_account_name: str = Form(""),
    azure_storage_account_key: str = Form(""),
    azure_storage_container_name: str = Form("marketdata-pipeline"),
    # Cloud - GCP
    gcp_project_id: str = Form(""),
    gcp_storage_bucket: str = Form("marketdata-pipeline"),
    gcp_credentials_path: str = Form(""),
):
    """Save database and cloud settings to .env file."""
    try:
        env_path = Path(".env")
        env_lines = []
        
        # Read existing .env file if it exists
        if env_path.exists():
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        key = line.split("=")[0].strip()
                        # Skip database and cloud settings - we'll rewrite them
                        if not any(key.startswith(prefix) for prefix in [
                            "CLICKHOUSE_", "POSTGRESQL_", "TIMESCALEDB_",
                            "AWS_", "AZURE_", "GCP_"
                        ]):
                            env_lines.append(line)
                    elif line.startswith("#"):
                        env_lines.append(line)
        
        # Add database and cloud settings
        env_lines.append("# =============================================================================")
        env_lines.append("# Database Configuration (ClickHouse)")
        env_lines.append("# =============================================================================")
        env_lines.append(f"CLICKHOUSE_HOST={ch_host}")
        env_lines.append(f"CLICKHOUSE_PORT={ch_port}")
        env_lines.append(f"CLICKHOUSE_DATABASE={ch_database}")
        env_lines.append(f"CLICKHOUSE_USERNAME={ch_username}")
        env_lines.append(f"CLICKHOUSE_PASSWORD={ch_password}")
        env_lines.append(f"CLICKHOUSE_SECURE={'true' if ch_secure else 'false'}")
        env_lines.append(f"CLICKHOUSE_COMPRESSION={'true' if ch_compression else 'false'}")
        env_lines.append(f"CLICKHOUSE_CONNECT_TIMEOUT={ch_connect_timeout}")
        env_lines.append(f"CLICKHOUSE_POOL_MIN={ch_pool_min}")
        env_lines.append(f"CLICKHOUSE_POOL_MAX={ch_pool_max}")
        
        env_lines.append("# PostgreSQL (alternative)")
        env_lines.append(f"POSTGRESQL_HOST={pg_host}")
        env_lines.append(f"POSTGRESQL_PORT={pg_port}")
        env_lines.append(f"POSTGRESQL_DATABASE={pg_database}")
        env_lines.append(f"POSTGRESQL_USERNAME={pg_username}")
        env_lines.append(f"POSTGRESQL_PASSWORD={pg_password}")
        env_lines.append(f"POSTGRESQL_POOL_MIN={pg_pool_min}")
        env_lines.append(f"POSTGRESQL_POOL_MAX={pg_pool_max}")
        env_lines.append(f"POSTGRESQL_CONNECT_TIMEOUT={pg_connect_timeout}")
        env_lines.append(f"POSTGRESQL_SSL_MODE={pg_ssl_mode}")
        env_lines.append(f"POSTGRESQL_APPLICATION_NAME={pg_application_name}")
        
        env_lines.append("# TimescaleDB (alternative - inherits PostgreSQL settings)")
        env_lines.append(f"TIMESCALEDB_CHUNK_TIME_INTERVAL={ts_chunk_time_interval}")
        
        env_lines.append("# =============================================================================")
        env_lines.append("# Cloud Storage (AWS S3)")
        env_lines.append("# =============================================================================")
        env_lines.append(f"AWS_ACCESS_KEY_ID={aws_access_key_id}")
        env_lines.append(f"AWS_SECRET_ACCESS_KEY={aws_secret_access_key}")
        env_lines.append(f"AWS_REGION={aws_region}")
        env_lines.append(f"AWS_S3_ENDPOINT_URL={aws_s3_endpoint_url}")
        
        env_lines.append("# Azure Blob Storage (if CLOUD_CLIENT=azure)")
        env_lines.append(f"AZURE_STORAGE_ACCOUNT_NAME={azure_storage_account_name}")
        env_lines.append(f"AZURE_STORAGE_ACCOUNT_KEY={azure_storage_account_key}")
        env_lines.append(f"AZURE_STORAGE_CONTAINER_NAME={azure_storage_container_name}")
        
        env_lines.append("# Google Cloud Storage (if CLOUD_CLIENT=gcp)")
        env_lines.append(f"GCP_PROJECT_ID={gcp_project_id}")
        env_lines.append(f"GCP_STORAGE_BUCKET={gcp_storage_bucket}")
        env_lines.append(f"GCP_CREDENTIALS_PATH={gcp_credentials_path}")
        
        # Write to .env file
        env_path = Path(".env")
        with open(env_path, "w") as f:
            f.write("\n".join(env_lines) + "\n")
        
        logger.info(".env file updated with database and cloud settings")
        
        # Reload settings to reflect changes
        settings = load_settings()
        
        return templates.TemplateResponse("config_editor.html", {
            "request": request,
            "settings": settings,
            "message": "Database and cloud settings saved to .env file",
            "error": None,
        })
    except Exception as e:
        logger.error(f"Failed to save .env config: {e}")
        settings = load_settings()
        return templates.TemplateResponse("config_editor.html", {
            "request": request,
            "settings": settings,
            "message": None,
            "error": f"Failed to save: {str(e)}",
        })


def save_settings(data: Dict[str, Any]) -> None:
    """Save dict back to settings.yaml and broker_config.yaml."""
    # Save main settings
    main_settings = {k: v for k, v in data.items() if k != "broker_configuration"}
    with open(SETTINGS_PATH, "w") as f:
        yaml.dump(main_settings, f, default_flow_style=False, sort_keys=False, indent=2)
    
    # Save broker config to separate file
    broker_config = data.get("broker_configuration", {})
    if broker_config:
        with open(BROKER_CONFIG_PATH, "w") as f:
            yaml.dump(broker_config, f, default_flow_style=False, sort_keys=False, indent=2)
    
    logger.info("settings.yaml and broker_config.yaml updated")


def parse_segments(raw_list: List[str]) -> tuple[List[str], List[str]]:
    """Parse segments list into (enabled, disabled). Preserves order and comments."""
    enabled = []
    disabled = []
    for item in raw_list:
        s = str(item).strip()
        if s.startswith("#"):
            disabled.append(s[1:].strip())
        else:
            enabled.append(s)
    return enabled, disabled


def build_segments_list(enabled: List[str], disabled: List[str], all_segments: List[str] = None) -> List[str]:
    """Rebuild segments list with # prefix for disabled, preserving original order."""
    if all_segments is None:
        all_segments = []
    # Use ALL_SEGMENTS as canonical order
    result = []
    enabled_set = set(enabled)
    disabled_set = set(disabled)
    for seg in all_segments:
        if seg in enabled_set:
            result.append(seg)
        elif seg in disabled_set:
            result.append(f"# {seg}")
    # Add any extra segments not in ALL_SEGMENTS
    for seg in enabled:
        if seg not in all_segments and seg not in result:
            result.append(seg)
    for seg in disabled:
        if seg not in all_segments and f"# {seg}" not in result:
            result.append(f"# {seg}")
    return result


def _get_broker_dict(broker_config: Any) -> Dict[str, Any]:
    """Convert broker config to dict (handles both BrokerConfiguration object and dict)."""
    if hasattr(broker_config, 'processable_segments'):
        # It's a BrokerConfiguration object
        return {
            "processable_segments": broker_config.processable_segments,
            "session_bounds": broker_config.session_bounds,
            "symbol_name_mapping": broker_config.symbol_name_mapping,
        }
    elif isinstance(broker_config, dict):
        return broker_config
    else:
        return {}


@router.get("/", response_class=HTMLResponse)
async def config_form(request: Request):
    """Show the settings editor form."""
    settings = load_settings()
    
    # Get current client
    client = settings.get("extractor_settings", {}).get("client", "upstox")
    
    # Get broker-specific segments
    all_segments = get_broker_segments(client)
    
    # Parse segments for the active broker
    broker_config = settings.get("broker_configuration", {}).get(client, {})
    broker_dict = _get_broker_dict(broker_config)
    raw_segments = broker_dict.get("processable_segments", [])
    enabled_segments, disabled_segments = parse_segments(raw_segments)
    
    # Get broker-specific session bounds and symbol mappings
    session_bounds = broker_dict.get("session_bounds", {})
    symbol_mappings = broker_dict.get("symbol_name_mapping", {})

    return templates.TemplateResponse("config_editor.html", {
        "request": request,
        "settings": settings,
        "enabled_segments": enabled_segments,
        "disabled_segments": disabled_segments,
        "all_segments": all_segments,
        "session_bounds": session_bounds,
        "symbol_mappings": symbol_mappings,
        "active_client": client,
        "message": None,
        "error": None,
    })


@router.get("/run", response_class=HTMLResponse)
async def run_page(request: Request):
    """Show the pipeline execution page."""
    settings = load_settings()
    client = settings.get("extractor_settings", {}).get("client", "upstox")
    
    # Get cache directory info
    cache_info = get_cache_info(client)
    
    return templates.TemplateResponse("run.html", {
        "request": request,
        "settings": settings,
        "client": client,
        "cache_info": cache_info,
    })


def get_cache_info(client: str) -> dict:
    """Get cache directory sizes and last modified times."""
    from pathlib import Path
    import os
    from datetime import datetime
    
    base_path = Path(__file__).parent.parent.parent / "cache" / client
    result = {}
    
    for layer in ["Bronze", "Silver"]:
        layer_path = base_path / layer
        if layer_path.exists():
            total_size = 0
            file_count = 0
            latest_mtime = 0
            for f in layer_path.rglob("*"):
                if f.is_file():
                    total_size += f.stat().st_size
                    file_count += 1
                    latest_mtime = max(latest_mtime, f.stat().st_mtime)
            
            result[layer] = {
                "size_bytes": total_size,
                "size_mb": round(total_size / (1024 * 1024), 2),
                "file_count": file_count,
                "last_modified": datetime.fromtimestamp(latest_mtime).isoformat() if latest_mtime else None,
            }
        else:
            result[layer] = {
                "size_bytes": 0,
                "size_mb": 0,
                "file_count": 0,
                "last_modified": None,
            }
    
    return result


@router.get("/run/cache-info")
async def cache_info():
    """Get cache directory info as JSON for live updates."""
    settings = load_settings()
    client = settings.get("extractor_settings", {}).get("client", "upstox")
    return get_cache_info(client)


@router.get("/broker-config/{broker_name}")
async def get_broker_config(broker_name: str):
    """Get broker-specific configuration for the UI."""
    settings = load_settings()
    
    # Ensure parsers are discovered (handles reload issues)
    from extraction.parser import parser_discovery
    parser_discovery()
    
    # Get all available segments for this broker
    all_segments = get_broker_segments(broker_name)
    
    # Get saved config for this broker
    broker_config = settings.get("broker_configuration", {}).get(broker_name.lower(), {})
    broker_dict = _get_broker_dict(broker_config)
    raw_segments = broker_dict.get("processable_segments", [])
    session_bounds = broker_dict.get("session_bounds", {})
    symbol_mappings = broker_dict.get("symbol_name_mapping", {})
    
    enabled_segments, disabled_segments = parse_segments(raw_segments)
    
    return {
        "all_segments": all_segments,
        "enabled_segments": enabled_segments,
        "disabled_segments": disabled_segments,
        "session_bounds": session_bounds,
        "symbol_mappings": symbol_mappings,
    }


@router.post("/", response_class=HTMLResponse)
async def save_config(
    request: Request,
    # Application
    app_name: str = Form("Market Data Extractor"),
    app_version: str = Form("1.0.0"),
    app_description: str = Form(""),
    app_environment: str = Form("development"),
    app_debug: Optional[str] = Form(None),
    # Extractor
    ext_client: str = Form("upstox"),
    ext_interval: int = Form(1),
    ext_max_retries: int = Form(3),
    ext_expiry_duration: int = Form(3),
    ext_start_date: str = Form(""),
    ext_end_date: str = Form(""),
    # Ingestion
    ing_enabled: Optional[str] = Form(None),
    ing_target: str = Form("clickhouse"),
    ing_batch_size: int = Form(10000),
    ing_create_tables: Optional[str] = Form(None),
    # Cloud
    cloud_client: str = Form("aws"),
    cloud_push_to_cloud: Optional[str] = Form(None),
    cloud_storage_name: str = Form("marketdata-pipeline"),
    cloud_destination_folder: str = Form("market_data_storage"),
    # Broker - segments (comma-separated from hidden input)
    segments_enabled: str = Form(""),
    segments_disabled: str = Form(""),
    # Broker - session bounds (JSON from hidden input)
    session_bounds_json: str = Form("{}"),
    # Broker - symbol mappings (JSON from hidden input)
    symbol_mappings_json: str = Form("{}"),
):
    """Save form data back to settings.yaml."""
    try:
        # Load current to preserve any fields not in form
        settings = load_settings()
        
        # Application
        settings.setdefault("application", {})
        settings["application"]["name"] = app_name
        settings["application"]["version"] = app_version
        settings["application"]["description"] = app_description
        settings["application"]["environment"] = app_environment
        settings["application"]["debug"] = bool(app_debug)
        
        # Extractor
        settings.setdefault("extractor_settings", {})
        settings["extractor_settings"]["client"] = ext_client
        settings["extractor_settings"]["interval"] = ext_interval
        settings["extractor_settings"]["max_retries"] = ext_max_retries
        settings["extractor_settings"]["expiry_duration"] = ext_expiry_duration
        settings["extractor_settings"]["start_date"] = ext_start_date
        settings["extractor_settings"]["end_date"] = ext_end_date
        
        # Ingestion
        settings.setdefault("ingestion_settings", {})
        settings["ingestion_settings"]["enabled"] = bool(ing_enabled)
        settings["ingestion_settings"]["target"] = ing_target
        settings["ingestion_settings"]["batch_size"] = ing_batch_size
        settings["ingestion_settings"]["create_tables"] = bool(ing_create_tables)
        
        # Cloud
        settings.setdefault("cloud_settings", {})
        settings["cloud_settings"]["client"] = cloud_client
        settings["cloud_settings"]["push_to_cloud"] = bool(cloud_push_to_cloud)
        settings["cloud_settings"]["storage_name"] = cloud_storage_name
        settings["cloud_settings"]["destination_folder"] = cloud_destination_folder
        
        # Broker - segments (per broker)
        enabled_list = [s.strip() for s in segments_enabled.split(",") if s.strip()]
        disabled_list = [s.strip() for s in segments_disabled.split(",") if s.strip()]
        settings.setdefault("broker_configuration", {})
        
        # Get existing broker config as dict (handle both dict and BrokerConfiguration object)
        existing_broker = settings["broker_configuration"].get(ext_client, {})
        broker_dict = _get_broker_dict(existing_broker)
        settings["broker_configuration"][ext_client] = broker_dict
        
        all_segments = get_broker_segments(ext_client)
        settings["broker_configuration"][ext_client]["processable_segments"] = build_segments_list(enabled_list, disabled_list, all_segments)
        
        # Broker - session bounds (per broker)
        import json
        try:
            bounds = json.loads(session_bounds_json) if session_bounds_json else {}
            settings["broker_configuration"][ext_client]["session_bounds"] = bounds
        except json.JSONDecodeError:
            pass
        
        # Broker - symbol mappings (per broker)
        try:
            mappings = json.loads(symbol_mappings_json) if symbol_mappings_json else {}
            settings["broker_configuration"][ext_client]["symbol_name_mapping"] = mappings
        except json.JSONDecodeError:
            pass
        
        save_settings(settings)
        
        # Reload for display
        settings = load_settings()
        client = settings.get("extractor_settings", {}).get("client", "upstox")
        all_segments = get_broker_segments(client)
        broker_config = settings.get("broker_configuration", {}).get(client, {})
        broker_dict = _get_broker_dict(broker_config)
        raw_segments = broker_dict.get("processable_segments", [])
        session_bounds = broker_dict.get("session_bounds", {})
        symbol_mappings = broker_dict.get("symbol_name_mapping", {})
        
        enabled_segments, disabled_segments = parse_segments(raw_segments)
        
        return templates.TemplateResponse("config_editor.html", {
            "request": request,
            "settings": settings,
            "enabled_segments": enabled_segments,
            "disabled_segments": disabled_segments,
            "all_segments": all_segments,
            "session_bounds": session_bounds,
            "symbol_mappings": symbol_mappings,
            "active_client": client,
            "message": "Settings saved successfully",
            "error": None,
        })
    except Exception as e:
        logger.error(f"Failed to save settings: {e}")
        settings = load_settings()
        client = settings.get("extractor_settings", {}).get("client", "upstox")
        all_segments = get_broker_segments(client)
        broker_config = settings.get("broker_configuration", {}).get(client, {})
        broker_dict = _get_broker_dict(broker_config)
        raw_segments = broker_dict.get("processable_segments", [])
        session_bounds = broker_dict.get("session_bounds", {})
        symbol_mappings = broker_dict.get("symbol_name_mapping", {})
        
        enabled_segments, disabled_segments = parse_segments(raw_segments)
        
        return templates.TemplateResponse("config_editor.html", {
            "request": request,
            "settings": settings,
            "enabled_segments": enabled_segments,
            "disabled_segments": disabled_segments,
            "all_segments": all_segments,
            "session_bounds": session_bounds,
            "symbol_mappings": symbol_mappings,
            "active_client": client,
            "message": None,
            "error": f"Failed to save: {str(e)}",
        })
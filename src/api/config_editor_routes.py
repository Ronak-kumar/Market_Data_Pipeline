"""Config editor routes - single page to edit settings.yaml"""
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from shared.observability import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/config-editor", tags=["config-editor"])

templates = Jinja2Templates(directory="src/api/templates")

SETTINGS_PATH = Path("src/shared/config/settings.yaml")

# All known segments for the UI
ALL_SEGMENTS = [
    "NSE_INDEX", "BSE_INDEX", "GLOBAL_INDEX", "GLOBAL_INDICATOR",
    "NSE_FO", "BSE_FO", "MCX_FO", "BCD_FO", "NCD_FO", "BSE_EQ",
    "NSE_EQ", "NSE_COM", "NSE_FNO", "BSE_FNO", "MCX_FNO",
    "NSE_CURRENCY", "BSE_CURRENCY", "BSE_COM"
]


def load_settings() -> Dict[str, Any]:
    """Load settings.yaml as dict."""
    if not SETTINGS_PATH.exists():
        return {}
    with open(SETTINGS_PATH, "r") as f:
        return yaml.safe_load(f) or {}


def save_settings(data: Dict[str, Any]) -> None:
    """Save dict back to settings.yaml."""
    with open(SETTINGS_PATH, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, indent=2)
    logger.info("settings.yaml updated")


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


def build_segments_list(enabled: List[str], disabled: List[str]) -> List[str]:
    """Rebuild segments list with # prefix for disabled, preserving original order."""
    # Use ALL_SEGMENTS as canonical order
    result = []
    enabled_set = set(enabled)
    disabled_set = set(disabled)
    for seg in ALL_SEGMENTS:
        if seg in enabled_set:
            result.append(seg)
        elif seg in disabled_set:
            result.append(f"# {seg}")
    # Add any extra segments not in ALL_SEGMENTS
    for seg in enabled:
        if seg not in ALL_SEGMENTS and seg not in result:
            result.append(seg)
    for seg in disabled:
        if seg not in ALL_SEGMENTS and f"# {seg}" not in result:
            result.append(f"# {seg}")
    return result


@router.get("/", response_class=HTMLResponse)
async def config_form(request: Request):
    """Show the settings editor form."""
    settings = load_settings()
    
    # Parse segments for template
    raw_segments = settings.get("broker_configuration", {}).get("processable_segments", [])
    enabled_segments, disabled_segments = parse_segments(raw_segments)
    
    return templates.TemplateResponse("config_editor.html", {
        "request": request,
        "settings": settings,
        "enabled_segments": enabled_segments,
        "disabled_segments": disabled_segments,
        "all_segments": ALL_SEGMENTS,
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
        
        # Broker - segments
        enabled_list = [s.strip() for s in segments_enabled.split(",") if s.strip()]
        disabled_list = [s.strip() for s in segments_disabled.split(",") if s.strip()]
        settings.setdefault("broker_configuration", {})
        settings["broker_configuration"]["processable_segments"] = build_segments_list(enabled_list, disabled_list)
        
        # Broker - session bounds
        import json
        try:
            bounds = json.loads(session_bounds_json) if session_bounds_json else {}
            settings["broker_configuration"]["session_bounds"] = bounds
        except json.JSONDecodeError:
            pass
        
        # Broker - symbol mappings
        try:
            mappings = json.loads(symbol_mappings_json) if symbol_mappings_json else {}
            settings["broker_configuration"]["symbol_name_mapping"] = mappings
        except json.JSONDecodeError:
            pass
        
        save_settings(settings)
        
        # Reload for display
        settings = load_settings()
        raw_segments = settings.get("broker_configuration", {}).get("processable_segments", [])
        enabled_segments, disabled_segments = parse_segments(raw_segments)
        
        return templates.TemplateResponse("config_editor.html", {
            "request": request,
            "settings": settings,
            "enabled_segments": enabled_segments,
            "disabled_segments": disabled_segments,
            "all_segments": ALL_SEGMENTS,
            "message": "Settings saved successfully",
            "error": None,
        })
    except Exception as e:
        logger.error(f"Failed to save settings: {e}")
        settings = load_settings()
        raw_segments = settings.get("broker_configuration", {}).get("processable_segments", [])
        enabled_segments, disabled_segments = parse_segments(raw_segments)
        
        return templates.TemplateResponse("config_editor.html", {
            "request": request,
            "settings": settings,
            "enabled_segments": enabled_segments,
            "disabled_segments": disabled_segments,
            "all_segments": ALL_SEGMENTS,
            "message": None,
            "error": f"Failed to save: {str(e)}",
        })
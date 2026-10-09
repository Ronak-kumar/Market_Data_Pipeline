from extraction.clients import MarketDataProvider
import requests
import json
from shared.observability import get_logger
from extraction.clients.registry import client_registry
from pathlib import Path
from datetime import datetime
from shared.utils import load_yaml_config
from shared.config.settings_model import AppSettings
from shared.utils import csv_reader
import csv

logger = get_logger(__name__)


@client_registry.register("dhan")
class DhanAdapter(MarketDataProvider):
    MASTER_URL_DETAILED = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
    HISTORICAL_INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"

    # Dhan segment mapping
    SEGMENT_MAP = {
        "NSE_EQ": "NSE_EQ",
        "NSE_FO": "NSE_FNO",
        "BSE_EQ": "BSE_EQ",
        "BSE_FO": "BSE_FNO",
        "NSE_INDEX": "NSE_IDX",
        "BSE_INDEX": "BSE_IDX",
        "MCX_FO": "MCX_FNO",
        "NSE_COM": "NSE_COM",
        "BSE_COM": "BSE_COM",
        "NSE_CURRENCY": "NSE_CUR",
        "BSE_CURRENCY": "BSE_CUR",
    }

    # Instrument type mapping
    INSTRUMENT_MAP = {
        "EQUITY": "EQUITY",
        "FUTIDX": "FUTIDX",
        "OPTIDX": "OPTIDX",
        "FUTSTK": "FUTSTK",
        "OPTSTK": "OPTSTK",
        "FUTCOM": "FUTCOM",
        "OPTCOM": "OPTCOM",
        "FUTCUR": "FUTCUR",
        "OPTCUR": "OPTCUR",
        "INDEX": "INDEX",
    }

    def _load_token(self) -> None:
        logger.debug("Loading Dhan access token")
        access_token_path = Path(__file__).parents[2] / "access_token" / "dhan.json"
        if not access_token_path.exists():
            logger.error("Access token file not found", extra={"path": str(access_token_path)})
            raise ValueError("Dhan access token file not found")

        try:
            with access_token_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse token file", extra={"path": str(access_token_path), "error": str(e)})
            raise

        token = data.get("access_token") or data.get("token")

        if not token or token == "":
            logger.error("Access token not found in token file", extra={"path": str(access_token_path)})
            raise ValueError("Access token not found in token file")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "access-token": token,
        }
        self.HEADERS = headers
        logger.debug("Dhan access token loaded successfully")

    def _fetch_master_instrument(self):
        """Fetch and parse Dhan instrument master CSV."""
        logger.info("Fetching Dhan master instrument list")
        self._load_token()
        try:
            logger.debug("Making request to master URL", extra={"url": self.MASTER_URL_DETAILED})
            response = self.session_client.get(self.MASTER_URL_DETAILED, timeout=self.timeout)
            response.raise_for_status()
            logger.debug("Master instrument response received", extra={"status_code": response.status_code, "size_bytes": len(response.content)})
            
            # Parse CSV
            data = csv_reader(response)
            logger.info("Dhan master instrument fetched and parsed", extra={"instrument_count": len(data)})
            
            # Convert to list of dicts for parser compatibility
            return data
        except requests.RequestException as e:
            logger.error("Error fetching Dhan master instrument URL", extra={"error": str(e), "url": self.MASTER_URL_DETAILED}, exc_info=True)
            raise
        except Exception as e:
            logger.error("Error parsing Dhan master instrument CSV", extra={"error": str(e)}, exc_info=True)
            raise

    def fetch_instrument(self, row: dict, variation: str, date: str, interval: int):
        """Fetch historical candles for a Dhan instrument."""
        security_id = str(row.get("SEM_SMST_SECURITY_ID") or row.get("security_id") or row.get("SECURITY_ID"))
        exchange_segment = self._get_exchange_segment(row)
        instrument_type = self._get_instrument_type(row)
        
        if not security_id or not exchange_segment or not instrument_type:
            logger.warning("Missing required fields for Dhan instrument", extra={"row": row})
            return None

        return self._fetch_intraday(security_id, exchange_segment, instrument_type, interval, date)

    def _get_exchange_segment(self, row: dict) -> str:
        """Map internal segment to Dhan exchangeSegment."""
        segment = row.get("SEM_EXM_EXCH_ID", "") + "_" + row.get("SEM_SEGMENT", "")
        # Dhan uses NSE_EQ, NSE_FNO, etc.
        exch = row.get("exchange", "NSE")
        seg = row.get("segment_code", "E")
        
        mapping = {
            ("NSE", "E"): "NSE_EQ",
            ("NSE", "D"): "NSE_FNO",
            ("BSE", "E"): "BSE_EQ",
            ("BSE", "D"): "BSE_FNO",
            ("NSE", "I"): "NSE_IDX",
            ("BSE", "I"): "BSE_IDX",
            ("MCX", "D"): "MCX_FNO",
            ("MCX", "M"): "MCX_COM",
            ("NSE", "C"): "NSE_CUR",
            ("BSE", "C"): "BSE_CUR",
        }
        return mapping.get((exch, seg), "NSE_EQ")

    def _get_instrument_type(self, row: dict) -> str:
        """Map Dhan instrument to API instrument type."""
        instrument = row.get("instrument_type", "").upper()
        mapping = {
            "EQUITY": "EQUITY",
            "FUTIDX": "FUTIDX",
            "OPTIDX": "OPTIDX",
            "FUTSTK": "FUTSTK",
            "OPTSTK": "OPTSTK",
            "FUTCOM": "FUTCOM",
            "OPTCOM": "OPTCOM",
            "FUTCUR": "FUTCUR",
            "OPTCUR": "OPTCUR",
            "INDEX": "INDEX",
        }
        return mapping.get(instrument, "EQUITY")

    def _fetch_intraday(self, security_id: str, exchange_segment: str, instrument_type: str, interval: int, date: str):
        """Fetch intraday data for a single date."""
        # Dhan intraday requires datetime range
        from datetime import datetime, timedelta
        start_dt = datetime.strptime(date + " 09:15:00", "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(date + " 15:30:00", "%Y-%m-%d %H:%M:%S")
        
        payload = {
            "securityId": security_id,
            "exchangeSegment": exchange_segment,
            "instrument": instrument_type,
            "interval": str(interval),
            "oi": False,
            "fromDate": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "toDate": end_dt.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logger.debug("Fetching Dhan intraday", extra={"security_id": security_id, "date": date, "interval": interval})
        response = self._retry_policy_post(self.HISTORICAL_INTRADAY_URL, payload)
        if response is None:
            return None
        return self._normalize_response(response, {"security_id": security_id, "instrument_key": security_id})

    def _retry_policy_post(self, url: str, payload: dict):
        """POST retry policy for Dhan API."""
        import time as tm
        logger.debug("Starting Dhan POST retry policy", extra={"url": url, "max_retries": self.max_retries, "timeout": self.timeout})
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug("Making HTTP POST request", extra={"url": url, "attempt": attempt, "max_retries": self.max_retries})
                response = self.session_client.post(url, headers=self.HEADERS, json=payload, timeout=self.timeout)

                if response.status_code == 429:
                    logger.warning("[RATE_LIMIT] Rate limited, resetting session", extra={"url": url, "attempt": attempt})
                    self._reset_client_session()
                    tm.sleep(1)
                    continue

                if response.status_code == 200:
                    logger.debug("[OK] Request successful", extra={"url": url, "attempt": attempt, "response_size": len(response.content)})
                    return response

                if 400 <= response.status_code < 500:
                    logger.error("[FATAL] Client error, not retrying", extra={"url": url, "status": response.status_code, "attempt": attempt, "response_preview": response.text[:500]})
                    break

                logger.warning("[RETRY] Server error, will retry", extra={"url": url, "status": response.status_code, "attempt": attempt, "max_retries": self.max_retries})

            except requests.exceptions.Timeout:
                logger.warning("[TIMEOUT] Request timed out", extra={"url": url, "attempt": attempt, "max_retries": self.max_retries, "timeout": f"{self.timeout}s"})
                tm.sleep(attempt * 2)

            except requests.exceptions.ConnectionError as e:
                logger.warning("[CONNECTION_ERROR] Connection failed", extra={"url": url, "attempt": attempt, "max_retries": self.max_retries, "error": str(e)[:200]})
                tm.sleep(attempt * 2)

            except requests.exceptions.RequestException as e:
                logger.warning("[REQUEST_ERROR] Request failed", extra={"url": url, "attempt": attempt, "max_retries": self.max_retries, "error": str(e)[:200]})
                tm.sleep(attempt * 2)

        logger.error("[EXHAUSTED] All retry attempts failed", extra={"url": url, "max_retries": self.max_retries})
        return None

    def _normalize_response(self, response: requests.Response, context: dict):
        """Normalize Dhan API response to standard candle format."""
        security_id = context.get("security_id", "unknown")
        
        logger.debug("Normalizing Dhan response", extra={"security_id": security_id, "status_code": response.status_code})

        if response.status_code != 200:
            logger.error("Non-200 response", extra={"status_code": response.status_code, "response_preview": response.text[:500]})
            raise ValueError(f"HTTP {response.status_code}: {response.text[:500]}")

        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON response", extra={"error": str(e)}, exc_info=True)
            raise ValueError(f"Invalid JSON response: {e}") from e

        # Dhan response format: {"status": "success", "data": {"open": [...], "high": [...], "low": [...], "close": [...], "volume": [...], "timestamp": [...]}}
        if not isinstance(payload, dict):
            raise ValueError(f"Expected dict at root, got {type(payload).__name__}")

        if payload.get("status") != "success":
            raise ValueError(f"Dhan API error: {payload}")

        data = payload.get("data", {})
        if not isinstance(data, dict):
            raise ValueError(f"Expected 'data' to be dict, got {type(data).__name__}")

        # Extract arrays
        opens = data.get("open", [])
        highs = data.get("high", [])
        lows = data.get("low", [])
        closes = data.get("close", [])
        volumes = data.get("volume", [])
        timestamps = data.get("timestamp", [])
        oi = data.get("open_interest", [])

        if not timestamps:
            logger.debug("No candles returned", extra={"security_id": security_id})
            return None

        # Validate all arrays same length
        n = len(timestamps)
        for name, arr in [("open", opens), ("high", highs), ("low", lows), ("close", closes), ("volume", volumes)]:
            if len(arr) != n:
                raise ValueError(f"Array length mismatch: {name} has {len(arr)} elements, expected {n}")

        name = context.get("symbol_name", "")
        symbol_asset_type = context.get("symbol_asset_class", "")
        validated_candles = []
        for i in range(n):
            try:
                ts = timestamps[i]
                dt = datetime.fromtimestamp(ts)
                
                candle = [
                    symbol_asset_type,
                    name,
                    dt.strftime("%d-%m-%Y"),
                    dt.strftime("%H:%M:%S"),
                    float(opens[i]),
                    float(highs[i]),
                    float(lows[i]),
                    float(closes[i]),
                    float(volumes[i]),
                    float(oi[i]) if i < len(oi) and oi[i] is not None else 0.0
                ]
                validated_candles.append(candle)
            except (ValueError, TypeError, IndexError) as e:
                logger.error("Error parsing candle", extra={"index": i, "error": str(e)})
                raise ValueError(f"Candle {i}: {e}") from e

        logger.debug("Normalized Dhan candles", extra={"security_id": security_id, "candle_count": len(validated_candles)})
        return validated_candles

from extraction.clients import MarketDataProvider
import requests
import json
import polars as pl
from shared.observability import get_logger
from extraction.clients.registry import client_registry
from pathlib import Path
from datetime import datetime

from shared.utils.utilities import unzipper

logger = get_logger(__name__)


@client_registry.register("zerodha")
class ZerodhaAdapter(MarketDataProvider):
    BASE_URL = "https://api.kite.trade"
    INSTRUMENTS_URL = "https://api.kite.trade/instruments"
    INSTRUMENTS_EXCHANGE_URL = "https://api.kite.trade/instruments/{exchange}"
    HISTORICAL_URL = "https://api.kite.trade/instruments/historical/{instrument_token}/{interval}"

    # Supported intervals
    INTERVAL_MAP = {
        1: "minute",
        3: "3minute",
        5: "5minute",
        10: "10minute",
        15: "15minute",
        30: "30minute",
        60: "60minute",
        1440: "day",  # daily
    }

    # Exchange mapping for segments
    SEGMENT_EXCHANGE_MAP = {
        "NSE_EQ": "NSE",
        "NSE_FNO": "NFO",
        "BSE_EQ": "BSE",
        "BSE_FNO": "BFO",
        "NSE_INDEX": "NSE",
        "BSE_INDEX": "BSE",
        "MCX_FNO": "MCX",
        "NSE_CURRENCY": "CDS",
        "BSE_CURRENCY": "BCD",
    }

    def _load_token(self) -> None:
        logger.debug("Loading Zerodha access token")
        access_token_path = Path(__file__).parents[2] / "access_token" / "zerodha.json"
        if not access_token_path.exists():
            logger.error("Access token file not found", extra={"path": str(access_token_path)})
            raise ValueError("Zerodha access token file not found")

        try:
            with access_token_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse token file", extra={"path": str(access_token_path), "error": str(e)})
            raise

        api_key = data.get("api_key")
        access_token = data.get("access_token")

        if not api_key or not access_token:
            logger.error("API key or access token not found in token file", extra={"path": str(access_token_path)})
            raise ValueError("API key or access token not found in token file")

        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {api_key}:{access_token}",
            "Accept": "application/json",
        }
        self.HEADERS = headers
        self.api_key = api_key
        logger.debug("Zerodha access token loaded successfully")

    def _fetch_master_instrument(self):
        """Fetch and parse Zerodha instrument master (gzipped CSV)."""
        logger.info("Fetching Zerodha master instrument list")
        self._load_token()
        try:
            logger.debug("Making request to instruments URL", extra={"url": self.INSTRUMENTS_URL})

            df = pl.read_csv(
                self.INSTRUMENTS_URL,
                schema_overrides={
                    "strike": pl.Float64,
                    "last_price": pl.Float64,
                    "tick_size": pl.Float64,
                },
            )
            logger.info(f"Zerodha master instrument fetched and parsed instrument_count : {len(df)}")
            
            return df
        except requests.RequestException as e:
            logger.error("Error fetching Zerodha master instrument URL", extra={"error": str(e), "url": self.INSTRUMENTS_URL}, exc_info=True)
            raise
        except Exception as e:
            logger.error("Error parsing Zerodha master instrument CSV", extra={"error": str(e)}, exc_info=True)
            raise

    def fetch_instrument(self, row: dict, variation: str, date: str, interval: int):
        """Fetch historical candles for a Zerodha instrument."""
        instrument_token = row.get("instrument_token")
        exchange = row.get("exchange", "")
        tradingsymbol = row.get("tradingsymbol", "")
        
        if not instrument_token:
            logger.warning("Missing instrument_token for Zerodha instrument", extra={"row": row})
            return None

        # Map interval to Zerodha format
        zerodha_interval = self.INTERVAL_MAP.get(interval, "day")
        
        if variation == "intraday":
            return self._fetch_intraday(instrument_token, zerodha_interval, date)
        elif variation == "historical":
            return self._fetch_historical(instrument_token, zerodha_interval, date)
        return None

    def _fetch_historical(self, instrument_token: int, interval: str, date: str):
        """Fetch historical data for a single date (or date range)."""
        url = self.HISTORICAL_URL.format(instrument_token=instrument_token, interval=interval)
        
        # For daily data, we fetch the specific date
        params = {
            "from": f"{date} 09:15:00",
            "to": f"{date} 15:30:00",
            "oi": "1",  # Include OI for F&O
        }
        
        logger.debug("Fetching Zerodha historical", extra={"instrument_token": instrument_token, "interval": interval, "date": date})
        response = self._retry_policy_get(url, params)
        if response is None:
            return None
        return self._normalize_response(response, {"instrument_token": instrument_token, "tradingsymbol": tradingsymbol})

    def _fetch_intraday(self, instrument_token: int, interval: str, date: str):
        """Fetch intraday data for a single date."""
        url = self.HISTORICAL_URL.format(instrument_token=instrument_token, interval=interval)
        
        params = {
            "from": f"{date} 09:15:00",
            "to": f"{date} 15:30:00",
            "oi": "1",
        }
        
        logger.debug("Fetching Zerodha intraday", extra={"instrument_token": instrument_token, "interval": interval, "date": date})
        response = self._retry_policy_get(url, params)
        if response is None:
            return None
        return self._normalize_response(response, {"instrument_token": instrument_token, "tradingsymbol": tradingsymbol})

    def _retry_policy_get(self, url: str, params: dict):
        """GET retry policy for Zerodha API."""
        import time as tm
        logger.debug("Starting Zerodha GET retry policy", extra={"url": url, "max_retries": self.max_retries, "timeout": self.timeout})
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug("Making HTTP GET request", extra={"url": url, "attempt": attempt, "max_retries": self.max_retries, "params": params})
                response = self.session_client.get(url, headers=self.HEADERS, params=params, timeout=self.timeout)

                if response.status_code == 429:
                    logger.warning("[RATE_LIMIT] Rate limited", extra={"url": url, "attempt": attempt})
                    self._reset_client_session()
                    tm.sleep(1)
                    continue

                if response.status_code == 403:
                    logger.error("[PERMISSION] Insufficient permission - check historical data add-on subscription", extra={"url": url, "attempt": attempt})
                    break

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
        """Normalize Zerodha API response to standard candle format."""
        instrument_token = context.get("instrument_token", "unknown")
        tradingsymbol = context.get("tradingsymbol", str(instrument_token))
        
        logger.debug("Normalizing Zerodha response", extra={"instrument_token": instrument_token, "status_code": response.status_code})

        if response.status_code != 200:
            logger.error("Non-200 response", extra={"status_code": response.status_code, "response_preview": response.text[:500]})
            raise ValueError(f"HTTP {response.status_code}: {response.text[:500]}")

        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON response", extra={"error": str(e)}, exc_info=True)
            raise ValueError(f"Invalid JSON response: {e}") from e

        # Zerodha response format: {"status": "success", "data": {"candles": [[timestamp, open, high, low, close, volume], ...]}}
        if not isinstance(payload, dict):
            raise ValueError(f"Expected dict at root, got {type(payload).__name__}")

        if payload.get("status") != "success":
            raise ValueError(f"Zerodha API error: {payload}")

        data = payload.get("data", {})
        if not isinstance(data, dict):
            raise ValueError(f"Expected 'data' to be dict, got {type(data).__name__}")

        candles = data.get("candles", [])
        if not candles:
            logger.debug("No candles returned", extra={"instrument_token": instrument_token})
            return None

        # Each candle: [timestamp, open, high, low, close, volume]
        name = context.get("symbol_name", "")
        symbol_asset_type = context.get("symbol_asset_class", "")
        validated_candles = []
        for i, candle in enumerate(candles):
            if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                logger.error("Candle format invalid", extra={"index": i, "candle": candle})
                raise ValueError(f"Candle {i}: expected at least 6 elements, got {len(candle) if isinstance(candle, (list, tuple)) else 'non-array'}")

            try:
                ts = candle[0]
                # Zerodha timestamps are ISO strings like "2024-01-15 10:30:00+05:30"
                if isinstance(ts, str):
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                else:
                    dt = datetime.fromtimestamp(ts)
                
                validated_candle = [
                    symbol_asset_type,  # asset class - will be overridden by parser
                    name,
                    dt.strftime("%d-%m-%Y"),
                    dt.strftime("%H:%M:%S"),
                    float(candle[1]),  # open
                    float(candle[2]),  # high
                    float(candle[3]),  # low
                    float(candle[4]),  # close
                    float(candle[5]),  # volume
                    float(candle[6]) if len(candle) > 6 and candle[6] is not None else 0.0  # OI
                ]
                validated_candles.append(validated_candle)
            except (ValueError, TypeError, IndexError) as e:
                logger.error("Error parsing candle", extra={"index": i, "candle": candle, "error": str(e)})
                raise ValueError(f"Candle {i}: {e}") from e

        logger.debug("Normalized Zerodha candles", extra={"instrument_token": instrument_token, "candle_count": len(validated_candles)})
        return validated_candles

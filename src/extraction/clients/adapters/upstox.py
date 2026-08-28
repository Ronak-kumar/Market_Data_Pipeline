from extraction.clients import MarketDataProvider
import requests
from extraction.utils import unzipper
import json
from extraction.observability import get_logger
from extraction.clients.registry import client_registry
from pathlib import Path

logger = get_logger(__name__)


@client_registry.register("upstox")
class UpstoxAdapter(MarketDataProvider):
    MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    INTRADAY_URL = "https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{interval}"
    HISTORICAL_URL = "https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/1/{end_date}/{start_date}"

    def _load_token(self) -> None:
        access_token_path = Path(__file__).parents[2] / "access_token" / "upstox.json"
        with access_token_path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        token = data.get("access_token")

        if not token:
            raise ValueError("Access token not found in token file")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        self.HEADERS = headers

    def _fetch_master_instrument(self):
        """Normalize master instrument for all the adapters"""
        try:
            self._load_token()
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            return unzipper(response)
        except requests.RequestException as e:
            logger.error(f"Error fetching the URL: {e}")
            raise
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error decompressing or decoding JSON: {e}")
            raise

    def fetch_instrument(self, context: dict, interval: int):
        instrument_key = context["instrument_key"]
        url = self.INTRADAY_URL.format(instrument_key=instrument_key, interval=interval)
        response = self._retry_policy(url=url, logger=logger)
        if response is None:
            return None, None
        return self._normalize_response(response=response, context=context)

    def fetch_historical_instrument(self, context: dict, interval: int, start_date: str, end_date: str):
        instrument_key = context["instrument_key"]
        url = self.HISTORICAL_URL.format(instrument_key=instrument_key, interval=interval, end_date=end_date, start_date=start_date)
        response = self._retry_policy(url=url, logger=logger)
        if response is None:
            return None, None
        return self._normalize_response(response=response, context=context)

    def _normalize_response(self, response: requests.Response, context: dict):
        """
        Strict response normalization - FAILS LOUDLY if API contract changes.
        Returns (candles_list, normalized_name) on success.
        Raises ValueError with details if response structure is unexpected.
        """
        # 1. Validate HTTP response
        if response.status_code != 200:
            raise ValueError(f"HTTP {response.status_code}: {response.text[:500]}")

        # 2. Parse JSON strictly
        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response: {e}") from e

        # 3. Validate top-level structure
        if not isinstance(payload, dict):
            raise ValueError(f"Expected dict at root, got {type(payload).__name__}")

        if "data" not in payload:
            raise ValueError(f"Missing 'data' key in response. Keys: {list(payload.keys())}")

        data = payload["data"]
        if not isinstance(data, dict):
            raise ValueError(f"Expected 'data' to be dict, got {type(data).__name__}")

        # 4. Validate candles array
        if "candles" not in data:
            raise ValueError(f"Missing 'candles' key in data. Keys: {list(data.keys())}")

        candles = data["candles"]
        if not isinstance(candles, list):
            raise ValueError(f"Expected 'candles' to be list, got {type(candles).__name__}")

        if not candles:
            logger.warning(f"No candles returned for {context.get('instrument_key')}")
            return None, None

        # 5. Validate each candle structure - Upstox format: [timestamp, open, high, low, close, volume, oi]
        validated_candles = []
        for i, candle in enumerate(candles):
            if not isinstance(candle, (list, tuple)):
                raise ValueError(f"Candle {i}: expected list/tuple, got {type(candle).__name__}")
            if len(candle) < 6:
                raise ValueError(f"Candle {i}: expected at least 6 elements (ts,o,h,l,c,v), got {len(candle)}: {candle}")
            
            # Validate timestamp is parseable
            ts = candle[0]
            if not isinstance(ts, str):
                raise ValueError(f"Candle {i}: timestamp must be string, got {type(ts).__name__}")
            
            # Validate numeric fields
            try:
                validated_candle = [
                    ts,  # timestamp (string)
                    float(candle[1]),  # open
                    float(candle[2]),  # high
                    float(candle[3]),  # low
                    float(candle[4]),  # close
                    float(candle[5]),  # volume
                ]
                # Optional: open interest (7th element)
                if len(candle) > 6 and candle[6] is not None:
                    validated_candle.append(float(candle[6]))
                else:
                    validated_candle.append(0.0)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Candle {i}: non-numeric OHLCV: {candle}") from e

            validated_candles.append(validated_candle)

        # 6. Build normalized name from trading_symbol (strict)
        segment = context.get("segment", "").lower()
        trading_symbol = context.get("trading_symbol", "")
        
        if "fo" in segment:
            parts = trading_symbol.split()
            if len(parts) < 6:
                raise ValueError(f"FO trading_symbol format unexpected: '{trading_symbol}' (expected 6+ parts)")
            # Format: SYMBOL EXPIRY STRIKE OPTION_TYPE (e.g., "NIFTY 24AUG 5000 CE")
            # parts[0]=symbol, parts[1]=expiry, parts[2]=strike, parts[3]=type
            # User's original: a[0] + a[-3] + a[-2] + a[-1] + a[1] + a[2]
            name = parts[0] + parts[-3] + parts[-2] + parts[-1] + parts[1] + parts[2]
        else:
            name = context.get("name", "").upper()
            if not name:
                raise ValueError(f"INDEX segment missing 'name' in context")


        logger.debug(f"Normalized {len(validated_candles)} candles for {name}")
        return validated_candles, name
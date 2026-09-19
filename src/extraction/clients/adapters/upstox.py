from extraction.clients import MarketDataProvider
import requests
from shared.utils import unzipper
import json
from shared.observability import get_logger
from extraction.clients.registry import client_registry
from pathlib import Path
from datetime import datetime, timezone

logger = get_logger(__name__)


@client_registry.register("upstox")
class UpstoxAdapter(MarketDataProvider):
    MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    INTRADAY_URL = "https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/minutes/{interval}"
    HISTORICAL_URL = "https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{interval}/{end_date}/{start_date}"
    EXPIRED_HISTORICAL_URL = "https://api.upstox.com/v2/expired-instruments/historical-candle/{instrument_key}/{interval}minute/{end_date}/{start_date}"

    def _load_token(self) -> None:
        logger.debug("Loading access token")
        access_token_path = Path(__file__).parents[2] / "access_token" / "upstox.json"
        if not access_token_path.exists():
            logger.error("Access token file not found", extra={"path": str(access_token_path)})
            raise ValueError("Access token file not found")

        try:
            with access_token_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse token file", extra={"path": str(access_token_path), "error": str(e)})
            raise

        token = data.get("access_token")

        if not token or token == "":
            logger.error("Access token not found in token file", extra={"path": str(access_token_path)})
            raise ValueError("Access token not found in token file")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        self.HEADERS = headers
        logger.debug("Access token loaded successfully")

    def _fetch_master_instrument(self):
        """Normalize master instrument for all the adapters"""
        logger.info("Fetching master instrument list")
        try:
            self._load_token()
            logger.debug("Making request to master URL", extra={"url": self.MASTER_URL})
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            logger.debug("Master instrument response received", extra={"status_code": response.status_code, "size_bytes": len(response.content)})
            data = unzipper(response)
            logger.info("Master instrument fetched and decompressed", extra={"instrument_count": len(data) if isinstance(data, list) else "unknown"})
            return data
        except requests.RequestException as e:
            logger.error("Error fetching master instrument URL", extra={"error": str(e), "url": self.MASTER_URL}, exc_info=True)
            raise
        except (OSError, json.JSONDecodeError) as e:
            logger.error("Error decompressing or decoding JSON", extra={"error": str(e)}, exc_info=True)
            raise

    def fetch_instrument(self, context: dict, interval: int):
        instrument_key = context["instrument_key"]
        url = self.INTRADAY_URL.format(instrument_key=instrument_key, interval=interval)
        logger.debug("Fetching intraday instrument", extra={"instrument_key": instrument_key, "interval": interval, "url": url})
        response = self._retry_policy(url=url)
        if response is None:
            logger.warning("All retry attempts exhausted for intraday fetch", extra={"instrument_key": instrument_key})
            return None, None
        return self._normalize_response(response=response, context=context)

    def fetch_historical_instrument(self, context: dict, interval: int, start_date: str, end_date: str):
        instrument_key = context["instrument_key"]
        url = self.HISTORICAL_URL.format(instrument_key=instrument_key, interval=interval, end_date=end_date, start_date=start_date)
        logger.debug("Fetching historical instrument", extra={"instrument_key": instrument_key, "interval": interval, "start_date": start_date, "end_date": end_date, "url": url})
        response = self._retry_policy(url=url)
        if response is None:
            logger.warning("All retry attempts exhausted for historical fetch", extra={"instrument_key": instrument_key})
            return None, None
        return self._normalize_response(response=response, context=context)

    def fetch_expired_historical_instrument(self, context: dict, interval: int, start_date: str, end_date: str):
        instrument_key = context["instrument_key"]
        url = self.EXPIRED_HISTORICAL_URL.format(instrument_key=instrument_key, interval=interval, end_date=end_date, start_date=start_date)
        logger.debug("Fetching expired historical instrument", extra={"instrument_key": instrument_key, "interval": interval, "start_date": start_date, "end_date": end_date, "url": url})
        response = self._retry_policy(url=url)
        if response is None:
            logger.warning("All retry attempts exhausted for expired historical fetch", extra={"instrument_key": instrument_key})
            return None, None
        return self._normalize_response(response=response, context=context)

    def _normalize_response(self, response: requests.Response, context: dict):
        """
        Strict response normalization - FAILS LOUDLY if API contract changes.
        Returns (candles_list, normalized_name) on success.
        Raises ValueError with details if response structure is unexpected.
        """
        instrument_key = context.get("instrument_key", "unknown")
        logger.debug("Normalizing response", extra={"instrument_key": instrument_key, "status_code": response.status_code})

        # 1. Validate HTTP response
        if response.status_code != 200:
            logger.error("Non-200 response", extra={"status_code": response.status_code, "response_preview": response.text[:500]})
            raise ValueError(f"HTTP {response.status_code}: {response.text[:500]}")

        # 2. Parse JSON strictly
        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON response", extra={"error": str(e)}, exc_info=True)
            raise ValueError(f"Invalid JSON response: {e}") from e

        # 3. Validate top-level structure
        if not isinstance(payload, dict):
            logger.error("Expected dict at root", extra={"actual_type": type(payload).__name__})
            raise ValueError(f"Expected dict at root, got {type(payload).__name__}")

        if "data" not in payload:
            logger.error("Missing 'data' key in response", extra={"keys": list(payload.keys())})
            raise ValueError(f"Missing 'data' key in response. Keys: {list(payload.keys())}")

        data = payload["data"]
        if not isinstance(data, dict):
            logger.error("Expected 'data' to be dict", extra={"actual_type": type(data).__name__})
            raise ValueError(f"Expected 'data' to be dict, got {type(data).__name__}")

        # 4. Validate candles array
        if "candles" not in data:
            logger.error("Missing 'candles' key in data", extra={"keys": list(data.keys())})
            raise ValueError(f"Missing 'candles' key in data. Keys: {list(data.keys())}")

        candles = data["candles"]
        if not isinstance(candles, list):
            logger.error("Expected 'candles' to be list", extra={"actual_type": type(candles).__name__})
            raise ValueError(f"Expected 'candles' to be list, got {type(candles).__name__}")

        if not candles:
            logger.debug("No candles returned", extra={"instrument_key": instrument_key})
            return None, None

        # 5. Validate each candle structure - Upstox format: [timestamp, open, high, low, close, volume, oi]
        validated_candles = []
        for i, candle in enumerate(candles):
            if not isinstance(candle, (list, tuple)):
                logger.error("Candle format invalid", extra={"index": i, "actual_type": type(candle).__name__})
                raise ValueError(f"Candle {i}: expected list/tuple, got {type(candle).__name__}")
            if len(candle) < 6:
                logger.error("Candle has insufficient elements", extra={"index": i, "length": len(candle), "candle": candle})
                raise ValueError(f"Candle {i}: expected at least 6 elements (ts,o,h,l,c,v), got {len(candle)}: {candle}")

            # Validate timestamp is parseable
            ts = candle[0]
            if not isinstance(ts, str):
                logger.error("Candle timestamp not string", extra={"index": i, "actual_type": type(ts).__name__})
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
                logger.error("Non-numeric OHLCV in candle", extra={"index": i, "candle": candle, "error": str(e)})
                raise ValueError(f"Candle {i}: non-numeric OHLCV: {candle}") from e

            validated_candles.append(validated_candle)

        # 6. Build normalized name from trading_symbol (strict)
        segment = context.get("segment", "").lower()
        trading_symbol = context.get("trading_symbol", "")

        if "fo" in segment:
            parts = trading_symbol.split()
            if len(parts) < 5:
                logger.error("FO trading_symbol format unexpected", extra={"trading_symbol": trading_symbol, "parts_count": len(parts)})
                raise ValueError(f"FO trading_symbol format unexpected: '{trading_symbol}' (expected 6+ parts)")
            # Format: SYMBOL EXPIRY STRIKE OPTION_TYPE (e.g., "NIFTY 24AUG 5000 CE")
            symbol = context.get("asset_symbol", "")
            strike = int(context.get("strike_price", ""))
            strike = "" if strike == 0 else str(strike)
            instrument_type = context.get("instrument_type", "")
            expiry = context.get("expiry", "")
            expiry_date = datetime.fromtimestamp(expiry / 1000, tz=timezone.utc).strftime("%d%b%y").upper()
            name = symbol + "_" + expiry_date + "_" + strike + "_" + instrument_type
        else:
            name = context.get("name", "").upper()
            if not name:
                logger.error("INDEX segment missing 'name' in context", extra={"context_keys": list(context.keys())})
                raise ValueError(f"INDEX segment missing 'name' in context")

        logger.debug("Normalized candles", extra={"instrument_key": instrument_key, "name": name, "candle_count": len(validated_candles)})
        return validated_candles, name
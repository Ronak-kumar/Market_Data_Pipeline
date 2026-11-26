from extraction.clients import client_registry
import requests
from shared.utils import csv_reader
import csv
from shared.observability import get_logger
from extraction.clients import MarketDataProvider
from pathlib import Path
import json
from datetime import datetime, timezone

logger = get_logger(__name__)

ACCESS_TOKEN_PATH = Path("src/shared/config/access_token.json")

@client_registry.register("groww")
class GrowwAdapter(MarketDataProvider):
    MASTER_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
    HISTORICAL_URL = (
        "https://api.groww.in/v1/historical/candles"
        "?exchange={exchange}"
        "&segment={segment}"
        "&groww_symbol={groww_symbol}"
        "&start_time={start_time}"
        "&end_time={end_time}"
        "&candle_interval={candle_interval}")

    def _load_token(self) -> None:
        try:
            # read_text() reads the whole file in one go, closing it automatically
            data = json.loads(ACCESS_TOKEN_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            logger.error(f"Access token file not found: path={ACCESS_TOKEN_PATH}")
            raise ValueError("Access token file not found")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse token file: path={ACCESS_TOKEN_PATH}, error={e}")
            raise

        token = data.get("access_token")
        if not token:
            logger.error(f"Access token not found in token file: path={ACCESS_TOKEN_PATH}")
            raise ValueError("Access token not found in token file")

        self.HEADERS = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        logger.debug("Access token loaded successfully")

    def _fetch_master_instrument(self):
        logger.info(f"Fetching Groww master instrument list: url={self.MASTER_URL}")
        self._load_token()

        try:
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            logger.debug(f"Master instrument response received: status_code={response.status_code}, size_bytes={len(response.content)}")
            data = csv_reader(response)
            instrument_count = len(data) if isinstance(data, list) else "unknown"
            logger.info(f"Groww master instrument fetched and parsed: instrument_count={instrument_count}")
            return data
        except requests.RequestException as e:
            logger.error(f"Error fetching Groww master instrument URL: error={e}, url={self.MASTER_URL}")
            raise
        except csv.Error as e:
            logger.error(f"Error parsing CSV response: error={e}")
            raise

    def fetch_instrument(self, row: dict, variation: str, date: str, interval: int):
        return self.fetch_historical_instrument(
            context=row,
            interval=interval,
            start_date=date,
            end_date=date,
        )

    def fetch_historical_instrument(self, context: dict, interval: int, start_date: str, end_date: str):
        exchange = context.get("exchange")
        segment = context.get("base_segment")
        groww_symbol = context.get("groww_symbol")
        start_time = start_date + " 09:15:00"
        end_time = end_date + " 15:40:00"
        interval = str(interval) + "minute"
        logger.debug(f"Fetching Groww instrument: groww_symbol={groww_symbol}")
        url = self.HISTORICAL_URL.format(exchange=exchange, segment=segment, groww_symbol=groww_symbol, end_time=end_time, start_time=start_time, candle_interval=interval)
        logger.debug(f"Fetching historical instrument: groww_symbol={groww_symbol}, interval={interval}, start_date={start_date}, end_date={end_date}, url={url}")
        response = self._retry_policy(url=url)
        if response is None:
            logger.warning(f"All retry attempts exhausted for historical fetch: groww_symbol={groww_symbol}")
            return None
        return self._normalize_response(response=response, context=context)


    def _normalize_response(self, response: requests.Response, context: dict):
        """
        Strict response normalization - FAILS LOUDLY if API contract changes.
        Returns (candles_list, normalized_name) on success.
        Raises ValueError with details if response structure is unexpected.
        """
        groww_symbol = context.get("groww_symbol", "unknown")
        trading_symbol = context.get("trading_symbol", "")

        logger.debug(f"Normalizing response: groww_symbol={groww_symbol}, status_code={response.status_code}")

        # 1. Validate HTTP response
        if response.status_code != 200:
            logger.error(f"Non-200 response: status_code={response.status_code}, response_preview={response.text[:500]}")
            raise ValueError(f"HTTP {response.status_code}: {response.text[:500]}")

        # 2. Parse JSON strictly
        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: error={e}")
            raise ValueError(f"Invalid JSON response: {e}") from e

        # 3. Validate top-level structure
        if not isinstance(payload, dict):
            logger.error(f"Expected dict at root: actual_type={type(payload).__name__}")
            raise ValueError(f"Expected dict at root, got {type(payload).__name__}")

        if "payload" not in payload:
            logger.error(f"Missing 'data' key in response: keys={list(payload.keys())}")
            raise ValueError(f"Missing 'data' key in response. Keys: {list(payload.keys())}")

        data = payload["payload"]
        if not isinstance(data, dict):
            logger.error(f"Expected 'data' to be dict: actual_type={type(data).__name__}")
            raise ValueError(f"Expected 'data' to be dict, got {type(data).__name__}")

        # 4. Validate candles array
        if "candles" not in data:
            logger.error(f"Missing 'candles' key in data: keys={list(data.keys())}")
            raise ValueError(f"Missing 'candles' key in data. Keys: {list(data.keys())}")

        candles = data["candles"]
        if not isinstance(candles, list):
            logger.error(f"Expected 'candles' to be list: actual_type={type(candles).__name__}")
            raise ValueError(f"Expected 'candles' to be list, got {type(candles).__name__}")

        if not candles:
            logger.debug(f"No candles returned: groww_symbol={groww_symbol}")
            return None

        # 5. Validate each candle structure - Upstox format: [timestamp, open, high, low, close, volume, oi]
        validated_candles = []
        for i, candle in enumerate(candles):
            if not isinstance(candle, (list, tuple)):
                logger.error(f"Candle format invalid: index={i}, actual_type={type(candle).__name__}")
                raise ValueError(f"Candle {i}: expected list/tuple, got {type(candle).__name__}")
            if len(candle) < 6:
                logger.error(f"Candle has insufficient elements: index={i}, length={len(candle)}, candle={candle}")
                raise ValueError(f"Candle {i}: expected at least 6 elements (ts,o,h,l,c,v), got {len(candle)}: {candle}")

            validated_candle = self.__normalize_candles(candle=candle, context=context)
            validated_candles.append(validated_candle)

        logger.debug(f"Normalized candles: groww_symbol={groww_symbol}, trading_symbol={trading_symbol}, candle_count={len(validated_candles)}")
        return validated_candles

    def __normalize_candles(self, candle, context) -> list:
        # Validate timestamp is parseable
        ts = candle[0]
        name = context.get("symbol_name", "")
        symbol_asset_type = context.get("symbol_asset_class", "")
        dt = datetime.fromisoformat(candle[0])

        if not isinstance(ts, str):
            logger.error(f"Candle timestamp not string: Trading_Symbol={name}, actual_type={type(ts).__name__}")
            raise ValueError(f"Candle {name}: timestamp must be string, got {type(ts).__name__}")

        # Validate numeric fields
        try:
            validated_candle = [
                symbol_asset_type,
                name,
                dt.strftime("%d-%m-%Y"),
                dt.strftime("%H:%M:%S"),
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
            logger.error(f"Non-numeric OHLCV in candle: Trading_Symbol={name}, candle={candle}, error={e}")
            raise ValueError(f"Trading_Symbol {name}: non-numeric OHLCV: {candle}") from e

        return validated_candle
from extraction.clients import MarketDataProvider
import requests
from extraction.utils import unzipper
import json
from extraction.observability import get_logger
from extraction.clients.registry import client_registry
from pathlib import Path
logger  =  get_logger(__name__)


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
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}'}
        self.HEADERS = headers

    def _fetch_master_instrument(self):
        """ Normalize master instrument for all the adapters """
        try:
            self._load_token()
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            return unzipper(response)
        except requests.RequestException as e:
            logger.error(f"Error fetching the URL: {e}")
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error decompressing or decoding JSON: {e}")

    def fetch_instrument(self,context: dict, interval:int):
        instrument_key = context["instrument_key"]
        URL = self.INTRADAY_URL.format(instrument_key=instrument_key, interval=interval)
        response = self._retry_policy(url=URL, logger=logger)
        response = self._retry_policy(url=URL, logger=logger)
        if response == None:
            return None, None
        return self._normalize_response(response=response, context=context)


    def fetch_historical_instrument(self, context: dict, interval:int, start_date: str, end_date: str):
        instrument_key = context["instrument_key"]
        URL = self.HISTORICAL_URL.format(instrument_key=instrument_key, interval=interval, end_date=end_date, start_date=start_date)
        response = self._retry_policy(url=URL, logger=logger)
        if response == None:
            return None, None
        return self._normalize_response(response=response, context=context)

    def _normalize_response(self, response: requests.Response, context:dict):
        """ To get normalize response from all the adapters """
        if "fo" in context["segment"].lower():
            try:
                a = context["trading_symbol"].split()
                name = a[0] + a[-3] + a[-2] + a[-1] + a[1] + a[2]
            except IndexError:
                return None, None
        elif "index" in context["segment"].lower():
            name = context["name"]
            name = name.upper()
        else:
            name = context["name"] + ".NSE_IDX"
            name = name.upper()

        try:
            candles = response.json()["data"]["candles"]
            if not candles:
                return None, None
        except Exception:
            return None, None

        return candles, name
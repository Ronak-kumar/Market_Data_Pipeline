from extraction.clients import MarketDataProvider
import requests
from extraction.utils import unzipper
import json
from extraction.observability import get_logger
from extraction.clients.registry import client_registry
logger  =  get_logger(__name__)
from pathlib import Path

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
        try:
            self._load_token()
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            return unzipper(response)
        except requests.RequestException as e:
            logger.error(f"Error fetching the URL: {e}")
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error decompressing or decoding JSON: {e}")

    def fetch_instrument(self, instrument_key: str, interval:int):
        URL = self.INTRADAY_URL.format(instrument_key=instrument_key, interval=interval)
        response = self.session_client.get(URL, headers=self.HEADERS, timeout=self.timeout)
        return response

    def fetch_historical_instrument(self, instrument_key: str, interval:int, start_date: str, end_date: str):
        URL = self.HISTORICAL_URL.format(instrument_key=instrument_key, interval=interval, end_date=end_date, start_date=start_date)
        response = self.session_client.get(URL, headers=self.HEADERS, timeout=self.timeout)
        return response
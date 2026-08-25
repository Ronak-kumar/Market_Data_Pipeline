from extraction.clients import MarketDataProvider
import requests
from extraction.utils import unzipper
import json
from extraction.observability import get_logger
from extraction.clients.registry import ClientRegistry
logger  =  get_logger(__name__)

@ClientRegistry.register("upstox")
class UpstoxAdapter(MarketDataProvider):
    MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    CANDLE_URL = "https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{interval}"

    def _fetch_master_instrument(self):
        try:
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            return unzipper(response)
        except requests.RequestException as e:
            logger.error(f"Error fetching the URL: {e}")
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error decompressing or decoding JSON: {e}")

    def fetch_instrument(self, instrument: str):
        data = self.session_client.get(instrument)
        return data
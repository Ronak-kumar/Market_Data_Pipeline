from extraction.clients import MarketDataProvider
import requests
from extraction.utils import csv_reader
import csv
from extraction.observability import get_logger
from extraction.clients.registry import client_registry
logger  =  get_logger(__name__)

@client_registry.register("groww")
class GrowwAdapter(MarketDataProvider):
    MASTER_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
    CANDLE_URL = "https://api.groww.in/v1/historical/candles"

    def _fetch_master_instrument(self):
        try:
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            return csv_reader(response)
        except requests.RequestException as e:
            logger.error(f"Error fetching the URL: {e}")
        except csv.Error as e:
            logger.error(f"Error parsing CSV response: {e}")

    def fetch_instrument(self, instrument: str):
        data = self.session_client.get(instrument)
        return data


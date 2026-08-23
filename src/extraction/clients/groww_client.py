from extraction.clients import MarketDataProvider
import requests
from extraction.utils import csv_reader
import csv

class GrowwAdapter(MarketDataProvider):
    MASTER_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
    CANDLE_URL = "https://api.groww.in/v1/historical/candles"

    def _fetch_master_instrument(self):
        try:
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            self.master_instrument_data = csv_reader(response)
        except requests.RequestException as e:
            print(f"Error fetching the URL: {e}")
        except csv.Error as e:
            print(f"Error parsing CSV response: {e}")
            self.master_instrument_data = []

    def extract_data(self, instrument: str):
        data = self.session_client.get(instrument)
        return data


from extraction.clients.market_adpter import MarketDataProvider
from extraction.utils.session import client_session
import requests
from extraction.utils.utillities import unzipper
import json

class UpstoxAdapter(MarketDataProvider):
    def __init__(self):
        self.upstox_client = client_session
        self.upstox_intraday_url = 'https://api.upstox.com/v3/historical-candle/intraday/%s/minutes/%s'

    def _master_instrument(self, timeout: int = 5):
        url = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz'
        try:
            response = self.upstox_client.get(url, timeout=timeout)
            response.raise_for_status()
            self.master_instrument_data = unzipper(response)
        except requests.RequestException as e:
            print(f"Error fetching the URL: {e}")
        except (OSError, json.JSONDecodeError) as e:
            print(f"Error decompressing or decoding JSON: {e}")
            self.data = None

    def extract_data(self, instrument: str):
        # Implement the data extraction logic using the Upstox client
        data = self.upstox_client.get_data(instrument)
        return data
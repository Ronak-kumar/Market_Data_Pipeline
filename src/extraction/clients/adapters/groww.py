from extraction.clients import MarketDataProvider
import requests
from shared.utils import csv_reader
import csv
from shared.observability import get_logger
from extraction.clients.registry import client_registry

logger = get_logger(__name__)


@client_registry.register("groww")
class GrowwAdapter(MarketDataProvider):
    MASTER_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
    CANDLE_URL = "https://api.groww.in/v1/historical/candles"

    def _fetch_master_instrument(self):
        logger.info("Fetching Groww master instrument list", extra={"url": self.MASTER_URL})
        try:
            response = self.session_client.get(self.MASTER_URL, timeout=self.timeout)
            response.raise_for_status()
            logger.debug("Master instrument response received", extra={"status_code": response.status_code, "size_bytes": len(response.content)})
            data = csv_reader(response)
            logger.info("Groww master instrument fetched and parsed", extra={"instrument_count": len(data) if isinstance(data, list) else "unknown"})
            return data
        except requests.RequestException as e:
            logger.error("Error fetching Groww master instrument URL", extra={"error": str(e), "url": self.MASTER_URL}, exc_info=True)
            raise
        except csv.Error as e:
            logger.error("Error parsing CSV response", extra={"error": str(e)}, exc_info=True)
            raise

    def fetch_instrument(self, instrument: str):
        logger.debug("Fetching Groww instrument", extra={"instrument": instrument})
        data = self.session_client.get(instrument)
        logger.debug("Groww instrument response received", extra={"status_code": getattr(data, 'status_code', 'unknown')})
        return data
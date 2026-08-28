from abc import ABC, abstractmethod
from extraction.utils import get_session
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List
import time as tm
import requests

class MarketDataProvider(ABC):
    def __init__(self, timeout:int=5, max_retries:int=3):
        self.timeout = timeout
        self.max_retries = max_retries
        self._expiry_suffixe = []
        self._reset_client_session()
        self._initialize()

    def get_expiry_suffixes(self, process_able_date: datetime, months_ahead: int = 2) -> List[str]:
        return [
            (process_able_date + relativedelta(months=i)).strftime("%b %y").upper()
            for i in range(months_ahead)]

    def _initialize(self):
        self.master_instrument_data = self._fetch_master_instrument()

    def _reset_client_session(self):
        self.session_client = get_session()

    def _retry_policy(self, url:str, logger):
        retries = self.max_retries
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session_client.get(url, headers=self.HEADERS, timeout=self.timeout)

                if response.status_code == 429:
                    ### Resetting session ###
                    self._reset_client_session()
                    logger.warning(f"[TOO MANY REQUESTS] Waiting 1 sec before next attempt and resetting session")
                    tm.sleep(1)
                    continue

                if response.status_code == 200:
                    return response

                if response.status_code in (400, 401, 403, 404):
                    logger.warning(f"[FATAL] {url} | {response.status_code} | {response.text}")
                    break

                logger.warning(f"[RETRY] {url} | {response.status_code} | attempt {attempt}/{retries}")

            except requests.exceptions.Timeout:
                tm.sleep((attempt * 2))
                logger.warning(f"[TIMEOUT] {url} | attempt {attempt}/{retries}")

            except requests.exceptions.ConnectionError:
                tm.sleep((attempt * 2))
                logger.warning(f"[TIMEOUT] {url} | attempt {attempt}/{retries}")

            except requests.exceptions.RequestException:
                tm.sleep((attempt * 2))
                logger.warning(f"[TIMEOUT] {url} | attempt {attempt}/{retries}")

        return None
    
    @abstractmethod
    def _load_token(self):
        """Load corresponding access token for per adapter"""
        ...

    @abstractmethod
    def _fetch_master_instrument(self):
        """Download and parse master instrument list"""
        ...

    @abstractmethod
    def fetch_instrument(self, instrument:str):
        """Fetch historical candles for a resolved instrument_key"""
        ...

    @abstractmethod
    def _normalize_response(self, response: requests.Response, context:dict):
        """ To get normalize response from all the adapters """
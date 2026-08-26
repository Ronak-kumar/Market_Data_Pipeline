from abc import ABC, abstractmethod
from extraction.utils import client_session
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List

class MarketDataProvider(ABC):
    def __init__(self, timeout:int=5, max_retries:int=3):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session_client = self._create_session()
        self._initialize()

    def get_expiry_suffixes(self, process_able_date: datetime, months_ahead: int = 2) -> List[str]:
        return [
            (process_able_date + relativedelta(months=i)).strftime("%b %y").upper()
            for i in range(months_ahead)]

    def _create_session(self):
        session = client_session
        return session

    def _initialize(self):
        self.master_instrument_data = self._fetch_master_instrument()

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
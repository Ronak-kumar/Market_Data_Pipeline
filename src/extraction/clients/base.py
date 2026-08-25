from abc import ABC, abstractmethod
from extraction.utils import client_session


class MarketDataProvider(ABC):
    def __init__(self, timeout:int=5, max_retries:int=3):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session_client = self._create_session()
        self._initialize()

    def _create_session(self):
        session = client_session
        return session

    def _initialize(self):
        self.master_instrument_data = self._fetch_master_instrument()
    
    @abstractmethod
    def _fetch_master_instrument(self):
        """Download and parse master instrument list"""
        ...

    @abstractmethod
    def fetch_instrument(self, instrument:str):
        """Fetch historical candles for a resolved instrument_key"""
        ...
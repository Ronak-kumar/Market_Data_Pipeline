from abc import ABC, abstractmethod
from shared.utils import get_session
from typing import Optional
import time as tm
import requests
from shared.observability import get_logger

logger = get_logger(__name__)


class MarketDataProvider(ABC):
    def __init__(self, timeout: int = 5, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self._expiry_suffixes = []
        logger.debug(f"Initializing MarketDataProvider: timeout={timeout}, max_retries={max_retries}")
        self._reset_client_session()
        self._initialize()

    def _initialize(self) -> None:
        logger.info("Initializing provider, fetching master instrument")
        try:
            self.master_instrument_data = self._fetch_master_instrument()
            count = len(self.master_instrument_data) if hasattr(self.master_instrument_data, '__len__') else "unknown"
            logger.info(f"Provider initialization complete: master_instrument_count={count}")
        except Exception as e:
            logger.error(f"Provider initialization failed: {e}")
            raise

    def _reset_client_session(self) -> None:
        logger.debug("Resetting client session")
        self.session_client = get_session()

    def _retry_policy(self, url: str) -> Optional[requests.Response]:
        """
        Retry policy that captures ALL errors for later inspection.
        Returns response on success, None on exhaustion.
        Errors are logged with full context for debugging/retry decisions.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session_client.get(url, headers=self.HEADERS, timeout=self.timeout)

                if response.status_code == 200:
                    return response

                if response.status_code == 429:
                    logger.warning(f"[RATE_LIMIT] Resetting session: url={url}, attempt={attempt}")
                    self._reset_client_session()
                    tm.sleep(1)
                    continue

                if 400 <= response.status_code < 500:
                    logger.error(f"[FATAL] Client error {response.status_code}: url={url}, attempt={attempt}")
                    break

                # 5xx errors (retryable)
                logger.warning(f"[RETRY] Server error {response.status_code}: url={url}, attempt={attempt}")
                tm.sleep(attempt * 2)  # Added missing sleep! Original code hammered the server instantly on 5xx.

            except requests.exceptions.RequestException as e:
                # RequestException is the base class for Timeout and ConnectionError
                logger.warning(f"[{type(e).__name__.upper()}] Request failed: url={url}, attempt={attempt}, error={str(e)[:200]}")
                tm.sleep(attempt * 2)

        logger.error(f"[EXHAUSTED] All retry attempts failed: url={url}, max_retries={self.max_retries}")
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
    def fetch_instrument(self, row: dict, variation: str, date: str, interval: int):
        """Fetch historical candles for a resolved instrument_key"""
        ...

    @abstractmethod
    def _normalize_response(self, response: requests.Response, context: dict):
        """Normalize response from all adapters - MUST raise on contract violation"""
        ...
from abc import ABC, abstractmethod
from extraction.utils import get_session
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Optional
import time as tm
import requests

class MarketDataProvider(ABC):
    def __init__(self, timeout: int = 5, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self._expiry_suffixes = []
        self._reset_client_session()
        self._initialize()

    def get_expiry_suffixes(self, process_able_date: datetime, months_ahead: int = 3) -> List[str]:
        return [
            (process_able_date + relativedelta(months=i)).strftime("%b %y").upper()
            for i in range(months_ahead)
        ]

    def _initialize(self) -> None:
        self.master_instrument_data = self._fetch_master_instrument()

    def _reset_client_session(self) -> None:
        self.session_client = get_session()

    def _retry_policy(self, url: str, logger) -> Optional[requests.Response]:
        """
        Retry policy that captures ALL errors for later inspection.
        Returns response on success, None on exhaustion.
        Errors are logged with full context for debugging/retry decisions.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session_client.get(url, headers=self.HEADERS, timeout=self.timeout)

                if response.status_code == 429:
                    # Rate limited - reset session and wait
                    self._reset_client_session()
                    logger.warning(
                        f"[RATE_LIMIT] {url} | attempt {attempt}/{self.max_retries} | "
                        f"retry_after=1s | session_reset=true"
                    )
                    tm.sleep(1)
                    continue

                if response.status_code == 200:
                    logger.debug(f"[OK] {url} | attempt {attempt}")
                    return response

                # 4xx = fatal, don't retry (except 429 handled above)
                if 400 <= response.status_code < 500:
                    logger.error(
                        f"[FATAL] {url} | status={response.status_code} | "
                        f"attempt={attempt} | response={response.text[:500]}"
                    )
                    break

                # 5xx = retryable
                logger.warning(
                    f"[RETRY] {url} | status={response.status_code} | "
                    f"attempt {attempt}/{self.max_retries} | response={response.text[:200]}"
                )

            except requests.exceptions.Timeout:
                logger.warning(
                    f"[TIMEOUT] {url} | attempt {attempt}/{self.max_retries} | "
                    f"timeout={self.timeout}s"
                )
                tm.sleep(attempt * 2)

            except requests.exceptions.ConnectionError as e:
                logger.warning(
                    f"[CONNECTION_ERROR] {url} | attempt {attempt}/{self.max_retries} | "
                    f"error={str(e)[:200]}"
                )
                tm.sleep(attempt * 2)

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"[REQUEST_ERROR] {url} | attempt {attempt}/{self.max_retries} | "
                    f"error={str(e)[:200]}"
                )
                tm.sleep(attempt * 2)

        logger.error(f"[EXHAUSTED] {url} | all {self.max_retries} attempts failed")
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
    def fetch_instrument(self, context: dict, interval: int):
        """Fetch historical candles for a resolved instrument_key"""
        ...

    @abstractmethod
    def _normalize_response(self, response: requests.Response, context: dict):
        """Normalize response from all adapters - MUST raise on contract violation"""
        ...
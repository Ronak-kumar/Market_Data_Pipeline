from abc import ABC, abstractmethod
from shared.utils import get_session
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Optional
import time as tm
import requests
from shared.observability import get_logger

logger = get_logger(__name__)


class MarketDataProvider(ABC):
    def __init__(self, timeout: int = 5, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self._expiry_suffixes = []
        logger.debug("Initializing MarketDataProvider", extra={"timeout": timeout, "max_retries": max_retries})
        self._reset_client_session()
        self._initialize()

    def get_expiry_suffixes(self, process_able_date: datetime, months_ahead: int = 3) -> List[str]:
        suffixes = [
            (process_able_date + relativedelta(months=i)).strftime("%b %y").upper()
            for i in range(months_ahead)
        ]
        logger.debug("Generated expiry suffixes", extra={"process_able_date": str(process_able_date), "months_ahead": months_ahead, "suffixes": suffixes})
        return suffixes

    def _initialize(self) -> None:
        logger.info("Initializing provider, fetching master instrument")
        try:
            self.master_instrument_data = self._fetch_master_instrument()
            logger.info("Provider initialization complete", extra={"master_instrument_count": len(self.master_instrument_data) if hasattr(self.master_instrument_data, '__len__') else "unknown"})
        except Exception as e:
            logger.error("Provider initialization failed", extra={"error": str(e)}, exc_info=True)
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
        logger.debug("Starting retry policy", extra={"url": url, "max_retries": self.max_retries, "timeout": self.timeout})
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug("Making HTTP request", extra={"url": url, "attempt": attempt, "max_retries": self.max_retries})
                response = self.session_client.get(url, headers=self.HEADERS, timeout=self.timeout)

                if response.status_code == 429:
                    # Rate limited - reset session and wait
                    logger.warning(
                        "[RATE_LIMIT] Rate limited, resetting session",
                        extra={
                            "url": url,
                            "attempt": attempt,
                            "max_retries": self.max_retries,
                            "retry_after": "1s",
                            "session_reset": True
                        }
                    )
                    self._reset_client_session()
                    tm.sleep(1)
                    continue

                if response.status_code == 200:
                    logger.debug(
                        "[OK] Request successful",
                        extra={"url": url, "attempt": attempt, "response_size": len(response.content)}
                    )
                    return response

                # 4xx = fatal, don't retry (except 429 handled above)
                if 400 <= response.status_code < 500:
                    logger.error(
                        "[FATAL] Client error, not retrying",
                        extra={
                            "url": url,
                            "status": response.status_code,
                            "attempt": attempt,
                            "response_preview": response.text[:500]
                        }
                    )
                    break

                # 5xx = retryable
                logger.warning(
                    "[RETRY] Server error, will retry",
                    extra={
                        "url": url,
                        "status": response.status_code,
                        "attempt": attempt,
                        "max_retries": self.max_retries,
                        "response_preview": response.text[:200]
                    }
                )

            except requests.exceptions.Timeout:
                logger.warning(
                    "[TIMEOUT] Request timed out",
                    extra={
                        "url": url,
                        "attempt": attempt,
                        "max_retries": self.max_retries,
                        "timeout": f"{self.timeout}s"
                    }
                )
                tm.sleep(attempt * 2)

            except requests.exceptions.ConnectionError as e:
                logger.warning(
                    "[CONNECTION_ERROR] Connection failed",
                    extra={
                        "url": url,
                        "attempt": attempt,
                        "max_retries": self.max_retries,
                        "error": str(e)[:200]
                    }
                )
                tm.sleep(attempt * 2)

            except requests.exceptions.RequestException as e:
                logger.warning(
                    "[REQUEST_ERROR] Request failed",
                    extra={
                        "url": url,
                        "attempt": attempt,
                        "max_retries": self.max_retries,
                        "error": str(e)[:200]
                    }
                )
                tm.sleep(attempt * 2)

        logger.error(
            "[EXHAUSTED] All retry attempts failed",
            extra={"url": url, "max_retries": self.max_retries}
        )
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
    def fetch_historical_instrument(self, context: dict, interval: int, start_date: str, end_date: str):
        """Fetch historical candles for a resolved instrument_key"""
        ...

    @abstractmethod
    def fetch_expired_historical_instrument(self, context: dict, interval: int, start_date: str, end_date: str):
        """Fetch historical candles for a resolved instrument_key"""
        ...

    @abstractmethod
    def _normalize_response(self, response: requests.Response, context: dict):
        """Normalize response from all adapters - MUST raise on contract violation"""
        ...
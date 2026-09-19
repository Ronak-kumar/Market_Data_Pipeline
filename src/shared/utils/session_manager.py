import requests
import logging

# Use basic logging to avoid circular import
logger = logging.getLogger(__name__)


def get_session() -> requests.Session:
    logger.debug("Creating new requests session with connection pooling")
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=0,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount('https://', adapter)
    logger.debug("Session created with HTTPAdapter", extra={"pool_connections": 10, "pool_maxsize": 10})
    return session


client_session = get_session()
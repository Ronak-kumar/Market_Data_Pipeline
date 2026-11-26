import requests
from shared.observability import get_logger

logger = get_logger(__name__)


def get_adapter() -> requests.adapters.HTTPAdapter:
    logger.debug("Creating HTTPAdapter with connection pooling")
    adapter = requests.adapters.HTTPAdapter(
        max_retries=0,
        pool_connections=10,
        pool_maxsize=10
    )
    logger.debug("HTTPAdapter created: pool_connections=10, pool_maxsize=10")
    return adapter


def get_session() -> requests.Session:
    logger.debug("Creating new requests session with connection pooling")
    session = requests.Session()
    adapter = get_adapter()
    session.mount('https://', adapter)
    logger.debug("Session created with HTTPAdapter: pool_connections=10, pool_maxsize=10")
    return session


client_session = get_session()
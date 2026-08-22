import requests
from urllib3.util.retry import Retry
from functools import lru_cache

@lru_cache
def get_session() -> requests.Session :
    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],
        allowed_methods=[
            "GET", "POST"
        ],
    )


    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount('https://', adapter)

    return session


client_session = get_session()
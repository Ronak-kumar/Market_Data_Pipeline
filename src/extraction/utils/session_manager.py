import requests
from urllib3.util.retry import Retry
from functools import lru_cache

def get_session() -> requests.Session :
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=0,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount('https://', adapter)

    return session


client_session = get_session()
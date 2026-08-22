from typing import Any, Dict, Optional
import requests
import gzip
import json
import yaml
from io import BytesIO

@staticmethod
def unzipper(response: requests.Response) -> Dict[str, Any]:
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
        data = json.load(f)
    return data

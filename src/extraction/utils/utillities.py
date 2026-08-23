from typing import Any, Dict, Optional
import requests
import gzip
import json
import yaml
from io import BytesIO, StringIO
import csv

def unzipper(response: requests.Response) -> Dict[str, Any]:
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
        data = json.load(f)
        return data

def csv_reader(response: requests.Response) -> list[Dict[str, Any]]:
    content = response.content.decode("utf-8")
    reader = csv.DictReader(StringIO(content))
    data = list(reader)
    return data

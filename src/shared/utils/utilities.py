from typing import Any, Dict
import requests
import gzip
import json
from io import BytesIO, StringIO
import csv
from shared.observability import get_logger

logger = get_logger(__name__)


def unzipper(response: requests.Response) -> Dict[str, Any]:
    logger.debug(f"Decompressing gzip response: size_bytes={len(response.content)}")
    try:
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
            data = json.load(f)
        keys = list(data.keys()) if isinstance(data, dict) else "non-dict"
        logger.debug(f"Gzip decompression successful: keys={keys}")
        return data
    except Exception as e:
        logger.error(f"Gzip decompression failed: error={e}, size_bytes={len(response.content)}")
        raise


def csv_reader(response: requests.Response) -> list[Dict[str, Any]]:
    logger.debug(f"Parsing CSV response: size_bytes={len(response.content)}")
    try:
        content = response.content.decode("utf-8")
        reader = csv.DictReader(StringIO(content))
        data = list(reader)
        columns = list(data[0].keys()) if data else []
        logger.debug(f"CSV parsing successful: row_count={len(data)}, columns={columns}")
        return data
    except Exception as e:
        logger.error(f"CSV parsing failed: error={e}, size_bytes={len(response.content)}")
        raise
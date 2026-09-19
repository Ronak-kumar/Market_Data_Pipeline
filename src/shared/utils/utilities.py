from typing import Any, Dict
import requests
import gzip
import json
from io import BytesIO, StringIO
import csv
import logging

logger = logging.getLogger(__name__)


def unzipper(response: requests.Response) -> Dict[str, Any]:
    logger.debug("Decompressing gzip response", extra={"size_bytes": len(response.content)})
    try:
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as f:
            data = json.load(f)
        logger.debug("Gzip decompression successful", extra={"keys": list(data.keys()) if isinstance(data, dict) else "non-dict"})
        return data
    except Exception as e:
        logger.error("Gzip decompression failed", extra={"error": str(e), "size_bytes": len(response.content)}, exc_info=True)
        raise


def csv_reader(response: requests.Response) -> list[Dict[str, Any]]:
    logger.debug("Parsing CSV response", extra={"size_bytes": len(response.content)})
    try:
        content = response.content.decode("utf-8")
        reader = csv.DictReader(StringIO(content))
        data = list(reader)
        logger.debug("CSV parsing successful", extra={"row_count": len(data), "columns": list(data[0].keys()) if data else []})
        return data
    except Exception as e:
        logger.error("CSV parsing failed", extra={"error": str(e), "size_bytes": len(response.content)}, exc_info=True)
        raise
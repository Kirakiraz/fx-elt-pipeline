import logging
import requests

from typing import Any
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# HTTP session — retry on transient errors only (5xx/429)
# ------------------------------------------------------------
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
session = requests.Session()
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ------------------------------------------------------------
# API
# ------------------------------------------------------------
API_BASE = "https://api.frankfurter.dev/v2"

# ============================================================
# Extract: API → return raw JSON
# ============================================================


def fetch_fx_data(start_date: str) -> list[dict[str, Any]]:
    url = f"{API_BASE}/rates"
    current_date = datetime.now().strftime("%Y-%m-%d")

    params = {
        "base": "USD",
        "quotes": "THB,JPY,EUR,GBP,SGD",
        "from": start_date,
        "to": current_date,
        "providers": "ECB",
    }

    logger.info(f"Fetching from {url}")
    response = session.get(url, params=params, timeout=10)
    response.raise_for_status()

    payload = response.json()
    if not payload:
        logger.info("no new data since watermark")
    else:
        dates = {record["date"] for record in payload}
        logger.info(f"Got {len(dates)} day(s) ({min(dates)} - {max(dates)})")

    return payload

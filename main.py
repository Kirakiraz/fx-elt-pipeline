import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Any

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ============================================================
# Config
# ============================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

API_BASE = "https://api.frankfurter.dev/v2"
SOURCE_NAME = "frankfurter"
SQL_DIR = Path(__file__).parent / "sql"

# ============================================================
# Main: orchestrate the ELT flow
# ============================================================


def main():
    engine = create_engine(DB_URL)

    try:
        start_date = get_last_loaded_date(engine)
        logger.info(f"Last loaded date found in DB: {start_date}")

        payload = fetch_fx_data(start_date)
        load_to_raw(payload, engine)
        logger.info("✓ Raw load done")

        transform_to_staging(engine)
        logger.info("✓ Staging complete")

        transform_to_fx_daily(engine)
        logger.info("✓ Mart: fx_daily (OBT) upsert complete")

        transform_to_fact(engine)
        logger.info("✓ Mart: fact_exchange_rate (star) upsert complete")

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        engine.dispose()

# ============================================================
# Read latest loaded date for incremental fetch
# ============================================================


def get_last_loaded_date(engine) -> str:
    """Return last loaded date for incremental fetch, or backfill date if empty."""

    query = text("SELECT MAX(source_date) FROM staging.stg_exchange_rate")

    with engine.connect() as conn:
        result = conn.execute(query)
        last_date = result.scalar()

    if last_date:
        return last_date.strftime("%Y-%m-%d")

    # First run: table empty → backfill from project's chosen start date
    return "2024-01-01"

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
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    payload = response.json()
    logger.info(f"Got {len(payload)} records")
    return payload

# ============================================================
# Load: INSERT raw JSON payload into raw.api_response
# ============================================================


def load_to_raw(payload: list[dict[str, Any]], engine) -> int:
    query = text("""
        INSERT INTO raw.api_response (source, payload)
        VALUES (:source, CAST(:payload AS JSONB))
        RETURNING id
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {
            "source": SOURCE_NAME,
            "payload": json.dumps(payload)
        })
        new_id = result.scalar()
        conn.commit()

    logger.info(f"Inserted into raw.api_response (id={new_id})")
    return new_id  # available for incremental staging transform (planned)

# ============================================================
# Transform: raw payload → staging.stg_exchange_rate (unnest + dedup)
# ============================================================


def transform_to_staging(engine) -> None:
    with open(SQL_DIR / "transform" / "stg_exchange_rate.sql", "r", encoding="utf-8") as f:
        staging_sql = f.read()

    with engine.connect() as conn:
        result = conn.execute(text(staging_sql))
        conn.commit()
    logger.info(f"staging upsert done ({result.rowcount} rows affected)")

# ============================================================
# Transform: staging → mart.fx_daily (denormalized OBT, window-function metrics)
# ============================================================


def transform_to_fx_daily(engine) -> None:
    with open(SQL_DIR / "transform" / "fx_daily.sql", "r", encoding="utf-8") as f:
        fx_daily_sql = f.read()

    with engine.connect() as conn:
        result = conn.execute(text(fx_daily_sql))
        conn.commit()
    logger.info(f"fx_daily upsert done ({result.rowcount} rows affected)")

# ============================================================
# Transform: staging → mart.fact_exchange_rate (star schema fact, raw rate only)
# ============================================================


def transform_to_fact(engine) -> None:
    with open(SQL_DIR / "transform" / "fact_exchange_rate.sql", "r", encoding="utf-8") as f:
        fact_sql = f.read()

    with engine.connect() as conn:
        result = conn.execute(text(fact_sql))
        conn.commit()
    logger.info(
        f"fact_exchange_rate upsert done ({result.rowcount} rows affected)")


if __name__ == "__main__":
    main()

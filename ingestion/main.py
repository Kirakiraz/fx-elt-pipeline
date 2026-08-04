import sys
import logging

from watermark import get_last_loaded_postgres_date
from db import get_engine
from bq import get_bq_client
from load_postgres import load_to_raw
from extract import fetch_fx_data
from transform_postgres import transform_to_staging, transform_to_fact
from watermark import get_last_loaded_postgres_date, get_last_loaded_bigquery_id
from sync_bigquery import sync_to_bigquery

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# Main: orchestrate the ELT flow
# ============================================================

def main():
    engine = get_engine()
    bq_client = get_bq_client()

    try:
        start_date = get_last_loaded_postgres_date(engine)
        logger.info(f"Last loaded date found in DB: {start_date}")

        payload = fetch_fx_data(start_date)
        load_to_raw(payload, engine)
        logger.info("✓ Raw load done")

        transform_to_staging(engine)
        logger.info("✓ Staging complete")

        transform_to_fact(engine)
        logger.info("✓ Mart: fact_exchange_rate upsert complete")

        last_synced_id = get_last_loaded_bigquery_id(bq_client)
        sync_to_bigquery(engine, bq_client, last_synced_id)
        logger.info("✓ BigQuery sync complete")

    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

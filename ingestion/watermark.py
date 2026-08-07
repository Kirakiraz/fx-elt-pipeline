from sqlalchemy import text
from bq import RAW_TABLE_ID
from google.api_core.exceptions import NotFound


def get_last_loaded_postgres_date(engine) -> str:
    """Return last loaded date for incremental fetch, or backfill date if empty."""

    query = text("SELECT MAX(source_date) FROM staging.stg_exchange_rate")

    with engine.connect() as conn:
        result = conn.execute(query)
        last_date = result.scalar()

    if last_date:
        return last_date.strftime("%Y-%m-%d")

    # First run: table empty → backfill from project's chosen start date
    return "2024-01-01"


def get_last_loaded_bigquery_id(client) -> int:
    """Return last id from raw_api_response in bigquery. Use id to sync Postgres and BigQuery"""

    query = f"SELECT MAX(id) AS max_id FROM `{RAW_TABLE_ID}`"
    try:
        row = next(iter(client.query(query).result()))
        return row.max_id if row.max_id is not None else 0
    except NotFound:
        # Table not created yet (clean start); the sync load job will create it
        return 0

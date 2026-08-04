from sqlalchemy import text


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


def get_last_loaded_bigquery_id(client) -> str:
    """Return last id from of raw_api_response in bq. Use id to sync Postgres and BigQuery"""

    query = "SELECT MAX(id) AS max_id FROM `currency-elt.fx_dataset.raw_api_response`"
    job = client.query(query)
    rows = job.result()
    row = next(iter(rows))
    start_id = row.max_id

    if start_id:
        return start_id

    # First run: Table empty → backfill from first Postgres's payload raw.api_response to latest payload
    return 0

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


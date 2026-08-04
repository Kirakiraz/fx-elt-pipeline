from sqlalchemy import text
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

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

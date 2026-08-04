import json
import logging
from typing import Any
from sqlalchemy import text

logger = logging.getLogger(__name__)
SOURCE_NAME = "frankfurter"

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

import json
import logging
from pathlib import Path
from sqlalchemy import text

from google.cloud import bigquery
from bq import RAW_TABLE_ID


logger = logging.getLogger(__name__)

# transient staging file — overwritten each run, must stay in .gitignore
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "raw.jsonl"


def sync_to_bigquery(engine, bq_client, last_synced_id) -> None:
    # 1. Pull only the delta from PG — rows BigQuery doesn't have yet
    query = text("""
        SELECT id, fetched_at, source, payload
        FROM raw.api_response
        WHERE id > :watermark
        ORDER BY id
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"watermark": last_synced_id}).fetchall()

    # 2. Nothing new → BigQuery is in sync, stop before touching the file/BQ
    if not rows:
        logger.info("BigQuery already up to date (nothing to sync)")
        return

    # 3. Write NDJSON — one object per line, not a JSON array
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for row in rows:
            record = {
                "id": row.id,
                "fetched_at": row.fetched_at.isoformat(),  # datetime → ISO string
                "source": row.source,
                # already a list (psycopg2 decodes JSONB)
                "payload": row.payload,
            }
            f.write(json.dumps(record) + "\n")

    # 4. Append to the existing BQ table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("payload", "JSON"),
        ],
    )
    with open(OUTPUT_PATH, "rb") as f:
        load_job = bq_client.load_table_from_file(
            f, RAW_TABLE_ID, job_config=job_config)
    load_job.result()  # block until done; raises on failure (load job is atomic, all-or-nothing)

    logger.info(
        f"Synced {len(rows)} row(s) to BigQuery (id > {last_synced_id})")

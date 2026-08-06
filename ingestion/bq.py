import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)


# ------------------------------------------------------------
# BigQuery
# ------------------------------------------------------------
def get_bq_client():
    project = os.getenv("BQ_PROJECT")
    if not project:
        raise ValueError("Missing env variable: BQ_PROJECT")
    return bigquery.Client(project=project)

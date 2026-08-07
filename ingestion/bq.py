import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)

BQ_PROJECT = os.getenv("BQ_PROJECT")
if not BQ_PROJECT:
    raise ValueError("Missing env variable: BQ_PROJECT")

BQ_DATASET = os.getenv("BQ_DATASET", "fx_dataset")
RAW_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.raw_api_response"

# ------------------------------------------------------------
# BigQuery
# ------------------------------------------------------------


def get_bq_client():
    return bigquery.Client(project=BQ_PROJECT)

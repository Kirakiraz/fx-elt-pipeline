import os
from google.cloud import bigquery


def get_bq_client():
    project = os.getenv("BQ_PROJECT")
    if not project:
        raise ValueError("Missing env variable: BQ_PROJECT")
    return bigquery.Client(project=project)

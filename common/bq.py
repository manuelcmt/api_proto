import os

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

PROJECT = os.environ["BQ_PROJECT"]
DATASET = os.environ["BQ_DATASET"]
ENDPOINT = os.getenv("BQ_ENDPOINT")  # set -> emulator; unset -> real BigQuery with default credentials

client = (
    bigquery.Client(project=PROJECT, credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=ENDPOINT))
    if ENDPOINT
    else bigquery.Client(project=PROJECT)
)

EVENTS = f"{PROJECT}.{DATASET}.events"

# One tidy table: each event with its packer already resolved.
EVENTS_SCHEMA = [
    bigquery.SchemaField("buffer_id", "STRING", mode="REQUIRED"),  # receiver buffer the event came from
    bigquery.SchemaField("event_id", "INTEGER", mode="REQUIRED"),  # receiver sequence within that buffer, used as the ETL cursor
    bigquery.SchemaField("registered_on", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("terminal", "STRING"),
    bigquery.SchemaField("production_line", "STRING"),
    bigquery.SchemaField("presentation", "STRING"),
    bigquery.SchemaField("unique_n", "INTEGER"),
    bigquery.SchemaField("packer_id", "STRING"),
    bigquery.SchemaField("packer_name", "STRING"),
    bigquery.SchemaField("packer_group", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def ensure_tables() -> None:
    client.create_table(bigquery.Table(EVENTS, schema=EVENTS_SCHEMA), exists_ok=True)


def query(sql: str, params: list = ()) -> list[dict]:
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=list(params)))
    return [dict(row) for row in job.result()]

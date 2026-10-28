
import json
from datetime import datetime, timezone

from google.cloud import bigquery, storage

PROJECT_ID = "gb-energy-analytics"
BUCKET = "gb-energy-analytics-raw-2026"
TABLE_ID = f"{PROJECT_ID}.energy_raw.carbon_intensity"

SOURCE_FILE = (
    "raw/carbon_intensity/"
    "year=2026/month=10/day=20/2026-10-20.json"
)


def load_carbon_intensity():
    # 1. Read original JSON from GCS
    storage_client = storage.Client(project=PROJECT_ID)
    blob = storage_client.bucket(BUCKET).blob(SOURCE_FILE)

    payload = json.loads(blob.download_as_text())
    observations = payload["data"]

    # 2. Flatten nested API response
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for item in observations:
        intensity = item.get("intensity") or {}

        rows.append({
            "period_start": item["from"],
            "period_end": item["to"],
            "actual_intensity": intensity.get("actual"),
            "forecast_intensity": intensity.get("forecast"),
            "ingested_at": ingested_at,
            "source_file": f"gs://{BUCKET}/{SOURCE_FILE}"
        })

    # 3. Define warehouse schema
    schema = [
        bigquery.SchemaField("period_start", "TIMESTAMP"),
        bigquery.SchemaField("period_end", "TIMESTAMP"),
        bigquery.SchemaField("actual_intensity", "INTEGER"),
        bigquery.SchemaField("forecast_intensity", "INTEGER"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
    ]

    # 4. Load into BigQuery
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_EMPTY"
    )

    job = client.load_table_from_json(
        rows,
        TABLE_ID,
        job_config=job_config,
        location="us-central1"
    )

    job.result()

    print(f"Loaded {job.output_rows} rows into {TABLE_ID}")


if __name__ == "__main__":
    load_carbon_intensity()

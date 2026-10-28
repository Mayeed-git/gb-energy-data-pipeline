
import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import requests
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery, storage


PROJECT_ID = "gb-energy-analytics"
BUCKET_NAME = "gb-energy-analytics-raw-2026"
DATASET_ID = "energy_raw"
LOCATION = "us-central1"

TARGET_TABLE = (
    f"{PROJECT_ID}.{DATASET_ID}.carbon_intensity"
)

API_BASE = "https://api.carbonintensity.org.uk"

SCHEMA = [
    bigquery.SchemaField("period_start", "TIMESTAMP"),
    bigquery.SchemaField("period_end", "TIMESTAMP"),
    bigquery.SchemaField("actual_intensity", "INTEGER"),
    bigquery.SchemaField("forecast_intensity", "INTEGER"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("source_file", "STRING"),
]


def fetch_api(day):
    """Fetch historical observations with basic retry handling."""
    url = f"{API_BASE}/intensity/date/{day}"

    for attempt in range(3):
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json"},
                timeout=30,
            )

            if response.status_code == 429 or (
                500 <= response.status_code < 600
            ):
                response.raise_for_status()

            response.raise_for_status()
            payload = response.json()

            if (
                not isinstance(payload.get("data"), list)
                or not payload["data"]
            ):
                raise ValueError(
                    f"No valid observations for {day}"
                )

            return payload

        except (requests.RequestException, ValueError):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def get_raw_payload(day, storage_client):
    """
    Reuse existing immutable raw data where available.
    Otherwise fetch from the API and archive it.
    """
    bucket = storage_client.bucket(BUCKET_NAME)

    path = (
        f"raw/carbon_intensity/"
        f"year={day.year}/"
        f"month={day.month:02d}/"
        f"day={day.day:02d}/"
        f"{day.isoformat()}.json"
    )

    blob = bucket.blob(path)

    if blob.exists():
        print(f"Using archived GCS data: {day}")
        payload = json.loads(blob.download_as_text())
    else:
        payload = fetch_api(day)

        try:
            blob.upload_from_string(
                json.dumps(payload),
                content_type="application/json",
                if_generation_match=0,
            )
            print(f"Archived new data: {day}")

        except PreconditionFailed:
            # Another run created the object first.
            payload = json.loads(
                blob.download_as_text()
            )

    if (
        not isinstance(payload.get("data"), list)
        or not payload["data"]
    ):
        raise ValueError(f"Invalid archived data for {day}")

    source_uri = f"gs://{BUCKET_NAME}/{path}"

    return payload, source_uri


def flatten_payload(payload, source_uri):
    """Transform nested API observations into BigQuery rows."""
    ingested_at = datetime.now(
        timezone.utc
    ).isoformat()

    rows = []

    for item in payload["data"]:
        intensity = item.get("intensity") or {}

        rows.append({
            "period_start": item["from"],
            "period_end": item["to"],
            "actual_intensity": intensity.get("actual"),
            "forecast_intensity": intensity.get("forecast"),
            "ingested_at": ingested_at,
            "source_file": source_uri,
        })

    return rows


def merge_into_bigquery(rows, client):
    """Stage a batch and merge into the existing target table."""

    staging_table = (
        f"{PROJECT_ID}.{DATASET_ID}."
        f"_stage_carbon_{uuid4().hex[:12]}"
    )

    config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        job = client.load_table_from_json(
            rows,
            staging_table,
            job_config=config,
            location=LOCATION,
        )
        job.result()

        merge_sql = f"""
        MERGE `{TARGET_TABLE}` AS target
        USING (
            SELECT
                period_start,
                period_end,
                actual_intensity,
                forecast_intensity,
                ingested_at,
                source_file
            FROM `{staging_table}`
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY period_start
                ORDER BY period_end DESC
            ) = 1
        ) AS source

        ON target.period_start = source.period_start

        WHEN MATCHED AND (
            target.period_end IS DISTINCT FROM source.period_end
            OR target.actual_intensity
                IS DISTINCT FROM source.actual_intensity
            OR target.forecast_intensity
                IS DISTINCT FROM source.forecast_intensity
        ) THEN UPDATE SET
            period_end = source.period_end,
            actual_intensity = source.actual_intensity,
            forecast_intensity = source.forecast_intensity,
            ingested_at = source.ingested_at,
            source_file = source.source_file

        WHEN NOT MATCHED THEN
            INSERT (
                period_start,
                period_end,
                actual_intensity,
                forecast_intensity,
                ingested_at,
                source_file
            )
            VALUES (
                source.period_start,
                source.period_end,
                source.actual_intensity,
                source.forecast_intensity,
                source.ingested_at,
                source.source_file
            )
        """

        query_job = client.query(
            merge_sql,
            location=LOCATION,
        )
        query_job.result()

        print(
            f"MERGE complete: "
            f"{query_job.num_dml_affected_rows} "
            f"rows affected"
        )

    finally:
        client.delete_table(
            staging_table,
            not_found_ok=True,
        )


def run_backfill(start_date, end_date):
    """Process an inclusive range of UK calendar dates."""
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)

    current = start_date

    while current <= end_date:
        print(f"\nProcessing {current}")

        payload, source_uri = get_raw_payload(
            current,
            storage_client,
        )

        rows = flatten_payload(payload, source_uri)

        # Stop rather than silently accepting duplicate
        # timestamps or incomplete malformed records.
        periods = [r["period_start"] for r in rows]

        if len(periods) != len(set(periods)):
            raise ValueError(
                f"Duplicate periods in API response: {current}"
            )

        merge_into_bigquery(rows, bq_client)

        print(
            f"Successfully processed {len(rows)} "
            f"observations for {current}"
        )

        current += timedelta(days=1)

    print("\nHistorical backfill completed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Historical carbon intensity backfill"
    )

    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        required=True,
    )

    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        required=True,
    )

    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error("Start date must precede end date")

    if args.end_date > datetime.now(
        timezone.utc
    ).date():
        parser.error("End date cannot be in the future")

    if (args.end_date - args.start_date).days > 30:
        parser.error(
            "Limit each backfill to 31 days"
        )

    run_backfill(args.start_date, args.end_date)


if __name__ == "__main__":
    main()

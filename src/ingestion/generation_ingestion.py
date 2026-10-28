
import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from uuid import uuid4

import requests
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery, storage


PROJECT_ID = "gb-energy-analytics"
BUCKET_NAME = "gb-energy-analytics-raw-2026"
LOCATION = "us-central1"
TABLE_ID = f"{PROJECT_ID}.energy_raw.generation_mix"
API_BASE = "https://api.carbonintensity.org.uk"
UK_TZ = ZoneInfo("Europe/London")

SCHEMA = [
    bigquery.SchemaField("period_start", "TIMESTAMP"),
    bigquery.SchemaField("period_end", "TIMESTAMP"),
    bigquery.SchemaField("fuel_type", "STRING"),
    bigquery.SchemaField("generation_pct", "FLOAT"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("source_file", "STRING"),
]


def utc_api_timestamp(value):
    """Format an aware datetime for the API."""
    return value.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%MZ"
    )


def uk_day_window(day):
    """Return UTC boundaries for a complete UK calendar day."""
    start_local = datetime(
        day.year, day.month, day.day, tzinfo=UK_TZ
    )
    next_day = day + timedelta(days=1)
    end_local = datetime(
        next_day.year,
        next_day.month,
        next_day.day,
        tzinfo=UK_TZ,
    )

    return (
        utc_api_timestamp(start_local),
        utc_api_timestamp(end_local),
    )


def fetch_generation(day):
    start, end = uk_day_window(day)
    url = f"{API_BASE}/generation/{start}/{end}"

    for attempt in range(3):
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json"},
                timeout=45,
            )
            response.raise_for_status()

            payload = response.json()

            if not isinstance(payload.get("data"), list):
                raise ValueError(
                    f"Invalid generation response for {day}"
                )

            if not payload["data"]:
                raise ValueError(
                    f"Empty generation response for {day}"
                )

            return payload

        except requests.RequestException as exc:
            if (
                isinstance(exc, requests.HTTPError)
                and exc.response is not None
                and exc.response.status_code < 500
                and exc.response.status_code != 429
            ):
                raise

            if attempt == 2:
                raise

            time.sleep(2 ** attempt)


def get_raw_generation(day, storage_client):
    path = (
        "raw/generation_mix/"
        f"year={day.year}/"
        f"month={day.month:02d}/"
        f"day={day.day:02d}/"
        f"{day.isoformat()}.json"
    )

    blob = storage_client.bucket(BUCKET_NAME).blob(path)

    if blob.exists():
        payload = json.loads(blob.download_as_text())
        print(f"Using archived GCS data: {day}")

    else:
        payload = fetch_generation(day)

        try:
            blob.upload_from_string(
                json.dumps(payload),
                content_type="application/json",
                if_generation_match=0,
            )
            print(f"Archived generation data: {day}")

        except PreconditionFailed:
            payload = json.loads(blob.download_as_text())

    if not isinstance(payload.get("data"), list):
        raise ValueError("Invalid archived generation response")

    return payload, f"gs://{BUCKET_NAME}/{path}"


def flatten_generation(payload, source_uri, day):
    rows = []
    seen = set()
    ingested_at = datetime.now(timezone.utc).isoformat()

    start, end = uk_day_window(day)
    window_start = datetime.fromisoformat(
        start.replace("Z", "+00:00")
    )
    window_end = datetime.fromisoformat(
        end.replace("Z", "+00:00")
    )

    for interval in payload["data"]:
        period_start = interval["from"]
        period_end = interval["to"]

        timestamp = datetime.fromisoformat(
            period_start.replace("Z", "+00:00")
        )

        # Keep only intervals beginning within the
        # requested UK calendar day.
        if not window_start <= timestamp < window_end:
            continue

        mix = interval.get("generationmix")

        if not isinstance(mix, list) or not mix:
            raise ValueError(
                f"Missing generation mix for {period_start}"
            )

        for fuel in mix:
            fuel_type = fuel["fuel"]
            percentage = fuel["perc"]

            if not isinstance(percentage, (int, float)):
                raise ValueError("Non-numeric generation share")

            if not 0 <= percentage <= 100:
                raise ValueError("Invalid generation percentage")

            key = (period_start, fuel_type)

            if key in seen:
                raise ValueError(
                    f"Duplicate generation record: {key}"
                )

            seen.add(key)

            rows.append({
                "period_start": period_start,
                "period_end": period_end,
                "fuel_type": fuel_type,
                "generation_pct": float(percentage),
                "ingested_at": ingested_at,
                "source_file": source_uri,
            })

    if not rows:
        raise ValueError(
            f"No generation rows in requested day: {day}"
        )

    return rows


def merge_generation(rows, client):
    stage_table = (
        f"{PROJECT_ID}.energy_raw."
        f"_stage_generation_{uuid4().hex[:12]}"
    )

    config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        load_job = client.load_table_from_json(
            rows,
            stage_table,
            job_config=config,
            location=LOCATION,
        )
        load_job.result()

        sql = f"""
        MERGE `{TABLE_ID}` AS target
        USING `{stage_table}` AS source
        ON target.period_start = source.period_start
           AND target.fuel_type = source.fuel_type

        WHEN MATCHED AND (
            target.period_end
                IS DISTINCT FROM source.period_end
            OR target.generation_pct
                IS DISTINCT FROM source.generation_pct
        )
        THEN UPDATE SET
            period_end = source.period_end,
            generation_pct = source.generation_pct,
            ingested_at = source.ingested_at,
            source_file = source.source_file

        WHEN NOT MATCHED THEN
            INSERT (
                period_start,
                period_end,
                fuel_type,
                generation_pct,
                ingested_at,
                source_file
            )
            VALUES (
                source.period_start,
                source.period_end,
                source.fuel_type,
                source.generation_pct,
                source.ingested_at,
                source.source_file
            )
        """

        job = client.query(sql, location=LOCATION)
        job.result()

        print(
            f"MERGE complete: "
            f"{job.num_dml_affected_rows} rows affected"
        )

    finally:
        client.delete_table(
            stage_table, not_found_ok=True
        )


def run_backfill(start_date, end_date):
    storage_client = storage.Client(project=PROJECT_ID)
    bigquery_client = bigquery.Client(project=PROJECT_ID)

    current = start_date

    while current <= end_date:
        print(f"\nProcessing generation mix: {current}")

        payload, source_uri = get_raw_generation(
            current, storage_client
        )

        rows = flatten_generation(
            payload, source_uri, current
        )

        merge_generation(rows, bigquery_client)

        print(
            f"Successfully processed {len(rows)} "
            f"generation rows for {current}"
        )

        current += timedelta(days=1)

    print("\nGeneration backfill completed.")


def main():
    parser = argparse.ArgumentParser()

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
        parser.error("Start date must be <= end date")

    if args.end_date > datetime.now(UK_TZ).date():
        parser.error("End date cannot be in the future")

    if (args.end_date - args.start_date).days > 30:
        parser.error("Use a maximum of 31 days per run")

    run_backfill(args.start_date, args.end_date)


if __name__ == "__main__":
    main()

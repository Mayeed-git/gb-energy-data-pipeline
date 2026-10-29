
import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo
import subprocess
import shutil

from google.cloud import bigquery, storage

from src.backfill import (
    fetch_api,
    flatten_payload,
    merge_into_bigquery,
)
from src.ingestion.generation_ingestion import (
    fetch_generation,
    flatten_generation,
    merge_generation,
)
from src.ingestion.weather_ingestion import (
    run_backfill as run_weather_backfill,
)

PROJECT_ID = "gb-energy-analytics"
BUCKET_NAME = "gb-energy-analytics-raw-2026"
UK_TZ = ZoneInfo("Europe/London")

# Absolute repository path, independent of terminal directory
ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "energy_analytics"


def archive_snapshot(
    storage_client,
    source_name,
    day,
    payload,
):
    """
    Archive each successful API response under a unique name.
    Existing raw files are never overwritten.
    """
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid4().hex[:8]
    )

    path = (
        f"raw/{source_name}/"
        f"year={day.year}/"
        f"month={day.month:02d}/"
        f"day={day.day:02d}/"
        f"snapshots/{run_id}.json"
    )

    blob = storage_client.bucket(BUCKET_NAME).blob(path)

    blob.upload_from_string(
        json.dumps(payload, ensure_ascii=False),
        content_type="application/json",
        if_generation_match=0,
    )

    return f"gs://{BUCKET_NAME}/{path}"


def process_electricity_day(
    day,
    storage_client,
    bq_client,
):
    print(f"\nProcessing electricity: {day}")

    # Always fetch fresh API responses during incremental runs.
    # Historical backfill continues to reuse its GCS archives.
    carbon_payload = fetch_api(day)
    generation_payload = fetch_generation(day)

    # Validate and flatten before writing to BigQuery.
    # The source URI is filled in after archiving.
    carbon_rows = flatten_payload(carbon_payload, "")

    generation_rows = flatten_generation(
        generation_payload,
        "",
        day,
    )

    # Detect duplicate source keys before merging.
    carbon_keys = [r["period_start"] for r in carbon_rows]
    generation_keys = [
        (r["period_start"], r["fuel_type"])
        for r in generation_rows
    ]

    if len(carbon_keys) != len(set(carbon_keys)):
        raise ValueError(f"Duplicate carbon periods: {day}")

    if len(generation_keys) != len(set(generation_keys)):
        raise ValueError(f"Duplicate generation keys: {day}")

    # Archive immutable source snapshots.
    carbon_uri = archive_snapshot(
        storage_client,
        "carbon_intensity",
        day,
        carbon_payload,
    )

    generation_uri = archive_snapshot(
        storage_client,
        "generation_mix",
        day,
        generation_payload,
    )

    for row in carbon_rows:
        row["source_file"] = carbon_uri

    for row in generation_rows:
        row["source_file"] = generation_uri

    # Reuse existing idempotent MERGE functions.
    merge_into_bigquery(carbon_rows, bq_client)
    merge_generation(generation_rows, bq_client)

    print(
        f"Processed {len(carbon_rows)} carbon rows "
        f"and {len(generation_rows)} generation rows"
    )


def run_incremental(lookback_days=3, run_dbt=True):
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Use UK calendar dates for electricity.
    today_uk = datetime.now(UK_TZ).date()
    yesterday = today_uk - timedelta(days=1)

    # Refresh yesterday and the preceding days.
    dates = [
        yesterday - timedelta(days=offset)
        for offset in reversed(range(lookback_days))
    ]

    for day in dates:
        process_electricity_day(
            day,
            storage_client,
            bq_client,
        )

    # ERA5 data typically becomes available days later.
    # Use a seven-day safety buffer.

# ERA5 weather data has a publication delay.
    latest_weather_day = (
        datetime.now(timezone.utc).date()
        - timedelta(days=7)
    )

    # Reprocess a rolling seven-day window to recover missed runs.
    earliest_weather_day = (
        latest_weather_day - timedelta(days=6)
    )

    print(
        f"\nProcessing weather from "
        f"{earliest_weather_day} to {latest_weather_day}"
    )

    run_weather_backfill(
        earliest_weather_day,
        latest_weather_day,
    )


    if run_dbt:
        print("\nBuilding and testing dbt models...")

        dbt_executable = shutil.which("dbt")

        if dbt_executable is None:
            raise RuntimeError(
                "dbt executable not found. "
                "Activate your virtual environment and install dbt-bigquery."
            )

        subprocess.run(
            [
                dbt_executable,
                "build",
            ],
            cwd=str(DBT_DIR),
            check=True,
        )

    print("\nIncremental pipeline completed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Incremental GB energy data pipeline"
    )

    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt build during testing",
    )

    args = parser.parse_args()

    if not 1 <= args.lookback_days <= 7:
        parser.error("lookback-days must be between 1 and 7")

    run_incremental(
        lookback_days=args.lookback_days,
        run_dbt=not args.skip_dbt,
    )


if __name__ == "__main__":
    main()

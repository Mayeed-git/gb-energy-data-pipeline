
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
LOCATION = "us-central1"
TABLE_ID = f"{PROJECT_ID}.energy_raw.weather"

API_URL = "https://archive-api.open-meteo.com/v1/archive"

LOCATIONS = {
    "london": (51.5074, -0.1278),
    "manchester": (53.4808, -2.2426),
    "edinburgh": (55.9533, -3.1883),
}

VARIABLES = [
    "temperature_2m",
    "wind_speed_10m",
    "cloud_cover",
]

SCHEMA = [
    bigquery.SchemaField("observation_time", "TIMESTAMP"),
    bigquery.SchemaField("location_id", "STRING"),
    bigquery.SchemaField("temperature_2m", "FLOAT"),
    bigquery.SchemaField("wind_speed_10m", "FLOAT"),
    bigquery.SchemaField("cloud_cover", "FLOAT"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("source_file", "STRING"),
]


def fetch_weather(day, location_id):
    """Fetch ERA5 historical weather for one location and day."""
    latitude, longitude = LOCATIONS[location_id]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
        "hourly": ",".join(VARIABLES),
        "timezone": "GMT",
        "models": "era5",
    }

    for attempt in range(3):
        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()

            hourly = payload.get("hourly")
            if not isinstance(hourly, dict):
                raise ValueError(
                    f"Missing hourly data: {location_id}, {day}"
                )

            if not hourly.get("time"):
                raise ValueError(
                    f"Empty weather data: {location_id}, {day}"
                )

            return payload

        except requests.RequestException as exc:
            status = getattr(
                getattr(exc, "response", None),
                "status_code",
                None,
            )

            if status is not None and (
                400 <= status < 500 and status != 429
            ):
                raise

            if attempt == 2:
                raise

            time.sleep(2 ** attempt)


def get_raw_weather(day, location_id, storage_client):
    """Reuse an archived JSON response or fetch and archive it."""
    path = (
        f"raw/weather/location={location_id}/"
        f"year={day.year}/"
        f"month={day.month:02d}/"
        f"day={day.day:02d}/"
        f"{day.isoformat()}.json"
    )

    blob = storage_client.bucket(BUCKET_NAME).blob(path)

    if blob.exists():
        payload = json.loads(blob.download_as_text())
        print(f"Using archived data: {location_id}, {day}")
    else:
        payload = fetch_weather(day, location_id)

        try:
            blob.upload_from_string(
                json.dumps(payload),
                content_type="application/json",
                if_generation_match=0,
            )
            print(f"Archived data: {location_id}, {day}")

        except PreconditionFailed:
            payload = json.loads(blob.download_as_text())

    return payload, f"gs://{BUCKET_NAME}/{path}"


def flatten_weather(payload, day, location_id, source_uri):
    """Convert column-oriented hourly arrays into rows."""
    hourly = payload["hourly"]
    times = hourly["time"]

    if len(times) != 24:
        raise ValueError(
            f"Expected 24 hourly records for {location_id} "
            f"on {day}; got {len(times)}"
        )

    for variable in VARIABLES:
        if variable not in hourly:
            raise ValueError(f"Missing variable: {variable}")
        if len(hourly[variable]) != len(times):
            raise ValueError(
                f"Array length mismatch: {variable}"
            )

    rows = []
    seen = set()
    ingested_at = datetime.now(timezone.utc).isoformat()

    for i, timestamp in enumerate(times):
        observed_at = datetime.fromisoformat(
            timestamp
        ).replace(tzinfo=timezone.utc)

        if observed_at.date() != day:
            raise ValueError(
                f"Unexpected date in response: {timestamp}"
            )

        key = (observed_at.isoformat(), location_id)
        if key in seen:
            raise ValueError(f"Duplicate observation: {key}")
        seen.add(key)

        rows.append({
            "observation_time": observed_at.isoformat(),
            "location_id": location_id,
            "temperature_2m": hourly["temperature_2m"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
            "cloud_cover": hourly["cloud_cover"][i],
            "ingested_at": ingested_at,
            "source_file": source_uri,
        })

    # Ensure we got 00:00 through 23:00 UTC.
    expected = {
        datetime(
            day.year, day.month, day.day, hour,
            tzinfo=timezone.utc
        ).isoformat()
        for hour in range(24)
    }

    if {row["observation_time"] for row in rows} != expected:
        raise ValueError(
            f"Incomplete hourly coverage: {location_id}, {day}"
        )

    return rows


def validate_rows(rows):
    """Validate all locations before writing to BigQuery."""
    keys = set()

    for row in rows:
        key = (row["observation_time"], row["location_id"])
        if key in keys:
            raise ValueError(f"Duplicate batch key: {key}")
        keys.add(key)

        temperature = row["temperature_2m"]
        wind = row["wind_speed_10m"]
        cloud = row["cloud_cover"]

        if any(v is None for v in (temperature, wind, cloud)):
            raise ValueError(f"Missing weather measurement: {key}")

        if wind < 0 or not 0 <= cloud <= 100:
            raise ValueError(f"Invalid weather measurement: {key}")


def merge_weather(rows, client):
    """Load temporary staging table, then MERGE into target."""
    stage = (
        f"{PROJECT_ID}.energy_raw."
        f"_stage_weather_{uuid4().hex[:12]}"
    )

    try:
        load_job = client.load_table_from_json(
            rows,
            stage,
            job_config=bigquery.LoadJobConfig(
                schema=SCHEMA,
                write_disposition="WRITE_TRUNCATE",
            ),
            location=LOCATION,
        )
        load_job.result()

        merge_sql = f"""
        MERGE `{TABLE_ID}` AS target
        USING `{stage}` AS source
        ON target.observation_time = source.observation_time
           AND target.location_id = source.location_id

        WHEN MATCHED AND (
            target.temperature_2m
                IS DISTINCT FROM source.temperature_2m
            OR target.wind_speed_10m
                IS DISTINCT FROM source.wind_speed_10m
            OR target.cloud_cover
                IS DISTINCT FROM source.cloud_cover
        ) THEN UPDATE SET
            temperature_2m = source.temperature_2m,
            wind_speed_10m = source.wind_speed_10m,
            cloud_cover = source.cloud_cover,
            ingested_at = source.ingested_at,
            source_file = source.source_file

        WHEN NOT MATCHED THEN
            INSERT (
                observation_time,
                location_id,
                temperature_2m,
                wind_speed_10m,
                cloud_cover,
                ingested_at,
                source_file
            )
            VALUES (
                source.observation_time,
                source.location_id,
                source.temperature_2m,
                source.wind_speed_10m,
                source.cloud_cover,
                source.ingested_at,
                source.source_file
            )
        """

        job = client.query(merge_sql, location=LOCATION)
        job.result()

        print(
            f"MERGE complete: "
            f"{job.num_dml_affected_rows} rows affected"
        )

    finally:
        client.delete_table(stage, not_found_ok=True)


def run_backfill(start_date, end_date):
    storage_client = storage.Client(project=PROJECT_ID)
    bigquery_client = bigquery.Client(project=PROJECT_ID)

    current = start_date

    while current <= end_date:
        print(f"\nProcessing weather: {current}")
        all_rows = []

        for location_id in LOCATIONS:
            payload, source_uri = get_raw_weather(
                current, location_id, storage_client
            )

            rows = flatten_weather(
                payload, current, location_id, source_uri
            )
            all_rows.extend(rows)

        validate_rows(all_rows)
        merge_weather(all_rows, bigquery_client)

        print(
            f"Successfully processed {len(all_rows)} "
            f"weather observations for {current}"
        )

        current += timedelta(days=1)

    print("\nWeather backfill completed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Historical weather backfill"
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

    # ERA5 typically arrives around five days after observation.
    latest_safe_date = (
        datetime.now(timezone.utc).date()
        - timedelta(days=7)
    )
    if args.end_date > latest_safe_date:
        parser.error(
            f"ERA5 backfill end date must be no later than "
            f"{latest_safe_date}"
        )

    if (args.end_date - args.start_date).days > 30:
        parser.error("Limit each run to 31 days")

    run_backfill(args.start_date, args.end_date)


if __name__ == "__main__":
    main()

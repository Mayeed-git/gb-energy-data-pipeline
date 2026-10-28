
from pathlib import Path
from google.cloud import storage

BUCKET_NAME = "gb-energy-analytics-raw-2026"

def upload_to_gcs(local_file, destination):
    client = storage.Client(project="gb-energy-analytics")
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination)

    blob.upload_from_filename(
        str(local_file),
        content_type="application/json",
        if_generation_match=0
    )

    print(f"Uploaded successfully to gs://{BUCKET_NAME}/{destination}")


if __name__ == "__main__":
    date = "2026-10-20"

    local_file = Path(
        f"data/raw/carbon_intensity/{date}.json"
    )

    destination = (
        f"raw/carbon_intensity/"
        f"year=2026/month=10/day=20/{date}.json"
    )

    if not local_file.exists():
        raise FileNotFoundError(f"File not found: {local_file}")

    upload_to_gcs(local_file, destination)


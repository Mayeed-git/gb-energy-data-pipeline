import json
from pathlib import Path

import requests

BASE_URL = "https://api.carbonintensity.org.uk"


def fetch_carbon_intensity(date: str) -> dict:
    url = f"{BASE_URL}/intensity/date/{date}"

    response = requests.get(
        url,
        headers={"Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    if "data" not in data:
        raise ValueError("API response missing data")

    return data


if __name__ == "__main__":
    date = "2026-10-20"

    data = fetch_carbon_intensity(date)

    output_dir = Path("data/raw/carbon_intensity")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{date}.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Retrieved {len(data['data'])} observations")
    print(f"Saved to {output_file}")

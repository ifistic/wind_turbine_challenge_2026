"""
Fetch today's weather data (wave height, air temperature) from Stormglass
for a given location, then load it into a PySpark DataFrame for inspection.
Intended to be correlated against turbine anomaly timestamps from the wind
turbine pipeline's gold_anomalies table.

Setup:
    pip install arrow requests python-dotenv pyspark

    Add to your .env file (same one used for Postgres credentials):
        STORMGLASS_API_KEY=your-real-key-here

Usage:
    python weather_api.py
    python weather_api.py --lat 58.7984 --lng 17.8081
"""
import argparse
import json
import os
import sys

import arrow
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

STORMGLASS_URL = "https://api.stormglass.io/v2/weather/point"
DEFAULT_PARAMS = ["waveHeight", "airTemperature"]


def fetch_weather(lat: float, lng: float, api_key: str) -> dict:
    """Fetch today's weather data for a given location from Stormglass."""
    start = arrow.now().floor("day")
    end = arrow.now().ceil("day")

    response = requests.get(
        STORMGLASS_URL,
        params={
            "lat": lat,
            "lng": lng,
            "params": ",".join(DEFAULT_PARAMS),
            "start": start.to("UTC").timestamp(),
            "end": end.to("UTC").timestamp(),
        },
        headers={"Authorization": api_key},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch weather data from Stormglass")
    parser.add_argument("--lat", type=float, default=58.7984, help="Latitude")
    parser.add_argument("--lng", type=float, default=17.8081, help="Longitude")
    parser.add_argument(
        "--out", type=str, default="weather_data.json", help="Output file path"
    )
    args = parser.parse_args()

    api_key = os.getenv("STORMGLASS_API_KEY")
    if not api_key:
        print(
            "ERROR: STORMGLASS_API_KEY not set. Add it to your .env file.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        data = fetch_weather(args.lat, args.lng, api_key)
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: Stormglass API request failed: {e}", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Network error contacting Stormglass: {e}", file=sys.stderr)
        sys.exit(1)

    with open(args.out, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Weather data saved to {args.out}")

    # Load the saved JSON into a PySpark DataFrame and flatten it.
    # Stormglass returns hourly readings under the "hours" key, and each
    # parameter (waveHeight, airTemperature) is itself a dict of
    # source -> value (e.g. {"noaa": 1.2, "sg": 1.3, "ecmwf": 1.1}).
    # We flatten each parameter down to a single numeric column by taking
    # Stormglass's own blended "sg" value where available, falling back to
    # whichever other source is present for that hour.
    spark = SparkSession.builder.appName("WeatherData").getOrCreate()

    hours_json_path = args.out.replace(".json", "_hours.json")
    with open(hours_json_path, "w") as f:
        for hour in data["hours"]:
            f.write(json.dumps(hour) + "\n")

    raw_df = spark.read.json(hours_json_path)

    def flatten_all_sources(df, struct_col: str):
        """Expand a {source: value} struct column into one column per source,
        e.g. airTemperature.sg -> airTemperature_sg, airTemperature_ecmwf, etc.
        Keeps every model's reading instead of collapsing to a single value."""
        field_names = df.schema[struct_col].dataType.fieldNames()
        # Replace ":" with "_" since colon isn't valid in a column alias
        # (e.g. Stormglass's "ecmwf:aifs" source)
        return [
            F.col(f"{struct_col}.{field}").alias(f"{struct_col}_{field.replace(':', '_')}")
            for field in field_names
        ]

    wave_cols = flatten_all_sources(raw_df, "waveHeight")
    temp_cols = flatten_all_sources(raw_df, "airTemperature")

    df = raw_df.select(
        F.col("time").cast("timestamp").alias("time"),
        *wave_cols,
        *temp_cols,
    ).orderBy("time")

    df.printSchema()
    df.show(df.count(), truncate=False)

    # Print the request metadata too (cost, quota, request window, etc.)
    # - separate from the hourly readings since it's a single record, not a
    # table of rows.
    meta = data.get("meta", {})
    print("\n--- Request metadata ---")
    for key, value in meta.items():
        print(f"{key}: {value}")

    spark.stop()


if __name__ == "__main__":
    main()
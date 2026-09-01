"""
Fetch today's weather data (wave height, air temperature) directly from
Stormglass and load it into a fully flattened PySpark DataFrame - all in
one run, no separate JSON file argument needed.

Setup:
    pip install arrow requests python-dotenv pyspark

    Add to your .env file (same one used for Postgres credentials):
        STORMGLASS_API_KEY=your-real-key-here

Usage:
    python weather_pattern.py
    python weather_pattern.py --lat 58.7984 --lng 17.8081
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


def flatten_all_sources(df, struct_col: str):
    """Expand a {source: value} struct column into one column per source."""
    field_names = df.schema[struct_col].dataType.fieldNames()
    return [
        F.col(f"{struct_col}.{field}").alias(f"{struct_col}_{field.replace(':', '_')}")
        for field in field_names
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch weather data from Stormglass and build a DataFrame")
    parser.add_argument("--lat", type=float, default=58.7984, help="Latitude")
    parser.add_argument("--lng", type=float, default=17.8081, help="Longitude")
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

    spark = SparkSession.builder.appName("WeatherPattern").getOrCreate()

    # Spark's JSON reader needs newline-delimited JSON, so we hand it the
    # in-memory API response directly via an RDD instead of writing a file
    # first - keeps this a single API-to-DataFrame flow with no JSON file
    # step in between.
    lines = [json.dumps(hour) for hour in data["hours"]]
    rdd = spark.sparkContext.parallelize(lines)
    raw_df = spark.read.json(rdd)

    wave_cols = flatten_all_sources(raw_df, "waveHeight")
    temp_cols = flatten_all_sources(raw_df, "airTemperature")

    df = raw_df.select(
        F.col("time").cast("timestamp").alias("time"),
        *wave_cols,
        *temp_cols,
    ).orderBy("time")

    df.printSchema()
    df.show(df.count(), truncate=False)
    print(f"\nTotal rows: {df.count()}")

    meta = data.get("meta", {})
    print("\n--- Request metadata ---")
    for key, value in meta.items():
        print(f"{key}: {value}")

    spark.stop()


if __name__ == "__main__":
    main()
"""
ingestion.py

Reads the raw wind turbine CSV files into a PySpark DataFrame.
"""

import shutil
import zipfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.config import CONFIG


TURBINE_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("turbine_id", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True),
    # Captures rows that fail to parse against the schema above,
    # rather than silently dropping/nulling them under PERMISSIVE mode.
    StructField("_corrupt_record", StringType(), True),
])


def extract_raw_data(
    source_zip: Path = None,
    force: bool = False,
) -> None:
    """
    Unzip the downloaded CSV archive into the project's raw_data directory.

    Args:
        source_zip:
            Path to the zip file containing the raw CSVs.
            Defaults to CONFIG.SOURCE_ZIP (~/Downloads/data.zip).
        force:
            If True, re-extract even if raw_data already has CSV files.

    Raises:
        FileNotFoundError:
            If the source zip file does not exist.
    """

    if source_zip is None:
        source_zip = CONFIG.SOURCE_ZIP

    raw_dir = Path(CONFIG.RAW_DIR)
    raw_dir.mkdir(parents=True, exist_ok=True)

    existing_csvs = list(raw_dir.glob("*.csv"))

    if existing_csvs and not force:
        print(
            f"\nraw_data/ already has {len(existing_csvs)} CSV file(s); "
            "skipping extraction (pass force=True to re-extract)."
        )
        return

    if not source_zip.exists():
        raise FileNotFoundError(
            f"Source zip file does not exist: {source_zip}"
        )

    print(f"\nExtracting {source_zip.name} -> {raw_dir}")

    with zipfile.ZipFile(source_zip, "r") as zip_ref:
        zip_ref.extractall(raw_dir)

    # ----------------------------------------------------------
    # Flatten: if the zip contained a nested folder, move any
    # CSVs found in subfolders up into raw_data/ directly.
    # ----------------------------------------------------------
    for nested_csv in raw_dir.rglob("*.csv"):
        if nested_csv.parent != raw_dir:
            target = raw_dir / nested_csv.name
            shutil.move(str(nested_csv), str(target))

    extracted_csvs = list(raw_dir.glob("*.csv"))
    print(f"Extraction complete: {len(extracted_csvs)} CSV file(s) in {raw_dir}")


def ingest_raw_data(
    spark: SparkSession,
) -> DataFrame:
    """
    Read all turbine CSV files from the raw data directory.

    Extracts the source zip into raw_data/ first if no CSVs are present.

    Displays:
        - Number of CSV files found
        - Row count for each CSV file
        - Total number of rows across all files
        - Number of corrupt/unparseable rows, if any

    Raises:
        FileNotFoundError:
            If the raw data directory does not exist
            or contains no CSV files.

    Returns:
        DataFrame containing all raw turbine readings, with the
        `_source_file` and `_corrupt_record` helper columns dropped.
    """

    extract_raw_data()

    raw_dir = Path(CONFIG.RAW_DIR)

    # ----------------------------------------------------------
    # Validate raw directory
    # ----------------------------------------------------------
    if not raw_dir.exists():
        raise FileNotFoundError(
            f"Raw data directory does not exist: {raw_dir}"
        )

    csv_files = sorted(
        raw_dir.glob("*.csv")
    )

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in: {raw_dir}"
        )

    print(f"\nInput directory: {raw_dir}")
    print(f"CSV files found: {len(csv_files)}")

    file_paths = [
        str(csv_file)
        for csv_file in csv_files
    ]

    # ----------------------------------------------------------
    # Read all CSV files in a single pass, tagging each row with
    # its source file so we can still report a per-file breakdown
    # without re-reading the data N+1 times.
    # ----------------------------------------------------------
    raw_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option(
            "timestampFormat",
            "yyyy-MM-dd HH:mm:ss",
        )
        .schema(TURBINE_SCHEMA)
        .csv(file_paths)
        .withColumn("_source_file", input_file_name())
    )

    raw_df.cache()

    # ----------------------------------------------------------
    # Per-file and total row counts (single job via cached df)
    # ----------------------------------------------------------
    counts_by_file = {
        Path(row["_source_file"]).name: row["count"]
        for row in raw_df.groupBy("_source_file").count().collect()
    }

    total_rows = 0
    print("\nRows per file:")
    print("-" * 50)
    for csv_file in csv_files:
        row_count = counts_by_file.get(csv_file.name, 0)
        total_rows += row_count
        print(
            f"{csv_file.name:<25} {row_count:>8} rows"
        )
    print("'" * 50)
    print(
        f"{'Total':<25} {total_rows:>8} rows"
    )

    # ----------------------------------------------------------
    # Report corrupt rows, if any, so bad data isn't silently lost
    # ----------------------------------------------------------
    corrupt_count = raw_df.filter(
        raw_df["_corrupt_record"].isNotNull()
    ).count()

    if corrupt_count:
        print(
            f"\nWARNING: {corrupt_count} row(s) failed schema parsing "
            "(see _corrupt_record column)."
        )

    raw_df = raw_df.drop("_source_file", "_corrupt_record")

    return raw_df
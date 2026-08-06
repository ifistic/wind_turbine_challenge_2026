"""
bronze.py

Bronze layer for the Wind Turbine Data Pipeline.

The Bronze layer lands the raw turbine data with minimal change:
  - the original CSV files (unzipped from the source download) are
    copied verbatim into CONFIG.BRONZE_DIR, byte-for-byte, for
    lineage and auditing
  - the corresponding Spark DataFrame is tagged with ingestion
    metadata only — column names are NOT standardized/renamed here,
    since Bronze should preserve the source schema as received
"""

import shutil
from pathlib import Path

from pyspark.sql import DataFrame

from src.ingestion.ingest import extract_raw_data
from src.utils.config import CONFIG
from src.utils.helpers import add_ingestion_metadata


def _land_raw_files(raw_dir: Path, bronze_dir: Path) -> None:
    """
    Copy every raw CSV file into the Bronze directory, unchanged.

    Args:
        raw_dir: Directory containing the unzipped source CSVs.
        bronze_dir: Destination Bronze directory.

    Raises:
        FileNotFoundError:
            If raw_dir contains no CSV files.
    """

    bronze_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(raw_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in raw directory: {raw_dir}"
        )

    print(f"\nLanding {len(csv_files)} raw file(s) into Bronze: {bronze_dir}")
    print("-" * 50)

    for csv_file in csv_files:
        target = bronze_dir / csv_file.name
        shutil.copy2(csv_file, target)
        print(f"{csv_file.name:<25} -> {target}")

    print("-" * 50)


def create_bronze_layer(raw_df: DataFrame) -> DataFrame:
    """
    Create the Bronze layer.

    The Bronze layer:
      - copies the original raw  from download dir to bronze layer
    """

    extract_raw_data()

    raw_dir = Path(CONFIG.RAW_DIR)
    bronze_dir = Path(CONFIG.BRONZE_DIR)

    _land_raw_files(raw_dir, bronze_dir)

    bronze_df = add_ingestion_metadata(raw_df)

    # Drop source_file 
    bronze_df = bronze_df.drop("source_file")

    return bronze_df
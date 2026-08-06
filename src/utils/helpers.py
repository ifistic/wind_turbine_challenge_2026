"""
Shared utilities for the Wind Turbine Data Pipeline.
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ensure_directory(path: Path) -> None:
    """Create a directory if it does not already exist."""
    path.mkdir(parents=True, exist_ok=True)


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """
    Add technical metadata used for lineage and auditing.
    """

    return (
        df
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
           "source_file",
           F.input_file_name()
        )
    )


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Convert column names to lowercase snake_case.
    """

    for column in df.columns:

        new_column = (
            column.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        df = df.withColumnRenamed(
            column,
            new_column
        )

    return df


def write_parquet(
    df: DataFrame,
    output_path: Path,
    mode: str = "overwrite",
) -> None:
    """
    Write a Spark DataFrame as Parquet.
    """

    (
        df.write
        .mode(mode)
        .parquet(str(output_path))
    )
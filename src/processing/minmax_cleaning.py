"""
minmax_cleaning.py

Standalone script for inspecting NULL values and testing
min/max replacement on the raw wind turbine data.

Run with:
    python -m src.processing.minmax_cleaning
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import CONFIG
from src.ingestion.ingest import ingest_raw_data


def show_null_rows(df: DataFrame) -> None:
    """
    Show only columns that contain NULL values
    and rows where those NULL values occur.
    """

    null_counts_row = df.agg(
        *[
            F.sum(
                F.when(
                    F.col(column).isNull(),
                    1,
                ).otherwise(0)
            ).alias(column)
            for column in df.columns
        ]
    ).first()

    null_columns = [
        column
        for column in df.columns
        if null_counts_row[column] > 0
    ]

    if not null_columns:
        print("\nNo NULL values found.")
        return

    print("\nNULL Value Report")
    print("-" * 60)

    for column in null_columns:
        print(
            f"{column}: "
            f"{null_counts_row[column]} NULL value(s)"
        )

    # Build NULL condition
    condition = F.col(
        null_columns[0]
    ).isNull()

    for column in null_columns[1:]:
        condition = (
            condition
            | F.col(column).isNull()
        )

    # Keep identifiers so we know which records are affected
    display_columns = [
        column
        for column in [
            "timestamp",
            "turbine_id",
            *null_columns,
        ]
        if column in df.columns
    ]

    # Remove duplicate column names
    display_columns = list(
        dict.fromkeys(display_columns)
    )

    print("\nRows containing NULL values:")

    (
        df
        .filter(condition)
        .select(*display_columns)
        .orderBy(
            "turbine_id",
            "timestamp",
        )
        .show(
            truncate=False
        )
    )


def show_min_max(df: DataFrame) -> None:
    """
    Display observed minimum and maximum values
    for the sensor columns.
    """

    sensor_columns = [
        "wind_speed",
        "wind_direction",
        "power_output",
    ]

    print("\nObserved Min / Max Values")
    print("-" * 60)

    for column in sensor_columns:

        stats = (
            df
            .agg(
                F.min(column).alias("minimum"),
                F.max(column).alias("maximum"),
            )
            .first()
        )

        minimum = stats["minimum"]
        maximum = stats["maximum"]

        if minimum is None and maximum is None:
            print(
                f"{column:<20} Entire column is NULL"
            )
        else:
            print(
                f"{column:<20} "
                f"min = {minimum:<10} "
                f"max = {maximum}"
            )


def replace_nulls_with_min(
    df: DataFrame,
) -> DataFrame:
    """
    Replace NULL sensor values with the observed minimum
    of that column.

    If an entire column is NULL, leave it unchanged.
    """

    sensor_columns = [
        "wind_speed",
        "wind_direction",
        "power_output",
    ]

    cleaned_df = df

    for column in sensor_columns:

        stats = (
            cleaned_df
            .agg(
                F.min(column).alias("minimum"),
                F.max(column).alias("maximum"),
            )
            .first()
        )

        minimum = stats["minimum"]
        maximum = stats["maximum"]

        if minimum is None or maximum is None:
            print(
                f"\n{column}: entire column is NULL "
                "- no replacement performed."
            )
            continue

        null_count = (
            cleaned_df
            .filter(
                F.col(column).isNull()
            )
            .count()
        )

        if null_count > 0:
            print(
                f"{column}: replacing "
                f"{null_count} NULL value(s) "
                f"with minimum {minimum}"
            )

            cleaned_df = (
                cleaned_df
                .withColumn(
                    column,
                    F.when(
                        F.col(column).isNull(),
                        F.lit(minimum),
                    ).otherwise(
                        F.col(column)
                    ),
                )
            )

    return cleaned_df


def main() -> None:
    """
    Run only the min/max cleaning test.
    """

    print("=" * 60)
    print("Min/Max Cleaning Test")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("Wind Turbine MinMax Cleaning")
        .master(CONFIG.SPARK_MASTER)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Read raw data
        df = ingest_raw_data(spark)

        print(
            f"\nTotal raw records: {df.count()}"
        )

        # BEFORE cleaning
        print("\n" + "=" * 60)
        print("BEFORE CLEANING")
        print("=" * 60)

        show_null_rows(df)

        show_min_max(df)

        # Clean
        cleaned_df = replace_nulls_with_min(df)

        # AFTER cleaning
        print("\n" + "=" * 60)
        print("AFTER CLEANING")
        print("=" * 60)

        show_null_rows(cleaned_df)

        show_min_max(cleaned_df)

        print("\n" + "=" * 60)
        print("Min/Max cleaning test completed.")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
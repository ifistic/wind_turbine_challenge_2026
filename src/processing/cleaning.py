"""
Reusable data-cleaning transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType

from src.utils.config import CONFIG


def find_duplicates(
    df: DataFrame,
    keys: list = None,
) -> DataFrame:
    """
     checking and removing_duplicates before it drops it.
    """

    if keys is None:
        keys = ["turbine_id", "timestamp"]

    window = Window.partitionBy(*keys)

    dupes = (
        df
        .withColumn("_dup_count", F.count("*").over(window))
        .filter(F.col("_dup_count") > 1)
        .drop("_dup_count")
        .orderBy(*keys)
    )

    dupes.show(1, truncate=False)

    return dupes


def remove_duplicates(
    df: DataFrame,
) -> DataFrame:
    """
    Remove duplicate turbine readings. and  a turbine should have one reading for a given timestamp.
    """

    return df.dropDuplicates([
        "turbine_id",
        "timestamp",
    ])


def null_out_invalid_values(
    df: DataFrame,
) -> DataFrame:
    """
    Replace physically impossible sensor values with null.

    """

    return (
        df
        .withColumn(
            "wind_speed",
            F.when(
                F.col("wind_speed").between(
                    CONFIG.MIN_WIND_SPEED,
                    CONFIG.MAX_WIND_SPEED,
                ),
                F.col("wind_speed"),
            )
        )
        .withColumn(
            "power_output",
            F.when(
                F.col("power_output").between(
                    CONFIG.MIN_POWER_MW,
                    CONFIG.RATED_CAPACITY_MW,
                ),
                F.col("power_output"),
            )
        )
        .withColumn(
            "wind_direction",
            F.when(
                F.col("wind_direction").between(
                    0,
                    360,
                ),
                F.col("wind_direction"),
            )
        )
    )


def impute_missing_values(
    df: DataFrame,
) -> DataFrame:
    """
    Impute missing numerical sensor values using the
    mean for each turbine. Turbine-level averages are preferable to a global average
    because different turbines may operate differently.
    """

    turbine_means = (
        df
        .groupBy("turbine_id")
        .agg(
            F.avg("wind_speed").alias(
                "mean_wind_speed"
            ),
            F.avg("wind_direction").alias(
                "mean_wind_direction"
            ),
            F.avg("power_output").alias(
                "mean_power_output"
            ),
        )
    )

    result = df.join(
        turbine_means,
        on="turbine_id",
        how="left",
    )

    result = (
        result
        .withColumn(
            "wind_speed",
            F.coalesce(
                F.col("wind_speed"),
                F.col("mean_wind_speed"),
            )
        )
        .withColumn(
            "wind_direction",
            F.coalesce(
                F.col("wind_direction"),
                F.col("mean_wind_direction"),
            )
        )
        .withColumn(
            "power_output",
            F.coalesce(
                F.col("power_output"),
                F.col("mean_power_output"),
            )
        )
        .drop(
            "mean_wind_speed",
            "mean_wind_direction",
            "mean_power_output",
        )
    )

    return result


def remove_invalid_keys(
    df: DataFrame,
) -> DataFrame:
    """
    Remove records that cannot be meaningfully recovered.
    """

    return df.dropna(
        subset=[
            "timestamp",
            "turbine_id",
        ]
    )


def clean_turbine_data(
    df: DataFrame,
) -> DataFrame:
    """
    Execute the complete cleaning process.
    """

    df = remove_invalid_keys(df)

    find_duplicates(df)

    df = remove_duplicates(df)

    df = null_out_invalid_values(df)

    df = impute_missing_values(df)

    # Cast to a fixed-precision decimal (4 digits after the point)
    
    df = (
        df
        .withColumn("wind_speed", F.col("wind_speed").cast(DecimalType(10, 4)))
        .withColumn("wind_direction", F.col("wind_direction").cast(DecimalType(10, 4)))
        .withColumn("power_output", F.col("power_output").cast(DecimalType(10, 4)))
    )

    print("\n--- Sample of cleaned data ---")
    df.show(1, truncate=False)

    return df
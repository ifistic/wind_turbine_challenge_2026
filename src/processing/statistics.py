"""
Summary statistics transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_summary_statistics(
    df: DataFrame,
    time_period: str = "1 day",
) -> DataFrame:
    """
    Calculate power-output statistics for every turbine, over a
    given time period (default: 24 hours / 1 day). Args:
        df: Turbine readings DataFrame (must have `timestamp`
            and `turbine_id` columns).
        time_period: Spark tumbling-window duration string,
            e.g. "1 day", "24 hours", "1 hour".
    Returns:
        DataFrame with one row per turbine per time window,
        containing min/max/avg/stddev power output and the
        number of readings in that window.
    """

    return (
        df
        .groupBy(
            "turbine_id",
            F.window("timestamp", time_period).alias("time_window"),
        )
        .agg(
            F.min("power_output").alias(
                "min_power_mw"
            ),

            F.max("power_output").alias(
                "max_power_mw"
            ),

            F.avg("power_output").alias(
                "avg_power_mw"
            ),

            F.stddev("power_output").alias(
                "stddev_power_mw"
            ),

            F.count("*").alias(
                "reading_count"
            ),
        )
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .drop("time_window")
        .orderBy("turbine_id", "window_start")
    )
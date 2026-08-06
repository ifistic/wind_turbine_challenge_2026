"""
anomaly.py

Detect anomalous turbine power-output readings.

"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.config import CONFIG


def detect_anomalies(
    df: DataFrame,
    time_period: str = "1 day",
) -> DataFrame:
    """
    Detect power-output anomalies for each turbine, within each
    time window.

    """

    # ----------------------------------------------------------
    # Tag each reading with its time window
    # ----------------------------------------------------------
    windowed_df = df.withColumn(
        "time_window",
        F.window("timestamp", time_period),
    )

    # ----------------------------------------------------------
    # Calculate statistics for each turbine, per time window
    # ----------------------------------------------------------
    turbine_stats = (
        windowed_df
        .groupBy("turbine_id", "time_window")
        .agg(
            F.avg("power_output").alias(
                "mean_power_mw"
            ),
            F.stddev("power_output").alias(
                "stddev_power_mw"
            ),
        )
    )

    # ----------------------------------------------------------
    # Calculate lower and upper anomaly boundaries
    # ----------------------------------------------------------
    turbine_stats = (
        turbine_stats
        .withColumn(
            "lower_bound_mw",
            F.col("mean_power_mw")
            - (
                F.lit(CONFIG.ANOMALY_STD_THRESHOLD)
                * F.col("stddev_power_mw")
            ),
        )
        .withColumn(
            "upper_bound_mw",
            F.col("mean_power_mw")
            + (
                F.lit(CONFIG.ANOMALY_STD_THRESHOLD)
                * F.col("stddev_power_mw")
            ),
        )
    )

    # ----------------------------------------------------------
    # Join statistics back to individual measurements, matching
    # on both turbine_id AND time_window (not turbine_id alone)
    # ----------------------------------------------------------
    result_df = windowed_df.join(
        turbine_stats,
        on=["turbine_id", "time_window"],
        how="left",
    )

    # ----------------------------------------------------------
    # Identify anomalies
    # ----------------------------------------------------------
    result_df = result_df.withColumn(
        "is_anomaly",
        (
            F.col("power_output")
            < F.col("lower_bound_mw")
        )
        |
        (
            F.col("power_output")
            > F.col("upper_bound_mw")
        ),
    )

    # ----------------------------------------------------------
    # Return only anomalous readings
    # ----------------------------------------------------------
    anomaly_df = (
        result_df
        .filter(
            F.col("is_anomaly") == True
        )
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .drop("time_window")
        .orderBy(
            "turbine_id",
            "timestamp",
        )
    )

    return anomaly_df
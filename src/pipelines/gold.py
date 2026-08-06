"""
Gold layer for the Wind Turbine Data Pipeline.

Creates analytics-ready summary statistics and anomaly datasets.
"""

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import asc

from src.utils.config import CONFIG
from src.processing.anomaly import detect_anomalies
from src.processing.statistics import calculate_summary_statistics
from src.pipelines.silver import write_to_postgres
from src.utils.helpers import write_parquet


def create_gold_layer(
    silver_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """
    Create Gold-layer datasets.

    Returns:
        summary_df:
            Summary statistics for each turbine.

        anomaly_df:
            Turbine readings identified as anomalies.
    """

    # Calculate turbine summary statistics
    summary_df = calculate_summary_statistics(
        silver_df
    )

    # Detect anomalous power-output readings
    anomaly_df = detect_anomalies(
        silver_df
    )

    # Gold output locations
    summary_path = (
        CONFIG.GOLD_DIR / "summary_statistics"
    )

    anomaly_path = (
        CONFIG.GOLD_DIR / "anomalies"
    )

    # Save as Parquet
    write_parquet(
        summary_df,
        summary_path,
        mode="overwrite",
    )

    write_parquet(
        anomaly_df,
        anomaly_path,
        mode="overwrite",
    )

    # Save to PostgreSQL (same database used by the Silver layer)
    write_to_postgres(
        summary_df,
        table_name="gold_summary_statistics",
    )

    write_to_postgres(
        anomaly_df.orderBy(asc("turbine_id")),
        table_name="gold_anomalies",
    )

    return summary_df, anomaly_df
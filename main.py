"""
main.py

Entry point for the Wind Turbine Data Pipeline.

Pipeline:

"""

"""
main.py
Entry point for the Wind Turbine Data Pipeline.
Pipeline:
Raw CSV to Bronze to Silver to Gold
"""
from pyspark.sql import SparkSession
from src.utils.config import CONFIG
from src.ingestion.ingest import ingest_raw_data
from src.pipelines.bronze import create_bronze_layer
from src.pipelines.silver import create_silver_layer
from src.pipelines.gold import create_gold_layer

def create_spark_session() -> SparkSession:
    """Create a local Spark session."""

    spark = (
        SparkSession.builder
        .appName(CONFIG.SPARK_APP_NAME)
        .master(CONFIG.SPARK_MASTER)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        CONFIG.SPARK_LOG_LEVEL
    )

    return spark


def main() -> None:
    """Run the complete Medallion pipeline."""

    print("*" * 60)
    print("Starting Wind Turbine Data Pipeline")
    print("*" * 60)

    spark = create_spark_session()

    try:
        # ======================================================
        # RAW
        # ======================================================
        print("\n Reading raw CSV files...")

        raw_df = ingest_raw_data(spark)

        print(f"Raw records: {raw_df.count()}")

        raw_df.show(1, truncate=False)

        # ======================================================
        # BRONZE
        # ======================================================
        print("\n Creating Bronze layer...")

        bronze_df = create_bronze_layer(raw_df)

        print(f"Bronze records: {bronze_df.count()}")

        # ======================================================
        # SILVER
        # ======================================================
        print("\n Creating Silver layer ( Processed Data)")

        silver_df = create_silver_layer(bronze_df)

        print(f"Silver records: {silver_df.count()}")

        # ======================================================
        # GOLD
        # ======================================================
        print("\n Creating Gold layer")

        summary_df, anomaly_df = create_gold_layer(
            silver_df
        )

        # ======================================================
        # RESULTS
        # ======================================================
        print("\nSummary Statistics")
        print("-" * 60)

        summary_df.orderBy(
            "turbine_id"
        ).show(2,
            truncate=False
        )

        print("\nDetected Anomalies")
        #print("-" * 40)

        anomaly_df.orderBy(
            "turbine_id",
            "timestamp"
        ).show(
            2,
            truncate=False
        )

        print("=" * 40)
        print("Pipeline completed successfully.")
        #print("=" * 60)

    except Exception as error:
        print("\nPipeline failed!")
        print(f"Error: {error}")
        raise

    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()

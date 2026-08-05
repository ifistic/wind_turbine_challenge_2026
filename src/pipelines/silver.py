"""
Silver layer for the Wind Turbine Data Pipeline.

The Silver layer contains cleaned and validated turbine data.
"""

import sqlite3

from pyspark.sql import DataFrame

from src.utils.config import CONFIG
from src.processing.cleaning import clean_turbine_data
from src.utils.helpers import write_parquet


def create_silver_layer(df: DataFrame) -> DataFrame:
    """
    Transform Bronze data into cleaned Silver data.

    Args:
        df: Bronze PySpark DataFrame.

    Returns:
        Cleaned Silver PySpark DataFrame.
    """

    silver_df = clean_turbine_data(df)

    write_parquet(
        silver_df,
        CONFIG.SILVER_DIR,
        mode="overwrite",
    )

    write_to_sqlite(silver_df)

    silver_df.show(5, truncate=False)

    return silver_df


def write_to_sqlite(
    df: DataFrame,
    table_name: str = "Cleaned_turbine_readings",
) -> None:
    """
    Write a DataFrame to the local SQLite database.

    Converts the Spark DataFrame to pandas and writes it via
    Python's built-in sqlite3 module — no external JDBC driver
    jar required. Suitable for small-to-moderate datasets that
    comfortably fit in driver memory (collects all rows to the
    driver via toPandas()).

    Args:
        df: DataFrame to write.
        table_name: Destination table name in the SQLite database.
    """

    print(f"\nWriting to SQLite: {CONFIG.SQLITE_DB_PATH} (table: {table_name})")

    pandas_df = df.toPandas()

    CONFIG.SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(CONFIG.SQLITE_DB_PATH) as conn:
        pandas_df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False,
        )

    print(f"Wrote {len(pandas_df)} row(s) to '{table_name}'")
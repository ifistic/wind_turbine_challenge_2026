"""
Silver layer for the Wind Turbine Data Pipeline.The Silver layer contains cleaned and validated turbine data.
"""

from pyspark.sql import DataFrame
from sqlalchemy import create_engine

from src.utils.config import CONFIG
from src.processing.cleaning import clean_turbine_data
from src.utils.helpers import write_parquet


def create_silver_layer(df: DataFrame) -> DataFrame:
    """
    Transform Bronze data into cleaned Silver data.

    """

    silver_df = clean_turbine_data(df)

    write_parquet(
        silver_df,
        CONFIG.SILVER_DIR,
        mode="overwrite",
    )

    write_to_postgres(silver_df, table_name="silver_turbine_readings")

    silver_df.show(5, truncate=False)

    return silver_df


def write_to_postgres(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Write a DataFrame to PostgreSQL.

    """

    print(f"\nWriting to PostgreSQL: {CONFIG.POSTGRES_DB} (table: {table_name})")

    pandas_df = df.toPandas()

    engine = create_engine(CONFIG.POSTGRES_URL)

    pandas_df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
    )

    engine.dispose()

    print(f"Wrote {len(pandas_df)} row(s) to '{table_name}'")
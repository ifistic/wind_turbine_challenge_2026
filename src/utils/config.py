"""
config.py

Central configuration for the Wind Turbine Data Pipeline.
"""

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


@dataclass(frozen=True)
class Config:
    """
    Application configuration for the Wind Turbine Pipeline.
    """

    # ==========================================================
    # Project Directories
    # ==========================================================
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    HOME_DIR: Path = Path.home()

    # ==========================================================
    # Spark
    # ==========================================================
    SPARK_APP_NAME: str = "WindTurbinePipeline"
    SPARK_MASTER: str = "local[*]"
    SPARK_LOG_LEVEL: str = "WARN"

    # ==========================================================
    # JDBC (used if writing/reading a JDBC sink, e.g. a warehouse)
    # TODO: confirm this path — placeholder based on a typical
    # local driver location; update to wherever your .jar lives.
    # ==========================================================
    JDBC_JAR: str = str(Path.home() / "jars" / "jdbc-driver.jar")

    # ==========================================================
    # Turbine physical/operating limits
    # TODO: confirm these against your actual turbine spec sheet
    # or transformation/anomaly-detection module — placeholders
    # based on typical onshore utility-scale turbine ranges.
    # ==========================================================
    MIN_WIND_SPEED: float = 0.0        # m/s — below this, turbine can't generate
    MAX_WIND_SPEED: float = 25.0       # m/s — cut-out speed, turbine shuts down
    RATED_CAPACITY_MW: float = 2.5     # MW — nameplate capacity per turbine
    MIN_POWER_MW: float = 0.0          # MW — floor for valid power output

    # ==========================================================
    # Anomaly detection
    # Per spec: anomalies are readings outside 2 standard
    # deviations from the mean, over the same time period as
    # the summary statistics window.
    # ==========================================================
    ANOMALY_STD_THRESHOLD: float = 2.0

    # ==========================================================
    # PostgreSQL
    # Set these in a .env file at your project root, e.g.:
    #   POSTGRES_HOST=localhost
    #   POSTGRES_PORT=5432
    #   POSTGRES_DB=wind_turbine_db
    #   POSTGRES_USER=postgres
    #   POSTGRES_PASSWORD=yourpassword
    # ==========================================================
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "wind_turbine_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")

    @property
    def DATA_DIR(self) -> Path:
        """
        Directory containing generated Medallion datasets.
        """
        return self.BASE_DIR / "data"

    @property
    def RAW_DIR(self) -> Path:
        """
        Directory containing the original supplied CSV files.
        """
        return self.BASE_DIR / "raw_data"

    @property
    def DOWNLOADS_DIR(self) -> Path:
        """
        User's Downloads directory, where the source data zip lives.
        """
        return self.HOME_DIR / "Downloads"

    @property
    def SOURCE_ZIP(self) -> Path:
        """
        Path to the downloaded raw data archive.
        """
        return self.DOWNLOADS_DIR / "data.zip"

    @property
    def BRONZE_DIR(self) -> Path:
        """
        Bronze layer output directory.
        """
        return self.DATA_DIR / "bronze"

    @property
    def SILVER_DIR(self) -> Path:
        """
        Silver layer output directory.
        """
        return self.DATA_DIR / "silver"

    @property
    def GOLD_DIR(self) -> Path:
        """
        Gold layer output directory.
        """
        return self.DATA_DIR / "gold"

    @property
    def SQLITE_DB_PATH(self) -> Path:
        """
        Path to the local SQLite database file.
        """
        return self.DATA_DIR / "wind_turbine.db"

    @property
    def POSTGRES_URL(self) -> str:
        """
        SQLAlchemy connection string for PostgreSQL.
        """
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


# Module-level singleton instance used throughout the pipeline,
# e.g. `from src.utils.config import CONFIG`
CONFIG = Config()
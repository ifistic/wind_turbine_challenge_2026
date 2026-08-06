# Wind Turbine Data Pipeline

A PySpark-based medallion architecture (Bronze - Silver - Gold) pipeline that ingests raw wind turbine sensor readings, cleans and validates the data, computes summary statistics and anomaly detection, and stores the results in PostgreSQL for further analysis.

## Architecture

```
Raw CSV (zip, downloaded locally) into  Bronze, Silver, Gold
```

- **Bronze**: Lands raw CSV files from source (unzipped from the source archive), tags rows with ingestion metadata.
- **Silver**: Cleans data (deduplication, null handling, imputation of missing sensor values, invalid-value removal), rounds numeric columns to 4 decimal places, writes to Parquet and PostgreSQL.
- **Gold**: Computes daily summary statistics (min/max/avg/stddev power output per turbine) and detects anomalies (readings outside 2 standard deviations from the mean, within the same 24-hour window). Both are written to Parquet and PostgreSQL.

## Prerequisites

- Python 3.12+
- Java (required by PySpark) — check with `java -version`
- PostgreSQL 16+
- Git
- VS Code (or any editor of your choice)

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/ifistic/wind_turbine_challenge_2026.git
cd wind_turbine_challenge_2026
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv project_venv
source project_venv/bin/activate
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Install and start PostgreSQL

```bash
# Linux
sudo apt install postgresql
sudo systemctl start postgresql
```

Confirm which port your cluster is running on (this project assumes the default `5432`, but **double-check** — some setups may use a non-default port):

```bash
pg_lsclusters
```

### 5. Create the database and set a password

```bash
sudo -u postgres createdb wind_turbine_db
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"
```

> The password above (`postgres`) is an example only — use a real password if this database will hold anything sensitive, and make sure it matches what you put in `.env` in step 6.

### 5.5. Allow password-based local connections (pg_hba.conf) and troubleshooting

Fresh PostgreSQL installs sometimes ship with `pg_hba.conf` rules that don't include a plain `127.0.0.1` entry (e.g. only allowing a specific external IP or IPv6 loopback), which causes a `FATAL: no pg_hba.conf entry for host "127.0.0.1"` error even with a correct password. Check first:

```bash
sudo cat /etc/postgresql/16/main/pg_hba.conf | grep -v "^#" | grep -v "^$"
```

If there's no line matching `host    all    all    127.0.0.1/32    ...`, add one:

```bash
sudo nano /etc/postgresql/16/main/pg_hba.conf
```

Add this line (near the other `host` entries):

```
host    all             all             127.0.0.1/32            scram-sha-256
```

Save (`Ctrl+O`, `Enter`, `Ctrl+X` in nano), then restart PostgreSQL to apply it:

```bash
sudo systemctl restart postgresql
```

### 6. Configure environment variables

Create a `.env` file in the project root:

```bash
cat > .env << 'EOF'
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=wind_turbine_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
EOF
```

> **Note:** if `pg_lsclusters` in step 4 showed a different port (e.g. `5433`), update `POSTGRES_PORT` in `.env` to match. A mismatched port is the most common cause of connection failures.

### 7. Add the raw data source

`src/ingestion/ingest.py` reads the source archive from `~/Downloads/data.zip` by default (update `SOURCE_ZIP` in `src/utils/config.py` if yours lives elsewhere). On first run, the pipeline automatically unzips it into  Bronze layer and lands a copy of those CSVs into `data/bronze/`.

## Running the pipeline

```bash
python main.py
```

This runs all four stages in sequence — Raw ingestion, Bronze, Silver, Gold — printing progress and sample output at each step, and writes:

- Parquet files to `data/bronze/`, `data/silver/`, `data/gold/`
- Database tables to PostgreSQL: `Processed_data` (cleaned Silver data), `gold_summary_statistics`, `gold_anomalies`

## Querying the data in PostgreSQL

### Option A — `psql` (command line)

Connect:

```bash
psql -h localhost -U postgres -d wind_turbine_db
```

(add `-p 5433` if your cluster runs on a non-default port — see step 4 above)

Useful queries once connected:

```sql
-- List all tables
\dt

-- Preview cleaned data
SELECT * FROM "Processed_data" LIMIT 10;

-- Row count
SELECT COUNT(*) FROM "Processed_data";

-- Summary stats for a specific turbine
SELECT * FROM gold_summary_statistics WHERE turbine_id = 1 ORDER BY window_start;

-- All flagged anomalies
SELECT * FROM gold_anomalies ORDER BY turbine_id, timestamp;

-- Which turbines have the most anomalies?
SELECT turbine_id, COUNT(*) AS anomaly_count
FROM gold_anomalies
GROUP BY turbine_id
ORDER BY anomaly_count DESC;

-- Exit
\q
```

> If a query's output looks paused with no way to type, press `q` to exit the pager. If column headers are missing from results, run `\pset tuples_only off`.

### Option B — DBeaver (GUI)

1. Install: `sudo snap install dbeaver-ce`
2. Launch: `dbeaver &`
3. **Database → New Database Connection → PostgreSQL**
4. Connection details:

   | Field | Value |
   |---|---|
   | Host | `localhost` |
   | Port | `5432` (or your actual port from `pg_lsclusters`) |
   | Database | `wind_turbine_db` |
   | Username | `postgres` |
   | Password | `postgres` |

5. Test Connection → Finish
6. Browse tables under `wind_turbine_db → Schemas → public → Tables`

### Option C — Python / pandas

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@localhost:5432/wind_turbine_db")
df = pd.read_sql("SELECT * FROM gold_summary_statistics", engine)
print(df.head())
```

## Project structure

```
.
├── artifacts/                # Spark runtime temp/event-log directory (auto-generated)
├── data/
│   ├── bronze/                # Landed raw CSVs (verbatim copies from raw_data/)
│   │   ├── data_group_1.csv
│   │   ├── data_group_2.csv
│   │   └── data_group_3.csv
│   ├── gold/
│   │   ├── anomalies/          # Parquet: flagged anomalous readings
│   │   │   ├── part-*.snappy.parquet
│   │   │   └── _SUCCESS
│   │   └── summary_statistics/ # Parquet: daily min/max/avg/stddev per turbine
│   │       ├── part-*.snappy.parquet
│   │       └── _SUCCESS
│   ├── silver/                 # Parquet: cleaned turbine readings
│   │   ├── part-*.snappy.parquet
│   │   └── _SUCCESS
│   └── wind_turbine.db          # Legacy SQLite artifact (project now uses PostgreSQL)
├── main.py                     # Entry point — runs the full pipeline
├── raw_data/                    # Unzipped source CSVs
│   ├── data_group_1.csv
│   ├── data_group_2.csv
│   └── data_group_3.csv
├── README.md
├── requirements.txt
├── src/
│   ├── ingestion/
│   │   ├── ingest.py            # Unzips source data, reads raw CSVs into Spark
│   │   └── __init__.py
│   ├── __init__.py
│   ├── pipelines/
│   │   ├── bronze.py            # Lands raw files, adds ingestion metadata
│   │   ├── gold.py              # Orchestrates summary statistics + anomaly detection
│   │   ├── __init__.py
│   │   └── silver.py            # Cleans data, writes to Parquet + PostgreSQL
│   ├── processing/
│   │   ├── anomaly.py           # 2-stddev anomaly detection, windowed per turbine
│   │   ├── cleaning.py          # Dedup, null handling, imputation, rounding
│   │   ├── __init__.py
│   │   └── statistics.py        # Windowed min/max/avg/stddev per turbine
│   └── utils/
│       ├── config.py            # Central configuration (paths, thresholds, DB credentials)
│       ├── helpers.py           # Shared write/metadata utilities
│       └── __init__.py
└── tests/
    └── __init__.py
```

## Solution Design & Assumptions

### Design

The solution is implemented entirely in Python, using **PySpark** as the core processing engine and a **medallion architecture** (Bronze, Silver, Gold) to separate raw ingestion, cleaning, and analytics-ready output into distinct, auditable stages.

- **Why PySpark**: the brief allows any framework; PySpark was chosen for its native support of windowed aggregations (`F.window()`), which map directly onto the "over a given time period (e.g., 24 hours)" requirement for both summary statistics and anomaly detection, and because it scales beyond the current dataset size without a rewrite.
- **Why a medallion architecture**: keeping raw, cleaned, and aggregated data in separate layers means each stage is independently inspectable and re-runnable. Bronze preserves an untouched audit trail of the source files, Silver is the single source of truth for "cleaned data," and Gold contains only derived analytics, so a bug in aggregation logic never risks corrupting the underlying cleaned data.
- **Cleaning approach**: missing values are imputed using the **per-turbine mean** (not a global mean), since different turbines can have systematically different output profiles (location, model, orientation); using a global average would bias imputed values toward whichever turbines dominate the dataset. Rows where a value was imputed are flagged with an `is_imputed` boolean column, so downstream consumers can distinguish real observed readings from filled-in ones.
- **Anomaly detection**: computed **per turbine, per time window** (not per turbine across the whole dataset, and not across all turbines combined), so a turbine's readings are only ever compared against its own recent behavior. This avoids flagging normal seasonal or daily variation as anomalous, and avoids one turbine's baseline being skewed by another's.
- **Storage**: PostgreSQL was used for the "store in a database for further analysis" requirement, chosen over SQLite for its native support of fixed-precision `NUMERIC` types (avoiding Python `Decimal`-to-driver binding issues) and because it's a more realistic choice for a production-style analytics database than a single local file.

### Assumptions

- **Timestamp granularity**: source readings are hourly; the "24-hour period" in the brief is interpreted as a calendar-day tumbling window per turbine (`F.window(timestamp, "1 day")`), not a rolling 24-hour lookback from each reading.
- **Turbine operating limits**: physically invalid values (wind speed, power output bounds) are nulled out using placeholder turbine specification limits (e.g. rated capacity, cut-out wind speed) rather than a supplied spec sheet, since none was provided with the dataset — these should be replaced with manufacturer-supplied values in a production setting.
- **Anomaly threshold**: fixed at exactly 2 standard deviations from the mean, per the brief's explicit definition, rather than treated as a tunable parameter — though it is exposed as a config value (`ANOMALY_STD_THRESHOLD`) for easy adjustment if requirements change.
- **Duplicate readings**: a duplicate is defined as matching `(turbine_id, timestamp)` — that is, a turbine should report at most one reading per timestamp; if two rows share both fields, one is dropped.
- **Missing vs. invalid values**: both are handled identically (imputed with the per-turbine mean) rather than treated as separate cases, since the brief groups them together ("missing values and outliers, which must be removed or imputed").
- **Anomalies in the database**: the requirement to "store the cleaned data and summary statistics" is interpreted as reasonably extending to storing detected anomalies too, since anomalies are themselves a first-class derived output the brief asks the pipeline to produce, even though the storage requirement's wording technically names only cleaned data and summary statistics.
- **Imputed values can be flagged as anomalous**: because imputed rows use the same per-window mean that anomaly bounds are calculated from, a day with many imputed readings can artificially shrink that day's standard deviation, making genuine sensor readings more likely to fall outside the bounds. The `is_imputed` flag lets this be filtered or accounted for during analysis.

## Troubleshooting

- **`ModuleNotFoundError: No module named 'src'`** — run scripts as modules from the project root (`python -m src.processing.cleaning`), not by path.
- **`FATAL: password authentication failed`** — check for multiple PostgreSQL instances running at once (`pg_lsclusters`, `sudo ss -ltnp | grep 5432`) and confirm `.env`'s `POSTGRES_PORT` matches the actual running cluster's port.
- **`FATAL: no pg_hba.conf entry for host "127.0.0.1"...`** — the password is correct but PostgreSQL has no rule permitting the connection at all. See setup step 5.5 — add a `host all all 127.0.0.1/32 scram-sha-256` line to `pg_hba.conf` and restart PostgreSQL.
- **`._data_group_*.csv` files with 0 rows** — macOS zip metadata junk (AppleDouble files); safe to delete from `raw_data/` and `data/bronze/`.
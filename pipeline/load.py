"""
load.py - Data Loading Layer
Responsible for persisting the transformed dataset to two targets:
  1. PostgreSQL - relational storage for SQL analytics
  2. Parquet    - cleaned file for PySpark consumption

Loading Strategy:
  - Zones table : full replace
  - Trips table : first batch replaces, later batches append
  - Indexes     : created after table load
  - Views       : created last
"""

import logging
import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

TRIPS_COLUMNS = [
    "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "pu_location_id", "do_location_id", "ratecode_id",
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
    "payment_type", "payment_type_desc", "ratecode_desc",
    "trip_duration_mins", "speed_mph", "fare_per_mile", "tip_percentage",
    "pickup_date", "pickup_hour", "pickup_day_of_week",
    "pickup_week", "is_weekend", "time_of_day", "distance_bucket",
    "pickup_borough", "pickup_zone", "pickup_service_zone",
    "dropoff_borough", "dropoff_zone", "dropoff_service_zone",
]


def build_connection_string(db: dict) -> str:
    """Build a SQLAlchemy PostgreSQL connection URL from config.

    The password is URL-encoded so special characters like '@', ':', '/',
    '#', '?', '%' or spaces do not break the URL parser.
    """
    safe_password = quote_plus(db["password"])
    return (
        f"postgresql+psycopg2://{db['user']}:{safe_password}"
        f"@{db['host']}:{db['port']}/{db['name']}"
    )

def ensure_database_exists(db: dict) -> None:
    """
    Connect to the postgres system database and create the target
    database if it does not already exist.
    """
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        user=db["user"],
        password=db["password"],
        dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db["name"],))
    if cur.fetchone():
        logger.info(f"Database '{db['name']}' already exists.")
    else:
        cur.execute(f'CREATE DATABASE "{db["name"]}"')
        logger.info(f"Database '{db['name']}' created.")

    cur.close()
    conn.close()
    
    
def run_sql_file(engine, filepath: str) -> None:
    """
    Execute a .sql file against the connected database.
    Handles multiple statements separated by semicolons.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with engine.connect() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
        conn.commit()

    logger.info(
        f"SQL file executed: {Path(filepath).name} "
        f"({len(statements)} statements)"
    )


def load_zones(zones_df: pd.DataFrame, engine) -> None:
    """
    Load the taxi zone lookup table into PostgreSQL.
    The zone table is small, so we replace it each run.
    """
    logger.info("Loading taxi_zones lookup table...")

    df = zones_df.rename(columns={
        "LocationID": "location_id",
        "Borough": "borough",
        "Zone": "zone",
        "service_zone": "service_zone",
    })

    df.to_sql(
        name="taxi_zones",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=500,
    )

    logger.info(f"taxi_zones loaded: {len(df):,} rows")
    

def load_trips(df: pd.DataFrame, engine, batch_size: int) -> None:
    """
    Load the cleaned trips DataFrame into PostgreSQL in batches.

    First batch: replace the table.
    Later batches: append rows.
    """
    cols = [c for c in TRIPS_COLUMNS if c in df.columns]
    df_load = df[cols].copy()
    total = len(df_load)

    logger.info(
        f"Loading taxi_trips: {total:,} rows | "
        f"batch size {batch_size:,} | {len(cols)} columns"
    )

    for i in range(0, total, batch_size):
        batch = df_load.iloc[i : i + batch_size]
        is_first = i == 0
        exists_arg = "replace" if is_first else "append"

        batch.to_sql(
            name="taxi_trips",
            con=engine,
            if_exists=exists_arg,
            index=False,
            method="multi",
            chunksize=2000,
        )

        loaded = min(i + batch_size, total)
        logger.info(f"Batch progress: {loaded:>10,} / {total:,} rows")

    logger.info(f"taxi_trips fully loaded: {total:,} rows")
    
    
    
def save_cleaned_parquet(df: pd.DataFrame, output_dir: str) -> None:
    """
    Persist the cleaned DataFrame as a single Parquet file for Spark.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    out_path = os.path.join(output_dir, "cleaned_yellow_trips_2024_01.parquet")

    df_save = df.copy()
    if "pickup_date" in df_save.columns:
        df_save["pickup_date"] = df_save["pickup_date"].astype(str)

    df_save.to_parquet(out_path, index=False, engine="pyarrow")

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    logger.info(f"Cleaned Parquet saved: {out_path} ({size_mb:.1f} MB)")
    
    
    
def load_all(
    df: pd.DataFrame,
    zones_df: pd.DataFrame,
    config: dict,
) -> None:
    """
    Execute the full load pipeline.
    """
    logger.info("=" * 55)
    logger.info("LOAD PIPELINE STARTED")
    logger.info("=" * 55)

    db = config["database"]
    batch_size = config["pipeline"]["batch_size"]
    sql_dir = Path(__file__).parent.parent / "sql"

    ensure_database_exists(db)

    engine = create_engine(build_connection_string(db), echo=False)
    logger.info(
        f"Connected to PostgreSQL: {db['host']}:{db['port']}/{db['name']}"
    )

    try:
        load_zones(zones_df, engine)
        load_trips(df, engine, batch_size)

        logger.info("Creating indexes...")
        run_sql_file(engine, str(sql_dir / "01_create_indexes.sql"))

        logger.info("Creating reporting views...")
        run_sql_file(engine, str(sql_dir / "03_reporting_views.sql"))

        save_cleaned_parquet(df, config["paths"]["cleaned_output"])

    finally:
        engine.dispose()
        logger.info("Database connection closed.")

    logger.info("=" * 55)
    logger.info("LOAD PIPELINE COMPLETE")
    logger.info("=" * 55)
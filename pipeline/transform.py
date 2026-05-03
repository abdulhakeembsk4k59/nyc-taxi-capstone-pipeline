"""
transform.py - Data Transformation Layer
Responsible for cleaning, validating, enriching, and feature-engineering
raw NYC Taxi trip data before it is loaded into PostgreSQL.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)





REQUIRED_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
]

PAYMENT_TYPE_MAP = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided Trip",
}

RATECODE_MAP = {
    1: "Standard Rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau / Westchester",
    5: "Negotiated Fare",
    6: "Group Ride",
}

DISTANCE_BINS = [0, 1, 3, 7, 15, 200]
DISTANCE_LABELS = [
    "Very Short (<1 mi)",
    "Short (1-3 mi)",
    "Medium (3-7 mi)",
    "Long (7-15 mi)",
    "Very Long (15+ mi)",
]

HOUR_BINS = [-1, 5, 11, 16, 20, 23]
HOUR_LABELS = ["Late Night", "Morning", "Afternoon", "Evening", "Night"]


def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Confirm all required columns are present and cast datetime fields.
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Schema validation failed - missing columns: {missing}")

    df = df.copy()
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"],
        errors="coerce",
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"],
        errors="coerce",
    )

    logger.info(f"Schema validation passed. Columns confirmed: {len(REQUIRED_COLUMNS)}")
    return df

def remove_invalid_records(df: pd.DataFrame, dq: dict) -> pd.DataFrame:
    """
    Remove records that violate data quality business rules.
    """
    initial = len(df)

    mask = (
        df["trip_distance"].between(
            dq["min_trip_distance_miles"],
            dq["max_trip_distance_miles"],
        )
        & df["fare_amount"].between(
            dq["min_fare_amount"],
            dq["max_fare_amount"],
        )
        & df["passenger_count"].between(
            dq["min_passengers"],
            dq["max_passengers"],
        )
        & (df["total_amount"] > 0)
        & df["tpep_pickup_datetime"].notna()
        & df["tpep_dropoff_datetime"].notna()
        & df["PULocationID"].notna()
        & df["DOLocationID"].notna()
        & (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"])
    )

    df = df[mask].copy()
    removed = initial - len(df)
    pct = removed / initial * 100 if initial else 0

    logger.info(
        f"Data quality filter: {removed:,} records removed ({pct:.1f}%) | "
        f"{len(df):,} clean records remain"
    )
    return df


def engineer_features(df: pd.DataFrame, max_duration: int) -> pd.DataFrame:
    """
    Derive new analytical columns from raw fields.
    """
    logger.info("Engineering features...")
    df = df.copy()

    df["trip_duration_mins"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds()
        / 60
    ).round(2)

    pre_dur = len(df)
    df = df[
        (df["trip_duration_mins"] >= 1)
        & (df["trip_duration_mins"] <= max_duration)
    ].copy()

    logger.info(
        f"  Duration filter removed {pre_dur - len(df):,} trips "
        f"(< 1 min or > {max_duration} min)"
    )

    df["speed_mph"] = (
        df["trip_distance"] / (df["trip_duration_mins"] / 60)
    ).round(2)
    
    
    
    
    df["fare_per_mile"] = (
        df["fare_amount"] / df["trip_distance"].replace(0, np.nan)
    ).round(2)

    df["tip_percentage"] = (
        (df["tip_amount"] / df["fare_amount"].replace(0, np.nan)) * 100
    ).round(2).clip(lower=0, upper=200)

    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.day_name()
    df["pickup_week"] = (
        df["tpep_pickup_datetime"].dt.isocalendar().week.astype(int)
    )
    df["is_weekend"] = df["tpep_pickup_datetime"].dt.dayofweek >= 5
    
    
    
    df["time_of_day"] = pd.cut(
        df["pickup_hour"],
        bins=HOUR_BINS,
        labels=HOUR_LABELS,
    ).astype(str)

    df["distance_bucket"] = pd.cut(
        df["trip_distance"],
        bins=DISTANCE_BINS,
        labels=DISTANCE_LABELS,
    ).astype(str)

    df["payment_type_desc"] = (
        df["payment_type"].map(PAYMENT_TYPE_MAP).fillna("Unknown")
    )
    df["ratecode_desc"] = (
        df["RatecodeID"].map(RATECODE_MAP).fillna("Unknown")
    )

    logger.info(
        f"Feature engineering complete. "
        f"Final shape: {df.shape[0]:,} rows x {df.shape[1]} columns"
    )
    return df


def enrich_with_zones(
    trips_df: pd.DataFrame,
    zones_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Left-join trip pickup and dropoff location IDs to zone names.
    """
    logger.info("Enriching trips with zone metadata...")

    pu = zones_df.rename(columns={
        "LocationID": "PULocationID",
        "Borough": "pickup_borough",
        "Zone": "pickup_zone",
        "service_zone": "pickup_service_zone",
    })

    do = zones_df.rename(columns={
        "LocationID": "DOLocationID",
        "Borough": "dropoff_borough",
        "Zone": "dropoff_zone",
        "service_zone": "dropoff_service_zone",
    })

    df = trips_df.merge(pu, on="PULocationID", how="left")
    df = df.merge(do, on="DOLocationID", how="left")

    unmatched_pu = df["pickup_zone"].isna().sum()
    unmatched_do = df["dropoff_zone"].isna().sum()
    logger.info(
        f"Zone enrichment done | "
        f"Unmatched pickups: {unmatched_pu:,} | "
        f"Unmatched dropoffs: {unmatched_do:,}"
    )
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename selected columns to consistent snake_case for SQL compatibility.
    """
    rename_map = {
        "VendorID": "vendor_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "RatecodeID": "ratecode_id",
    }
    df = df.rename(columns=rename_map)
    logger.info("Column names normalized to snake_case.")
    return df


def transform_all(
    trips_df: pd.DataFrame,
    zones_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """
    Execute the full transformation pipeline in sequence.
    """
    logger.info("=" * 55)
    logger.info("TRANSFORMATION PIPELINE STARTED")
    logger.info("=" * 55)

    dq = config["data_quality"]
    max_duration = dq["max_trip_duration_minutes"]

    df = validate_schema(trips_df)
    df = remove_invalid_records(df, dq)
    df = engineer_features(df, max_duration)
    df = enrich_with_zones(df, zones_df)
    df = rename_columns(df)
    df = df.reset_index(drop=True)

    logger.info("=" * 55)
    logger.info(
        f"TRANSFORMATION COMPLETE | {len(df):,} records | {df.shape[1]} columns"
    )
    logger.info("=" * 55)
    return df
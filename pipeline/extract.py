"""
extract.py - Data Extraction Layer
Responsible for loading raw data sources from disk into memory.

Sources:
  - yellow_tripdata_2024-01.parquet
  - taxi_zone_lookup.csv
"""

import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

def extract_trips(raw_path: str) -> pd.DataFrame:
    """
    Load raw NYC Yellow Taxi trip data from a Parquet file.

    Args:
        raw_path: Absolute path to the .parquet source file.

    Returns:
        Raw trips DataFrame.

    Raises:
        FileNotFoundError: If the Parquet file does not exist.
    """
    
    path = Path(raw_path)
    if not path.exists():
        raise FileNotFoundError(f"Raw trips file not found: {raw_path}")
    
    logger.info(f"Reading trips Parquet: {path.name}")
    df = pd.read_parquet(raw_path, engine="pyarrow")
    
    logger.info(
        f"  Rows    : {len(df):>12,}\n"
        f"  Columns : {df.shape[1]}\n"
        f"  Memory  : {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB\n"
        f"  Schema  : {dict(df.dtypes)}"
    )
    return df



def extract_zones(zones_path: str) -> pd.DataFrame:
    """
    Load NYC Taxi Zone lookup table from a CSV file.

    Args:
        zones_path: Absolute path to taxi_zone_lookup.csv.

    Returns:
        Zone lookup DataFrame with LocationID, Borough, Zone, and service_zone.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
    """
    path = Path(zones_path)
    if not path.exists():
        raise FileNotFoundError(f"Zone lookup file not found: {zones_path}")
    
    logger.info(f"Reading zones CSV: {path.name}")
    df = pd.read_csv(zones_path)
    
    
    logger.info(f"  Zone records loaded: {len(df):,}")
    logger.info(f"  Boroughs found    : {sorted(df['Borough'].dropna().unique().tolist())}")
    return df


def extract_all(config: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract all data sources defined in the pipeline config.

    Args:
        config: Full pipeline configuration dictionary from config.yaml.

    Returns:
        Tuple of (trips_df, zones_df).
    """
    logger.info("Starting extraction of all data sources...")

    trips_df = extract_trips(config["paths"]["raw_trips"])
    zones_df = extract_zones(config["paths"]["raw_zones"])

    logger.info(
        f"Extraction complete - "
        f"{len(trips_df):,} trip records | {len(zones_df)} zone records"
    )
    return trips_df, zones_df
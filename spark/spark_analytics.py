"""
spark_analytics.py - PySpark Large-Scale Analytics

Reads the cleaned Parquet output from the ETL pipeline and generates
six aggregated CSV reports using Apache Spark.

Usage:
  python -m spark.spark_analytics
"""

import logging
import os
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def load_config() -> dict:
    """Load project config.yaml."""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
    
    
def create_spark_session(spark_cfg: dict) -> SparkSession:
    """
    Build and return a SparkSession using settings from config.yaml.
    """
    hadoop_home = spark_cfg.get("hadoop_home", "C:/hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home

    spark = (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .master(spark_cfg["master"])
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))
    return spark

def save_report(df, reports_dir: str, filename: str) -> None:
    """
    Save a small Spark report DataFrame as a single CSV file.
    """
    Path(reports_dir).mkdir(parents=True, exist_ok=True)
    out_path = os.path.join(reports_dir, filename)

    pdf = df.toPandas()
    pdf.to_csv(out_path, index=False)

    logger.info(f"Report saved: {filename} ({len(pdf)} rows)")
    
    
def report_borough_revenue(df):
    """
    Report 1: Revenue aggregated by pickup borough.
    """
    logger.info("Generating Report 1: Revenue by Borough...")

    revenue_window = Window.orderBy(F.lit(1)).rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing,
    )

    return (
        df.filter(F.col("pickup_borough").isNotNull())
        .groupBy("pickup_borough")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
        )
        .withColumn(
            "pct_of_total_revenue",
            F.round(
                F.col("total_revenue")
                * 100
                / F.sum("total_revenue").over(revenue_window),
                2,
            ),
        )
        .orderBy(F.desc("total_revenue"))
    )
    
    
def report_hourly_demand(df):
    """
    Report 2: Trip volume and revenue by hour of day.
    """
    logger.info("Generating Report 2: Hourly Demand Pattern...")

    total_trips = df.count()

    return (
        df.groupBy("pickup_hour", "time_of_day")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_mins"), 2).alias("avg_duration_mins"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
        .withColumn(
            "pct_of_daily_trips",
            F.round(F.col("total_trips") * 100.0 / F.lit(total_trips), 2),
        )
        .orderBy("pickup_hour")
    )
    
def report_top_routes(df, top_n: int = 20):
    """
    Report 3: Most popular pickup to dropoff zone routes.
    """
    logger.info(f"Generating Report 3: Top {top_n} Routes...")

    return (
        df.filter(
            F.col("pickup_zone").isNotNull()
            & F.col("dropoff_zone").isNotNull()
        )
        .groupBy("pickup_zone", "dropoff_zone", "pickup_borough", "dropoff_borough")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_mins"), 2).alias("avg_duration_mins"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
        .withColumn(
            "route_label",
            F.concat(F.col("pickup_zone"), F.lit(" -> "), F.col("dropoff_zone")),
        )
        .orderBy(F.desc("total_trips"))
        .limit(top_n)
    )
    

def report_daily_trend(df):
    """
    Report 4: Daily revenue and trip volume trend.
    """
    logger.info("Generating Report 4: Daily Revenue Trend...")

    daily = (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("daily_trips"),
            F.round(F.sum("total_amount"), 2).alias("daily_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
        .orderBy("pickup_date")
    )

    window_all = Window.orderBy("pickup_date").rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    window_prev = Window.orderBy("pickup_date")

    return (
        daily
        .withColumn("cumulative_trips", F.sum("daily_trips").over(window_all))
        .withColumn(
            "cumulative_revenue",
            F.round(F.sum("daily_revenue").over(window_all), 2),
        )
        .withColumn(
            "revenue_change_vs_prev_day",
            F.round(
                F.col("daily_revenue")
                - F.lag("daily_revenue").over(window_prev),
                2,
            ),
        )
    )
    
    
def report_distance_analysis(df):
    """
    Report 5: Trip statistics by distance bucket.
    """
    logger.info("Generating Report 5: Distance Bucket Analysis...")

    return (
        df.filter(
            F.col("distance_bucket").isNotNull()
            & (F.col("distance_bucket") != "nan")
        )
        .groupBy("distance_bucket")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_duration_mins"), 2).alias("avg_duration_mins"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("avg_fare")
    )    
    

def report_payment_analysis(df):
    """
    Report 6: Payment method breakdown.
    """
    logger.info("Generating Report 6: Payment Method Breakdown...")

    total_trips = df.count()

    return (
        df.groupBy("payment_type_desc")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        )
        .withColumn(
            "pct_of_trips",
            F.round(F.col("total_trips") * 100.0 / F.lit(total_trips), 2),
        )
        .orderBy(F.desc("total_trips"))
    )
    
    
    
    

def main() -> None:
    """
    Orchestrate all Spark analytics reports.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=" * 55)
    logger.info("SPARK ANALYTICS - NYC TAXI CAPSTONE")
    logger.info("=" * 55)

    t_start = time.time()
    df = None
    spark = None

    config = load_config()
    spark_cfg = config["spark"]
    reports_dir = config["paths"]["reports_output"]
    parquet_dir = config["paths"]["cleaned_output"]

    Path(reports_dir).mkdir(parents=True, exist_ok=True)

    spark = create_spark_session(spark_cfg)
    logger.info(f"SparkSession created: {spark_cfg['app_name']}")

    
    try:
        parquet_path = os.path.join(
            parquet_dir,
            "cleaned_yellow_trips_2024_01.parquet",
        )

        if not Path(parquet_path).exists():
            logger.error(
                f"Cleaned Parquet not found: {parquet_path}\n"
                "Run the ETL pipeline first: python -m pipeline.pipeline"
            )
            sys.exit(1)

        df = spark.read.parquet(parquet_path)
        record_count = df.count()
        logger.info(f"Loaded {record_count:,} records from cleaned Parquet")
        logger.info(f"Schema: {len(df.columns)} columns")

        df.cache()

        logger.info("-" * 55)
        logger.info("GENERATING ANALYTICS REPORTS")
        logger.info("-" * 55)

        reports = {
            "borough_revenue.csv": report_borough_revenue(df),
            "hourly_demand.csv": report_hourly_demand(df),
            "top_routes.csv": report_top_routes(df, top_n=20),
            "daily_trend.csv": report_daily_trend(df),
            "distance_analysis.csv": report_distance_analysis(df),
            "payment_analysis.csv": report_payment_analysis(df),
        }

        for filename, report_df in reports.items():
            save_report(report_df, reports_dir, filename)

        elapsed = time.time() - t_start
        logger.info("-" * 55)
        logger.info("SPARK ANALYTICS SUMMARY")
        logger.info("-" * 55)
        logger.info(f"Records analyzed : {record_count:,}")
        logger.info(f"Reports generated: {len(reports)}")
        logger.info("Output directory : data/reports/")
        logger.info(f"Total runtime    : {elapsed:.2f}s")
        logger.info("Status           : SUCCESS")
        
    finally:
        if df is not None:
            df.unpersist()

        if spark is not None:
            spark.stop()
            logger.info("SparkSession stopped.")
            
            
            
if __name__ == "__main__":
    main()
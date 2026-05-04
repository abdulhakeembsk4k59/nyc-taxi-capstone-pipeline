"""
pipeline.py - Main Pipeline Orchestrator

Entry point for the NYC Taxi Capstone data pipeline.

Responsibilities:
  - Bootstrap structured logging
  - Load and validate configuration
  - Coordinate Extract -> Transform -> Load stages
  - Provide simple retry logic
  - Capture and report execution metrics
  - Exit with a clear success or failure status

Usage:
  python -m pipeline.pipeline
"""

import functools
import logging
import os 
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import yaml

logger = logging.getLogger(__name__)


def setup_logging(log_dir: str, level: str = "INFO") -> str:
    """
    Configure logging to both console and a timestamped file.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"pipeline_{run_ts}.log")

    fmt = "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(fmt, datefmt))
    root.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt, datefmt))
    root.addHandler(file_handler)

    for noisy_logger in ("sqlalchemy.engine", "sqlalchemy.pool", "py4j", "urllib3"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)

    return log_path


def load_config(config_path: str) -> dict:
    """
    Load and validate the YAML pipeline configuration file.
    """
    if not Path(config_path).exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    required = ["database", "paths", "pipeline", "spark", "data_quality"]
    missing = [section for section in required if section not in config]

    if missing:
        raise KeyError(f"Config is missing required section(s): {missing}")

    logger.info(f"Config loaded from: {config_path}")
    return config



def with_retry(max_attempts: int = 3, delay_seconds: int = 5):
    """
    Decorator factory that retries the wrapped function on failure.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            fn_logger = logging.getLogger(func.__module__)

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_attempts:
                        fn_logger.error(
                            f"[{func.__name__}] failed after "
                            f"{max_attempts} attempts. Last error: {exc}"
                        )
                        raise

                    fn_logger.warning(
                        f"[{func.__name__}] attempt {attempt}/{max_attempts} "
                        f"failed: {exc}. Retrying in {delay_seconds}s..."
                    )
                    time.sleep(delay_seconds)

        return wrapper

    return decorator


@with_retry(max_attempts=3, delay_seconds=5)
def stage_extract(config: dict):
    """Stage 1 - Extract raw data from source files."""
    from pipeline.extract import extract_all

    logger.info("-" * 55)
    logger.info("STAGE 1 - EXTRACT")
    logger.info("-" * 55)

    t0 = time.time()
    result = extract_all(config)
    logger.info(f"Extract completed in {time.time() - t0:.2f}s")
    return result

@with_retry(max_attempts=2, delay_seconds=3)
def stage_transform(trips_df, zones_df, config: dict):
    """Stage 2 - Clean, validate, and enrich the raw data."""
    from pipeline.transform import transform_all

    logger.info("-" * 55)
    logger.info("STAGE 2 - TRANSFORM")
    logger.info("-" * 55)

    t0 = time.time()
    result = transform_all(trips_df, zones_df, config)
    logger.info(f"Transform completed in {time.time() - t0:.2f}s")
    return result

@with_retry(max_attempts=3, delay_seconds=10)
def stage_load(clean_df, zones_df, config: dict) -> None:
    """Stage 3 - Persist data to PostgreSQL and Parquet."""
    from pipeline.load import load_all

    logger.info("-" * 55)
    logger.info("STAGE 3 - LOAD")
    logger.info("-" * 55)

    t0 = time.time()
    load_all(clean_df, zones_df, config)
    logger.info(f"Load completed in {time.time() - t0:.2f}s")
    

def print_summary(metrics: dict) -> None:
    """Print a formatted pipeline execution summary to the log."""
    logger.info("")
    logger.info("=" * 55)
    logger.info("NYC TAXI PIPELINE - EXECUTION SUMMARY")
    logger.info("=" * 55)

    for key, value in metrics.items():
        logger.info(f"{key:<32} {value}")

    logger.info("=" * 55)
    logger.info("")
    
    

def main() -> None:
    """
    Orchestrate the full pipeline: Extract -> Transform -> Load.
    """
    pipeline_start = time.time()
    metrics = {}

    project_root = Path(__file__).parent.parent
    log_dir = str(project_root / "logs")
    log_path = setup_logging(log_dir, "INFO")
    
    try:
        config_path = str(project_root / "config" / "config.yaml")
        config = load_config(config_path)

        log_path = setup_logging(
            log_dir,
            config["pipeline"].get("log_level", "INFO"),
        )
        metrics["Log file"] = Path(log_path).name

        logger.info("=" * 55)
        logger.info("NYC TAXI DATA PIPELINE - CAPSTONE PROJECT")
        logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 55)

        trips_df, zones_df = stage_extract(config)
        metrics["Raw trip records"] = f"{len(trips_df):,}"
        metrics["Zone lookup records"] = f"{len(zones_df):,}"
        
        clean_df = stage_transform(trips_df, zones_df, config)
        removed = len(trips_df) - len(clean_df)
        removed_pct = removed / len(trips_df) * 100 if len(trips_df) else 0

        metrics["Clean records"] = f"{len(clean_df):,}"
        metrics["Records removed (DQ)"] = f"{removed:,} ({removed_pct:.1f}%)"
        metrics["Feature columns"] = str(clean_df.shape[1])

        stage_load(clean_df, zones_df, config)

        metrics["PostgreSQL taxi_trips"] = "loaded"
        metrics["PostgreSQL taxi_zones"] = "loaded"
        metrics["Indexes created"] = "yes"
        metrics["Reporting views"] = "yes"
        metrics["Cleaned Parquet"] = "saved"

        total = time.time() - pipeline_start
        metrics["Total runtime"] = f"{total:.2f}s"
        metrics["Pipeline status"] = "SUCCESS"

        print_summary(metrics)
        
    except FileNotFoundError as exc:
        logger.error(f"File not found: {exc}")
        metrics["Pipeline status"] = f"FAILED - {exc}"
        print_summary(metrics)
        sys.exit(1)

    except KeyError as exc:
        logger.error(f"Configuration error: {exc}")
        metrics["Pipeline status"] = f"FAILED - Config error: {exc}"
        print_summary(metrics)
        sys.exit(1)

    except Exception as exc:
        logger.exception(f"Unhandled pipeline failure: {exc}")
        metrics["Pipeline status"] = f"FAILED - {type(exc).__name__}: {exc}"
        print_summary(metrics)
        sys.exit(1)
        

if __name__ == "__main__":
    main()
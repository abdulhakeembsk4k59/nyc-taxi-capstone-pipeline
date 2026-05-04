"""
test_load.py - Manual end-to-end test for the Video 5 load layer.

Runs extract -> transform -> load against the configured PostgreSQL
database. Use this to verify the load pipeline before recording.

Run from the project root (capstone_project/):
    python test_load.py

To capture output to a file (recommended):
    python test_load.py 2>&1 | Tee-Object -FilePath load_test_output.log
"""

import logging
import sys
import traceback

import yaml

from pipeline.extract import extract_all
from pipeline.transform import transform_all
from pipeline.load import load_all


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("test_load")

    try:
        with open("config/config.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        log.info("Config loaded. Sections: %s", list(config.keys()))

        trips_df, zones_df = extract_all(config)
        clean_df = transform_all(trips_df, zones_df, config)
        load_all(clean_df, zones_df, config)

        print("=" * 55)
        print("Load test complete.")
        print("=" * 55)
        return 0

    except Exception as exc:
        print("=" * 55)
        print("LOAD TEST FAILED")
        print("=" * 55)
        print(f"Error type: {type(exc).__name__}")
        print(f"Error msg : {exc}")
        print("-" * 55)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

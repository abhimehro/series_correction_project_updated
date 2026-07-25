import os
import sys

# flake8: noqa: E402
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.batch_correction import batch_process, BatchConfig

if __name__ == "__main__":
    print("[DEBUG] Running manual batch_process call...")
    config = BatchConfig(
        series_selection="all",
        river_miles=[54.0, 53.0],
        years=(1995, 1996),
        dry_run=False,
        config_path="scripts/config.json",
        output_dir="data/output",
    )
    batch_process(config)
    print("[DEBUG] manual_batch_run.py completed.")

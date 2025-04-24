import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.batch_correction import batch_process

if __name__ == "__main__":
    print("[DEBUG] Running manual batch_process call...")
    batch_process(
        series_selection="all",
        river_miles=[54.0, 53.0],
        years=(1995, 1996),
        dry_run=False,
        config_path="scripts/config.json",
        output_dir="data/output"
    )
    print("[DEBUG] manual_batch_run.py completed.")

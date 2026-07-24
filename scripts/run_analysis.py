import json
import os

# Import your batch processing function directly
from scripts.batch_correction import BatchConfig, batch_process

# Use direct, absolute paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
CONFIG_PATH = os.path.join(PROJECT_PATH, "scripts", "config.json")
OUTPUT_DIR = os.path.join(PROJECT_PATH, "data", "output")

print(f"Using config at: {CONFIG_PATH}")
print(f"Output will be saved to: {OUTPUT_DIR}")

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load and update config with correct threshold
try:
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    # Set threshold to 3.0 for better outlier detection
    config["defaults"]["threshold"] = 3.0

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

    print("Updated threshold to 3.0 for better outlier detection")
except Exception:
    print("Error updating config")
    print("Will continue with existing config")

# Run batch processing
print("Processing data files...")
try:
    config = BatchConfig(
        series_selection="all",
        river_miles=None,
        years=(1995, 2014),
        dry_run=False,
        config_path=CONFIG_PATH,
        output_dir=OUTPUT_DIR,
    )
    summary = batch_process(config)
    print(f"Successfully processed {len(summary)} files")
except Exception:
    print("Error during processing")

print("Processing complete! Check the output directory for Excel files.")

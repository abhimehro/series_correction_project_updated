import json
import os

# Import your batch processing function directly
from scripts.batch_correction import batch_process

# Use direct, absolute paths
PROJECT_PATH = "/Users/abhimehrotra/PycharmProjects/series_correction_project_updated"
CONFIG_PATH = os.path.join(PROJECT_PATH, "scripts", "config.json")
OUTPUT_DIR = os.path.join(PROJECT_PATH, "data", "output")

print(f"Using config at: {CONFIG_PATH}")
print(f"Output will be saved to: {OUTPUT_DIR}")

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load and update config with correct threshold
try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)

    # Set threshold to 3.0 for better outlier detection
    config['defaults']['threshold'] = 3.0

    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=2)

    print("Updated threshold to 3.0 for better outlier detection")
except Exception as e:
    print(f"Error updating config: {e}")
    print("Will continue with existing config")

# Run batch processing
print("Processing data files...")
try:
    summary = batch_process(
        series_selection="all",
        river_miles=None,
        years=(1995, 2014),
        dry_run=False,
        config_path=CONFIG_PATH,
        output_dir=OUTPUT_DIR
    )
    print(f"Successfully processed {len(summary)} files")
except Exception as e:
    print(f"Error during processing: {e}")

print("Processing complete! Check the output directory for Excel files.")

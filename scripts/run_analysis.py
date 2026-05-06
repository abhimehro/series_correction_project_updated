import json
import os

from scripts.batch_correction import batch_process

PROJECT_PATH = "/Users/abhimehrotra/PycharmProjects/series_correction_project_updated"
CONFIG_PATH = os.path.join(PROJECT_PATH, "scripts", "config.json")
OUTPUT_DIR = os.path.join(PROJECT_PATH, "data", "output")


def update_config_threshold(config_path, new_threshold=3.0):
    try:
        with open(config_path, "r") as f:
            config = json.load(f)

        config["defaults"]["threshold"] = new_threshold

        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        print(f"Updated threshold to {new_threshold} for better outlier detection")
    except Exception as e:
        print(f"Error updating config: {e}")
        print("Will continue with existing config")


def run_batch_process(config_path, output_dir):
    print("Processing data files...")
    try:
        summary = batch_process(
            series_selection="all",
            river_miles=None,
            years=(1995, 2014),
            dry_run=False,
            config_path=config_path,
            output_dir=output_dir,
        )
        print(f"Successfully processed {len(summary)} files")
    except Exception as e:
        print(f"Error during processing: {e}")


def main():
    print(f"Using config at: {CONFIG_PATH}")
    print(f"Output will be saved to: {OUTPUT_DIR}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    update_config_threshold(CONFIG_PATH)
    run_batch_process(CONFIG_PATH, OUTPUT_DIR)

    print("Processing complete! Check the output directory for Excel files.")


if __name__ == '__main__':
    main()

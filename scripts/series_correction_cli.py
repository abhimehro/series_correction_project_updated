#!/usr/bin/env python3
import sys
import os
import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path

# Adjust system path for custom module imports
SCRIPT_DIR = os.path.dirname(__file__)
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, os.path.abspath(os.path.join(SCRIPT_DIR, '..')))

from loaders import load_config, load_series_data
from batch_correction import batch_process


def print_schedule(mode):
    commands = {
        "cron": "0 2 * * 1 /path/to/venv/bin/python /path/to/scripts/series_correction_cli.py --series all --archive >> /path/to/logs/$(date +%F)_run.log 2>&1",
        "windows": "SCHTASKS /CREATE /SC WEEKLY /D MON /TN \"SeriesCorrection\" /TR \"python C:\\path\\to\\scripts\\series_correction_cli.py --series all --archive\" /ST 02:00"
    }
    print(commands.get(mode, "Unsupported schedule mode. Use 'cron' or 'windows'."))


def main():
    parser = argparse.ArgumentParser(description="CLI for batch correction of Seatek sensor data.")
    parser.add_argument("-s", "--series", nargs="+", default=["all"],
                        help="Series IDs to process (e.g., 26 27) or 'all'.")
    parser.add_argument("--river-miles", type=float, nargs="+",
                        help="River miles to filter (e.g., 54.0 50.5)")
    parser.add_argument("--sensors", type=int, nargs="+",
                        help="Specific sensor IDs to process (overrides --river-miles)")
    parser.add_argument("--years", type=int, nargs=2, metavar=("START", "END"),
                        help="Year range to process (e.g., 1995 2014)")
    parser.add_argument("--window", type=int, default=5,
                        help="Window size for begin/end averaging")
    parser.add_argument("--threshold", type=float, default=0.10,
                        help="Threshold for discontinuity detection")
    parser.add_argument("--blank-policy", choices=["zero", "ignore"], default="zero",
                        help="How to handle blank values")
    parser.add_argument("--archive", action="store_true",
                        help="Save a date-stamped archive of the master workbook")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run detection only; skip corrections and file writes")
    parser.add_argument("--schedule", choices=["cron", "windows"],
                        help="Print scheduler snippet and exit")
    parser.add_argument("--log-path", type=str, default="logs",
                        help="Directory for run logs and scheduler logs")

    args = parser.parse_args()

    if args.schedule:
        print_schedule(args.schedule)
        return

    scripts_dir = Path('scripts')
    outputs_dir = Path('outputs')
    config = load_config(scripts_dir / 'config.json')
    rm_map = config.get('series', {})
    all_series = list(config['series'].keys())

    selected_sensors = (
        args.sensors or
        [sensor for mile in args.river_miles for sensor in rm_map.get(str(int(mile)), [])] if args.river_miles else
        [sensor for sub in rm_map.values() for sensor in sub] if rm_map else []
    )

    print(f"Selected sensors: {selected_sensors}")

    series_ids = [int(s) for s in all_series] if args.series == ['all'] else [int(s) for s in args.series]

    series_data = {sid: load_series_data(sid, config) for sid in series_ids}

    raw_df, corrected_df, change_df = batch_process(
        series_data,
        window_size=args.window,
        blank_policy=args.blank_policy,
        diff_threshold=args.threshold,
        sensors=selected_sensors,
        river_miles=args.river_miles,
        years=tuple(args.years) if args.years else None,
        log_dir=args.log_path
    )

    if args.dry_run:
        print("Raw summary rows:", len(raw_df))
        print("Corrections flagged:", len(change_df))
        return

    master_wb = "Series_Correction_Master.xlsx"
    raw_df.to_csv("Raw_Summary.csv", index=False)
    corrected_df.to_csv("Corrected_Summary.csv", index=False)
    change_df.to_csv("Change_Log.csv", index=False)

    if args.archive:
        archive_dir = outputs_dir / 'archive'
        archive_dir.mkdir(parents=True, exist_ok=True)
        timestamped = f"Series_Correction_Master_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.xlsx"
        shutil.copy(master_wb, archive_dir / timestamped)

    print("Batch correction complete.")


if __name__ == '__main__':
    main()

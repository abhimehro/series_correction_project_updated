#!/usr/bin/env python3
import argparse
import logging
import sys

from .batch_correction import batch_process


def main():
    """Main entry point for the series correction CLI."""
    parser = argparse.ArgumentParser(
        description=("Run series correction batch processing on sensor data.")
    )
    parser.add_argument(
        "--series",
        default="all",
        help=("Series number to process, or 'all' for all available series."),
    )
    parser.add_argument(
        "--river-miles",
        nargs=2,
        type=float,
        required=True,
        help=("Upstream and downstream river mile markers " "(e.g., 54.0 53.0)."),
    )
    parser.add_argument(
        "--years",
        nargs=2,
        type=int,
        required=True,
        help=("Start and end years of data to process " "(e.g., 1995 2014)."),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=("If set, process data without saving output files."),
    )
    args = parser.parse_args()

    # Configure logging to file with timestamp
    logging.basicConfig(
        filename="processing_log.txt",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info("Starting Seatek Analysis")

    # Ensure year range is in ascending order
    years = sorted(args.years)
    try:
        batch_process(args.series, args.river_miles, years, args.dry_run)
    except Exception as e:
        logging.error(f"Error in processing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

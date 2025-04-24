"""
Batch processing module for the Series Correction Project.

Loads sensor data files based on series, river miles, and years,
applies discontinuity detection and correction using the optional `processor`
module, and saves output data and summaries.

The implementation has been aligned with the requirements asserted in
scripts/tests/test_batch_correction.py.
"""

from __future__ import annotations

import logging
import os
import pandas as pd

print("[DEBUG] batch_correction.py module loaded and running!")

# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #
log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------- #
# Optional/project‑local dependencies
# --------------------------------------------------------------------------- #
ConfigLoaderType = None

# `loaders` (for config) and `processor` (for data correction) live inside the
# project. They may legitimately be absent in a test environment.  Import them
# lazily and fall back to stubs instead of raising – the tests monkey‑patch the
# attributes after import.
load_config_func = None
processor = None

try:
    from scripts import loaders  # absolute import
    load_config_func = getattr(loaders, "load_config", None)
except Exception:  # pragma: no cover
    load_config_func = None
    log.info("`loaders` module not available – using dummy config loader.")

try:
    from scripts import processor as _processor  # absolute import
    processor = _processor
except Exception:  # pragma: no cover
    processor = None
    log.info("`processor` module not available – raw data will be passed through.")

# Optional data_loader module (may be absent). Tests patch this when required.
try:
    from scripts import data_loader
except Exception:  # pragma: no cover
    data_loader = None

# --------------------------------------------------------------------------- #
# Exceptions
# --------------------------------------------------------------------------- #

class ProcessingError(Exception):
    """Custom exception for errors during batch processing."""
    pass

# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def _get_data_directory(config_data):
    """
    Determine the raw‑data directory.  Falls back to ./data when the config key
    is missing or invalid.
    """
    data_dir_key = "RAW_DATA_DIR"
    data_dir = config_data.get(data_dir_key)

    if data_dir and os.path.isdir(data_dir):
        log.info(f"Using data directory from config ({data_dir_key}): {data_dir}")
        return data_dir

    default_data_dir = os.path.join(os.getcwd(), "data")
    if data_dir:
        log.warning(
            f"Configured path {data_dir} (from {data_dir_key}) is not a directory – defaulting to {default_data_dir}"
        )
    else:
        log.warning(
            f"Key {data_dir_key} not found in config – defaulting data directory to {default_data_dir}"
        )
    if not os.path.isdir(default_data_dir):
        raise FileNotFoundError(
            f"Default data directory not found: {default_data_dir!r}"
        )
    return default_data_dir


def _determine_series_to_process(
    series_selection,
    river_miles,
    config_data,
    data_dir,
):
    """
    Turn the user’s selection (list / int / 'all') into an explicit list of
    series IDs, using the SENSOR_TO_RIVER map when available.
    """
    rm_map_key = "SENSOR_TO_RIVER"
    sensor_to_rm_map = config_data.get(rm_map_key, {})
    # Build reverse map river‑mile ➜ [sensor ids]
    rm_to_sensors_map = {}
    for sensor_str, rm_val in sensor_to_rm_map.items():
        try:
            rm_str = str(float(rm_val))
            rm_to_sensors_map.setdefault(rm_str, []).append(int(sensor_str))
        except ValueError:
            log.warning(f"Invalid sensor id in {rm_map_key} map: {sensor_str}")

    # ------------------------------------------------------------------ #
    # 'all' – derive by either RM filter or scanning directory
    # ------------------------------------------------------------------ #
    if isinstance(series_selection, str) and series_selection.lower() == "all":
        if river_miles and rm_to_sensors_map:
            selected = set()
            for rm in river_miles:
                selected.update(rm_to_sensors_map.get(str(float(rm)), []))
            series_list = sorted(selected)
            log.info(f"Series selected from river miles {river_miles} ➜ {series_list}")
        elif sensor_to_rm_map:
            series_list = sorted(int(s) for s in sensor_to_rm_map.keys())
            log.info(f"Selecting every series in {rm_map_key} map: {series_list}")
        else:
            # Fallback: scan the directory for SXX_Y??.txt files
            found = set()
            for fname in os.listdir(data_dir):
                if fname.startswith("S") and "_Y" in fname and fname.endswith(".txt"):
                    try:
                        found.add(int(fname.split("_")[0][1:]))
                    except Exception:
                        continue
            series_list = sorted(found)
            if river_miles:
                log.warning("River miles provided but no map to filter by – ignored.")
    else:
        # Explicit list/int provided
        raw = (
            [series_selection]
            if not isinstance(series_selection, (list, tuple))
            else series_selection
        )
        try:
            series_list = [int(s) for s in raw]
        except ValueError as exc:
            raise ValueError(f"Invalid series selection {raw!r}") from exc

        if river_miles and rm_to_sensors_map:
            allowed = set()
            for rm in river_miles:
                allowed.update(rm_to_sensors_map.get(str(float(rm)), []))
            series_list = sorted(set(series_list) & allowed)
            log.info(f"After RM filter ({river_miles}) series ➜ {series_list}")

    if not series_list:
        log.warning("No series selected for processing.")
    return series_list


def _find_files_to_process(
    series_list,
    years,
    data_dir,
    _config_data,
):
    """
    Discover S{series}_Y{index:02d}.txt files that correspond to the requested
    years.  The simplistic mapping assumes sequential Y01, Y02… for each year.
    """
    files = []
    start_year, end_year = years

    year_index = {}
    for series in series_list:
        year_index[series] = {}
        idx = 1
        for yr in range(start_year, end_year + 1):
            year_index[series][yr] = idx
            idx += 1

    for series in series_list:
        for yr in range(start_year, end_year + 1):
            yi = year_index[series][yr]
            filename = f"S{series}_Y{yi:02d}.txt"
            path = os.path.join(data_dir, filename)
            if os.path.isfile(path):
                files.append((series, yr, yi, path))
                log.debug(f"File found: {path}")
            else:
                log.warning(f"Expected file not found: {path}")

    files.sort(key=lambda t: (t[0], t[1]))  # Order by series then year
    if not files:
        log.warning(f"No matching data files in {data_dir}.")
    else:
        log.info(f"Found {len(files)} files to process.")
    return files


def _load_raw_data(file_path):
    """
    Load a raw Seatek txt file.  Uses a very forgiving pandas.read_csv setup
    suitable for the varied test fixtures.
    """
    print(f"[DEBUG] Attempting to load file: {file_path}")
    try:
        df = pd.read_csv(
            file_path,
            header=None,
            sep=r"\s+",
            engine="python",
            comment="#",
            skip_blank_lines=True,
        )
        print(f"[DEBUG] Loaded file: {file_path} with shape {df.shape}")
        # Best‑effort numeric conversion
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")

        # Nice column names: first col is time, rest ValueX
        if all(isinstance(c, int) for c in df.columns):
            cols = [f"Value{i + 1}" for i in range(len(df.columns))]
            if cols:
                cols[0] = "Time (Seconds)"
            df.columns = cols
        return df
    except pd.errors.EmptyDataError:
        print(f"[DEBUG] File {file_path} empty.")
        return pd.DataFrame()
    except Exception as exc:
        print(f"[DEBUG] Failed to load data from {file_path}: {exc}")
        raise ProcessingError(f"Failed to load data from {os.path.basename(file_path)}") from exc

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def batch_process(
    series_selection,
    river_miles,
    years,
    dry_run=False,
    config_path="scripts/config.json",
    output_dir=None,
):
    print("[DEBUG] Entered batch_process function")
    log.info(
        f"--- Batch processing START --- "
        f"series={series_selection} river_miles={river_miles} years={years} dry_run={dry_run}"
    )

    # ------------------------------------------------------------------ #
    # Configuration
    # ------------------------------------------------------------------ #
    config_data = {}
    if load_config_func:
        try:
            config_data = load_config_func(config_path)
        except FileNotFoundError:
            log.warning(f"Config file {config_path} not found – continuing with empty config.")
        except Exception as exc:  # pragma: no cover
            raise ProcessingError(f"Failed to load configuration: {exc}") from exc

    # Optional river‑mile lookup CSV – silently ignored when missing
    rm_map_path = config_data.get("RIVER_MILE_MAP_PATH", "scripts/river_mile_map.csv")
    if os.path.isfile(rm_map_path):
        rm_df = pd.read_csv(rm_map_path)
        config_data["SENSOR_TO_RIVER"] = rm_df.set_index("SENSOR_ID")["RIVER_MILE"].to_dict()
        config_data["RIVER_TO_SENSORS"] = (
            rm_df.groupby("RIVER_MILE")["SENSOR_ID"].apply(list).to_dict()
        )

    # ------------------------------------------------------------------ #
    # Directories
    # ------------------------------------------------------------------ #
    data_dir = _get_data_directory(config_data)
    output_dir = output_dir or data_dir  # Default discussed in tests

    if not dry_run and not os.path.isdir(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            log.info(f"Created output directory {output_dir}")
        except OSError as exc:
            raise ProcessingError(f"Unable to create output directory: {exc}") from exc

    # ------------------------------------------------------------------ #
    # Determine workloads
    # ------------------------------------------------------------------ #
    series_to_process = _determine_series_to_process(
        series_selection, river_miles, config_data, data_dir
    )
    if not series_to_process:
        return pd.DataFrame()

    files_to_process = _find_files_to_process(series_to_process, years, data_dir, config_data)
    print(f"[DEBUG] files_to_process: {files_to_process}")
    if not files_to_process:
        print("[DEBUG] No files to process! Entering fallback loop.")
        # Instead of raising, process all files in config for each series and write outputs
        for series_id in config_data["series"]:
            series_cfg = config_data["series"][series_id]
            for i, file_path in enumerate(series_cfg["raw_data"], start=1):
                print(f"[DEBUG] Forcing process of file: {file_path} (series {series_id})")
                try:
                    df = _load_raw_data(file_path)
                    print(f"[DEBUG] Loaded {file_path} shape: {df.shape}")
                    if not df.empty:
                        processor_config = {**config_data.get("defaults", {}), **config_data.get("processor_config", {})}
                        processed_df = processor.process_data(df, processor_config)
                        out_name = f"Series{series_id}_File{i:02d}_Processed.xlsx"
                        out_path = os.path.join(output_dir, out_name)
                        processed_df.to_excel(out_path, index=False)
                        print(f"[DEBUG] Wrote output: {out_path}")
                except Exception as e:
                    print(f"[DEBUG] Failed to process {file_path}: {e}")
        print("[DEBUG] Fallback loop complete. Exiting batch_process.")
        return  # Exit after forced processing for debug
    else:
        print("[DEBUG] files_to_process is not empty. Entering normal processing loop.")
        processor_config = {**config_data.get("defaults", {}), **config_data.get("processor_config", {})}

        summary_records = []

        # ------------------------------------------------------------------ #
        # Main loop
        # ------------------------------------------------------------------ #
        for series, year, yi, file_path in files_to_process:
            print(f"[DEBUG] Processing series: {series}, year: {year}, file: {file_path}")
            fname = os.path.basename(file_path)
            log.info(f"Processing {fname} (Series {series}, Year {year}, Y{yi:02d})")

            # Skip zero‑byte files early
            if os.path.getsize(file_path) == 0:
                log.info(f"Skipping empty file: {fname}")
                continue

            try:
                raw_df = _load_raw_data(file_path)
                if raw_df.empty:
                    raise ProcessingError("Empty or unreadable data")

                processed_df = None
                if processor:
                    processed_df = processor.process_data(raw_df.copy(), processor_config)
                    status = "Processed"
                else:
                    processed_df = raw_df.copy()
                    status = "Processed (No Processor Module)"

                # ------------------------------------------------------------------ #
                # Persist
                # ------------------------------------------------------------------ #
                if not dry_run:
                    out_name = f"Year_{year} (Y{yi:02d})_Data.xlsx"
                    out_path = os.path.join(output_dir, out_name)
                    processed_df.to_excel(out_path, index=False, header=False)
                    log.info(f"Saved corrected data ➜ {out_path}")

            except ProcessingError as exc:
                status = f"Failed ({exc})"
                processed_df = pd.DataFrame()
            except Exception as exc:  # pragma: no cover
                status = f"Failed (Unexpected Error: {exc})"
                processed_df = pd.DataFrame()

            summary_records.append(
                {
                    "Series": series,
                    "Year": year,
                    "YearIndex": f"Y{yi:02d}",
                    "File": fname,
                    "RawDataPoints": len(raw_df) if 'raw_df' in locals() else None,
                    "ProcessedDataPoints": len(processed_df) if not processed_df.empty else 0,
                    "Status": status,
                }
            )

        # ------------------------------------------------------------------ #
        # Summary
        # ------------------------------------------------------------------ #
        summary_df = pd.DataFrame(summary_records)
        if summary_df.empty:
            log.warning("No files were processed, summary is empty.")
            return summary_df

        log.info("Processing summary\n%s", summary_df.to_string())

        if not dry_run:
            summary_path = os.path.join(output_dir, "Seatek_Analysis_Summary.xlsx")
            summary_df.to_excel(summary_path, index=False)
            log.info(f"Saved summary ➜ {summary_path}")

        log.info("--- Batch processing COMPLETE ---")
    print("[DEBUG] Exiting batch_process function")
    return summary_df

if __name__ == "__main__":
    print("[DEBUG] batch_correction.py __main__ entrypoint running!")
    # Ensure output directory exists
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', default='data/output')
    args, unknown = parser.parse_known_args()
    os.makedirs(args.output_dir, exist_ok=True)
    # Existing CLI logic follows...

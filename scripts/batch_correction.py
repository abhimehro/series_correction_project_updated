"""
Batch processing module for the Series Correction Project.

Loads sensor data files based on series, river miles, and years,
applies discontinuity detection and correction using the optional `processor`
module, and saves output data and summaries.

The implementation has been aligned with the requirements asserted in
scripts/tests/test_batch_correction.py.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import glob
import logging
import os
import re
from typing import Dict, Any, List, Tuple

import pandas as pd

# Import optional dependencies from the helper module if possible
try:
    from batch_correction import load_config_func, processor, data_loader
except ImportError:
    # We'll define these later in the file if the import fails
    load_config_func, processor, data_loader = None, None, None

# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #
log = logging.getLogger(__name__)
# Only configure logging if this is the first import and no handlers exist on root logger
if not logging.getLogger().handlers and not log.handlers:
    logging.basicConfig(level=logging.INFO)

log.debug("batch_correction module loaded")

# --------------------------------------------------------------------------- #
# Optional/project‑local dependencies
# --------------------------------------------------------------------------- #
ConfigLoaderType = None
load_config_func = None
processor = None
data_loader = None


def _optional_import(path: str, fallback_msg: str):
    """Import a module with graceful fallback if not found.
    
    Args:
        path: The module path to import
        fallback_msg: Message to log if module is not found
        
    Returns:
        The imported module or None if not found
    """
    try:
        return __import__(path, fromlist=[''])
    except ModuleNotFoundError:
        log.info(fallback_msg)
        return None
    except ImportError:
        log.exception("Import error while loading %s", path)
        return None
    except (SyntaxError, TypeError, ValueError) as err:
        log.exception("Syntax or type error in %s: %s", path, err)
        raise

# `loaders` (for config) and `processor` (for data correction) live inside the
# project. They may legitimately be absent in a test environment.  Import them
# lazily and fall back to stubs instead of raising – the tests monkey‑patch the
# attributes after import.
loaders = _optional_import("scripts.loaders", "`loaders` module not available – using dummy config loader.")
if loaders is not None:
    load_config_func = getattr(loaders, "load_config", None)

processor = _optional_import("scripts.processor", "`processor` module not available – raw data will be passed through.")
data_loader = _optional_import("scripts.data_loader", "`data_loader` module not available – using built-in loader.")

# --------------------------------------------------------------------------- #
# Exceptions
# --------------------------------------------------------------------------- #

class ProcessingError(Exception):
    """Custom exception for errors during batch processing."""
    pass

# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def _get_data_directory(config_data: Dict[str, Any], create_if_missing: bool = False) -> str:
    """
    Determine the raw‑data directory from configuration.
    
    Args:
        config_data: Configuration dictionary
        create_if_missing: If True, create the directory if it doesn't exist
        
    Returns:
        Path to the data directory
        
    Raises:
        FileNotFoundError: If directory doesn't exist and create_if_missing is False
    """
    data_dir_key = "RAW_DATA_DIR"
    data_dir = config_data.get(data_dir_key)

    # Use configured directory if it exists
    if data_dir and os.path.isdir(data_dir):
        log.info(f"Using data directory from config ({data_dir_key}): {data_dir}")
        return data_dir

    # Compute default directory - use package root if possible
    package_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_data_dir = os.path.join(package_root, "data")

    # Log warning about fallback
    if data_dir:
        log.warning(
            f"Configured path {data_dir} (from {data_dir_key}) is not a directory – defaulting to {default_data_dir}"
        )
    else:
        log.warning(
            f"Key {data_dir_key} not found in config – defaulting data directory to {default_data_dir}"
        )

    # Create directory if requested
    if create_if_missing and not os.path.isdir(default_data_dir):
        try:
            os.makedirs(default_data_dir, exist_ok=True)
            log.info(f"Created data directory: {default_data_dir}")
        except OSError as e:
            raise FileNotFoundError(
                f"Cannot create default data directory {default_data_dir!r}: {e}"
            )
    elif not os.path.isdir(default_data_dir):
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
        series_list: List[int],
        years: Tuple[int, int],
        data_dir: str,
        config_data: Dict[str, Any] = None,
) -> List[Tuple[int, int, int, str]]:
    """
    Discover S{series}_Y{index:02d}.txt files that correspond to the requested
    years.  The simplistic mapping assumes sequential Y01, Y02… for each year.

    Args:
        series_list: List of series IDs to process
        years: Tuple of (start_year, end_year) to include
        data_dir: Directory where data files are stored
        config_data: Optional configuration data

    Returns:
        List of tuples containing (series, year, index, filename)
    """
    log.info(f"Finding files for series {series_list} in years {years}")

    if not os.path.isdir(data_dir):
        log.error(f"Data directory does not exist: {data_dir}")
        return []

    files_to_process = []

    year_start, year_end = years
    years_to_process = range(year_start, year_end + 1)

    # Get year-to-index mapping from config if available
    year_index_map = {}
    if config_data and 'year_index_map' in config_data:
        year_index_map = config_data['year_index_map']

    for series in series_list:
        series_pattern = f"S{series}_Y*.txt"
        series_files = glob.glob(os.path.join(data_dir, series_pattern))

        for file_path in series_files:
            file_name = os.path.basename(file_path)

            # Extract Y-index from filename (e.g., Y01, Y02)
            match = re.search(r'Y(\d+)\.txt$', file_name)
            if not match:
                continue

            y_index = int(match.group(1))

            # Determine which year this index corresponds to
            if year_index_map:
                # Use mapping from config if available
                year = None
                for y, idx in year_index_map.items():
                    if idx == y_index and int(y) in years_to_process:
                        year = int(y)
                        break
            else:
                # Use simple sequential mapping: Y01 = first year, Y02 = second year, etc.
                if y_index <= len(years_to_process):
                    year = years_to_process[y_index - 1]
                else:
                    continue

            if year is not None and year_start <= year <= year_end:
                files_to_process.append((series, year, y_index, file_path))

    if not files_to_process:
        log.warning(f"No matching files found for series {series_list} and years {years}")
    else:
        log.info(f"Found {len(files_to_process)} files to process")

    return sorted(files_to_process)


def _load_raw_data(file_path):
    """
    Load a raw Seatek txt file.  Uses a very forgiving pandas.read_csv setup
    suitable for the varied test fixtures.
    """
    log.debug(f"Attempting to load file: {file_path}")
    try:
        df = pd.read_csv(
            file_path,
            header=None,
            sep=r"\s+",
            engine="python",
            comment="#",
            skip_blank_lines=True,
        )
        log.debug(f"Loaded file: {file_path} with shape {df.shape}")
        # Best-effort numeric conversion
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
        log.debug(f"File {file_path} empty.")
        return pd.DataFrame()
    except Exception as exc:
        log.error(f"Failed to load data from {file_path}: {exc}")
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
    """
    Process multiple sensor data files based on series, river miles, and years.
    
    Args:
        series_selection: Series IDs to process (int, list, or "all")
        river_miles: Optional list of river miles to filter series by
        years: Tuple of (start_year, end_year) to process
        dry_run: If True, don't write output files
        config_path: Path to configuration file
        output_dir: Directory for output files (defaults to data directory)
        
    Returns:
        DataFrame with summary of processed files
    """
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

    # Optional river-mile lookup CSV – silently ignored when missing
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
    log.debug(f"Files to process: {files_to_process}")

    if not files_to_process:
        log.warning("No matching files found! Entering fallback processing mode.")
        summary_records = []

        # Fallback: try to process any available files for the selected series
        if "series" in config_data and processor is not None:
            for series_id in series_to_process:
                str_series_id = str(series_id)
                if str_series_id in config_data.get("series", {}):
                    series_cfg = config_data["series"][str_series_id]
                    for i, file_path in enumerate(series_cfg.get("raw_data", []), start=1):
                        log.info(f"Fallback processing file: {file_path} (series {series_id})")
                        try:
                            df = _load_raw_data(file_path)
                            if not df.empty:
                                processor_config = {
                                    **config_data.get("defaults", {}),
                                    **config_data.get("processor_config", {})
                                }
                                processed_df = processor.process_data(df, processor_config)

                                if not dry_run:
                                    out_name = f"Series{series_id}_File{i:02d}_Processed.xlsx"
                                    out_path = os.path.join(output_dir, out_name)
                                    processed_df.to_excel(out_path, index=False)
                                    log.info(f"Wrote output: {out_path}")

                                summary_records.append({
                                    "Series": series_id,
                                    "Year": None,
                                    "Y-Index": i,
                                    "Filename": os.path.basename(file_path),
                                    "Status": "Fallback Processed",
                                    "Records": len(processed_df)
                                })
                        except Exception as e:
                            log.error(f"Failed to process {file_path}: {e}")
                            summary_records.append({
                                "Series": series_id,
                                "Year": None,
                                "Y-Index": i,
                                "Filename": os.path.basename(file_path),
                                "Status": f"Failed ({e})",
                                "Records": 0
                            })

            if summary_records:
                return pd.DataFrame(summary_records)

        log.warning("Fallback processing found no viable files. Returning empty DataFrame.")
        return pd.DataFrame()

    processor_config = {**config_data.get("defaults", {}), **config_data.get("processor_config", {})}
    summary_records = []

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    for series, year, yi, file_path in files_to_process:
        log.debug(f"Processing series: {series}, year: {year}, file: {file_path}")
        fname = os.path.basename(file_path)
        log.info(f"Processing {fname} (Series {series}, Year {year}, Y{yi:02d})")

        # Skip zero-byte files early
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
                log.info(f"Saved corrected data to {out_path}")

        except ProcessingError as exc:
            status = f"Failed ({exc})"
            processed_df = pd.DataFrame()
        except Exception as exc:  # pragma: no cover
            status = f"Failed (Unexpected Error: {exc})"
            processed_df = pd.DataFrame()

        summary_records.append({
            "Series": series,
            "Year": year,
            "Y-Index": yi,
            "Filename": fname,
            "Status": status,
            "Records": len(processed_df) if not processed_df.empty else 0
        })

    # Create a summary DataFrame and return it
    summary_df = pd.DataFrame(summary_records)
    log.info(f"--- Batch processing COMPLETE --- Processed {len(summary_records)} files")
    return summary_df
# ----------------------------------------------------------------------

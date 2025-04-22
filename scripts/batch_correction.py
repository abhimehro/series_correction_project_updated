"""
Batch processing module for the Series Correction Project.

Loads sensor data files based on series, river miles, and years,
applies discontinuity detection and correction using the 'processor' module,
and saves output data and summaries.
"""

import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

# Define a type alias for the config loading function
ConfigLoaderType = Callable[..., Dict[str, Any]]

# Import project modules
# Use try-except to handle potential missing modules gracefully, though
# processor is now expected based on the audit. Loaders is also used.
try:
    from . import loaders  # Assuming loaders.py is in the same directory
    from . import processor  # Import the new processor module

    load_config_func: Optional[ConfigLoaderType] = loaders.load_config
    log = logging.getLogger(__name__)  # Use standard logging setup
    log.info("Successfully imported loaders and processor modules.")

except ImportError as e:
    # Log an error if essential modules are missing
    logging.basicConfig(level=logging.ERROR)  # Ensure logging is configured
    log = logging.getLogger(__name__)
    log.error(
        "Failed to import required modules (loaders, processor): %s", e, exc_info=True
    )
    # Define fallbacks or raise an error to prevent execution without core components
    load_config_func = None
    processor = None  # type: ignore
    raise ImportError(
        "Core modules 'loaders' or 'processor' could not be imported. Cannot proceed."
    ) from e

# Optional data_loader module (may be absent). Provide a stub so tests can patch it.
try:
    from . import data_loader  # type: ignore
except ImportError:  # pragma: no cover
    data_loader = None  # type: ignore


class ProcessingError(Exception):
    """Custom exception for errors during batch processing."""


def _get_data_directory(config_data: Dict[str, Any]) -> str:
    """Determines the data directory path from config or defaults."""
    data_dir_key = "RAW_DATA_DIR"
    data_dir = config_data.get(data_dir_key)

    if data_dir and os.path.isdir(data_dir):
        log.info("Using data directory from config ('%s'): %s", data_dir_key, data_dir)
        return data_dir
    else:
        default_data_dir = os.path.join(os.getcwd(), "data")
        if data_dir:
            log.warning(
                "Path '%s' from config key '%s' is not a valid directory. Using default: %s",
                data_dir,
                data_dir_key,
                default_data_dir,
            )
        else:
            log.warning(
                "'%s' not found in config. Using default data directory: %s",
                data_dir_key,
                default_data_dir,
            )
        if not os.path.isdir(default_data_dir):
            raise FileNotFoundError(
                f"Default data directory not found and not created: {default_data_dir}"
            )
        return default_data_dir


def _determine_series_to_process(
    series_selection: Any,
    river_miles: Optional[List[float]],
    config_data: Dict[str, Any],
    data_dir: str,
) -> List[int]:
    """Determines the list of series IDs to process based on selection criteria."""
    series_list: List[int] = []
    rm_map_key = "SENSOR_TO_RIVER"
    sensor_to_rm_map = config_data.get(rm_map_key, {})
    rm_to_sensors_map: Dict[str, List[int]] = {}
    if sensor_to_rm_map:
        for sensor_str, rm_val in sensor_to_rm_map.items():
            rm_str = str(float(rm_val))
            rm_to_sensors_map.setdefault(rm_str, [])
            try:
                rm_to_sensors_map[rm_str].append(int(sensor_str))
            except ValueError:
                log.warning(
                    "Could not parse sensor ID '%s' in %s map.", sensor_str, rm_map_key
                )

    if isinstance(series_selection, str) and series_selection.lower() == "all":
        if rm_to_sensors_map and river_miles:
            log.info("Selecting series based on provided river miles using config map.")
            selected = set()
            for rm in river_miles:
                rm_str = str(float(rm))
                selected.update(rm_to_sensors_map.get(rm_str, []))
            series_list = sorted(selected)
            log.info(
                "Selected series from river miles %s: %s", river_miles, series_list
            )
        elif sensor_to_rm_map:
            log.info("Selecting all series found in the %s map.", rm_map_key)
            all_sensors = []
            for sensor_str in sensor_to_rm_map.keys():
                try:
                    all_sensors.append(int(sensor_str))
                except ValueError:
                    continue
            series_list = sorted(set(all_sensors))
            log.info("Selected all series from map: %s", series_list)
        else:
            log.warning(
                "Config map '%s' not found or empty. Scanning data dir '%s'.",
                rm_map_key,
                data_dir,
            )
            found = set()
            for fname in os.listdir(data_dir):
                if fname.startswith("S") and "_Y" in fname and fname.endswith(".txt"):
                    try:
                        sid = int(fname.split("_")[0][1:])
                        found.add(sid)
                    except Exception:
                        continue
            series_list = sorted(found)
            log.info("Found series by scanning directory: %s", series_list)
            if river_miles:
                log.warning(
                    "River miles provided, but no map to filter. Cannot filter by RM."
                )
    else:
        raw = (
            [series_selection]
            if not isinstance(series_selection, (list, tuple))
            else list(series_selection)
        )
        try:
            series_list = [int(s) for s in raw]
            log.info("Processing specified series: %s", series_list)
        except ValueError as e:
            raise ValueError(f"Invalid series selection: {raw}") from e
        if rm_to_sensors_map and river_miles:
            filt = set()
            orig = list(series_list)
            for rm in river_miles:
                rm_str = str(float(rm))
                filt.update(rm_to_sensors_map.get(rm_str, []))
            if not filt.intersection(series_list):
                log.warning(
                    "No series in the specified list match the provided river miles. "
                    "Returning an empty list."
                )
            series_list = sorted(set(series_list) & filt)
            log.info(
                "Filtered specified series by river miles %s -> %s",
                river_miles,
                series_list,
            )
        elif river_miles:
            log.warning(
                "River miles provided, but no %s map. Cannot filter.", rm_map_key
            )
    if not series_list:
        log.warning("No series selected for processing based on criteria.")
    return series_list


def _find_files_to_process(
    series_list: List[int],
    years: Tuple[int, int],
    data_dir: str,
    config_data: Dict[str, Any],
) -> List[Tuple[int, int, int, str]]:
    """Finds existing data files matching the series and year range."""
    files = []
    start, end = years
    year_map: Dict[int, Dict[int, int]] = {}
    idx_map: Dict[int, int] = {}
    for s in series_list:
        idx_map[s] = 1
        year_map[s] = {}
        for y in range(start, end + 1):
            year_map[s][y] = idx_map[s]
            idx_map[s] += 1
    for s in series_list:
        for y in range(start, end + 1):
            yi = year_map[s].get(y)
            if yi is None:
                continue
            fn = f"S{s}_Y{yi:02d}.txt"
            path = os.path.join(data_dir, fn)
            if os.path.isfile(path):
                files.append((s, y, yi, path))
                log.debug("Found matching file: %s", path)
            else:
                log.warning("Expected file not found: %s", path)
    if not files:
        log.warning("No data files found matching pattern in '%s'.", data_dir)
    files.sort(key=lambda x: (x[0], x[1]))
    log.info("Found %d potential files to process.", len(files))
    return files


def _load_raw_data(file_path: str) -> pd.DataFrame:
    """Loads raw data from a file path, trying different methods."""
    fname = os.path.basename(file_path)
    try:
        raw_df = pd.read_csv(
            file_path,
            header=None,
            sep=r"\s+",
            engine="python",
            dtype=str,
            skipinitialspace=True,
            comment="#",
            skip_blank_lines=True,
        )
        log.info("Loaded data using pandas read_csv for %s.", fname)
        for col in raw_df.columns:
            try:
                raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce")
            except ValueError:
                log.debug(
                    "Column %s in %s could not be converted to numeric.", col, fname
                )
        raw_df = raw_df.infer_objects()
        if raw_df.empty:
            log.warning("Loaded DataFrame is empty for file: %s", fname)
            return pd.DataFrame()
        if all(isinstance(c, int) for c in raw_df.columns):
            cols = [f"Value{i+1}" for i in range(len(raw_df.columns))]
            if cols:
                cols[0] = "Time (Seconds)"
            raw_df.columns = cols
            log.debug("Assigned default column names: %s", cols)
        return raw_df
    except pd.errors.EmptyDataError:
        log.warning("File %s is empty or comments only.", fname)
        return pd.DataFrame()
    except Exception as e:
        log.error("Failed to load data from %s: %s", fname, e, exc_info=True)
        raise ProcessingError(f"Failed to load data from {fname}") from e


def batch_process(
    series_selection: Any,
    river_miles: Optional[List[float]],
    years: Tuple[int, int],
    dry_run: bool = False,
    config_path: str = "scripts/config.json",
    output_dir: Optional[str] = None,
) -> pd.DataFrame:
    """Process sensor data files for the given series and year range."""
    log.info("--- Starting Batch Processing ---")
    log.info(
        "Series: %s, River Miles: %s, Years: %s, Dry Run: %s",
        series_selection,
        river_miles,
        years,
        dry_run,
    )

    config_data: Dict[str, Any] = {}
    if load_config_func:
        try:
            config_data = load_config_func(config_path)
            log.info("Configuration loaded successfully from %s.", config_path)
            rm_map_path = config_data.get(
                "RIVER_MILE_MAP_PATH", "scripts/river_mile_map.csv"
            )
            if os.path.isfile(rm_map_path):
                rm_data = pd.read_csv(rm_map_path)
                config_data["SENSOR_TO_RIVER"] = rm_data.set_index("SENSOR_ID")[
                    "RIVER_MILE"
                ].to_dict()
                config_data["RIVER_TO_SENSORS"] = (
                    rm_data.groupby("RIVER_MILE")["SENSOR_ID"].apply(list).to_dict()
                )
                log.info("Loaded river mile map from %s", rm_map_path)
            else:
                log.warning("River mile map file not found at %s", rm_map_path)
        except FileNotFoundError:
            log.error("Configuration file not found at %s.", config_path)
            raise
        except Exception as e:
            log.error("Failed to load configuration: %s", e, exc_info=True)
            raise ProcessingError(f"Failed to load configuration: {e}") from e
    else:
        log.error("Config loader function not available. Cannot proceed.")
        raise ImportError("load_config function is missing.")

    data_dir = _get_data_directory(config_data)
    if output_dir is None:
        output_dir = os.path.join(data_dir, "output")
        log.info("Output directory not specified, using default: %s", output_dir)
    else:
        log.info("Using specified output directory: %s", output_dir)
    if not dry_run and not os.path.isdir(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            log.info("Created output directory: %s", output_dir)
        except OSError as e:
            log.error("Failed to create output directory %s: %s", output_dir, e)
            raise ProcessingError(f"Could not create output directory: {e}") from e

    series_to_process = _determine_series_to_process(
        series_selection, river_miles, config_data, data_dir
    )
    if not series_to_process:
        log.warning("No series identified for processing. Exiting.")
        return pd.DataFrame()

    files_to_process = _find_files_to_process(
        series_to_process, years, data_dir, config_data
    )
    if not files_to_process:
        log.warning("No data files found to process. Exiting.")
        return pd.DataFrame()

    summary_records = []
    if dry_run:
        log.info("*** Dry Run Mode Enabled: No output files will be written. ***")

    processor_config = config_data.get("processor_config", {})
    processor_config.update(config_data.get("defaults", {}))

    for series, year, yi, file_path in files_to_process:
        fname = os.path.basename(file_path)
        log.info(
            "--- Processing File: %s (Series: %d, Year: %d, Index: Y%02d) ---",
            fname,
            series,
            year,
            yi,
        )
        file_status = "Processed"
        raw_data_points = None
        processed_data_points = None
        raw_df = pd.DataFrame()
        processed_df = pd.DataFrame()
        try:
            log.debug("Loading raw data from: %s", file_path)
            raw_df = _load_raw_data(file_path)
            if raw_df.empty:
                log.warning("Skipping processing for empty file: %s", fname)
                summary_records.append(
                    {
                        "Series": series,
                        "Year": year,
                        "YearIndex": f"Y{yi:02d}",
                        "File": fname,
                        "RawDataPoints": 0,
                        "ProcessedDataPoints": 0,
                        "Status": "Skipped (Empty/Load Error)",
                    }
                )
                continue
            raw_data_points = len(raw_df)
            log.info("Successfully loaded %d data points.", raw_data_points)
            if processor:
                processed_df = processor.process_data(
                    raw_df.copy(), config=processor_config
                )
                processed_data_points = len(processed_df)
                log.info(
                    "Processing complete. Resulting data points: %d",
                    processed_data_points,
                )
            else:
                log.warning("Processor module not available. Using raw data.")
                processed_df = raw_df.copy()
                processed_data_points = raw_data_points
                file_status = "Processed (No Processor Module)"
            if not dry_run:
                base_name = f"S{series}_Y{yi:02d}_{year}_CorrectedData.csv"
                out_path = os.path.join(output_dir, base_name)
                try:
                    processed_df.to_csv(out_path, index=False)
                    log.info("Corrected data saved to: %s", out_path)
                except Exception as e:
                    log.error(
                        "Failed to save output for %s: %s", fname, e, exc_info=True
                    )
                    file_status = "Processed (Save Failed)"
        except ProcessingError as e:
            log.error("Processing error for file %s: %s", fname, e, exc_info=True)
            file_status = f"Failed ({e})"
        except Exception as e:
            log.error(
                "Unexpected error during processing for %s: %s", fname, e, exc_info=True
            )
            file_status = "Failed (Unexpected Error)"
        summary_records.append(
            {
                "Series": series,
                "Year": year,
                "YearIndex": f"Y{yi:02d}",
                "File": fname,
                "RawDataPoints": raw_data_points,
                "ProcessedDataPoints": processed_data_points,
                "Status": file_status,
            }
        )
        log.info("--- Finished Processing File: %s ---", fname)

    log.info("--- Batch Processing Complete ---")
    summary_df = pd.DataFrame(summary_records)
    if not summary_df.empty:
        log.info("Processing Summary:\n%s", summary_df.to_string())
        if not dry_run:
            summary_path = os.path.join(output_dir, "Batch_Processing_Summary.csv")
            try:
                summary_df.to_csv(summary_path, index=False)
                log.info("Summary report saved to: %s", summary_path)
            except Exception as e:
                log.error("Failed to save summary report: %s", e, exc_info=True)
    else:
        log.warning("No files were processed, summary is empty.")

    return summary_df

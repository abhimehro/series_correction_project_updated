"""Batch processing module for the Series Correction Project.

It provides the `batch_process` function to load sensor data files,
detect and correct discontinuities, and save output data and summaries.
"""

import os
import logging
from typing import Optional, Callable, Any, Dict, List, Tuple
import pandas as pd

# Define a type alias for the config loading function
ConfigLoaderType = Callable[..., Dict[str, Any]]

# Import project modules if available
try:  # type: ignore
    import data_loader  # type: ignore
    import processor  # type: ignore
    from .loaders import load_config as load_config_func_real

except ImportError:
    logging.warning(
        "Optional modules 'data_loader', 'processor', or 'loaders' not found. "
        "Using basic fallbacks."
    )
    # Define fallbacks if modules are not found
    data_loader = None
    processor = None

load_config_func: Optional[ConfigLoaderType] = (
    load_config_func_real if "load_config_func_real" in locals() else None
)  # noqa: N806


class ProcessingError(Exception):
    """Custom exception for errors during batch processing."""


def batch_process(
    series_selection: Any,
    river_miles: Optional[List[float]],
    years: Tuple[int, int],
    dry_run: bool = False,
) -> pd.DataFrame:
    """
    Process sensor data files for the given series and year range, applying
    discontinuity detection and correction. Optionally runs in dry-run mode
    (no output files saved).

    Parameters:
        series_selection: Series identifier(s) to process. Can be 'all' for all
                          available series or a specific series number/list.
        river_miles: List or tuple of river mile values corresponding to the
                     series (used to filter series if applicable).
        years: Tuple or list (start_year, end_year) inclusive range of years.
        dry_run: If True, perform a dry run without saving output files.

    Returns:
        A pandas DataFrame containing the summary of the processing.

    Raises:
        FileNotFoundError: If no valid data files are found for the criteria.
        ValueError: If series selection is invalid or config is missing required info.
        ProcessingError: For general processing issues.
    """
    start_year, end_year = years
    config_data: Dict[str, Any] = {}
    series_list: List[int] = []

    # Load configuration if loader function is available
    if load_config_func is not None:
        try:
            # Assuming default config path is handled within the function
            config_data = load_config_func()
            logging.info("Configuration loaded successfully.")
        except Exception as e:
            logging.warning("Failed to load configuration: %s", e)
            # Continue without config, relying on fallbacks

    # Determine data directory
    # Use config if available, otherwise default to ./data relative to cwd
    data_dir = config_data.get("RAW_DATA_DIR", None)
    if not data_dir or not os.path.isdir(data_dir):
        default_data_dir = os.path.join(os.getcwd(), "data")
        if not data_dir:
            logging.warning(
                "RAW_DATA_DIR not found in config. Using default: %s",
                default_data_dir,
            )
        else:
            logging.warning(
                "RAW_DATA_DIR '%s' from config is not a valid directory. "
                "Using default: %s",
                data_dir,
                default_data_dir,
            )
        data_dir = default_data_dir

    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    # Determine series list to process
    rm_to_series_map = config_data.get("RIVER_MILE_TO_SERIES", {})

    if isinstance(series_selection, str) and series_selection.lower() == "all":
        if rm_to_series_map and river_miles:
            # Use mapping to get series from provided river miles
            logging.info(
                "Selecting series based on provided river miles using config map."
            )
            for rm in river_miles:
                # Config keys might be strings, ensure comparison works
                series = rm_to_series_map.get(str(rm)) or rm_to_series_map.get(rm)
                if series:
                    series_list.append(int(series))
            series_list = sorted(list(set(series_list)))
            logging.info("Selected series from river miles: %s", series_list)
        elif rm_to_series_map:
            # Use all series from the map if 'all' and no river miles specified
            logging.info("Selecting all series from RIVER_MILE_TO_SERIES map.")
            series_list = sorted([int(s) for s in set(rm_to_series_map.values())])
            logging.info("Selected series from map: %s", series_list)
        else:
            # Fallback: gather all series IDs from available files in data_dir
            logging.warning(
                "Config 'RIVER_MILE_TO_SERIES' not found or empty. "
                "Scanning data directory for series files."
            )
            found_series = set()
            try:
                for fname in os.listdir(data_dir):
                    if fname.startswith("S") and fname.endswith(".txt"):
                        parts = fname.split("_")
                        if len(parts) > 0 and parts[0].startswith("S"):
                            sid = parts[0][1:]
                            if sid.isdigit():
                                found_series.add(int(sid))
            except FileNotFoundError as exc:
                # Already checked data_dir existence, but handle potential race condition
                raise FileNotFoundError(
                    f"Data directory disappeared: {data_dir}"
                ) from exc

            series_list = sorted(list(found_series))
            logging.info("Found series by scanning directory: %s", series_list)
            if river_miles:
                logging.warning(
                    "River miles provided, but no map in config. "
                    "Cannot filter by river mile when scanning."
                )
    else:
        # Process specific series (int, str, or list/tuple)
        if isinstance(series_selection, (list, tuple)):
            try:
                series_list = [int(s) for s in series_selection]
            except ValueError as e:
                raise ValueError(
                    f"Invalid series value in list: {series_selection}"
                ) from e
        else:
            try:
                series_list = [int(series_selection)]
            except ValueError as e:
                raise ValueError(
                    f"Invalid series selection: {series_selection}. "
                    "Must be 'all', an integer, or list/tuple of integers."
                ) from e
        logging.info("Processing specified series: %s", series_list)

        # Filter specified series by river_miles if mapping is available
        if rm_to_series_map and river_miles:
            original_series_list = list(series_list)  # Keep a copy for logging
            series_list = [
                s
                for s in series_list
                if any(
                    str(rm_to_series_map.get(str(rm))) == str(s)
                    or str(rm_to_series_map.get(rm)) == str(s)
                    for rm in river_miles
                )
            ]
            logging.info(
                "Filtered series %s by river miles %s -> %s",
                original_series_list,
                river_miles,
                series_list,
            )
        elif river_miles:
            logging.warning(
                "River miles provided, but no RIVER_MILE_TO_SERIES map found. "
                "Cannot filter specified series list by river mile."
            )

    # Gather all files to process
    files_to_process = []
    year_map: Dict[int, Dict[int, int]] = {}  # Map year to year_index (Y01, Y02…)
    current_year_index: Dict[int, int] = {}  # Track index per series

    # Pre-calculate year indices
    for series in series_list:
        current_year_index[series] = 1
        year_map[series] = {}
        for year in range(start_year, end_year + 1):
            year_map[series][year] = current_year_index[series]
            current_year_index[series] += 1

    # Find the actual files
    for series in series_list:
        for year in range(start_year, end_year + 1):
            year_index = year_map[series].get(year)
            if year_index is None:
                continue

            filename = f"S{series}_Y{year_index:02d}.txt"
            file_path = os.path.join(data_dir, filename)

            if os.path.isfile(file_path):
                # Store series, year, year_index, and path
                files_to_process.append((series, year, year_index, file_path))
            else:
                logging.warning("Expected file not found: %s", file_path)

    if not files_to_process:
        raise FileNotFoundError(
            "No valid data files found for processing based on the "
            "specified series and years."
        )

    logging.info("Found %d valid files to process", len(files_to_process))

    # Sort files by series and year for orderly processing
    files_to_process.sort(key=lambda x: (x[0], x[1]))

    # Summary records for final report
    summary_records = []

    if dry_run:
        logging.info("Dry-run mode: no output files will be written")

    # Process each file
    for series, year, year_index, file_path in files_to_process:
        fname = os.path.basename(file_path)
        try:
            size = os.path.getsize(file_path)
        except OSError as e:
            logging.error("Could not get size of file %s: %s. Skipping.", fname, e)
            continue

        if size == 0:
            logging.info("Skipping empty file: %s (0 bytes)", fname)
            continue

        logging.info("Processing file: %s (%d bytes)", fname, size)

        # Load raw data
        raw_data: Any = None  # Use Any type hint for flexibility
        try:
            if data_loader and hasattr(data_loader, "load_data"):
                raw_data = data_loader.load_data(file_path)
                logging.info("Loaded data using data_loader module for %s.", fname)
            else:
                # Basic loading: try pandas first, then read lines
                try:
                    # Attempt to read as CSV/TSV, adjust separator if needed
                    raw_data = pd.read_csv(
                        file_path, header=None, sep=r"\s+", engine="python"
                    )
                    logging.info(
                        "Loaded data using basic pandas read_csv for %s.", fname
                    )
                except Exception as pd_err:
                    logging.warning(
                        "Pandas load failed for %s: %s. Reading lines.",
                        fname, pd_err
                    )
                    # Specify encoding for robustness
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        raw_data = f.readlines()  # List of strings
        except Exception as e:
            logging.error("Failed to load data from %s: %s", fname, e)
            summary_records.append(
                {
                    "Series": series,
                    "Year": year,
                    "YearIndex": f"Y{year_index:02d}",
                    "File": fname,
                    "DataPoints": None,
                    "Status": "Load Failed",
                }
            )
            continue  # Skip to next file if loading fails

        # Apply discontinuity correction
        corrected_data: Any = None
        try:
            if processor and hasattr(processor, "process_data"):
                corrected_data = processor.process_data(raw_data)
                logging.info("Processed data using processor module for %s.", fname)
            else:
                # If no processor module, use raw data as corrected data
                corrected_data = raw_data
                logging.info(
                    "No processor module found/used for %s. "
                    "Corrected data is same as raw.",
                    fname,
                )
        except Exception as e:
            logging.error("Failed to process data for %s: %s", fname, e)
            corrected_data = raw_data  # Fallback to raw data
            status = "Process Failed"
        else:
            status = "Processed"

        # Record summary info
        num_points = None
        try:
            # Check common types that have a length
            if hasattr(corrected_data, "__len__"):
                num_points = len(corrected_data)
        except TypeError:
            logging.warning(
                "Could not determine data points for %s (type %s).",
                fname,
                type(corrected_data).__name__,
            )

        summary_records.append(
            {
                "Series": series,
                "Year": year,
                "YearIndex": f"Y{year_index:02d}",
                "File": fname,
                "DataPoints": num_points,
                "Status": status,
            }
        )

        if dry_run:
            continue  # Skip saving files

        # Define output filenames using year_index
        raw_out_name = f"Raw_Data_Year_{year} (Y{year_index:02d}).xlsx"
        data_out_name = f"Year_{year} (Y{year_index:02d})_Data.xlsx"
        # Output to the same directory as input data for now
        raw_out_path = os.path.join(data_dir, raw_out_name)
        data_out_path = os.path.join(data_dir, data_out_name)

        # Save raw data to Excel
        try:
            raw_df = (
                raw_data
                if isinstance(raw_data, pd.DataFrame)
                else pd.DataFrame(raw_data)
            )
            raw_df.to_excel(raw_out_path, index=False, header=False)
            logging.info(
                "Raw data for year %d (Y%02d) exported to: %s",
                year,
                year_index,
                raw_out_path,
            )
        except Exception as e:
            logging.error("Failed to save raw data to %s: %s", raw_out_path, e)

        # Save corrected data to Excel
        try:
            corr_df = (
                corrected_data
                if isinstance(corrected_data, pd.DataFrame)
                else pd.DataFrame(corrected_data)
            )
            corr_df.to_excel(data_out_path, index=False, header=False)
            logging.info(
                "Corrected data for year %d (Y%02d) saved to: %s",
                year,
                year_index,
                data_out_path,
            )
        except Exception as e:
            logging.error("Failed to save corrected data to %s: %s", data_out_path, e)

    # After processing all files, save summary report (if not a dry run)
    summary_df = pd.DataFrame(summary_records)
    if not dry_run:
        if not summary_df.empty:
            summary_path = os.path.join(data_dir, "Seatek_Analysis_Summary.xlsx")
            try:
                summary_df.to_excel(summary_path, index=False)
                logging.info("Summary data saved to: %s", summary_path)
            except Exception as e:
                logging.error("Failed to save summary data to %s: %s", summary_path, e)
    return summary_df

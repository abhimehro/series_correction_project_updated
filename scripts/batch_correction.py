"""
Batch processing module for the Series Correction Project.

Loads sensor data files based on series, river miles, and years,
applies discontinuity detection and correction using the optional `processor`
module, and saves output data and summaries.

The implementation has been aligned with the requirements asserted in
scripts/tests/test_batch_correction.py.
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import logging
import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pandas as pd

from scripts import spreadsheet_safety

# ⚡ Bolt: Compile regex once for performance
_FILE_NAME_REGEX = re.compile(r"S(.+?)_Y(\d+)\.txt$")
_DEFAULT_BASE_YEAR = 1995
SUMMARY_FILENAME = "Batch_Processing_Summary.csv"

# Import optional dependencies from the helper module if possible
try:
    from batch_correction import load_config_func, processor
except ImportError:
    # We'll define these later in the file if the import fails
    load_config_func, processor = None, None

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


def _optional_import(path: str, fallback_msg: str):
    """Import a module with graceful fallback if not found.

    Args:
        path: The module path to import
        fallback_msg: Message to log if module is not found

    Returns:
        The imported module or None if not found
    """
    try:
        return __import__(path, fromlist=[""])
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
loaders = _optional_import(
    "scripts.loaders", "`loaders` module not available – using dummy config loader."
)
if loaders is not None:
    load_config_func = getattr(loaders, "load_config", None)

processor = _optional_import(
    "scripts.processor",
    "`processor` module not available – raw data will be passed through.",
)
data_loader = _optional_import(
    "scripts.data_loader", "`data_loader` module not available – using built-in loader."
)

# --------------------------------------------------------------------------- #
# Exceptions
# --------------------------------------------------------------------------- #


class ProcessingError(Exception):
    """Custom exception for errors during batch processing."""


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def _get_data_directory(
    config_data: dict[str, Any], create_if_missing: bool = False
) -> str:
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

    if data_dir and os.path.isdir(data_dir):
        log.info(f"Using data directory from config ({data_dir_key}): {data_dir}")
        return data_dir

    package_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_data_dir = os.path.join(package_root, "data")

    if data_dir:
        log.warning(
            f"Configured path {data_dir} (from {data_dir_key}) is not a directory – defaulting to {default_data_dir}"
        )
    else:
        log.warning(
            f"Key {data_dir_key} not found in config – defaulting data directory to {default_data_dir}"
        )

    if os.path.isdir(default_data_dir):
        return default_data_dir

    if not create_if_missing:
        raise FileNotFoundError("Default data directory not found")

    try:
        os.makedirs(default_data_dir, exist_ok=True)
        log.info(f"Created data directory: {default_data_dir}")
        return default_data_dir
    except OSError:
        log.exception("Cannot create default data directory %r", default_data_dir)
        raise FileNotFoundError("Cannot create default data directory") from None


def _build_rm_to_sensors_map(sensor_to_rm_map: dict) -> dict:
    rm_to_sensors_map = {}
    for sensor_str, rm_val in sensor_to_rm_map.items():
        try:
            rm_str = str(float(rm_val))
            rm_to_sensors_map.setdefault(rm_str, []).append(int(sensor_str))
        except ValueError:
            log.warning(f"Invalid sensor id in SENSOR_TO_RIVER map: {sensor_str}")
    return rm_to_sensors_map


def _get_series_from_all(
    river_miles: list[float] | None,
    rm_to_sensors_map: dict,
    sensor_to_rm_map: dict,
    data_dir: str,
) -> list[int]:
    if river_miles and rm_to_sensors_map:
        selected = set()
        for rm in river_miles:
            selected.update(rm_to_sensors_map.get(str(float(rm)), []))
        series_list = sorted(selected)
        log.info(f"Series selected from river miles {river_miles} ➜ {series_list}")
    elif sensor_to_rm_map:
        series_list = sorted(int(s) for s in sensor_to_rm_map.keys())
        log.info(f"Selecting every series in SENSOR_TO_RIVER map: {series_list}")
    else:
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
    return series_list


def _get_explicit_series(
    series_selection: int | list[int] | str,
    river_miles: list[float] | None,
    rm_to_sensors_map: dict,
) -> list[int]:
    raw = (
        [series_selection]
        if not isinstance(series_selection, (list, tuple))
        else series_selection
    )
    try:
        series_list = [int(s) for s in raw]
    except ValueError:
        log.exception("Invalid series selection %r", raw)
        raise ValueError("Invalid series selection") from None

    if river_miles and rm_to_sensors_map:
        allowed = set()
        for rm in river_miles:
            allowed.update(rm_to_sensors_map.get(str(float(rm)), []))
        series_list = sorted(set(series_list) & allowed)
        log.info(f"After RM filter ({river_miles}) series ➜ {series_list}")
    return series_list


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
    rm_to_sensors_map = _build_rm_to_sensors_map(sensor_to_rm_map)

    if isinstance(series_selection, str) and series_selection.lower() == "all":
        series_list = _get_series_from_all(
            river_miles, rm_to_sensors_map, sensor_to_rm_map, data_dir
        )
    else:
        series_list = _get_explicit_series(
            series_selection, river_miles, rm_to_sensors_map
        )

    if not series_list:
        log.warning("No series selected for processing.")
    return series_list


def _determine_year_for_index(
    y_index: int, reverse_year_index_map: dict, base_year: int
) -> int | None:
    """Helper function to map a Y-index back to a specific year."""
    if reverse_year_index_map:
        return reverse_year_index_map.get(y_index)

    if y_index < 1:
        return None
    return base_year + y_index - 1


def _resolve_base_year(config_data: dict[str, Any] | None, year_index_map: dict) -> int:
    """Resolve the configured base year or use the default."""
    if year_index_map:
        return _DEFAULT_BASE_YEAR
    if config_data and "base_year" in config_data:
        return int(config_data["base_year"])

    log.warning(
        "No 'year_index_map' or 'base_year' in config; assuming Y01 = %d",
        _DEFAULT_BASE_YEAR,
    )
    return _DEFAULT_BASE_YEAR


def _parse_and_validate_file(
    file_name: str,
    series_map: dict,
    reverse_year_index_map: dict,
    base_year: int,
    year_start: int,
    year_end: int,
    data_dir: str,
) -> tuple[int, int, int, str] | None:
    """Parses a filename and returns structured info if valid."""
    if not (file_name.startswith("S") and file_name.endswith(".txt")):
        return None

    match = _FILE_NAME_REGEX.search(file_name)
    if not match:
        return None

    series_str = match.group(1)
    if series_str not in series_map:
        return None

    y_index = int(match.group(2))
    year = _determine_year_for_index(y_index, reverse_year_index_map, base_year)

    if year is not None and year_start <= year <= year_end:
        original_series = series_map[series_str]
        file_path = os.path.join(data_dir, file_name)
        return (original_series, year, y_index, file_path)
    return None


def _find_files_to_process(
    series_list: list[int],
    years: tuple[int, int],
    data_dir: str,
    config_data: dict[str, Any] | None = None,
) -> list[tuple[int, int, int, str]]:
    """
    Discover S{series}_Y{index:02d}.txt files that correspond to the requested
    years. The Y-index is anchored to the base year (Y01 = base_year, default
    1995) or mapped through year_index_map.

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

    year_start, year_end = years
    year_index_map = config_data.get("year_index_map", {}) if config_data else {}
    base_year = _resolve_base_year(config_data, year_index_map)

    # Pre-compute reverse map for O(1) lookups
    reverse_year_index_map = {idx: int(y) for y, idx in year_index_map.items()}

    # ⚡ Bolt: Optimize file discovery by using a single os.listdir call
    # instead of globbing in a loop for each series, which requires repeated directory scans.
    series_map = {str(s): s for s in series_list}
    all_files = os.listdir(data_dir)
    files_by_series = {s: [] for s in series_list}

    for file_name in all_files:
        result = _parse_and_validate_file(
            file_name,
            series_map,
            reverse_year_index_map,
            base_year,
            year_start,
            year_end,
            data_dir,
        )
        if result:
            original_series = result[0]
            files_by_series[original_series].append(result)

    # Flatten and preserve the original ordering grouping by series
    files_to_process = []
    for series in series_list:
        files_to_process.extend(files_by_series[series])

    if not files_to_process:
        log.warning(
            f"No matching files found for series {series_list} and years {years}"
        )
    else:
        log.info(f"Found {len(files_to_process)} files to process")

    return sorted(files_to_process)


def _safe_numeric(series):
    # ⚡ Bolt: Avoid pd.to_numeric overhead if series is already numeric
    if pd.api.types.is_numeric_dtype(series):
        return series
    try:
        return pd.to_numeric(series)
    except (ValueError, TypeError):
        return series


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
            comment="#",
            skip_blank_lines=True,
        )
        log.debug(f"Loaded file: {file_path} with shape {df.shape}")

        # Best-effort numeric conversion (pandas 2+ removed errors="ignore"; try/except preserves columns)
        # ⚡ Bolt: Use a dictionary comprehension to reconstruct the DataFrame directly
        # instead of iterative column assignment, which is significantly faster.
        df = pd.DataFrame({col: _safe_numeric(df[col]) for col in df.columns})

        # Nice column names: first col is time, rest ValueX
        if pd.api.types.is_integer_dtype(df.columns):
            n = len(df.columns)
            if n > 0:
                df.columns = [
                    "Time (Seconds)",
                    *[f"Value{i}" for i in range(2, n + 1)],
                ]
        return df
    except pd.errors.EmptyDataError:
        log.debug(f"File {file_path} empty.")
        return pd.DataFrame()
    except Exception:
        log.exception(f"Failed to load data from {file_path}")
        raise ProcessingError("Failed to load data from file") from None


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def _load_and_enrich_config(config_path):
    """Load configuration and enrich with river mile mappings."""
    config_data = {}
    if load_config_func:
        try:
            config_data = load_config_func(config_path)
        except FileNotFoundError:
            log.warning(
                f"Config file {config_path} not found – continuing with empty config."
            )
        except Exception:  # pragma: no cover
            log.exception("Failed to load configuration")
            raise ProcessingError("Failed to load configuration") from None

    _enrich_config_with_river_mappings(config_data)
    return config_data


def _enrich_config_with_river_mappings(config_data):
    """Enrich configuration with river mile mappings if available."""
    rm_map_path = config_data.get("RIVER_MILE_MAP_PATH", "scripts/river_mile_map.csv")
    # SECURITY: Prevent path traversal (CWE-22) before checking file existence
    base_dir = os.path.realpath(os.getcwd())
    resolved = os.path.realpath(rm_map_path)
    try:
        if os.path.commonpath([base_dir, resolved]) != base_dir:
            log.warning(
                "Path traversal detected in RIVER_MILE_MAP_PATH: %s", rm_map_path
            )
            return
    except ValueError:
        log.warning("Path traversal detected in RIVER_MILE_MAP_PATH: %s", rm_map_path)
        return

    if os.path.isfile(resolved):
        rm_df = pd.read_csv(resolved)
        config_data["SENSOR_TO_RIVER"] = rm_df.set_index("SENSOR_ID")[
            "RIVER_MILE"
        ].to_dict()
        config_data["RIVER_TO_SENSORS"] = (
            rm_df.groupby("RIVER_MILE")["SENSOR_ID"].agg(list).to_dict()
        )


def _ensure_output_directory(output_dir, dry_run):
    """Ensure output directory exists if not in dry run mode."""
    if not dry_run and not os.path.isdir(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            log.info(f"Created output directory {output_dir}")
        except OSError:
            log.exception("Unable to create output directory")
            raise ProcessingError("Unable to create output directory") from None


@dataclass
class BatchConfig:
    series_selection: int | list[int] | str
    river_miles: list[float] | None
    years: tuple[int, int]
    dry_run: bool = False
    config_path: str = "scripts/config.json"
    output_dir: str | None = None


def batch_process(config: BatchConfig):
    """
    Process multiple sensor data files based on configuration.

    Args:
        config: BatchConfig object containing:
            - series_selection: Series IDs to process (int, list, or "all")
            - river_miles: Optional list of river miles to filter series by
            - years: Tuple of (start_year, end_year) to process
            - dry_run: If True, don't write output files
            - config_path: Path to configuration file
            - output_dir: Directory for output files (defaults to data directory)

    Returns:
        DataFrame with summary of processed files; writes
        Batch_Processing_Summary.csv to the output directory unless dry_run.
    """
    series_selection = config.series_selection
    river_miles = config.river_miles
    years = config.years
    dry_run = config.dry_run
    config_path = config.config_path
    output_dir = config.output_dir

    log.info(
        f"--- Batch processing START --- "
        f"series={series_selection} river_miles={river_miles} years={years} dry_run={dry_run}"
    )

    config_data = _load_and_enrich_config(config_path)
    data_dir = _get_data_directory(config_data)
    output_dir = output_dir or data_dir
    _ensure_output_directory(output_dir, dry_run)

    series_to_process = _determine_series_to_process(
        series_selection, river_miles, config_data, data_dir
    )
    if not series_to_process:
        return pd.DataFrame()

    files_to_process = _find_files_to_process(
        series_to_process, years, data_dir, config_data
    )
    log.debug(f"Files to process: {files_to_process}")

    if not files_to_process:
        log.warning("No matching files found! Entering fallback processing mode.")
        summary_df = _process_fallback_mode(
            series_to_process, config_data, output_dir, dry_run, years
        )
        _write_summary(summary_df, output_dir, dry_run)
        return summary_df

    processor_config = {
        **config_data.get("defaults", {}),
        **config_data.get("processor_config", {}),
    }

    summary_df = _process_main_mode(
        files_to_process, processor_config, output_dir, dry_run
    )
    _write_summary(summary_df, output_dir, dry_run)
    return summary_df


def count_failed(summary_df: pd.DataFrame | None) -> int:
    """Return the number of summary rows whose Status starts with 'Failed'."""
    if summary_df is None or "Status" not in summary_df.columns:
        return 0
    return int(summary_df["Status"].astype(str).str.startswith("Failed").sum())


def _write_summary(summary_df: pd.DataFrame, output_dir: str, dry_run: bool) -> None:
    if dry_run or summary_df.empty:
        return
    out_path = os.path.join(output_dir, SUMMARY_FILENAME)
    spreadsheet_safety.write_csv_safely(summary_df, out_path, index=False)
    log.info("Wrote batch processing summary to %s", out_path)


def _process_fallback_mode(
    series_to_process: list[int],
    config_data: dict[str, Any],
    output_dir: str,
    dry_run: bool,
    years: tuple[int, int],
) -> pd.DataFrame:
    if "series" not in config_data or processor is None:
        log.warning(
            "Fallback processing found no viable files. Returning empty DataFrame."
        )
        return pd.DataFrame()

    series_config = config_data["series"]
    year_index_map = config_data.get("year_index_map", {})
    base_year = _resolve_base_year(config_data, year_index_map)
    reverse_year_index_map = {idx: int(y) for y, idx in year_index_map.items()}
    context = _FallbackContext(config_data, output_dir, dry_run, processor)
    summary_records = []
    for series_id, position, file_path in _iter_fallback_inputs(
        series_to_process, series_config
    ):
        y_index = _y_index_from_path(file_path)
        # Files without a parseable Y-index have no derivable year and are
        # always processed; Y-indexed files are skipped when their year is
        # undeterminable or outside the requested range.
        if y_index is not None:
            year = _determine_year_for_index(y_index, reverse_year_index_map, base_year)
            if year is None:
                log.warning(
                    "Skipping %s: Y%02d cannot be mapped to a year",
                    file_path,
                    y_index,
                )
                continue
            if not years[0] <= year <= years[1]:
                log.info(
                    "Skipping %s: year %d outside requested years %s",
                    file_path,
                    year,
                    years,
                )
                continue
        fallback_input = _FallbackInput(series_id, position, file_path, y_index)
        record = _process_fallback_file(fallback_input, context)
        if record is not None:
            summary_records.append(record)

    if summary_records:
        return pd.DataFrame(summary_records)

    log.warning("Fallback processing found no viable files. Returning empty DataFrame.")
    return pd.DataFrame()


@dataclass(frozen=True)
class _FallbackInput:
    """Describe one fallback input file."""

    series_id: int
    position: int
    file_path: str
    y_index: int | None


@dataclass(frozen=True)
class _FallbackContext:
    """Provide the shared configuration for fallback processing."""

    config_data: dict[str, Any]
    output_dir: str
    dry_run: bool
    processor: Any


def _iter_fallback_inputs(
    series_to_process: list[int], series_config: dict[str, Any]
) -> Iterator[tuple[int, int, str]]:
    """Yield configured fallback inputs with their positions."""
    for series_id in series_to_process:
        raw_files = series_config.get(str(series_id), {}).get("raw_data", [])
        for position, file_path in enumerate(raw_files, start=1):
            yield series_id, position, file_path


def _y_index_from_path(file_path: str) -> int | None:
    """Return the filename's Y-index when it matches the expected pattern."""
    match = _FILE_NAME_REGEX.search(os.path.basename(file_path))
    return int(match.group(2)) if match else None


def _fallback_summary_record(
    fallback_input: _FallbackInput, status: str, records: int
) -> dict[str, Any]:
    """Build a summary record for a processed or failed input."""
    return {
        "Series": fallback_input.series_id,
        "Year": None,
        "Y-Index": fallback_input.y_index,
        "Filename": os.path.basename(fallback_input.file_path),
        "Status": status,
        "Records": records,
    }


def _write_fallback_output(
    processed_df: pd.DataFrame,
    fallback_input: _FallbackInput,
    context: _FallbackContext,
) -> None:
    """Write processed fallback data unless this is a dry run."""
    if context.dry_run:
        return

    if fallback_input.y_index is not None:
        out_name = (
            f"Series{fallback_input.series_id}_File"
            f"{fallback_input.y_index:02d}_Processed.xlsx"
        )
    else:
        out_name = (
            f"Series{fallback_input.series_id}_Pos"
            f"{fallback_input.position:02d}_Processed.xlsx"
        )
    out_path = os.path.join(context.output_dir, out_name)
    spreadsheet_safety.write_excel_safely(processed_df, out_path, index=False)
    log.info("Wrote output: %s", out_path)


def _run_fallback_file(
    fallback_input: _FallbackInput, context: _FallbackContext
) -> dict[str, Any] | None:
    """Load, process, and write one fallback input."""
    raw_data = _load_raw_data(fallback_input.file_path)
    if raw_data.empty:
        return None

    processor_config = {
        **context.config_data.get("defaults", {}),
        **context.config_data.get("processor_config", {}),
    }
    processed_df = context.processor.process_data(raw_data, processor_config)
    _write_fallback_output(processed_df, fallback_input, context)
    return _fallback_summary_record(
        fallback_input, "Fallback Processed", len(processed_df)
    )


def _process_fallback_file(
    fallback_input: _FallbackInput, context: _FallbackContext
) -> dict[str, Any] | None:
    """Process one fallback file and return its summary record."""
    log.info(
        "Fallback processing file: %s (series %s)",
        fallback_input.file_path,
        fallback_input.series_id,
    )
    try:
        return _run_fallback_file(fallback_input, context)
    except Exception:
        log.exception("Failed to process %s", fallback_input.file_path)
        return _fallback_summary_record(fallback_input, "Failed (Processing Error)", 0)


def _process_single_file(
    series: int,
    year: int,
    yi: int,
    file_path: str,
    processor_config: dict[str, Any],
    output_dir: str,
    dry_run: bool,
) -> dict[str, Any] | None:
    """Helper function to process a single file in main mode, reducing cognitive complexity."""
    fname = os.path.basename(file_path)
    log.info(f"Processing {fname} (Series {series}, Year {year}, Y{yi:02d})")

    if os.path.getsize(file_path) == 0:
        log.info(f"Skipping empty file: {fname}")
        return None

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

        if not dry_run:
            out_name = f"Year_{year} (Y{yi:02d})_Data.xlsx"
            out_path = os.path.join(output_dir, out_name)
            spreadsheet_safety.write_excel_safely(
                processed_df, out_path, index=False, header=False
            )
            log.info(f"Saved corrected data to {out_path}")

    except ProcessingError as err:
        log.error("Failed to process %s: %s", fname, err)
        status = "Failed (Processing Error)"
        processed_df = None
    except Exception:
        log.exception("Unexpected error processing %s", fname)
        status = "Failed (Unexpected Error)"
        processed_df = None

    return {
        "Series": series,
        "Year": year,
        "Y-Index": yi,
        "Filename": fname,
        "Status": status,
        "Records": (
            len(processed_df)
            if processed_df is not None and not processed_df.empty
            else 0
        ),
    }


def _process_main_mode(
    files_to_process: list[tuple[int, int, int, str]],
    processor_config: dict[str, Any],
    output_dir: str,
    dry_run: bool,
) -> pd.DataFrame:
    summary_records = []
    for series, year, yi, file_path in files_to_process:
        log.debug(f"Processing series: {series}, year: {year}, file: {file_path}")
        record = _process_single_file(
            series, year, yi, file_path, processor_config, output_dir, dry_run
        )
        if record:
            summary_records.append(record)

    # Create a summary DataFrame and return it
    summary_df = pd.DataFrame(summary_records)
    failed = count_failed(summary_df)
    succeeded = len(summary_df) - failed
    log_message = (
        "--- Batch processing COMPLETE --- Processed %d files, %d failed",
        succeeded,
        failed,
    )
    if failed:
        log.warning(*log_message)
    else:
        log.info(*log_message)
    return summary_df


# ----------------------------------------------------------------------

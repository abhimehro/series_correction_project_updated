"""
Data processor module for the Series Correction Project.

Implements algorithms for detecting and correcting discontinuities
in Seatek sensor time-series data based on the audit report suggestions.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from scripts.discontinuity_utils import (
    DiscontinuityConfig,
    _build_gaps_dataframe,
    _calculate_outlier_replacements,
    _calculate_outlier_z_scores,
    _perform_interpolation,
    _process_discontinuity,
    _validate_and_convert_time_col,
    _validate_value_col,
)

# Configure logging for this module
log = logging.getLogger(__name__)


def detect_gaps(
    data: pd.DataFrame, time_col: str = "Time (Seconds)", threshold_factor: float = 3.0
) -> list[int]:
    """
    Detect gaps in time series data based on time differences.

    Gaps are identified where the time difference between consecutive points
    exceeds a threshold multiple of the median time difference.

    Args:
        data: DataFrame containing time series data, sorted by time_col.
        time_col: Name of the time column.
        threshold_factor: Factor to multiply the median time difference by
                          to identify gaps.

    Returns:
        List of indices *before* which gaps are detected (i.e., the index
        of the first point *after* the gap). Returns an empty list if
        fewer than 2 data points exist or no gaps are found.
    """
    if len(data) < 2:
        log.debug("Not enough data points (< 2) to detect gaps.")
        return []

    # ⚡ Bolt: Vectorize time difference calculation using NumPy
    # instead of pandas Series.diff() and .median() for significant performance improvement
    time_col_np = data[time_col].to_numpy()

    # np.diff computes a[n+1] - a[n], returning an array of length N-1
    time_diffs_np = np.diff(time_col_np)

    if len(time_diffs_np) == 0:
        log.debug("No valid time differences to calculate median.")
        return []

    # Calculate the median time difference
    median_diff = np.median(time_diffs_np)

    if median_diff <= 0:
        log.warning(
            "Median time difference is non-positive (%s). Cannot reliably detect gaps.",
            median_diff,
        )
        return []

    # Define the gap threshold
    gap_threshold = threshold_factor * median_diff

    # Identify indices where the time difference exceeds the threshold
    # The index corresponds to the row *after* the gap.
    # Since np.diff reduces length by 1, index i in time_diffs_np corresponds to i+1 in original array
    gap_indices_np = np.where(time_diffs_np > gap_threshold)[0] + 1

    # Map back to original DataFrame index
    gap_indices = data.index[gap_indices_np].tolist()

    if gap_indices:
        log.info(
            "Detected %d potential gap(s) with threshold %s (median diff: %s). Indices: %s",
            len(gap_indices),
            gap_threshold,
            median_diff,
            gap_indices,
        )
    else:
        log.debug("No gaps detected with threshold %s.", gap_threshold)

    return gap_indices


def detect_jumps(
    data: pd.DataFrame, value_col: str, window_size: int = 5, threshold: float = 3.0
) -> list[int]:
    """
    Detect jumps/shifts in sensor values using a simplified CUSUM-like method
    based on rolling statistics.

    Identifies points where the cumulative deviation from the rolling mean,
    normalized by rolling standard deviation, exceeds a threshold.

    Args:
        data: DataFrame containing sensor data.
        value_col: Name of the value column to analyze.
        window_size: Size of the moving window for calculating rolling statistics.
        threshold: Threshold (in standard deviations) for detecting jumps.

    Returns:
        List of indices where jumps are detected. Returns an empty list if
        fewer than 2 * window_size data points exist or no jumps are found.
    """
    n = len(data)
    if n < window_size * 2:
        log.debug(
            "Not enough data points (< %d) for jump detection with window size %d.",
            window_size * 2,
            window_size,
        )
        return []

    # Calculate rolling mean and standard deviation
    rolling_mean = data[value_col].rolling(window=window_size).mean().to_numpy()
    rolling_std = data[value_col].rolling(window=window_size).std().to_numpy()
    values = data[value_col].to_numpy()

    # ⚡ Bolt: Vectorize normalized deviation calculation before CUSUM loop
    mean_prev_window = np.roll(rolling_mean, 1)
    std_prev_window = np.roll(rolling_std, 1)

    valid_mask = np.arange(n) >= window_size

    deviations = np.zeros(n)
    np.subtract(values, mean_prev_window, out=deviations, where=valid_mask)

    normalized_dev = np.zeros(n)

    with np.errstate(invalid="ignore"):
        std_mask = (std_prev_window > 1e-6) & valid_mask & ~np.isnan(std_prev_window)

    np.divide(deviations, std_prev_window, out=normalized_dev, where=std_mask)

    # Initialize CUSUM variables and list for jump indices
    jumps = []
    cusum = 0.0

    # Start after the first window is filled
    start_idx = window_size

    # Process each point from the end of the first window
    for i in range(start_idx, n):
        cusum += normalized_dev[i]

        if abs(cusum) > threshold:
            jumps.append(i)
            cusum = 0.0

    if jumps:
        log.info("Detected %d jump(s)", len(jumps))

    return jumps


def detect_outliers(
    data: pd.DataFrame, value_col: str, window_size: int = 5, threshold: float = 3.0
) -> list[int]:
    """
    Detect outliers using modified Z-scores based on the median absolute
    deviation (MAD) within rolling windows.

    This method is robust to existing outliers within the window.

    Args:
        data: DataFrame containing sensor data.
        value_col: Name of the value column to analyze.
        window_size: Size of the moving window for calculating statistics.
        threshold: Threshold for detecting outliers (modified Z-scores).
                     A common value is 3.5 for ~3 sigma equivalent with MAD.

    Returns:
        List of indices where outliers are detected. Returns an empty list if
        fewer than window_size data points exist or no outliers are found.
    """
    n = len(data)
    if n < window_size:
        log.debug(
            "Not enough data points (< %d) for outlier detection with window size %d.",
            n,
            window_size,
        )
        return []

    outliers = []
    values = data[value_col]
    values_np = values.to_numpy()

    # Calculate rolling median
    rolling_median = values.rolling(window=window_size, center=True).median().to_numpy()

    z_scores, valid_mask = _calculate_outlier_z_scores(
        values_np, rolling_median, window_size, threshold
    )
    outlier_mask = valid_mask & (z_scores > threshold)
    outliers = np.where(outlier_mask)[0].tolist()

    if outliers:
        log.info(
            "Detected %d potential outlier(s) with window %d, threshold %s. Indices: %s",
            len(outliers),
            window_size,
            threshold,
            outliers,
        )
    else:
        log.debug(
            "No outliers detected with window %d, threshold %s.", window_size, threshold
        )

    return outliers


def correct_gaps(
    data: pd.DataFrame,
    gap_indices: list[int],
    time_col: str = "Time (Seconds)",
    value_cols: list[str] | None = None,
    method: str = "time",
) -> pd.DataFrame:
    """
    Fill gaps in time series data by interpolating missing time points and values.

    First, it inserts rows with linearly spaced timestamps within the gap,
    then interpolates the values in `value_cols` using the specified method.

    Args:
        data: DataFrame containing time series data.
        gap_indices: List of indices *before* which gaps are detected (output from detect_gaps).
        time_col: Name of the time column.
        value_cols: List of value columns to interpolate. If None, interpolates
                    all numeric columns except time_col.
        method: Interpolation method passed to pandas.DataFrame.interpolate()
                (e.g., 'linear', 'time', 'spline', 'polynomial', 'akima').
                'time' is often suitable for time-based data.

    Returns:
        DataFrame with gaps filled. Returns a copy of the original if no gaps.
    """
    if not gap_indices:
        return data.copy()

    result_df = data.copy()

    if value_cols is None:
        value_cols = [
            col
            for col in result_df.select_dtypes(include=np.number).columns
            if col != time_col
        ]
        log.debug("Auto-detected value columns for gap correction: %s", value_cols)

    if not value_cols:
        log.warning("No numeric value columns found to interpolate for gap correction.")
        return result_df

    result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
    gaps_df = _build_gaps_dataframe(result_df, gap_indices, time_col)
    if gaps_df is not None:
        result_df = pd.concat([result_df, gaps_df], ignore_index=True)
        result_df = result_df.sort_values(by=time_col).reset_index(drop=True)

    log.info(
        "Interpolating values for columns %s using method '%s'.", value_cols, method
    )
    result_df = _perform_interpolation(result_df, value_cols, method, time_col)

    log.info(
        "Gap correction complete. DataFrame size changed from %d to %d.",
        len(data),
        len(result_df),
    )
    return result_df


def correct_jumps(
    data: pd.DataFrame, jump_indices: list[int], value_col: str, window_size: int = 5
) -> pd.DataFrame:
    """
    Correct jumps/shifts in sensor values by applying an offset.

    The offset is calculated as the difference between the median values
    in windows immediately before and after the detected jump point.
    This offset is then added to all data points from the jump point onwards.

    Args:
        data: DataFrame containing sensor data.
        jump_indices: List of indices where jumps are detected.
        value_col: Name of the value column to correct.
        window_size: Size of the window before and after the jump for
                     calculating the median offset.

    Returns:
        DataFrame with jumps corrected. Returns a copy of the original if no jumps.
    """
    if not jump_indices:
        return data.copy()

    result_df = data.copy()
    n = len(result_df)

    sorted_jump_indices = sorted(
        [j for j in jump_indices if window_size <= j < n - window_size]
    )
    if not sorted_jump_indices:
        return result_df

    # Cast to float to avoid UFuncOutputCastingError if the data was originally ints
    values_np = result_df[value_col].astype(float).to_numpy(copy=True)

    # ⚡ Bolt: Vectorized offset calculation for all jumps
    valid_jumps = np.array(sorted_jump_indices)

    # Build a 2D array of windows before and after the jump indices
    # ⚡ Bolt: Use sliding_window_view for ~3x faster O(1) memory window extraction
    # instead of list comprehensions which have high Python iteration overhead
    all_windows = sliding_window_view(values_np, window_shape=window_size)
    before_windows = all_windows[valid_jumps - window_size]
    after_windows = all_windows[valid_jumps]

    # Calculate medians in bulk
    mb = np.nanmedian(before_windows, axis=1)
    ma = np.nanmedian(after_windows, axis=1)

    # Find valid medians (not NaN)
    valid_medians_mask = ~(np.isnan(mb) | np.isnan(ma))

    # Calculate differences for valid medians
    diffs = mb[valid_medians_mask] - ma[valid_medians_mask]

    # Place differences in a zero-initialized array at respective indices
    offsets = np.zeros(n)
    np.add.at(offsets, valid_jumps[valid_medians_mask], diffs)

    # Apply globally across the entire sequence
    result_df[value_col] = values_np + np.cumsum(offsets)

    log.info("Jump correction complete for column '%s'.", value_col)
    return result_df


def correct_outliers(
    data: pd.DataFrame,
    outlier_indices: list[int],
    value_col: str,
    window_size: int = 5,
    method: str = "median",
) -> pd.DataFrame:
    """
    Correct outliers in sensor values by replacing them with a calculated value.

    Args:
        data: DataFrame containing sensor data.
        outlier_indices: List of indices where outliers are detected.
        value_col: Name of the value column to correct.
        window_size: Size of the window around the outlier used for calculating
                     the replacement value.
        method: Method for replacing outliers: 'median', 'mean', 'interpolate', 'remove'.

    Returns:
        DataFrame with outliers corrected based on the chosen method.
    """
    if not outlier_indices:
        return data.copy()

    result_df = data.copy()

    log.info(
        "Correcting %d outliers in column '%s' using method '%s'.",
        len(outlier_indices),
        value_col,
        method,
    )

    if method == "interpolate":
        result_df.loc[outlier_indices, value_col] = np.nan
        result_df[value_col] = result_df[value_col].interpolate(
            method="linear", limit_direction="both"
        )
        log.info("Outliers replaced via linear interpolation.")

    elif method == "remove":
        result_df.loc[outlier_indices, value_col] = np.nan
        log.info("Outliers replaced with NaN.")

    elif method in ["median", "mean"]:
        values_np = result_df[value_col].astype(float).to_numpy(copy=True)
        values_np = _calculate_outlier_replacements(
            values_np, outlier_indices, window_size, method
        )
        result_df[value_col] = values_np
    else:
        log.error(
            "Invalid outlier correction method specified: '%s'. No correction applied.",
            method,
        )
        return result_df

    log.info("Outlier correction complete for column '%s'.", value_col)
    return result_df


def _merge_config(config: dict[str, Any] | None) -> dict[str, Any]:
    default_config = {
        "window_size": 5,
        "threshold": 3.0,
        "gap_threshold_factor": 3.0,
        "gap_method": "time",
        "outlier_method": "median",
        "jump_method": "offset",
        "time_col": "Time (Seconds)",
        "value_col": None,
    }
    if config is None:
        config = {}
    return {**default_config, **(config or {})}


def _get_processing_steps(
    merged_config: dict[str, Any], time_col: str, value_col: str
) -> list[DiscontinuityConfig]:
    window_size = merged_config["window_size"]
    threshold = merged_config["threshold"]
    gap_threshold_factor = merged_config["gap_threshold_factor"]
    gap_method = merged_config["gap_method"]
    outlier_method = merged_config["outlier_method"]

    return [
        DiscontinuityConfig(
            step_name="Step 1: Detecting and Correcting Gaps",
            detect_func=detect_gaps,
            correct_func=correct_gaps,
            detect_kwargs={
                "time_col": time_col,
                "threshold_factor": gap_threshold_factor,
            },
            correct_kwargs={
                "time_col": time_col,
                "value_cols": [value_col],
                "method": gap_method,
            },
            sort_time_col=time_col,
        ),
        DiscontinuityConfig(
            step_name="Step 2: Detecting and Correcting Outliers",
            detect_func=detect_outliers,
            correct_func=correct_outliers,
            detect_kwargs={
                "value_col": value_col,
                "window_size": window_size,
                "threshold": threshold,
            },
            correct_kwargs={
                "value_col": value_col,
                "window_size": window_size,
                "method": outlier_method,
            },
            sort_time_col=None,
        ),
        DiscontinuityConfig(
            step_name="Step 3: Detecting and Correcting Jumps",
            detect_func=detect_jumps,
            correct_func=correct_jumps,
            detect_kwargs={
                "value_col": value_col,
                "window_size": window_size,
                "threshold": threshold,
            },
            correct_kwargs={"value_col": value_col, "window_size": window_size},
            sort_time_col=None,
        ),
    ]


def process_data(
    data: pd.DataFrame, config: dict[str, Any] | None = None
) -> pd.DataFrame:
    """
    Process sensor data to detect and correct discontinuities (gaps, outliers, jumps).

    Applies detection and correction functions sequentially based on configuration.

    Args:
        data: DataFrame containing sensor data.
        config: Configuration dictionary with processing parameters.
    """
    merged_config = _merge_config(config)
    log.info("Processing data with configuration: %s", merged_config)

    processed_data = data.copy()
    time_col = merged_config["time_col"]
    processed_data = _validate_and_convert_time_col(processed_data, time_col)

    value_col = _validate_value_col(
        processed_data, merged_config["value_col"], time_col
    )
    merged_config["value_col"] = value_col

    log.debug("Sorting data by time column: '%s'", time_col)
    processed_data = processed_data.sort_values(by=time_col).reset_index(drop=True)

    steps = _get_processing_steps(merged_config, time_col, value_col)
    for step in steps:
        processed_data = _process_discontinuity(processed_data, step)

    log.info("Data processing complete for value column '%s'.", value_col)
    return processed_data

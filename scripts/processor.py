"""
Data processor module for the Series Correction Project.

Implements algorithms for detecting and correcting discontinuities
in Seatek sensor time-series data based on the audit report suggestions.
"""

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# Configure logging for this module
log = logging.getLogger(__name__)


def detect_gaps(
    data: pd.DataFrame, time_col: str = "Time (Seconds)", threshold_factor: float = 3.0
) -> List[int]:
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

    # Calculate time differences between consecutive points
    time_diffs = data[time_col].diff()

    # Exclude the first value (which is NaN)
    time_diffs_valid = time_diffs.iloc[1:]

    if time_diffs_valid.empty:
        log.debug("No valid time differences to calculate median.")
        return []

    # Calculate the median time difference
    median_diff = pd.Series(time_diffs_valid).median()

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
    gap_indices = time_diffs[time_diffs > gap_threshold].index.tolist()

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
) -> List[int]:
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
    rolling_mean = data[value_col].rolling(window=window_size).mean()
    rolling_std = data[value_col].rolling(window=window_size).std()

    # Initialize CUSUM variables and list for jump indices
    jumps = []
    cusum = 0.0
    # Start after the first window is filled
    start_idx = window_size

    # Process each point from the end of the first window
    for i in range(start_idx, n):
        mean_prev_window = rolling_mean.iloc[i - 1]
        std_prev_window = rolling_std.iloc[i - 1]

        # Current deviation from the previous window's mean
        deviation = data[value_col].iloc[i] - mean_prev_window

        # Normalize by previous window's standard deviation
        if pd.notna(std_prev_window) and std_prev_window > 1e-6:
            normalized_dev = deviation / std_prev_window
        else:
            normalized_dev = 0.0

        cusum += normalized_dev

        if abs(cusum) > threshold:
            jumps.append(i)
            cusum = 0.0
            log.debug("Jump detected at index %d (CUSUM exceeded threshold %s)", i, threshold)

    if jumps:
        log.info(
            "Detected %d potential jump(s) with window %d, threshold %s. Indices: %s",
            len(jumps), window_size, threshold, jumps,
        )
    else:
        log.debug(
            "No jumps detected with window %d, threshold %s.", window_size, threshold
        )

    return jumps


def detect_outliers(
    data: pd.DataFrame, value_col: str, window_size: int = 5, threshold: float = 3.0
) -> List[int]:
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

    # Calculate rolling median
    rolling_median = values.rolling(window=window_size, center=True).median()

    # Calculate rolling MAD
    rolling_mad = values.rolling(window=window_size, center=True).apply(
        lambda x: pd.Series(x).sub(pd.Series(x).median()).abs().median(), raw=True
    )

    mad_scale_factor = 1.4826
    rolling_scaled_mad = rolling_mad * mad_scale_factor

    for i in range(n):
        median_i = rolling_median.iloc[i]
        scaled_mad_i = rolling_scaled_mad.iloc[i]
        current_value = values.iloc[i]

        if pd.isna(median_i) or pd.isna(scaled_mad_i):
            continue

        if scaled_mad_i < 1e-6:
            if abs(current_value - median_i) > 1e-6:
                if abs(current_value - median_i) > threshold * 1e-6:
                    z_score = np.inf
                else:
                    z_score = 0.0
            else:
                z_score = 0.0
        else:
            z_score = abs(current_value - median_i) / scaled_mad_i

        if z_score > threshold:
            outliers.append(i)
            log.debug(
                "Outlier detected at index %d (Value: %s, Median: %s, Scaled MAD: %s, Z: %s > Threshold: %s)",
                i, current_value, median_i, scaled_mad_i, z_score, threshold
            )

    if outliers:
        log.info(
            "Detected %d potential outlier(s) with window %d, threshold %s. Indices: %s",
            len(outliers), window_size, threshold, outliers,
        )
    else:
        log.debug(
            "No outliers detected with window %d, threshold %s.", window_size, threshold
        )

    return outliers


def correct_gaps(
    data: pd.DataFrame,
    gap_indices: List[int],
    time_col: str = "Time (Seconds)",
    value_cols: Optional[List[str]] = None,
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
    processed_gap_indices = set()

    for gap_idx in sorted(gap_indices, reverse=True):
        if gap_idx in processed_gap_indices or gap_idx == 0:
            continue

        idx_before = gap_idx - 1
        idx_after = gap_idx

        time_before = result_df[time_col].iloc[idx_before]
        time_after = result_df[time_col].iloc[idx_after]

        if idx_before > 0:
            time_prev = result_df[time_col].iloc[idx_before - 1]
            normal_step = time_before - time_prev
        elif len(result_df) > idx_after + 1:
            time_next = result_df[time_col].iloc[idx_after + 1]
            normal_step = time_next - time_after
        else:
            log.warning("Cannot determine normal time step for gap at index %d. Skipping.", gap_idx)
            continue

        if normal_step <= 0:
            log.warning("Estimated normal time step is non-positive (%s) for gap at index %d. Skipping.", normal_step, gap_idx)
            continue

        num_missing_points = round((time_after - time_before) / normal_step) - 1

        if num_missing_points <= 0:
            log.debug("Calculated 0 or negative missing points for gap at index %d. Skipping.", gap_idx)
            continue

        log.info(
            "Filling gap at index %d: %d points missing between %s and %s (step: %s).",
            gap_idx, num_missing_points, time_before, time_after, normal_step
        )

        new_times = np.linspace(
            time_before + normal_step,
            time_after - normal_step,
            num=num_missing_points,
            dtype=type(time_before)
        )

        new_rows_list = []
        for t in new_times:
            new_row = {time_col: t}
            for col in result_df.columns:
                if col != time_col:
                    new_row[col] = np.nan
            new_rows_list.append(new_row)

        if not new_rows_list:
            continue

        new_rows_df = pd.DataFrame(new_rows_list)

        result_df = pd.concat([
            result_df.iloc[:gap_idx],
            new_rows_df,
            result_df.iloc[gap_idx:]
        ]).reset_index(drop=True)

        processed_gap_indices.add(gap_idx)

    log.info("Interpolating values for columns %s using method '%s'.", value_cols, method)
    if method == 'time' and isinstance(result_df.index, pd.DatetimeIndex):
        result_df_indexed = result_df.set_index(time_col)
        result_df_indexed[value_cols] = result_df_indexed[value_cols].interpolate(method=method, limit_direction='both')
        result_df = result_df_indexed.reset_index()
    elif method == 'time':
        log.warning("Cannot use 'time' interpolation without a valid time column index. Falling back to 'linear'.")
        result_df[value_cols] = result_df[value_cols].interpolate(method='linear', limit_direction='both')
    else:
        result_df[value_cols] = result_df[value_cols].interpolate(method=method, limit_direction='both')

    log.info("Gap correction complete. DataFrame size changed from %d to %d.", len(data), len(result_df))
    return result_df


def correct_jumps(
    data: pd.DataFrame, jump_indices: List[int], value_col: str, window_size: int = 5
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

    sorted_jump_indices = sorted(jump_indices)

    for jump_idx in sorted_jump_indices:
        if jump_idx < window_size or jump_idx >= n - window_size:
            log.warning(
                "Skipping jump correction at index %d: insufficient data for window size %d.",
                jump_idx, window_size
            )
            continue

        window_before = result_df[value_col].iloc[jump_idx - window_size : jump_idx]
        window_after = result_df[value_col].iloc[jump_idx : jump_idx + window_size]

        median_before = pd.Series(window_before).median()
        median_after = pd.Series(window_after).median()

        if pd.isna(median_before) or pd.isna(median_after):
            log.warning("Skipping jump correction at index %d: NaN median in window.", jump_idx)
            continue

        local_offset = median_before - median_after

        log.info(
            "Correcting jump at index %d: Median before=%s, Median after=%s, Offset=%s",
            jump_idx, median_before, median_after, local_offset
        )

        result_df.loc[result_df.index >= jump_idx, value_col] += local_offset
        log.info("Applied offset %s to data from index %d onwards.", local_offset, jump_idx)

    log.info("Jump correction complete for column '%s'.", value_col)
    return result_df


def correct_outliers(
    data: pd.DataFrame,
    outlier_indices: List[int],
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
    n = len(result_df)
    outlier_indices_set = set(outlier_indices)

    log.info("Correcting %d outliers in column '%s' using method '%s'.", len(outlier_indices), value_col, method)

    if method == "interpolate":
        result_df.loc[outlier_indices, value_col] = np.nan
        result_df[value_col] = result_df[value_col].interpolate(method='linear', limit_direction='both')
        log.info("Outliers replaced via linear interpolation.")

    elif method == "remove":
        result_df.loc[outlier_indices, value_col] = np.nan
        log.info("Outliers replaced with NaN.")

    elif method in ["median", "mean"]:
        for outlier_idx in outlier_indices:
            start_idx = max(0, outlier_idx - window_size // 2)
            end_idx = min(n, outlier_idx + window_size // 2 + 1)
            window_indices = range(start_idx, end_idx)
            valid_indices_in_window = [idx for idx in window_indices if idx != outlier_idx and idx not in outlier_indices_set]
            if not valid_indices_in_window:
                log.warning("Cannot calculate replacement for outlier at index %d: no valid surrounding points.", outlier_idx)
                continue
            surrounding_values = result_df[value_col].loc[valid_indices_in_window]
            if method == "median":
                replacement_value = pd.Series(list(surrounding_values)).median()
            else:
                replacement_value = pd.Series(list(surrounding_values)).mean()
            if pd.notna(replacement_value):
                original_value = result_df.loc[outlier_idx, value_col]
                result_df.loc[outlier_idx, value_col] = replacement_value
                log.debug("Replaced outlier at index %d (Original: %s) with %s value: %s", outlier_idx, original_value, method, replacement_value)
            else:
                log.warning("Could not compute valid %s replacement for outlier at index %d.", method, outlier_idx)
    else:
        log.error("Invalid outlier correction method specified: '%s'. No correction applied.", method)
        return result_df

    log.info("Outlier correction complete for column '%s'.", value_col)
    return result_df


def process_data(
    data: pd.DataFrame, config: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Process sensor data to detect and correct discontinuities (gaps, outliers, jumps).

    Applies detection and correction functions sequentially based on configuration.

    Args:
        data: DataFrame containing sensor data.
        config: Configuration dictionary with processing parameters.
    """
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
    merged_config = {**default_config, **(config or {})}
    log.info("Processing data with configuration: %s", merged_config)

    processed_data = data.copy()

    time_col = merged_config["time_col"]
    if time_col not in processed_data.columns:
        log.warning("Time column '{time_col}' not found in data columns: {list(processed_data.columns)}")
        raise ValueError("Time column '{time_col}' not found in data columns: {list(processed_data.columns)}")

    if not pd.api.types.is_numeric_dtype(processed_data[time_col]):
        try:
            processed_data[time_col] = pd.to_datetime(processed_data[time_col])
            processed_data[time_col] = (processed_data[time_col] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
            log.info("Converted time column '%s' to numeric (Unix timestamp).", time_col)
        except Exception as e:
            raise ValueError('Time column \'{time_col}\' is not numeric and could not be converted: {e}')
    value_col = merged_config["value_col"]
    if value_col is None:
        numeric_cols = processed_data.select_dtypes(include=np.number).columns
        potential_value_cols = [col for col in numeric_cols if col != time_col]
        if not potential_value_cols:
            log.warning(
                "No numeric value columns found in the data (excluding time column '%s'). Please specify a valid value column in the configuration.",
                time_col
            )
            raise ValueError(
                f"No numeric value columns found in the data (excluding time column '{time_col}')."
            )
        value_col = potential_value_cols[0]
        merged_config["value_col"] = value_col
        log.info("Auto-detected value column: '%s'", value_col)
    elif value_col not in processed_data.columns:
        raise ValueError(
            "Specified value column '{value_col}' not found in data columns: {list(processed_data.columns)}")
    elif not pd.api.types.is_numeric_dtype(processed_data[value_col]):
        raise ValueError("Specified value column '{value_col}' is not numeric.")

    window_size = merged_config["window_size"]
    threshold = merged_config["threshold"]
    gap_threshold_factor = merged_config["gap_threshold_factor"]
    gap_method = merged_config["gap_method"]
    outlier_method = merged_config["outlier_method"]

    log.debug("Sorting data by time column: '%s'", time_col)
    processed_data = processed_data.sort_values(by=time_col).reset_index(drop=True)

    log.info("--- Step 1: Detecting and Correcting Gaps ---")
    gap_indices = detect_gaps(
        processed_data, time_col=time_col, threshold_factor=gap_threshold_factor
    )
    if gap_indices:
        processed_data = correct_gaps(
            processed_data,
            gap_indices,
            time_col=time_col,
            value_cols=[value_col],
            method=gap_method,
        )
        processed_data = processed_data.sort_values(by=time_col).reset_index(drop=True)
    else:
        log.info("No gaps detected or corrected.")

    log.info("--- Step 2: Detecting and Correcting Outliers ---")
    outlier_indices = detect_outliers(
        processed_data,
        value_col=value_col,
        window_size=window_size,
        threshold=threshold,
    )
    if outlier_indices:
        processed_data = correct_outliers(
            processed_data,
            outlier_indices,
            value_col=value_col,
            window_size=window_size,
            method=outlier_method,
        )
    else:
        log.info("No outliers detected or corrected.")

    log.info("--- Step 3: Detecting and Correcting Jumps ---")
    jump_indices = detect_jumps(
        processed_data,
        value_col=value_col,
        window_size=window_size,
        threshold=threshold,
    )
    if jump_indices:
        processed_data = correct_jumps(
            processed_data,
            jump_indices,
            value_col=value_col,
            window_size=window_size,
        )
    else:
        log.info("No jumps detected or corrected.")

    log.info("Data processing complete for value column '%s'.", value_col)
    return processed_data
"""
Data processing module for Seatek sensor time-series data.
Contains functions for detecting and correcting gaps, jumps, and outliers.
"""
import logging
from typing import List, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# --- Helper Functions to Extract Complex Logic ---

def _compute_z_scores_vectorized(
    values_np: np.ndarray,
    rolling_median_np: np.ndarray,
    rolling_scaled_mad_np: np.ndarray,
    threshold: float,
    n: int
) -> Tuple[List[int], np.ndarray]:
    """Helper function to compute Z-scores in a vectorized manner."""
    abs_diff = np.abs(values_np - rolling_median_np)
    z_scores = np.zeros(n)

    valid_mask = ~(pd.isna(rolling_median_np) | pd.isna(rolling_scaled_mad_np))
    small_mad_mask = valid_mask & (rolling_scaled_mad_np < 1e-6)
    normal_mad_mask = valid_mask & (rolling_scaled_mad_np >= 1e-6)

    # normal mad
    z_scores[normal_mad_mask] = abs_diff[normal_mad_mask] / rolling_scaled_mad_np[normal_mad_mask]

    # small mad
    small_mad_diff_large = small_mad_mask & (abs_diff > 1e-6)
    small_mad_diff_very_large = small_mad_diff_large & (abs_diff > threshold * 1e-6)
    z_scores[small_mad_diff_very_large] = np.inf

    # find outlier indices
    outlier_indices = np.where(valid_mask & (z_scores > threshold))[0]
    outliers = outlier_indices.tolist()

    return outliers, z_scores


# --- End Helper Functions ---

def detect_gaps(
    data: pd.DataFrame, time_col: str, threshold_factor: float = 2.5
) -> List[int]:
    """
    Detect gaps in time-series data based on the median time difference.

    Args:
        data: DataFrame containing time-series data.
        time_col: Name of the time column.
        threshold_factor: Multiplier for the median time difference to define a gap.

    Returns:
        List of indices immediately *after* the detected gaps. Returns an empty
        list if not enough valid time differences exist or if no gaps are found.
    """
    if len(data) < 2:
        log.debug("Not enough data points (< 2) for gap detection.")
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
    gap_series = time_diffs > gap_threshold
    gap_indices = gap_series[gap_series].index.tolist()

    if gap_indices:
        log.info(
            "Detected %d potential gap(s) with threshold factor %s. Indices (after gap): %s",
            len(gap_indices),
            threshold_factor,
            gap_indices,
        )
    else:
        log.debug(
            "No gaps detected with threshold factor %s (threshold: %s).",
            threshold_factor,
            gap_threshold,
        )

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

    # Convert Pandas Series to raw NumPy arrays for faster access
    rolling_mean_np = rolling_mean.to_numpy()
    rolling_std_np = rolling_std.to_numpy()
    values_np = data[value_col].to_numpy()

    # Initialize CUSUM variables and list for jump indices
    jumps = []
    cusum = 0.0
    # Start after the first window is filled
    start_idx = window_size

    # Process each point from the end of the first window
    for i in range(start_idx, n):
        mean_prev_window = rolling_mean_np[i - 1]
        std_prev_window = rolling_std_np[i - 1]

        # Current deviation from the previous window's mean
        deviation = values_np[i] - mean_prev_window

        # Normalize by previous window's standard deviation
        if pd.notna(std_prev_window) and std_prev_window > 1e-6:
            normalized_dev = deviation / std_prev_window
        else:
            normalized_dev = 0.0

        cusum += normalized_dev

        if abs(cusum) > threshold:
            jumps.append(i)
            cusum = 0.0
            log.debug(
                "Jump detected at index %d (CUSUM exceeded threshold %s)", i, threshold
            )

    if jumps:
        log.info(
            "Detected %d potential jump(s) with window %d, threshold %s. Indices: %s",
            len(jumps),
            window_size,
            threshold,
            jumps,
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

    values = data[value_col]

    # Calculate rolling median
    rolling_median = values.rolling(window=window_size, center=True).median()

    # Calculate rolling MAD
    # ⚡ Bolt: Replaced pd.Series instantiation inside lambda with pure NumPy operations.
    # Using np.nanmedian directly on the raw numpy array avoids significant Pandas object
    # creation overhead inside the rolling apply loop, vastly improving performance.
    rolling_mad = values.rolling(window=window_size, center=True).apply(
        lambda x: np.nanmedian(np.abs(x - np.nanmedian(x))), raw=True
    )

    mad_scale_factor = 1.4826
    rolling_scaled_mad = rolling_mad * mad_scale_factor

    # Convert to NumPy arrays for faster access
    rolling_median_np = rolling_median.to_numpy()
    rolling_scaled_mad_np = rolling_scaled_mad.to_numpy()
    values_np = values.to_numpy()

    outliers, z_scores = _compute_z_scores_vectorized(
        values_np, rolling_median_np, rolling_scaled_mad_np, threshold, n
    )

    for i in outliers:
        log.debug(
            "Outlier detected at index %d (Value: %s, Median: %s, Scaled MAD: %s, Z: %s > Threshold: %s)",
            i,
            values_np[i],
            rolling_median_np[i],
            rolling_scaled_mad_np[i],
            z_scores[i],
            threshold,
        )

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
    time_col: str,
    gap_indices: List[int],
    value_cols: List[str] = None,
    method: str = "linear",
) -> pd.DataFrame:
    """
    Correct gaps by inserting rows and interpolating values.

    Rows are inserted at intervals approximating the typical sampling rate.
    Missing values in specified columns are then filled using interpolation.

    Args:
        data: DataFrame containing time-series data.
        time_col: Name of the time column.
        gap_indices: List of indices indicating the end of gaps.
        value_cols: List of column names to interpolate. If None, auto-detects
                    numeric columns excluding the time column.
        method: Interpolation method (e.g., 'linear', 'time', 'polynomial').
                Default is 'linear'. If 'time' is selected, index must be DatetimeIndex.

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
            log.warning(
                "Cannot determine normal time step for gap at index %d. Skipping.",
                gap_idx,
            )
            continue

        if normal_step <= 0:
            log.warning(
                "Estimated normal time step is non-positive (%s) for gap at index %d. Skipping.",
                normal_step,
                gap_idx,
            )
            continue

        num_missing_points = round((time_after - time_before) / normal_step) - 1

        if num_missing_points <= 0:
            log.debug(
                "Estimated missing points is <= 0 for gap at index %d. Skipping interpolation.",
                gap_idx,
            )
            continue

        log.info(
            "Filling gap at index %d: adding %d points between %s and %s (estimated step: %s)",
            gap_idx,
            num_missing_points,
            time_before,
            time_after,
            normal_step,
        )

        new_times = np.linspace(
            time_before + normal_step,
            time_after - normal_step,
            num=num_missing_points,
            dtype=type(time_before),
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

        result_df = pd.concat(
            [result_df.iloc[:gap_idx], new_rows_df, result_df.iloc[gap_idx:]]
        ).reset_index(drop=True)

        processed_gap_indices.add(gap_idx)

    log.info(
        "Interpolating values for columns %s using method '%s'.", value_cols, method
    )
    if method == "time" and isinstance(result_df.index, pd.DatetimeIndex):
        result_df_indexed = result_df.set_index(time_col)
        result_df_indexed[value_cols] = result_df_indexed[value_cols].interpolate(
            method=method, limit_direction="both"
        )
        result_df = result_df_indexed.reset_index()
    elif method == "time":
        log.warning(
            "Cannot use 'time' interpolation without a valid time column index. Falling back to 'linear'."
        )
        result_df[value_cols] = result_df[value_cols].interpolate(
            method="linear", limit_direction="both"
        )
    else:
        result_df[value_cols] = result_df[value_cols].interpolate(
            method=method, limit_direction="both"
        )

    return result_df


def correct_jumps(
    data: pd.DataFrame, value_col: str, jump_indices: List[int], window_size: int = 5
) -> pd.DataFrame:
    """
    Correct identified jumps by applying a local median-based offset.

    Calculates the median before and after the jump within a specified window
    and shifts subsequent data by the difference to smooth out the discontinuity.

    Args:
        data: DataFrame containing sensor data.
        value_col: Name of the value column to correct.
        jump_indices: List of indices where jumps were detected.
        window_size: Size of the window used to calculate local medians for offset.

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
                jump_idx,
                window_size,
            )
            continue

        window_before = result_df[value_col].iloc[jump_idx - window_size : jump_idx]
        window_after = result_df[value_col].iloc[jump_idx : jump_idx + window_size]

        median_before = pd.Series(window_before).median()
        median_after = pd.Series(window_after).median()

        if pd.isna(median_before) or pd.isna(median_after):
            log.warning(
                "Skipping jump correction at index %d: NaN median in window.", jump_idx
            )
            continue

        local_offset = median_before - median_after

        log.info(
            "Correcting jump at index %d: Median before=%s, Median after=%s, Offset=%s",
            jump_idx,
            median_before,
            median_after,
            local_offset,
        )

        result_df.loc[jump_idx:, value_col] += local_offset

    return result_df


def correct_outliers(
    data: pd.DataFrame,
    value_col: str,
    outlier_indices: List[int],
    method: str = "interpolate",
) -> pd.DataFrame:
    """
    Handle detected outliers using the specified method.

    Currently supports replacing outliers with NaN followed by interpolation.

    Args:
        data: DataFrame containing sensor data.
        value_col: Name of the value column to correct.
        outlier_indices: List of indices identifying outliers.
        method: Method for correction. Default is 'interpolate'.

    Returns:
        DataFrame with outliers corrected. Returns a copy of the original if
        no outliers are provided.

    Raises:
        ValueError: If an unsupported correction method is specified.
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
    else:
        error_msg = f"Unsupported outlier correction method: '{method}'. Supported methods: 'interpolate'."
        log.error(error_msg)
        raise ValueError(error_msg)

    return result_df


def process_data(data: pd.DataFrame, config: dict = None) -> pd.DataFrame:
    """
    Main processing pipeline for sensor data.

    Executes a sequence of data cleaning steps:
    1. Sorts data by time.
    2. Detects and corrects gaps.
    3. Detects and corrects jumps.
    4. Detects and corrects outliers.

    Args:
        data: DataFrame containing raw sensor data.
        config: Configuration dictionary overriding default parameters.
                Expected to contain nested dictionaries: 'gaps', 'jumps', 'outliers'.

    Returns:
        DataFrame containing the processed and corrected data.
    """
    if data is None or data.empty:
        log.warning("Empty or None DataFrame received for processing.")
        return data

    if config is None:
        config = {}

    processed_data = data.copy()

    # Sort data to ensure chronological order before processing
    if "Year" in processed_data.columns:
        processed_data = processed_data.sort_values(by="Year").reset_index(drop=True)
    else:
        log.warning("No 'Year' column found for sorting before processing.")

    # Apply configuration defaults if not provided
    gaps_cfg = config.get("gaps", {"threshold_factor": 2.5, "interpolation": "linear"})
    jumps_cfg = config.get("jumps", {"window_size": 5, "threshold": 3.0})
    outliers_cfg = config.get("outliers", {"window_size": 5, "threshold": 3.0})

    value_columns = [col for col in processed_data.columns if col != "Year"]

    for col in value_columns:
        log.info("--- Processing column: %s ---", col)

        # 1. Gap Correction
        gap_indices = detect_gaps(
            processed_data,
            time_col="Year",
            threshold_factor=gaps_cfg.get("threshold_factor", 2.5),
        )
        if gap_indices:
            processed_data = correct_gaps(
                processed_data,
                time_col="Year",
                gap_indices=gap_indices,
                value_cols=[col],
                method=gaps_cfg.get("interpolation", "linear"),
            )

        # 2. Jump Correction
        jump_indices = detect_jumps(
            processed_data,
            value_col=col,
            window_size=jumps_cfg.get("window_size", 5),
            threshold=jumps_cfg.get("threshold", 3.0),
        )
        if jump_indices:
            processed_data = correct_jumps(
                processed_data,
                value_col=col,
                jump_indices=jump_indices,
                window_size=jumps_cfg.get("window_size", 5),
            )

        # 3. Outlier Correction
        outlier_indices = detect_outliers(
            processed_data,
            value_col=col,
            window_size=outliers_cfg.get("window_size", 5),
            threshold=outliers_cfg.get("threshold", 3.0),
        )
        if outlier_indices:
            processed_data = correct_outliers(
                processed_data,
                value_col=col,
                outlier_indices=outlier_indices,
                method="interpolate",
            )

    log.info("Processing pipeline completed.")
    return processed_data

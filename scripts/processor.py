"""
Data processor module for the Series Correction Project.
Implements algorithms for detecting and correcting discontinuities in Seatek sensor time-series data.
"""

from __future__ import annotations
import logging
from typing import Any
import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

log = logging.getLogger(__name__)


def detect_gaps(data: pd.DataFrame, time_col: str = "Time (Seconds)", threshold_factor: float = 3.0) -> list[int]:
    """Detect gaps in time series data based on time differences."""
    if len(data) < 2:
        log.debug("Not enough data points (< 2) to detect gaps.")
        return []
    
    time_col_np = data[time_col].to_numpy()
    time_diffs_np = np.diff(time_col_np)
    
    if len(time_diffs_np) == 0:
        log.debug("No valid time differences to calculate median.")
        return []
    
    median_diff = np.median(time_diffs_np)
    if median_diff <= 0:
        log.warning("Median time difference is non-positive (%s). Cannot reliably detect gaps.", median_diff)
        return []
    
    gap_threshold = threshold_factor * median_diff
    gap_indices_np = np.where(time_diffs_np > gap_threshold)[0] + 1
    gap_indices = data.index[gap_indices_np].tolist()
    
    if gap_indices:
        log.info("Detected %d potential gap(s) with threshold %s (median diff: %s). Indices: %s",
                len(gap_indices), gap_threshold, median_diff, gap_indices)
    else:
        log.debug("No gaps detected with threshold %s.", gap_threshold)
    
    return gap_indices


def detect_jumps(data: pd.DataFrame, value_col: str, window_size: int = 5, threshold: float = 3.0) -> list[int]:
    """Detect jumps/shifts in sensor values using a simplified CUSUM-like method."""
    n = len(data)
    if n < window_size * 2:
        log.debug("Not enough data points (< %d) for jump detection with window size %d.", window_size * 2, window_size)
        return []
    
    rolling_mean = data[value_col].rolling(window=window_size).mean().to_numpy()
    rolling_std = data[value_col].rolling(window=window_size).std().to_numpy()
    values = data[value_col].to_numpy()
    
    mean_prev_window = np.roll(rolling_mean, 1)
    std_prev_window = np.roll(rolling_std, 1)
    valid_mask = np.arange(n) >= window_size
    
    deviations = np.zeros(n)
    np.subtract(values, mean_prev_window, out=deviations, where=valid_mask)
    
    normalized_dev = np.zeros(n)
    with np.errstate(invalid="ignore"):
        std_mask = (std_prev_window > 1e-6) & valid_mask & ~np.isnan(std_prev_window)
    np.divide(deviations, std_prev_window, out=normalized_dev, where=std_mask)
    
    jumps = []
    cusum = 0.0
    for i in range(window_size, n):
        cusum += normalized_dev[i]
        if abs(cusum) > threshold:
            jumps.append(i)
            cusum = 0.0
    
    if jumps:
        log.info("Detected %d jump(s)", len(jumps))
    return jumps


def _calculate_outlier_z_scores(values_np, rolling_median, window_size, threshold):
    n = len(values_np)
    mads, nw = [], n - window_size + 1
    for s in range(0, nw, 50000):
        e = min(s + 50000, nw)
        cw = sliding_window_view(values_np[s : e + window_size - 1], window_shape=window_size)
        pad = window_size // 2
        cm = rolling_median[s + pad : e + pad, np.newaxis]
        cmads = np.nanmedian(np.abs(cw - cm), axis=1)
        cmads[np.isnan(cw).sum(axis=1) > 0] = np.nan
        mads.append(cmads)
    
    m = np.concatenate(mads) if mads else np.array([])
    rolling_mad = np.pad(m, (window_size // 2, n - len(m) - window_size // 2), constant_values=np.nan)
    rolling_scaled_mad = rolling_mad * 1.4826
    
    with np.errstate(invalid="ignore", divide="ignore"):
        abs_diff = np.abs(values_np - rolling_median)
        z_scores = np.where(rolling_scaled_mad < 1e-6,
                           np.where(abs_diff > 1e-6, np.where(abs_diff > threshold * 1e-6, np.inf, 0.0), 0.0),
                           abs_diff / rolling_scaled_mad)
        valid_mask = ~np.isnan(rolling_median) & ~np.isnan(rolling_scaled_mad)
    return z_scores, valid_mask


def detect_outliers(data: pd.DataFrame, value_col: str, window_size: int = 5, threshold: float = 3.0) -> list[int]:
    """Detect outliers using modified Z-scores based on the median absolute deviation (MAD)."""
    n = len(data)
    if n < window_size:
        log.debug("Not enough data points (< %d) for outlier detection with window size %d.", n, window_size)
        return []
    
    values = data[value_col]
    values_np = values.to_numpy()
    rolling_median = values.rolling(window=window_size, center=True).median().to_numpy()
    
    z_scores, valid_mask = _calculate_outlier_z_scores(values_np, rolling_median, window_size, threshold)
    outlier_mask = valid_mask & (z_scores > threshold)
    outliers = np.where(outlier_mask)[0].tolist()
    
    if outliers:
        log.info("Detected %d potential outlier(s) with window %d, threshold %s. Indices: %s",
                len(outliers), window_size, threshold, outliers)
    else:
        log.debug("No outliers detected with window %d, threshold %s.", window_size, threshold)
    return outliers


def _calculate_normal_step(time_col_arr, idx_before, idx_after, max_len):
    if idx_before > 0:
        return time_col_arr[idx_before] - time_col_arr[idx_before - 1]
    elif max_len > idx_after + 1:
        return time_col_arr[idx_after + 1] - time_col_arr[idx_after]
    return None


def _is_valid_step(normal_step):
    if isinstance(normal_step, pd.Timedelta):
        return normal_step.total_seconds() > 0
    elif isinstance(normal_step, np.timedelta64):
        return normal_step > np.timedelta64(0, "ns")
    return normal_step > 0


def _generate_missing_times(time_before, time_after, normal_step, num_missing_points):
    start_time, end_time = time_before + normal_step, time_after - normal_step
    if isinstance(start_time, (pd.Timestamp, np.datetime64)):
        return pd.date_range(start=pd.Timestamp(start_time), end=pd.Timestamp(end_time), periods=num_missing_points)
    elif hasattr(start_time, "value"):
        return pd.to_datetime(np.linspace(start_time.value, end_time.value, num=num_missing_points))
    return np.linspace(start_time, end_time, num=num_missing_points, dtype=type(time_before))


def _validate_gap_parameters(gap_idx, normal_step, time_before, time_after):
    if normal_step is None:
        log.warning("Cannot determine normal time step for gap at index %d. Skipping.", gap_idx)
        return None
    if not _is_valid_step(normal_step):
        log.warning("Estimated normal time step is non-positive (%s) for gap at index %d. Skipping.", normal_step, gap_idx)
        return None
    num_missing_points = round((time_after - time_before) / normal_step) - 1
    if num_missing_points <= 0:
        log.debug("Calculated 0 or negative missing points for gap at index %d. Skipping.", gap_idx)
        return None
    return num_missing_points


def _build_gaps_dataframe(result_df, gap_indices, time_col):
    processed_gap_indices = set()
    all_new_rows = []
    time_col_arr = result_df[time_col].to_numpy()
    max_len = len(result_df)
    
    for gap_idx in sorted(gap_indices, reverse=True):
        if gap_idx in processed_gap_indices or gap_idx == 0:
            continue
        idx_before, idx_after = gap_idx - 1, gap_idx
        time_before, time_after = time_col_arr[idx_before], time_col_arr[idx_after]
        normal_step = _calculate_normal_step(time_col_arr, idx_before, idx_after, max_len)
        num_missing_points = _validate_gap_parameters(gap_idx, normal_step, time_before, time_after)
        if num_missing_points is None:
            continue
        log.info("Filling gap at index %d: %d points missing between %s and %s (step: %s).",
                gap_idx, num_missing_points, time_before, time_after, normal_step)
        new_times = _generate_missing_times(time_before, time_after, normal_step, num_missing_points)
        all_new_rows.append(new_times)
        processed_gap_indices.add(gap_idx)
    
    if not all_new_rows:
        return None
    concatenated_times = np.concatenate(all_new_rows)
    gaps_df = pd.DataFrame(np.nan, index=range(len(concatenated_times)), columns=result_df.columns)
    gaps_df[time_col] = concatenated_times
    return gaps_df


def _perform_interpolation(result_df, value_cols, method, time_col):
    if method == "time" and isinstance(result_df.index, pd.DatetimeIndex):
        result_df_indexed = result_df.set_index(time_col)
        result_df_indexed[value_cols] = result_df_indexed[value_cols].interpolate(method=method, limit_direction="both")
        return result_df_indexed.reset_index()
    elif method == "time":
        log.warning("Cannot use 'time' interpolation without a valid time column index. Falling back to 'linear'.")
    result_df[value_cols] = result_df[value_cols].interpolate(method="linear" if method == "time" else method, limit_direction="both")
    return result_df


def correct_gaps(data: pd.DataFrame, gap_indices: list[int], time_col: str = "Time (Seconds)",
                 value_cols: list[str] | None = None, method: str = "time") -> pd.DataFrame:
    """Fill gaps in time series data by interpolating missing time points and values."""
    if not gap_indices:
        return data.copy()
    
    result_df = data.copy()
    if value_cols is None:
        value_cols = [col for col in result_df.select_dtypes(include=np.number).columns if col != time_col]
        log.debug("Auto-detected value columns for gap correction: %s", value_cols)
    if not value_cols:
        log.warning("No numeric value columns found to interpolate for gap correction.")
        return result_df
    
    result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
    gaps_df = _build_gaps_dataframe(result_df, gap_indices, time_col)
    if gaps_df is not None:
        result_df = pd.concat([result_df, gaps_df], ignore_index=True)
        result_df = result_df.sort_values(by=time_col).reset_index(drop=True)
    
    log.info("Interpolating values for columns %s using method '%s'.", value_cols, method)
    result_df = _perform_interpolation(result_df, value_cols, method, time_col)
    log.info("Gap correction complete. DataFrame size changed from %d to %d.", len(data), len(result_df))
    return result_df


def correct_jumps(data: pd.DataFrame, jump_indices: list[int], value_col: str, window_size: int = 5) -> pd.DataFrame:
    """Correct jumps/shifts in sensor values by applying an offset."""
    if not jump_indices:
        return data.copy()
    
    result_df = data.copy()
    n = len(result_df)
    sorted_jump_indices = sorted([j for j in jump_indices if window_size <= j < n - window_size])
    if not sorted_jump_indices:
        return result_df
    
    values_np = result_df[value_col].astype(float).to_numpy(copy=True)
    valid_jumps = np.array(sorted_jump_indices)
    all_windows = sliding_window_view(values_np, window_shape=window_size)
    before_windows = all_windows[valid_jumps - window_size]
    after_windows = all_windows[valid_jumps]
    
    mb = np.nanmedian(before_windows, axis=1)
    ma = np.nanmedian(after_windows, axis=1)
    valid_medians_mask = ~(np.isnan(mb) | np.isnan(ma))
    diffs = mb[valid_medians_mask] - ma[valid_medians_mask]
    
    offsets = np.zeros(n)
    np.add.at(offsets, valid_jumps[valid_medians_mask], diffs)
    result_df[value_col] = values_np + np.cumsum(offsets)
    log.info("Jump correction complete for column '%s'.", value_col)
    return result_df


def _calculate_outlier_replacements(values_np: np.ndarray, outlier_indices: list[int], window_size: int, method: str) -> np.ndarray:
    n = len(values_np)
    outlier_mask = np.zeros(n, dtype=bool)
    outlier_mask[outlier_indices] = True
    calc_values = values_np.copy()
    calc_values[outlier_mask] = np.nan
    
    pad_width = window_size // 2
    padded_values = np.pad(calc_values, (pad_width, pad_width), mode="constant", constant_values=np.nan)
    windows = sliding_window_view(padded_values, window_shape=pad_width * 2 + 1)
    outlier_windows = windows[outlier_indices]
    
    with np.errstate(invalid="ignore"):
        replacements = np.nanmedian(outlier_windows, axis=1) if method == "median" else np.nanmean(outlier_windows, axis=1)
    
    valid_replacements = ~np.isnan(replacements)
    invalid_indices = np.array(outlier_indices)[~valid_replacements]
    for idx in invalid_indices:
        log.warning("Could not compute valid %s replacement for outlier at index %d.", method, idx)
    
    valid_indices = np.array(outlier_indices)[valid_replacements]
    for idx, orig_val, repl_val in zip(valid_indices, values_np[valid_indices], replacements[valid_replacements]):
        log.debug("Replaced outlier at index %d (Original: %s) with %s value: %s", idx, orig_val, method, repl_val)
    
    values_np[valid_indices] = replacements[valid_replacements]
    return values_np


def correct_outliers(data: pd.DataFrame, outlier_indices: list[int], value_col: str, window_size: int = 5, method: str = "median") -> pd.DataFrame:
    """Correct outliers in sensor values by replacing them with a calculated value."""
    if not outlier_indices:
        return data.copy()
    
    result_df = data.copy()
    log.info("Correcting %d outliers in column '%s' using method '%s'.", len(outlier_indices), value_col, method)
    
    if method == "interpolate":
        result_df.loc[outlier_indices, value_col] = np.nan
        result_df[value_col] = result_df[value_col].interpolate(method="linear", limit_direction="both")
        log.info("Outliers replaced via linear interpolation.")
    elif method == "remove":
        result_df.loc[outlier_indices, value_col] = np.nan
        log.info("Outliers replaced with NaN.")
    elif method in ["median", "mean"]:
        values_np = result_df[value_col].astype(float).to_numpy(copy=True)
        values_np = _calculate_outlier_replacements(values_np, outlier_indices, window_size, method)
        result_df[value_col] = values_np
    else:
        log.error("Invalid outlier correction method specified: '%s'. No correction applied.", method)
        return result_df
    
    log.info("Outlier correction complete for column '%s'.", value_col)
    return result_df


def _validate_and_convert_time_col(processed_data, time_col):
    if time_col not in processed_data.columns:
        log.warning("Time column '%s' not found in data columns: %s", time_col, list(processed_data.columns))
        raise ValueError("Time column not found in data columns")
    if not pd.api.types.is_numeric_dtype(processed_data[time_col]):
        try:
            processed_data[time_col] = pd.to_datetime(processed_data[time_col], format="mixed")
            processed_data[time_col] = (processed_data[time_col] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")
            log.info("Converted time column '%s' to numeric (Unix timestamp).", time_col)
        except Exception as exc:
            log.exception(f"Time column '{time_col}' is not numeric and could not be converted: {exc}")
            raise ValueError("Time column is not numeric and could not be converted") from None
    return processed_data


def _auto_detect_value_col(processed_data, time_col):
    numeric_cols = processed_data.select_dtypes(include=np.number).columns
    potential_value_cols = [col for col in numeric_cols if col != time_col]
    if not potential_value_cols:
        log.warning("No numeric value columns found in the data (excluding time column '%s'). Please specify a valid value column in the configuration.", time_col)
        raise ValueError("No numeric value columns found in the data")
    value_col = potential_value_cols[0]
    log.info("Auto-detected value column: '%s'", value_col)
    return value_col


def _validate_specified_value_col(processed_data, value_col):
    if value_col not in processed_data.columns:
        log.warning(f"Specified value column '{value_col}' not found in data columns: {list(processed_data.columns)}")
        raise ValueError("Specified value column not found in data columns")
    if not pd.api.types.is_numeric_dtype(processed_data[value_col]):
        log.warning(f"Specified value column '{value_col}' is not numeric.")
        raise ValueError("Specified value column is not numeric.")
    return value_col


def _validate_value_col(processed_data, value_col, time_col):
    if value_col is None:
        return _auto_detect_value_col(processed_data, time_col)
    return _validate_specified_value_col(processed_data, value_col)


def _process_gaps(processed_data, time_col, value_col, config):
    log.info("--- Step 1: Detecting and Correcting Gaps ---")
    gap_indices = detect_gaps(processed_data, time_col=time_col, threshold_factor=config["gap_threshold_factor"])
    if gap_indices:
        processed_data = correct_gaps(processed_data, gap_indices, time_col=time_col, value_cols=[value_col], method=config["gap_method"])
        processed_data = processed_data.sort_values(by=time_col).reset_index(drop=True)
    else:
        log.info("No gaps detected or corrected.")
    return processed_data


def _process_outliers(processed_data, value_col, config):
    log.info("--- Step 2: Detecting and Correcting Outliers ---")
    outlier_indices = detect_outliers(processed_data, value_col=value_col, window_size=config["window_size"], threshold=config["threshold"])
    if outlier_indices:
        processed_data = correct_outliers(processed_data, outlier_indices, value_col=value_col, window_size=config["window_size"], method=config["outlier_method"])
    else:
        log.info("No outliers detected or corrected.")
    return processed_data


def _process_jumps(processed_data, value_col, window_size, threshold):
    log.info("--- Step 3: Detecting and Correcting Jumps ---")
    jump_indices = detect_jumps(processed_data, value_col=value_col, window_size=window_size, threshold=threshold)
    if jump_indices:
        processed_data = correct_jumps(processed_data, jump_indices, value_col=value_col, window_size=window_size)
    else:
        log.info("No jumps detected or corrected.")
    return processed_data


def process_data(data: pd.DataFrame, config: dict[str, Any] | None = None) -> pd.DataFrame:
    """Process sensor data to detect and correct discontinuities (gaps, outliers, jumps)."""
    default_config = {
        "window_size": 5, "threshold": 3.0, "gap_threshold_factor": 3.0,
        "gap_method": "time", "outlier_method": "median", "jump_method": "offset",
        "time_col": "Time (Seconds)", "value_col": None,
    }
    
    if config is None:
        config = {}
    merged_config = {**default_config, **(config or {})}
    log.info("Processing data with configuration: %s", merged_config)
    
    processed_data = data.copy()
    time_col = merged_config["time_col"]
    processed_data = _validate_and_convert_time_col(processed_data, time_col)
    
    value_col = merged_config["value_col"]
    value_col = _validate_value_col(processed_data, value_col, time_col)
    merged_config["value_col"] = value_col
    
    log.debug("Sorting data by time column: '%s'", time_col)
    processed_data = processed_data.sort_values(by=time_col).reset_index(drop=True)
    
    processed_data = _process_gaps(processed_data, time_col, value_col, merged_config)
    processed_data = _process_outliers(processed_data, value_col, merged_config)
    processed_data = _process_jumps(processed_data, value_col, merged_config["window_size"], merged_config["threshold"])
    
    log.info("Data processing complete for value column '%s'.", value_col)
    return processed_data
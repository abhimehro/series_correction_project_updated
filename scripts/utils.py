"""
Utility functions for the Series Correction Project.

Contains helper functions for time validation, gap processing, and data validation
used by the main processor module.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

# Configure logging for this module
log = logging.getLogger(__name__)


def _calculate_normal_step(time_col_arr, idx_before, idx_after, max_len):
    return (
        time_col_arr[idx_before] - time_col_arr[idx_before - 1]
        if idx_before > 0
        else (
            time_col_arr[idx_after + 1] - time_col_arr[idx_after]
            if max_len > idx_after + 1
            else None
        )
    )


def _is_valid_step(normal_step):
    if isinstance(normal_step, pd.Timedelta):
        return normal_step.total_seconds() > 0
    elif isinstance(normal_step, np.timedelta64):
        return normal_step > np.timedelta64(0, "ns")
    else:
        return normal_step > 0


def _generate_missing_times(time_before, time_after, normal_step, num_missing_points):
    start_time, end_time = time_before + normal_step, time_after - normal_step

    if isinstance(start_time, (pd.Timestamp, np.datetime64)):
        return pd.date_range(
            start=pd.Timestamp(start_time),
            end=pd.Timestamp(end_time),
            periods=num_missing_points,
        )
    elif hasattr(start_time, "value"):
        return pd.to_datetime(
            np.linspace(start_time.value, end_time.value, num=num_missing_points)
        )
    else:
        return np.linspace(
            start_time, end_time, num=num_missing_points, dtype=type(time_before)
        )


def _validate_gap_parameters(gap_idx, normal_step, time_before, time_after):
    """Validate gap parameters and return num_missing_points or None if invalid."""
    if normal_step is None:
        log.warning(
            "Cannot determine normal time step for gap at index %d. Skipping.", gap_idx
        )
        return None

    if not _is_valid_step(normal_step):
        log.warning(
            "Estimated normal time step is non-positive (%s) for gap at index %d. Skipping.",
            normal_step,
            gap_idx,
        )
        return None

    num_missing_points = round((time_after - time_before) / normal_step) - 1
    if num_missing_points <= 0:
        log.debug(
            "Calculated 0 or negative missing points for gap at index %d. Skipping.",
            gap_idx,
        )
        return None

    return num_missing_points


def _validate_and_convert_time_col(processed_data, time_col):
    if time_col not in processed_data.columns:
        log.warning(
            "Time column '%s' not found in data columns: %s",
            time_col,
            list(processed_data.columns),
        )
        raise ValueError("Time column not found in data columns")

    if not pd.api.types.is_numeric_dtype(processed_data[time_col]):
        try:
            processed_data[time_col] = pd.to_datetime(
                processed_data[time_col], format="mixed"
            )
            processed_data[time_col] = (
                processed_data[time_col] - pd.Timestamp("1970-01-01")
            ) // pd.Timedelta("1s")
            log.info(
                "Converted time column '%s' to numeric (Unix timestamp).", time_col
            )
        except Exception as exc:
            log.exception(
                f"Time column '{time_col}' is not numeric and could not be converted: {exc}"
            )
            raise ValueError(
                "Time column is not numeric and could not be converted"
            ) from None
    return processed_data


def _validate_value_col(processed_data, value_col, time_col):
    if value_col is None:
        numeric_cols = processed_data.select_dtypes(include=np.number).columns
        potential_value_cols = [col for col in numeric_cols if col != time_col]
        if not potential_value_cols:
            log.warning(
                "No numeric value columns found in the data (excluding time column '%s'). Please specify a valid value column in the configuration.",
                time_col,
            )
            raise ValueError("No numeric value columns found in the data")
        value_col = potential_value_cols[0]
        log.info("Auto-detected value column: '%s'", value_col)
    elif value_col not in processed_data.columns:
        log.warning(
            f"Specified value column '{value_col}' not found in data columns: {list(processed_data.columns)}"
        )
        raise ValueError("Specified value column not found in data columns")
    elif not pd.api.types.is_numeric_dtype(processed_data[value_col]):
        log.warning(f"Specified value column '{value_col}' is not numeric.")
        raise ValueError("Specified value column is not numeric.")
    return value_col


def _calculate_outlier_z_scores(values_np, rolling_median, window_size, threshold):
    from numpy.lib.stride_tricks import sliding_window_view

    n = len(values_np)
    mads, nw = [], n - window_size + 1
    for s in range(0, nw, 50000):
        e = min(s + 50000, nw)
        cw = sliding_window_view(
            values_np[s : e + window_size - 1], window_shape=window_size
        )
        cm = np.nanmedian(cw, axis=1, keepdims=True)
        cmads = np.nanmedian(np.abs(cw - cm), axis=1)
        cmads[np.isnan(cw).sum(axis=1) > 0] = np.nan
        mads.append(cmads)

    m = np.concatenate(mads) if mads else np.array([])
    rolling_mad = np.pad(
        m, (window_size // 2, n - len(m) - window_size // 2), constant_values=np.nan
    )
    rolling_scaled_mad = rolling_mad * 1.4826

    with np.errstate(invalid="ignore", divide="ignore"):
        abs_diff = np.abs(values_np - rolling_median)
        z_scores = np.where(
            rolling_scaled_mad < 1e-6,
            np.where(
                abs_diff > 1e-6, np.where(abs_diff > threshold * 1e-6, np.inf, 0.0), 0.0
            ),
            abs_diff / rolling_scaled_mad,
        )
        valid_mask = ~np.isnan(rolling_median) & ~np.isnan(rolling_scaled_mad)

    return z_scores, valid_mask


def _build_gaps_dataframe(
    result_df, gap_indices, time_col
):
    """Helper to isolate gap generation logic and reduce correct_gaps complexity."""
    processed_gap_indices = set()
    all_new_rows = []
    time_col_arr = result_df[time_col].to_numpy()
    max_len = len(result_df)

    for gap_idx in sorted(gap_indices, reverse=True):
        if gap_idx in processed_gap_indices or gap_idx == 0:
            continue

        idx_before, idx_after = gap_idx - 1, gap_idx
        time_before, time_after = time_col_arr[idx_before], time_col_arr[idx_after]
        normal_step = _calculate_normal_step(
            time_col_arr, idx_before, idx_after, max_len
        )

        num_missing_points = _validate_gap_parameters(
            gap_idx, normal_step, time_before, time_after
        )
        if num_missing_points is None:
            continue

        log.info(
            "Filling gap at index %d: %d points missing between %s and %s (step: %s).",
            gap_idx,
            num_missing_points,
            time_before,
            time_after,
            normal_step,
        )

        new_times = _generate_missing_times(
            time_before, time_after, normal_step, num_missing_points
        )
        all_new_rows.append(new_times)
        processed_gap_indices.add(gap_idx)

    if not all_new_rows:
        return None

    concatenated_times = np.concatenate(all_new_rows)
    gaps_df = pd.DataFrame(
        np.nan, index=range(len(concatenated_times)), columns=result_df.columns
    )
    gaps_df[time_col] = concatenated_times
    return gaps_df


def _perform_interpolation(result_df, value_cols, method, time_col):
    """Perform interpolation on the dataframe using the specified method."""
    if method == "time" and isinstance(result_df.index, pd.DatetimeIndex):
        result_df_indexed = result_df.set_index(time_col)
        result_df_indexed[value_cols] = result_df_indexed[value_cols].interpolate(
            method=method, limit_direction="both"
        )
        return result_df_indexed.reset_index()
    elif method == "time":
        log.warning(
            "Cannot use 'time' interpolation without a valid time column index. Falling back to 'linear'."
        )
        result_df[value_cols] = result_df[value_cols].interpolate(
            method="linear", limit_direction="both"
        )
        return result_df
    else:
        result_df[value_cols] = result_df[value_cols].interpolate(
            method=method, limit_direction="both"
        )
        return result_df


def _calculate_outlier_replacements(
    values_np: np.ndarray,
    outlier_indices: list[int],
    window_size: int,
    method: str,
) -> np.ndarray:
    n = len(values_np)
    outlier_mask = np.zeros(n, dtype=bool)
    outlier_mask[outlier_indices] = True

    calc_values = values_np.copy()
    calc_values[outlier_mask] = np.nan

    pad_width = window_size // 2
    padded_values = np.pad(
        calc_values, (pad_width, pad_width), mode="constant", constant_values=np.nan
    )

    windows = sliding_window_view(padded_values, window_shape=pad_width * 2 + 1)
    outlier_windows = windows[outlier_indices]

    with np.errstate(invalid="ignore"):
        replacements = (
            np.nanmedian(outlier_windows, axis=1)
            if method == "median"
            else np.nanmean(outlier_windows, axis=1)
        )

    valid_replacements = ~np.isnan(replacements)
    invalid_indices = np.array(outlier_indices)[~valid_replacements]
    for idx in invalid_indices:
        log.warning(
            "Could not compute valid %s replacement for outlier at index %d.",
            method,
            idx,
        )

    valid_indices = np.array(outlier_indices)[valid_replacements]
    for idx, orig_val, repl_val in zip(
        valid_indices, values_np[valid_indices], replacements[valid_replacements]
    ):
        log.debug(
            "Replaced outlier at index %d (Original: %s) with %s value: %s",
            idx,
            orig_val,
            method,
            repl_val,
        )

    values_np[valid_indices] = replacements[valid_replacements]
    return values_np
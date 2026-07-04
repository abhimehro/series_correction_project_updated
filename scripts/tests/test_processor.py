import logging
import pytest
import pandas as pd
import numpy as np

from scripts.processor import detect_outliers, process_data, correct_gaps


def test_detect_outliers_basic():
    """Test detecting a clear outlier in a mostly stable series."""
    data = pd.DataFrame({"value": [1.0, 1.1, 0.9, 1.0, 100.0, 1.2, 0.8, 1.0, 1.1, 0.9]})

    # window_size=5, outlier at index 4 (100.0)
    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == [4]


def test_detect_outliers_no_outliers():
    """Test series with normal variance and no outliers."""
    data = pd.DataFrame({"value": [1.0, 1.1, 0.9, 1.0, 1.2, 0.8, 1.0, 1.1, 0.9]})

    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == []


def test_detect_outliers_small_data():
    """Test dataframe smaller than the window size."""
    data = pd.DataFrame({"value": [1.0, 1.1, 100.0]})

    # window_size is 5, but data length is 3
    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == []


def test_detect_outliers_with_nans():
    """Test handling of NaNs in the data."""
    data = pd.DataFrame(
        {"value": [1.0, 1.1, np.nan, 1.0, 100.0, 1.2, np.nan, 1.0, 1.1, 0.9]}
    )

    # The outlier 100.0 is at index 4
    outliers = detect_outliers(data, value_col="value", window_size=3, threshold=3.0)
    assert outliers == [4]


def test_detect_outliers_zero_mad():
    """Test when the median absolute deviation is zero (identical values)."""
    data = pd.DataFrame({"value": [1.0, 1.0, 1.0, 100.0, 1.0, 1.0, 1.0]})

    # window_size=5, outlier at index 3 (100.0)
    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == [3]


def test_process_data_time_col_parsing_failure(caplog):
    """Test that process_data correctly raises ValueError and logs an exception when time_col parsing fails."""
    df = pd.DataFrame(
        {"Time (Seconds)": ["not_a_time", "also_not_a_time"], "Value": [1.0, 2.0]}
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(
            ValueError, match="Time column is not numeric and could not be converted"
        ):
            process_data(df)

    assert (
        "Time column 'Time (Seconds)' is not numeric and could not be converted"
        in caplog.text
    )


def test_correct_gaps_no_gaps():
    """Test when no gaps are provided, the original DataFrame is returned."""
    df = pd.DataFrame({"Time (Seconds)": [1.0, 2.0, 3.0], "Value": [10.0, 20.0, 30.0]})
    result = correct_gaps(df, gap_indices=[])
    pd.testing.assert_frame_equal(result, df)


def test_correct_gaps_basic_linear():
    """Test basic interpolation with numeric time column."""
    # A gap before index 2 (between time 2.0 and 5.0)
    df = pd.DataFrame(
        {"Time (Seconds)": [1.0, 2.0, 5.0, 6.0], "Value": [10.0, 20.0, 50.0, 60.0]}
    )
    result = correct_gaps(
        df, gap_indices=[2], time_col="Time (Seconds)", method="linear"
    )

    # Expected output: Rows added for time 3.0 and 4.0
    expected_times = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    expected_values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]

    assert list(result["Time (Seconds)"]) == expected_times
    assert list(result["Value"]) == expected_values


def test_correct_gaps_no_value_cols(caplog):
    """Test when no numeric value columns are available."""
    df = pd.DataFrame({"Time (Seconds)": [1.0, 2.0, 5.0], "StringCol": ["a", "b", "c"]})

    with caplog.at_level(logging.WARNING):
        result = correct_gaps(df, gap_indices=[2])

    assert "No numeric value columns found" in caplog.text
    pd.testing.assert_frame_equal(result, df)


def test_correct_gaps_explicit_value_cols():
    """Test specifying explicit value columns to interpolate."""
    df = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 5.0],
            "Val1": [10.0, 20.0, 50.0],
            "Val2": [100.0, 200.0, 500.0],
        }
    )

    # Interpolate only Val1
    result = correct_gaps(df, gap_indices=[2], value_cols=["Val1"], method="linear")

    assert list(result["Val1"]) == [10.0, 20.0, 30.0, 40.0, 50.0]
    # Val2 should be NaN in the inserted rows
    assert pd.isna(result["Val2"].iloc[2])
    assert pd.isna(result["Val2"].iloc[3])


def test_correct_gaps_time_fallback(caplog):
    """Test the 'time' method without DatetimeIndex falls back to linear."""
    df = pd.DataFrame({"Time (Seconds)": [1.0, 2.0, 5.0], "Value": [10.0, 20.0, 50.0]})

    with caplog.at_level(logging.WARNING):
        result = correct_gaps(df, gap_indices=[2], method="time")

    assert (
        "Cannot use 'time' interpolation without a valid time column index"
        in caplog.text
    )
    assert list(result["Value"]) == [10.0, 20.0, 30.0, 40.0, 50.0]

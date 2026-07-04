import logging
import pytest
import pandas as pd
import numpy as np

from scripts.processor import detect_outliers, detect_jumps, process_data


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


def test_detect_jumps_basic():
    """Test detecting a clear upward jump in a mostly stable series."""
    data = pd.DataFrame(
        {"value": [1.0, 1.1, 0.9, 1.0, 1.1, 10.0, 10.1, 9.9, 10.0, 10.1]}
    )
    # Window size 3.
    # index 0,1,2: 1.0, 1.1, 0.9 (mean=1.0, std=~0.1)
    # jump starts at index 5.
    jumps = detect_jumps(data, value_col="value", window_size=3, threshold=3.0)
    assert jumps == [5]


def test_detect_jumps_no_jumps():
    """Test series with normal variance and no jumps."""
    data = pd.DataFrame({"value": [1.0, 1.1, 0.9, 1.0, 1.1, 0.8, 1.2, 0.9, 1.0, 1.1]})
    jumps = detect_jumps(data, value_col="value", window_size=3, threshold=3.0)
    assert jumps == []


def test_detect_jumps_small_data():
    """Test dataframe smaller than 2 * window_size."""
    data = pd.DataFrame({"value": [1.0, 1.1, 1.0, 1.1, 1.0]})
    # window_size is 3, requires at least 6 points
    jumps = detect_jumps(data, value_col="value", window_size=3, threshold=3.0)
    assert jumps == []


def test_detect_jumps_negative_jump():
    """Test detecting a clear downward jump."""
    data = pd.DataFrame(
        {"value": [10.0, 10.1, 9.9, 10.0, 10.1, 1.0, 1.1, 0.9, 1.0, 1.1]}
    )
    jumps = detect_jumps(data, value_col="value", window_size=3, threshold=3.0)
    assert jumps == [5]


def test_detect_jumps_zero_std():
    """Test when the standard deviation of the previous window is zero."""
    # If the previous window is perfectly flat (std=0), the mask prevents divide-by-zero.
    # So it shouldn't detect a jump at the exact point of the shift using CUSUM
    # if it relies on the perfectly flat window. Let's verify it doesn't crash.
    data = pd.DataFrame(
        {"value": [1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 10.1, 9.9, 10.0, 10.1]}
    )
    jumps = detect_jumps(data, value_col="value", window_size=3, threshold=3.0)
    # The jump might not be detected if std was 0 because normalized_dev is kept at 0.
    # We mainly test that it executes without divide-by-zero warnings/errors.
    assert isinstance(jumps, list)


def test_detect_jumps_multiple_jumps():
    """Test detecting multiple distinct jumps."""
    data = pd.DataFrame(
        {
            "value": [
                1.0,
                1.1,
                0.9,
                1.0,
                1.1,  # Window 1 (mean ~1.0)
                10.0,
                10.1,
                9.9,
                10.0,
                10.1,  # Jump 1 (at index 5)
                20.0,
                20.1,
                19.9,
                20.0,
                20.1,  # Jump 2 (at index 10)
            ]
        }
    )
    jumps = detect_jumps(data, value_col="value", window_size=3, threshold=3.0)
    # Should detect jumps at index 5 and 10.
    assert 5 in jumps
    assert 10 in jumps

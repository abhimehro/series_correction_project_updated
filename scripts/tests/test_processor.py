import logging
import pytest
import pandas as pd
import numpy as np

from scripts.processor import detect_outliers, process_data, correct_outliers


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


def test_correct_outliers_empty_indices():
    """Test when no outliers are provided."""
    data = pd.DataFrame({"value": [1.0, 2.0, 3.0]})
    result = correct_outliers(data, outlier_indices=[], value_col="value")
    # Should return a copy, not the original object
    assert result is not data
    pd.testing.assert_frame_equal(result, data)


def test_correct_outliers_remove():
    """Test removing outliers (replaced with NaN)."""
    data = pd.DataFrame({"value": [1.0, 100.0, 3.0]})
    result = correct_outliers(
        data, outlier_indices=[1], value_col="value", method="remove"
    )
    assert pd.isna(result.loc[1, "value"])
    assert result.loc[0, "value"] == 1.0
    assert result.loc[2, "value"] == 3.0


def test_correct_outliers_interpolate():
    """Test interpolating outliers."""
    data = pd.DataFrame({"value": [1.0, 100.0, 3.0]})
    result = correct_outliers(
        data, outlier_indices=[1], value_col="value", method="interpolate"
    )
    assert result.loc[1, "value"] == 2.0  # (1.0 + 3.0) / 2
    assert result.loc[0, "value"] == 1.0
    assert result.loc[2, "value"] == 3.0


def test_correct_outliers_median():
    """Test replacing outliers with the median of the window."""
    # window_size = 3 (padding = 1 on each side)
    # Outlier at index 2 (100.0). Window is [2.0, NaN, 4.0] -> Median 3.0
    data = pd.DataFrame({"value": [1.0, 2.0, 100.0, 4.0, 5.0]})
    result = correct_outliers(
        data, outlier_indices=[2], value_col="value", window_size=3, method="median"
    )
    assert result.loc[2, "value"] == 3.0

    # window_size = 5 (padding = 2 on each side)
    # Window is [1.0, 2.0, NaN, 4.0, 5.0] -> Median 3.0
    result2 = correct_outliers(
        data, outlier_indices=[2], value_col="value", window_size=5, method="median"
    )
    assert result2.loc[2, "value"] == 3.0


def test_correct_outliers_mean():
    """Test replacing outliers with the mean of the window."""
    # window_size = 3 (padding = 1 on each side)
    # Outlier at index 2. Window is [2.0, NaN, 4.0] -> Mean 3.0
    data = pd.DataFrame({"value": [1.0, 2.0, 100.0, 4.0, 5.0]})
    result = correct_outliers(
        data, outlier_indices=[2], value_col="value", window_size=3, method="mean"
    )
    assert result.loc[2, "value"] == 3.0

    # window_size = 3. Window [2.0, NaN, 10.0] -> Mean 6.0
    data2 = pd.DataFrame({"value": [1.0, 2.0, 100.0, 10.0, 5.0]})
    result3 = correct_outliers(
        data2, outlier_indices=[2], value_col="value", window_size=3, method="mean"
    )
    assert result3.loc[2, "value"] == 6.0

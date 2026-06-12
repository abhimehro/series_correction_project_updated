import logging
import pytest
import pandas as pd
import numpy as np

from scripts.processor import detect_outliers, process_data


def test_detect_outliers_basic():
    """Test detecting a clear outlier in a mostly stable series."""
    data = pd.DataFrame({
        "value": [1.0, 1.1, 0.9, 1.0, 100.0, 1.2, 0.8, 1.0, 1.1, 0.9]
    })

    # window_size=5, outlier at index 4 (100.0)
    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == [4]


def test_detect_outliers_no_outliers():
    """Test series with normal variance and no outliers."""
    data = pd.DataFrame({
        "value": [1.0, 1.1, 0.9, 1.0, 1.2, 0.8, 1.0, 1.1, 0.9]
    })

    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == []


def test_detect_outliers_small_data():
    """Test dataframe smaller than the window size."""
    data = pd.DataFrame({
        "value": [1.0, 1.1, 100.0]
    })

    # window_size is 5, but data length is 3
    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == []


def test_detect_outliers_with_nans():
    """Test handling of NaNs in the data."""
    data = pd.DataFrame({
        "value": [1.0, 1.1, np.nan, 1.0, 100.0, 1.2, np.nan, 1.0, 1.1, 0.9]
    })

    # The outlier 100.0 is at index 4
    outliers = detect_outliers(data, value_col="value", window_size=3, threshold=3.0)
    assert outliers == [4]


def test_detect_outliers_zero_mad():
    """Test when the median absolute deviation is zero (identical values)."""
    data = pd.DataFrame({
        "value": [1.0, 1.0, 1.0, 100.0, 1.0, 1.0, 1.0]
    })

    # window_size=5, outlier at index 3 (100.0)
    outliers = detect_outliers(data, value_col="value", window_size=5, threshold=3.0)
    assert outliers == [3]

import logging
import pytest
from scripts.processor import process_data


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

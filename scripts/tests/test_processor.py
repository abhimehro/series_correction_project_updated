import logging

import numpy as np
import pandas as pd
import pytest

from scripts.processor import correct_jumps, detect_outliers, process_data


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

    with caplog.at_level(logging.ERROR), pytest.raises(
        ValueError, match="Time column is not numeric and could not be converted"
    ):
        process_data(df)

    assert (
        "Time column 'Time (Seconds)' is not numeric and could not be converted"
        in caplog.text
    )


def test_correct_jumps_empty():
    """Test correct_jumps with empty jump_indices list."""
    data = pd.DataFrame({"value": [1.0, 1.1, 1.2, 1.0, 1.1]})
    result = correct_jumps(data, jump_indices=[], value_col="value")
    pd.testing.assert_frame_equal(data, result)


def test_correct_jumps_basic():
    """Test basic jump correction."""
    # Base level 1.0, jumps to 10.0 at index 5
    data = pd.DataFrame(
        {"value": [1.0, 1.0, 1.0, 1.0, 1.0, 10.0, 10.0, 10.0, 10.0, 10.0]}
    )
    # window_size=2: median before=[1.0, 1.0]=1.0, after=[10.0, 10.0]=10.0
    # diff = 1.0 - 10.0 = -9.0. Added from index 5 onwards.
    result = correct_jumps(data, jump_indices=[5], value_col="value", window_size=2)
    expected = pd.DataFrame({"value": [1.0] * 10})
    pd.testing.assert_frame_equal(expected, result)


def test_correct_jumps_multiple():
    """Test multiple sequential jumps."""
    data = pd.DataFrame(
        {
            "value": [
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                10.0,
                10.0,
                10.0,
                10.0,
                10.0,
                5.0,
                5.0,
                5.0,
                5.0,
                5.0,
            ]
        }
    )
    # Jump 1 at 5. Offset=-9. Result after jump 1: [..., 1.0, 1.0, ..., -4.0, -4.0, ...]
    # Jump 2 at 10. The original values are used for windows!
    # Wait, the code calculates all offsets simultaneously based on original data!
    # At index 5: diff = 1.0 - 10.0 = -9.0.
    # At index 10: diff = 10.0 - 5.0 = 5.0.
    # Offsets array at valid_jumps: [5] = -9.0, [10] = 5.0
    # cumsum(offsets) applied to values_np:
    # 0-4: +0 = 1.0
    # 5-9: 10.0 + (-9.0) = 1.0
    # 10-14: 5.0 + (-9.0 + 5.0) = 5.0 - 4.0 = 1.0
    result = correct_jumps(data, jump_indices=[5, 10], value_col="value", window_size=2)
    expected = pd.DataFrame({"value": [1.0] * 15})
    pd.testing.assert_frame_equal(expected, result)


def test_correct_jumps_invalid_indices():
    """Test jumps too close to boundaries are ignored."""
    data = pd.DataFrame({"value": [1.0] * 10})
    # window_size=3, valid indices are 3 to 6
    result = correct_jumps(
        data, jump_indices=[0, 1, 2, 7, 8, 9], value_col="value", window_size=3
    )
    pd.testing.assert_frame_equal(data, result)


def test_correct_jumps_with_nans():
    """Test jump correction when there are NaNs in windows."""
    # data: [1.0, NaN, 1.0, 10.0, NaN, 10.0]
    data = pd.DataFrame({"value": [1.0, np.nan, 1.0, 10.0, np.nan, 10.0, 10.0, 10.0]})
    # Jump at index 3. window_size=3
    # before_window (idx 0,1,2): [1.0, NaN, 1.0], median = 1.0
    # after_window (idx 3,4,5): [10.0, NaN, 10.0], median = 10.0
    # diff = -9.0
    result = correct_jumps(data, jump_indices=[3], value_col="value", window_size=3)
    expected = pd.DataFrame({"value": [1.0, np.nan, 1.0, 1.0, np.nan, 1.0, 1.0, 1.0]})
    pd.testing.assert_frame_equal(expected, result)

import logging

import numpy as np
import pandas as pd

from scripts.processor import correct_gaps


def test_correct_gaps_empty_indices():
    """Test correct_gaps with empty gap_indices returns a copy of input."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 3.0],
            "Value": [10.0, 20.0, 30.0],
        }
    )
    result = correct_gaps(data, gap_indices=[], time_col="Time (Seconds)")
    pd.testing.assert_frame_equal(data, result)
    # Check that it is a shallow copy (different DataFrame instance)
    assert result is not data


def test_correct_gaps_auto_detect_value_cols():
    """Test that value_cols is auto-detected from numeric columns when None."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 5.0],
            "Sensor_A": [10.0, 20.0, 50.0],
            "Sensor_B": [1.0, 2.0, 5.0],
            "Tag": ["A", "B", "C"],
        }
    )
    # Gap is at index 2 (between 2.0 and 5.0 with normal step 1.0 -> fills 3.0 and 4.0)
    result = correct_gaps(
        data, gap_indices=[2], time_col="Time (Seconds)", value_cols=None
    )

    assert len(result) == 5
    assert list(result["Time (Seconds)"]) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert list(result["Sensor_A"]) == [10.0, 20.0, 30.0, 40.0, 50.0]
    assert list(result["Sensor_B"]) == [1.0, 2.0, 3.0, 4.0, 5.0]


def test_correct_gaps_no_numeric_value_columns(caplog):
    """Test correct_gaps when there are no numeric value columns to interpolate."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 5.0],
            "Tag": ["A", "B", "C"],
        }
    )
    with caplog.at_level(logging.WARNING):
        result = correct_gaps(
            data, gap_indices=[2], time_col="Time (Seconds)", value_cols=None
        )

    assert (
        "No numeric value columns found to interpolate for gap correction."
        in caplog.text
    )
    pd.testing.assert_frame_equal(result, data)


def test_correct_gaps_explicit_value_cols():
    """Test specifying explicit value_cols, leaving other numeric columns unmutated/unfilled."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 4.0],
            "Sensor_1": [10.0, 20.0, 40.0],
            "Sensor_2": [100.0, 200.0, 400.0],
        }
    )
    result = correct_gaps(
        data,
        gap_indices=[2],
        time_col="Time (Seconds)",
        value_cols=["Sensor_1"],
        method="linear",
    )
    assert len(result) == 4
    assert list(result["Time (Seconds)"]) == [1.0, 2.0, 3.0, 4.0]
    assert list(result["Sensor_1"]) == [10.0, 20.0, 30.0, 40.0]
    # Sensor_2 was not specified in value_cols, so the newly inserted row at index 2 has NaN
    assert np.isnan(result.loc[result["Time (Seconds)"] == 3.0, "Sensor_2"].iloc[0])


def test_correct_gaps_time_interpolation_fallback(caplog):
    """Test method='time' falls back to 'linear' when index is not a DatetimeIndex."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 4.0],
            "Value": [10.0, 20.0, 40.0],
        }
    )
    with caplog.at_level(logging.WARNING):
        result = correct_gaps(
            data,
            gap_indices=[2],
            time_col="Time (Seconds)",
            value_cols=["Value"],
            method="time",
        )

    assert (
        "Cannot use 'time' interpolation without a valid time column index"
        in caplog.text
    )
    assert len(result) == 4
    assert list(result["Time (Seconds)"]) == [1.0, 2.0, 3.0, 4.0]
    assert list(result["Value"]) == [10.0, 20.0, 30.0, 40.0]


def test_correct_gaps_multiple_gaps():
    """Test correct_gaps with multiple gap indices."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 5.0, 6.0, 9.0],
            "Value": [1.0, 2.0, 5.0, 6.0, 9.0],
        }
    )
    # Gaps at index 2 (after 2.0, before 5.0) and index 4 (after 6.0, before 9.0)
    result = correct_gaps(
        data,
        gap_indices=[2, 4],
        time_col="Time (Seconds)",
        value_cols=["Value"],
        method="linear",
    )
    expected_times = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    assert list(result["Time (Seconds)"]) == expected_times
    assert list(result["Value"]) == expected_times


def test_correct_gaps_invalid_step_or_no_missing_points():
    """Test when gap parameters produce no missing points (gaps_df is None)."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 2.0],
            "Value": [10.0, 20.0, 20.0],
        }
    )
    # Gap index 2 has diff 0.0, num_missing_points <= 0 -> gaps_df is None
    result = correct_gaps(
        data,
        gap_indices=[2],
        time_col="Time (Seconds)",
        value_cols=["Value"],
    )
    assert len(result) == 3
    assert result["Value"].tolist() == [10.0, 20.0, 20.0]


def test_correct_gaps_boundary_zero_index():
    """Test gap index 0 is safely ignored by _build_gaps_dataframe."""
    data = pd.DataFrame(
        {
            "Time (Seconds)": [1.0, 2.0, 3.0],
            "Value": [10.0, 20.0, 30.0],
        }
    )
    result = correct_gaps(
        data,
        gap_indices=[0],
        time_col="Time (Seconds)",
        value_cols=["Value"],
    )
    assert len(result) == 3
    pd.testing.assert_frame_equal(result, data)

import pandas as pd
from scripts.processor import (
    correct_gaps,
    correct_outliers,
    correct_jumps,
    process_data,
)


def test_correct_gaps_not_empty():
    data = pd.DataFrame({"time": [1, 2, 3], "value": [10.0, None, 30.0]})
    gap_indices = [1]
    result = correct_gaps(
        data, gap_indices, time_col="time", value_cols=["value"], method="linear"
    )
    assert not result["value"].isnull().any()


def test_correct_outliers_not_empty():
    data = pd.DataFrame({"time": [1, 2, 3], "value": [10.0, 100.0, 30.0]})
    outlier_indices = [1]
    result = correct_outliers(
        data, outlier_indices, value_col="value", method="interpolate"
    )
    assert result["value"][1] != 100.0


def test_correct_jumps_not_empty():
    data = pd.DataFrame({"time": [1, 2, 3], "value": [10.0, 100.0, 100.0]})
    jump_indices = [1]
    result = correct_jumps(data, jump_indices, value_col="value")
    assert result is not None


def test_process_data_shallow_copy():
    data = pd.DataFrame({"Time (Seconds)": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    result = process_data(data, config={"time_col": "Time (Seconds)"})
    assert isinstance(result, pd.DataFrame)
    assert not result.empty

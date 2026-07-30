import pytest
import numpy as np
import pandas as pd
from scripts.discontinuity_utils import (
    _calculate_normal_step,
    _is_valid_step,
    _validate_gap_parameters
)

def test_calculate_normal_step_with_prior():
    time_col_arr = [0, 10, 20, 50, 60]
    idx_before = 2  # value 20
    idx_after = 3   # value 50
    max_len = 5

    # Expected: time_col_arr[2] - time_col_arr[1] = 20 - 10 = 10
    step = _calculate_normal_step(time_col_arr, idx_before, idx_after, max_len)
    assert step == 10

def test_calculate_normal_step_no_prior():
    time_col_arr = [0, 50, 60, 70]
    idx_before = 0  # value 0
    idx_after = 1   # value 50
    max_len = 4

    # Expected: time_col_arr[2] - time_col_arr[1] = 60 - 50 = 10
    step = _calculate_normal_step(time_col_arr, idx_before, idx_after, max_len)
    assert step == 10

def test_calculate_normal_step_no_prior_no_after():
    time_col_arr = [0, 50]
    idx_before = 0
    idx_after = 1
    max_len = 2

    # Expected: None, because idx_before == 0 and max_len <= idx_after + 1 (2 <= 2)
    step = _calculate_normal_step(time_col_arr, idx_before, idx_after, max_len)
    assert step is None

def test_is_valid_step_numeric():
    assert _is_valid_step(10) is True
    assert _is_valid_step(0) is False
    assert _is_valid_step(-5) is False

def test_is_valid_step_timedelta():
    assert _is_valid_step(pd.Timedelta("1s")) is True
    assert _is_valid_step(pd.Timedelta("0s")) is False
    assert _is_valid_step(pd.Timedelta("-1s")) is False

def test_is_valid_step_np_timedelta():
    assert bool(_is_valid_step(np.timedelta64(1, "s"))) is True
    assert bool(_is_valid_step(np.timedelta64(0, "s"))) is False
    assert bool(_is_valid_step(np.timedelta64(-1, "s"))) is False

def test_validate_gap_parameters_normal_step_none():
    assert _validate_gap_parameters(1, None, 10, 20) is None

def test_validate_gap_parameters_invalid_step():
    assert _validate_gap_parameters(1, 0, 10, 20) is None
    assert _validate_gap_parameters(1, -1, 10, 20) is None

def test_validate_gap_parameters_valid():
    # step = 10, time_before = 10, time_after = 40
    # num_missing_points = round((40 - 10) / 10) - 1 = round(3) - 1 = 2
    assert _validate_gap_parameters(1, 10, 10, 40) == 2

def test_validate_gap_parameters_zero_missing_points():
    # step = 10, time_before = 10, time_after = 20
    # num_missing_points = round((20 - 10) / 10) - 1 = 0
    assert _validate_gap_parameters(1, 10, 10, 20) is None

def test_validate_gap_parameters_negative_missing_points():
    # step = 10, time_before = 10, time_after = 15
    # num_missing_points = round((15 - 10) / 10) - 1 = 0 - 1 = -1
    assert _validate_gap_parameters(1, 10, 10, 15) is None

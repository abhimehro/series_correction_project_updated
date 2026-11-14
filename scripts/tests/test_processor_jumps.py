import numpy as np
import pandas as pd

from scripts.processor import _calculate_jump_deviations, detect_jumps


def test_calculate_jump_deviations():
    values = np.array([1.0, 1.0, 1.0, 10.0, 10.0, 10.0])
    rolling_mean = np.array([np.nan, np.nan, 1.0, 4.0, 7.0, 10.0])
    rolling_std = np.array([np.nan, np.nan, 0.0, 5.196, 5.196, 0.0])
    n = len(values)
    window_size = 3

    deviations = _calculate_jump_deviations(
        values, rolling_mean, rolling_std, window_size, n
    )
    assert len(deviations) == n


def test_detect_jumps_empty_or_small():
    data = pd.DataFrame({"value": [1.0, 2.0]})
    jumps = detect_jumps(data, "value", window_size=3, threshold=2.0)
    assert jumps == []


def test_detect_jumps_basic():
    # Base level 1.0, jumps to 10.0 at index 5
    data = pd.DataFrame(
        {"value": [1.0, 1.0, 1.1, 0.9, 1.0, 10.0, 10.1, 9.9, 10.0, 10.0]}
    )
    jumps = detect_jumps(data, "value", window_size=3, threshold=3.0)
    assert jumps == [5]


def test_detect_jumps_multiple():
    # Level 1.0, jumps to 10.0 at index 5, then jumps to 20.0 at index 10
    data = pd.DataFrame(
        {
            "value": [
                1.0,
                1.0,
                1.1,
                0.9,
                1.0,
                10.0,
                10.1,
                9.9,
                10.0,
                10.0,
                20.0,
                20.1,
                19.9,
                20.0,
                20.0,
            ]
        }
    )
    jumps = detect_jumps(data, "value", window_size=3, threshold=3.0)
    assert jumps == [5, 10]

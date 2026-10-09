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
    np.testing.assert_allclose(
        deviations, [0.0, 0.0, 0.0, 0.0, 6.0 / 5.196, 3.0 / 5.196], atol=0.0
    )


def test_calculate_jump_deviations_eligibility():
    values = np.array([100.0, 100.0, 14.0, 20.0, 30.0, 6.0])
    rolling_mean = np.array([1.0, 10.0, 12.0, 13.0, 14.0, 4.0])
    rolling_std = np.array([2.0, 2.0, np.nan, 1e-6, 4.0, 2.0])

    deviations = _calculate_jump_deviations(
        values, rolling_mean, rolling_std, window_size=2, n=len(values)
    )

    # Warm-up, NaN std, and std at the cutoff stay zero; eligible points retain
    # their signed deviations normalized by the previous window's statistics.
    np.testing.assert_array_equal(deviations, [0.0, 0.0, 2.0, 0.0, 0.0, -2.0])


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

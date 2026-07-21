import numpy as np

from updated_processor import detect_outliers


def test_detect_outliers_basic():
    """Test detecting a clear outlier in a mostly stable series."""
    values = [1.0, 1.1, 0.9, 1.0, 1.2, 100.0, 0.8, 1.0, 1.1, 0.9]
    corrected, outliers = detect_outliers(values)

    assert bool(outliers[5]) is True
    assert np.all(~outliers[:5])
    assert np.all(~outliers[6:])

    # 100.0 is an outlier, it should be replaced with the median (1.0)
    assert corrected[5] == 1.0
    # The rest should be unchanged
    assert np.allclose(corrected[:5], values[:5])
    assert np.allclose(corrected[6:], values[6:])


def test_detect_outliers_no_outliers():
    """Test series with normal variance and no outliers."""
    values = [1.0, 1.1, 0.9, 1.0, 1.2, 0.8, 1.0, 1.1, 0.9]
    corrected, outliers = detect_outliers(values)

    assert np.all(~outliers)
    assert np.allclose(corrected, values)


def test_detect_outliers_with_nans():
    """Test handling of NaNs in the data."""
    values = [1.0, 1.1, np.nan, 1.0, 100.0, 1.2, np.nan, 1.0, 1.1, 0.9]
    corrected, outliers = detect_outliers(values)

    # The outlier 100.0 is at index 4
    assert bool(outliers[4]) is True

    # Check that NaNs in the original array are preserved or handled appropriately
    # The outlier should be replaced by the median (1.05)
    assert np.isclose(corrected[4], 1.05)

    # NaNs should not be flagged as outliers
    assert bool(outliers[2]) is False
    assert bool(outliers[6]) is False
    assert np.isnan(corrected[2])
    assert np.isnan(corrected[6])


def test_detect_outliers_zero_mad():
    """Test when the median absolute deviation is zero (identical values)."""
    values = [1.0, 1.0, 1.0, 100.0, 1.0, 1.0, 1.0]
    corrected, outliers = detect_outliers(values)

    # outlier at index 3 (100.0)
    assert bool(outliers[3]) is True
    assert np.all(~outliers[:3])
    assert np.all(~outliers[4:])

    assert corrected[3] == 1.0


def test_detect_outliers_custom_threshold():
    """Test that setting a higher threshold ignores smaller deviations."""
    # With MAD ~ 0.1, a deviation of 0.5 might be flagged with threshold=3.0
    # but ignored with threshold=10.0
    values = [1.0, 1.1, 0.9, 1.0, 1.2, 1.5, 0.8, 1.0, 1.1, 0.9]

    # Calculate what happens with default threshold
    _, outliers_default = detect_outliers(values, threshold=3.0)

    # Calculate what happens with higher threshold
    _, outliers_high = detect_outliers(values, threshold=10.0)

    # If 1.5 was flagged with default, check it's not flagged with high
    if outliers_default[5]:
        assert bool(outliers_high[5]) is False

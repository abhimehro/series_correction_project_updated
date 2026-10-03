import numpy as np
import pandas as pd

from scripts.processor import correct_outliers


def test_correct_outliers_median():
    """Test replacing outliers using median replacement method."""
    df = pd.DataFrame({"Value": [1.0, 1.1, 100.0, 1.0, 1.1]})
    res = correct_outliers(df, [2], value_col="Value", window_size=5, method="median")
    assert np.isclose(res.loc[2, "Value"], 1.05)


def test_correct_outliers_mean():
    """Test replacing outliers using mean replacement method."""
    df = pd.DataFrame({"Value": [1.0, 1.1, 100.0, 1.0, 1.1]})
    res = correct_outliers(df, [2], value_col="Value", window_size=5, method="mean")
    assert np.isclose(res.loc[2, "Value"], 1.05)

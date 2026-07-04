import pandas as pd
import numpy as np
from unittest.mock import patch

from scripts.export_comparison_sheets import (
    detect_outliers_series,
    find_matching_raw_file,
)


def test_detect_outliers_series_basic():
    """Test detecting an obvious outlier."""
    values = pd.Series([1.0, 1.1, 0.9, 1.0, 100.0, 1.2, 0.8, 1.0, 1.1, 0.9])
    outliers = detect_outliers_series(values, window_size=5, threshold=3.0)
    assert outliers == [4]


def test_detect_outliers_series_short():
    """Test with a series shorter than window_size."""
    values = pd.Series([1.0, 100.0, 1.0])
    outliers = detect_outliers_series(values, window_size=5, threshold=3.0)
    assert outliers == []


def test_detect_outliers_series_flat():
    """Test with a flat series (zero MAD)."""
    values = pd.Series([1.0, 1.0, 1.0, 1.0, 1.0, 100.0, 1.0, 1.0, 1.0, 1.0])
    outliers = detect_outliers_series(values, window_size=5, threshold=3.0)
    assert outliers == [5]


def test_detect_outliers_series_with_nans():
    """Test handling of NaNs."""
    values = pd.Series([1.0, 1.1, np.nan, 1.0, 100.0, 1.2, np.nan, 1.0, 1.1, 0.9])
    outliers = detect_outliers_series(values, window_size=3, threshold=3.0)
    assert outliers == [4]


@patch("scripts.export_comparison_sheets.os.path.isfile")
def test_find_matching_raw_file_series_format(mock_isfile):
    """Test matching 'Series26_File01_Processed.xlsx' format."""
    mock_isfile.return_value = True
    result = find_matching_raw_file("Series26_File01_Processed.xlsx")
    assert result is not None
    assert "S26_Y01.txt" in result


@patch("scripts.export_comparison_sheets.os.listdir")
def test_find_matching_raw_file_year_format(mock_listdir):
    """Test matching 'Year_1995 (Y01)_Data.xlsx' format."""
    mock_listdir.return_value = ["S26_Y01.txt", "S26_Y02.txt"]
    result = find_matching_raw_file("Year_1995 (Y01)_Data.xlsx")
    assert result is not None
    assert "S26_Y01.txt" in result


def test_find_matching_raw_file_no_match():
    """Test when filename matches no known pattern."""
    result = find_matching_raw_file("Unknown_Format.xlsx")
    assert result is None

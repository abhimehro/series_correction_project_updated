import pandas as pd
import pytest
import numpy as np
from scripts.apply_refined_corrections import calculate_non_zero_average

def test_calculate_non_zero_average_basic():
    """Test with basic numeric values."""
    series = pd.Series([1.0, 2.0, 3.0])
    assert calculate_non_zero_average(series) == 2.0

def test_calculate_non_zero_average_with_zeros():
    """Test with zeros included."""
    series = pd.Series([1.0, 0.0, 3.0, 0.0])
    assert calculate_non_zero_average(series) == 2.0

def test_calculate_non_zero_average_with_nans():
    """Test with NaNs included."""
    series = pd.Series([1.0, np.nan, 3.0])
    assert calculate_non_zero_average(series) == 2.0

def test_calculate_non_zero_average_mixed_zeros_nans():
    """Test with both zeros and NaNs."""
    series = pd.Series([1.0, 0.0, np.nan, 3.0])
    assert calculate_non_zero_average(series) == 2.0

def test_calculate_non_zero_average_all_zeros():
    """Test with all zero values."""
    series = pd.Series([0.0, 0.0, 0.0])
    assert calculate_non_zero_average(series) == 0.0

def test_calculate_non_zero_average_empty():
    """Test with an empty series."""
    series = pd.Series([], dtype=float)
    assert calculate_non_zero_average(series) == 0.0

def test_calculate_non_zero_average_all_nans():
    """Test with all NaN values."""
    series = pd.Series([np.nan, np.nan])
    assert calculate_non_zero_average(series) == 0.0

def test_calculate_non_zero_average_numeric_strings():
    """Test with numeric strings that need coercion."""
    series = pd.Series(["1", "2", "3"])
    assert calculate_non_zero_average(series) == 2.0

def test_calculate_non_zero_average_non_numeric_strings():
    """Test with non-numeric strings that coerce to NaN."""
    series = pd.Series(["a", "b"])
    assert calculate_non_zero_average(series) == 0.0

def test_calculate_non_zero_average_mixed_strings():
    """Test with mixed numeric and unparseable strings."""
    series = pd.Series(["1", "a", "3"])
    assert calculate_non_zero_average(series) == 2.0

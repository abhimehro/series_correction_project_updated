"""Tests for discontinuity_utils module error handling and fallback behavior."""

import logging
from unittest.mock import patch

import pandas as pd
import pytest

from scripts.discontinuity_utils import _convert_time_col_to_numeric


def test_convert_time_col_success():
    """Verify successful conversion of datetime string column to numeric unix timestamp."""
    input_df = pd.DataFrame(
        {"time": ["1970-01-01 00:00:00", "1970-01-01 00:00:10"], "val": [1, 2]}
    )
    result = _convert_time_col_to_numeric(input_df, "time")
    assert list(result["time"]) == [0, 10]


def test_convert_time_col_unparseable(caplog):
    """Verify that unparseable datetime string raises ValueError and logs error."""
    input_df = pd.DataFrame({"time": ["not_a_time", "also_not_a_time"], "val": [1, 2]})
    with caplog.at_level(logging.ERROR), pytest.raises(
        ValueError, match="Time column is not numeric and could not be converted"
    ):
        _convert_time_col_to_numeric(input_df, "time")

    assert "Time column 'time' is not numeric and could not be converted" in caplog.text


def test_convert_time_col_datetime_error(caplog):
    """Verify that unexpected exceptions in pd.to_datetime raise ValueError and log error."""
    input_df = pd.DataFrame(
        {"time": ["1970-01-01 00:00:00", "1970-01-01 00:00:10"], "val": [1, 2]}
    )
    with patch(
        "scripts.discontinuity_utils.pd.to_datetime",
        side_effect=RuntimeError("to_datetime unexpected failure"),
    ), caplog.at_level(logging.ERROR), pytest.raises(
        ValueError, match="Time column is not numeric and could not be converted"
    ):
        _convert_time_col_to_numeric(input_df, "time")

    assert "Time column 'time' is not numeric and could not be converted" in caplog.text

"""Security tests for scripts/batch_correction.py."""

import os
from unittest.mock import patch

import pytest

from scripts.batch_correction import _enrich_config_with_river_mappings


def test_enrich_config_blocks_path_traversal(caplog: pytest.LogCaptureFixture) -> None:
    """Test that path traversal attempts in RIVER_MILE_MAP_PATH are blocked."""
    config_data = {"RIVER_MILE_MAP_PATH": "../../../etc/passwd"}

    with patch("os.path.isfile") as mock_isfile, patch(
        "pandas.read_csv"
    ) as mock_read_csv:
        _enrich_config_with_river_mappings(config_data)

        # Ensure containment check prevented calling os.path.isfile and pd.read_csv
        mock_isfile.assert_not_called()
        mock_read_csv.assert_not_called()

    assert "SENSOR_TO_RIVER" not in config_data
    assert "Path traversal attempt detected in RIVER_MILE_MAP_PATH" in caplog.text


def test_enrich_config_allows_valid_relative_path(
    tmp_path: os.PathLike, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a valid relative path within the working directory is allowed."""
    working_dir = os.path.realpath(os.getcwd())
    valid_file = os.path.join(working_dir, "scripts", "test_river_map.csv")

    config_data = {"RIVER_MILE_MAP_PATH": valid_file}

    with patch("os.path.isfile", return_value=False) as mock_isfile:
        _enrich_config_with_river_mappings(config_data)
        mock_isfile.assert_called_once_with(os.path.realpath(valid_file))

    assert "Path traversal attempt detected" not in caplog.text

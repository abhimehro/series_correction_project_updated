"""
Security tests for batch_correction module (CWE-22 Path Traversal prevention).
"""

import logging
from unittest.mock import patch

from scripts.batch_correction import _enrich_config_with_river_mappings


def test_enrich_config_with_river_mappings_path_traversal(caplog):
    """Test that escaping path traversal attempts in RIVER_MILE_MAP_PATH are rejected."""
    config_data = {"RIVER_MILE_MAP_PATH": "../../../../etc/passwd"}

    with patch("pandas.read_csv") as mock_read_csv, caplog.at_level(logging.WARNING):
        _enrich_config_with_river_mappings(config_data)

        # Ensure pandas.read_csv was NOT called for the unsafe path
        mock_read_csv.assert_not_called()

        # Ensure a path traversal warning was logged
        assert "Path traversal detected in RIVER_MILE_MAP_PATH" in caplog.text

        # Ensure config_data was not populated with mappings from the unsafe file
        assert "SENSOR_TO_RIVER" not in config_data
        assert "RIVER_TO_SENSORS" not in config_data


def test_enrich_config_with_river_mappings_value_error(caplog):
    """Test that ValueError from os.path.commonpath is handled safely."""
    config_data = {"RIVER_MILE_MAP_PATH": "D:\\some\\other\\drive\\map.csv"}

    with patch("os.path.commonpath", side_effect=ValueError("Different drives")), patch(
        "pandas.read_csv"
    ) as mock_read_csv, caplog.at_level(logging.WARNING):
        _enrich_config_with_river_mappings(config_data)

        mock_read_csv.assert_not_called()
        assert "Path traversal detected in RIVER_MILE_MAP_PATH" in caplog.text
        assert "SENSOR_TO_RIVER" not in config_data
        assert "RIVER_TO_SENSORS" not in config_data

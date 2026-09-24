"""
Security unit tests for path traversal prevention in batch_correction module.
"""

import scripts.batch_correction as bc


def test_enrich_config_river_mile_map_path_traversal(caplog):
    """Test that path traversal attempts in RIVER_MILE_MAP_PATH are rejected and logged."""
    config_data = {"RIVER_MILE_MAP_PATH": "../../../etc/passwd"}
    bc._enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" not in config_data
    assert "Path traversal attempt detected in RIVER_MILE_MAP_PATH" in caplog.text


def test_enrich_config_river_mile_map_invalid_path_value_error(caplog, mocker):
    """Test ValueError handling during path validation in _enrich_config_with_river_mappings."""
    mocker.patch("os.path.commonpath", side_effect=ValueError("Paths differ"))
    config_data = {"RIVER_MILE_MAP_PATH": "D:\\some\\path"}
    bc._enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" not in config_data
    assert "Invalid path in RIVER_MILE_MAP_PATH" in caplog.text

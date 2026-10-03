from unittest.mock import patch

from scripts.batch_correction import _enrich_config_with_river_mappings


def test_river_mile_map_path_traversal_rejected(caplog):
    config_data = {"RIVER_MILE_MAP_PATH": "../../../../etc/passwd"}
    _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" not in config_data
    assert "Path traversal attempt in RIVER_MILE_MAP_PATH" in caplog.text


def test_river_mile_map_path_traversal_value_error(caplog):
    config_data = {"RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv"}
    with patch(
        "os.path.commonpath", side_effect=ValueError("Paths on different drives")
    ):
        _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" not in config_data
    assert "Path traversal attempt in RIVER_MILE_MAP_PATH" in caplog.text

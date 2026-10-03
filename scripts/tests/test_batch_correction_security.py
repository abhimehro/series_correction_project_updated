from unittest.mock import patch

import scripts.batch_correction as bc


def test_path_traversal_relative_and_absolute_rejection(caplog):
    """Test that relative and absolute path traversal attempts in RAW_DATA_DIR and RIVER_MILE_MAP_PATH are rejected."""
    caplog.set_level("WARNING")

    # Relative path traversal
    config_data_rel = {
        "RAW_DATA_DIR": "../../../../etc",
        "RIVER_MILE_MAP_PATH": "../../../../etc/passwd",
    }
    dir_result_rel = bc._get_data_directory(config_data_rel, create_if_missing=False)
    assert "../../../../etc" not in dir_result_rel

    bc._enrich_config_with_river_mappings(config_data_rel)
    assert "SENSOR_TO_RIVER" not in config_data_rel

    # Absolute path traversal with '..'
    config_data_abs = {
        "RAW_DATA_DIR": "/app/../../etc/passwd",
        "RIVER_MILE_MAP_PATH": "/app/../../etc/passwd",
    }
    dir_result_abs = bc._get_data_directory(config_data_abs, create_if_missing=False)
    assert "/etc/passwd" not in dir_result_abs

    bc._enrich_config_with_river_mappings(config_data_abs)
    assert "SENSOR_TO_RIVER" not in config_data_abs

    assert "escapes working directory" in caplog.text


def test_path_traversal_value_error_handling(caplog):
    """Test that ValueError raised during commonpath calculation is caught and handled safely."""
    caplog.set_level("WARNING")

    config_data = {
        "RAW_DATA_DIR": "some/path",
        "RIVER_MILE_MAP_PATH": "some/mapping.csv",
    }

    with patch("os.path.commonpath", side_effect=ValueError("Different drives")):
        dir_result = bc._get_data_directory(config_data, create_if_missing=False)
        assert dir_result != "some/path"

        bc._enrich_config_with_river_mappings(config_data)
        assert "SENSOR_TO_RIVER" not in config_data

    assert "escapes working directory" in caplog.text

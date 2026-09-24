import logging

from scripts.batch_correction import _enrich_config_with_river_mappings


def test_enrich_config_with_river_mappings_path_traversal(caplog):
    """Verify that RIVER_MILE_MAP_PATH traversing outside working directory is rejected."""
    config_data = {"RIVER_MILE_MAP_PATH": "../../../../etc/passwd"}

    with caplog.at_level(logging.WARNING):
        _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" not in config_data
    assert "RIVER_TO_SENSORS" not in config_data
    assert "Path traversal detected in RIVER_MILE_MAP_PATH" in caplog.text


def test_enrich_config_with_river_mappings_valid_path(tmp_path, monkeypatch):
    """Verify that a valid RIVER_MILE_MAP_PATH inside working directory is loaded correctly."""
    monkeypatch.setattr("os.getcwd", lambda: str(tmp_path))

    csv_file = tmp_path / "river_map.csv"
    csv_file.write_text("SENSOR_ID,RIVER_MILE\n1,54.0\n2,53.0\n")

    config_data = {"RIVER_MILE_MAP_PATH": str(csv_file)}
    _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" in config_data
    assert config_data["SENSOR_TO_RIVER"] == {1: 54.0, 2: 53.0}

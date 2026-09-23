import os
from scripts.batch_correction import _enrich_config_with_river_mappings


def test_enrich_config_path_traversal_prevented(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    outside_file = tmp_path.parent / "secret_river_map.csv"
    outside_file.write_text("SENSOR_ID,RIVER_MILE\n1,10.0\n")

    # Attempt path traversal pointing to file outside working directory
    config_data = {"RIVER_MILE_MAP_PATH": str(outside_file)}

    _enrich_config_with_river_mappings(config_data)

    # SENSOR_TO_RIVER should NOT be loaded due to path traversal prevention
    assert "SENSOR_TO_RIVER" not in config_data


def test_enrich_config_valid_path(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    valid_file = tmp_path / "river_map.csv"
    valid_file.write_text("SENSOR_ID,RIVER_MILE\n101,50.5\n")

    config_data = {"RIVER_MILE_MAP_PATH": str(valid_file)}

    _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" in config_data
    assert config_data["SENSOR_TO_RIVER"] == {101: 50.5}

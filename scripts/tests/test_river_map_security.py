import os

import pandas as pd

from scripts.batch_correction import _enrich_config_with_river_mappings


def test_enrich_config_path_traversal(caplog):
    caplog.set_level("WARNING")
    config_data = {"RIVER_MILE_MAP_PATH": "../../../../etc/passwd"}
    _enrich_config_with_river_mappings(config_data)

    assert "Path traversal detected in RIVER_MILE_MAP_PATH" in caplog.text
    assert "SENSOR_TO_RIVER" not in config_data


def test_enrich_config_valid_path(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    map_file = tmp_path / "river_mile_map.csv"
    df = pd.DataFrame({"SENSOR_ID": [10, 11], "RIVER_MILE": [50.0, 51.0]})
    df.to_csv(map_file, index=False)

    config_data = {"RIVER_MILE_MAP_PATH": str(map_file)}
    _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" in config_data
    assert config_data["SENSOR_TO_RIVER"] == {10: 50.0, 11: 51.0}

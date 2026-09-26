import json
import os

import pytest

from scripts.loaders import load_config


def test_load_config_valid_path(tmp_path, monkeypatch):
    # Mock os.getcwd() to be the tmp_path so the traversal check passes
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))

    config_file = tmp_path / "config.json"
    config_data = {"test": "data"}
    config_file.write_text(json.dumps(config_data))

    loaded = load_config(str(config_file))
    assert loaded == config_data


def test_load_config_path_traversal():
    with pytest.raises(ValueError, match="Path traversal detected"):
        load_config("../../../../etc/passwd")


def test_is_safe_path():
    from scripts.loaders import is_safe_path

    cwd = os.getcwd()
    assert is_safe_path("scripts/config.json", cwd) is True
    assert is_safe_path("../../../../etc/passwd", cwd) is False


def test_enrich_config_with_river_mappings_path_traversal(caplog):
    import logging

    from scripts.batch_correction import _enrich_config_with_river_mappings

    config_data = {"RIVER_MILE_MAP_PATH": "../../../../etc/passwd"}
    with caplog.at_level(logging.WARNING):
        _enrich_config_with_river_mappings(config_data)

    assert "Path traversal attempt blocked" in caplog.text
    assert "SENSOR_TO_RIVER" not in config_data

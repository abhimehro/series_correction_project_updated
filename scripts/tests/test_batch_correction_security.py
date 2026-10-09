import os
import logging
from scripts.batch_correction import _enrich_config_with_river_mappings


def test_enrich_config_path_traversal_blocked(tmp_path, monkeypatch, caplog):
    # Set cwd to base_dir
    base_dir = tmp_path / "app_dir"
    base_dir.mkdir()
    monkeypatch.setattr(os, "getcwd", lambda: str(base_dir))

    # Create secret file outside base_dir
    secret_dir = tmp_path / "secret_dir"
    secret_dir.mkdir()
    secret_file = secret_dir / "secret_river.csv"
    secret_file.write_text("SENSOR_ID,RIVER_MILE\n1,10.5\n")

    config_data = {"RIVER_MILE_MAP_PATH": str(secret_file)}

    with caplog.at_level(logging.WARNING):
        _enrich_config_with_river_mappings(config_data)

    # Verify keys were not populated
    assert "SENSOR_TO_RIVER" not in config_data
    assert "RIVER_TO_SENSORS" not in config_data
    assert "Path traversal detected" in caplog.text


def test_enrich_config_symlink_path_traversal_blocked(tmp_path, monkeypatch, caplog):
    base_dir = tmp_path / "app_dir"
    base_dir.mkdir()
    monkeypatch.setattr(os, "getcwd", lambda: str(base_dir))

    secret_dir = tmp_path / "secret_dir"
    secret_dir.mkdir()
    secret_file = secret_dir / "secret_river.csv"
    secret_file.write_text("SENSOR_ID,RIVER_MILE\n1,10.5\n")

    symlink_file = base_dir / "link_river.csv"
    try:
        os.symlink(str(secret_file), str(symlink_file))
    except (OSError, NotImplementedError):
        return  # Skip if symlinks not supported

    config_data = {"RIVER_MILE_MAP_PATH": str(symlink_file)}

    with caplog.at_level(logging.WARNING):
        _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" not in config_data
    assert "RIVER_TO_SENSORS" not in config_data
    assert "Path traversal detected" in caplog.text

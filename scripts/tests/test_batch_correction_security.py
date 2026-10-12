"""Security tests for batch_correction module."""

import os
import pytest
from scripts.batch_correction import _enrich_config_with_river_mappings


def test_enrich_config_path_traversal_prevented(tmp_path, monkeypatch):
    """Ensure path traversal in RIVER_MILE_MAP_PATH is detected and prevented."""
    # Set current working directory to app_dir
    app_dir = tmp_path / "app_dir"
    app_dir.mkdir()
    monkeypatch.chdir(app_dir)

    # Place a sensitive/outside file in tmp_path / "secret.csv"
    secret_file = tmp_path / "secret.csv"
    secret_file.write_text("SENSOR_ID,RIVER_MILE\n1,10.0\n", encoding="utf-8")

    # Pass a path attempting path traversal to reach secret.csv outside app_dir
    traversal_path = os.path.relpath(secret_file, app_dir)
    config_data = {"RIVER_MILE_MAP_PATH": traversal_path}

    _enrich_config_with_river_mappings(config_data)

    # SENSOR_TO_RIVER should NOT be loaded due to path traversal restriction
    assert "SENSOR_TO_RIVER" not in config_data
    assert "RIVER_TO_SENSORS" not in config_data


def test_enrich_config_symlink_path_traversal_prevented(tmp_path, monkeypatch):
    """Ensure symlink path traversal in RIVER_MILE_MAP_PATH is detected and prevented."""
    app_dir = tmp_path / "app_dir"
    app_dir.mkdir()
    monkeypatch.chdir(app_dir)

    secret_file = tmp_path / "secret.csv"
    secret_file.write_text("SENSOR_ID,RIVER_MILE\n1,10.0\n", encoding="utf-8")

    # Create a symlink inside app_dir pointing to secret.csv outside app_dir
    symlink_path = app_dir / "link.csv"
    try:
        symlink_path.symlink_to(secret_file)
    except (OSError, NotImplementedError):
        pytest.skip("Symlinks not supported in environment")

    config_data = {"RIVER_MILE_MAP_PATH": str(symlink_path)}

    _enrich_config_with_river_mappings(config_data)

    assert "SENSOR_TO_RIVER" not in config_data
    assert "RIVER_TO_SENSORS" not in config_data

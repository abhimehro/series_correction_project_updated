import logging

import pytest

from scripts.batch_correction import _enrich_config_with_river_mappings
from scripts.loaders import validate_safe_path


def test_validate_safe_path_valid():
    resolved = validate_safe_path("scripts/config.json")
    assert resolved.endswith("scripts/config.json")


def test_validate_safe_path_traversal_raises():
    with pytest.raises(ValueError, match="Path traversal detected"):
        validate_safe_path("../../../../etc/passwd")


def test_enrich_config_river_mile_map_path_traversal_blocked(caplog):
    config_data = {"RIVER_MILE_MAP_PATH": "../../../../etc/passwd"}
    with caplog.at_level(logging.WARNING):
        _enrich_config_with_river_mappings(config_data)

    assert "Path traversal detected in RIVER_MILE_MAP_PATH" in caplog.text
    assert "SENSOR_TO_RIVER" not in config_data

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

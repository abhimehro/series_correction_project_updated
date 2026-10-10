import os
import pytest
from scripts.loaders import load_config


def test_load_config_symlink_path_traversal(tmp_path, monkeypatch):
    # Set current working directory to tmp_path/app_dir
    base_dir = tmp_path / "app_dir"
    base_dir.mkdir()
    monkeypatch.setattr(os, "getcwd", lambda: str(base_dir))

    # Create a secret directory outside base_dir
    secret_dir = tmp_path / "secret"
    secret_dir.mkdir()
    target_file = secret_dir / "secret.json"
    target_file.write_text('{"secret": "key"}')

    # Create a symlink inside base_dir pointing outside
    symlink_file = base_dir / "link_config.json"
    try:
        os.symlink(str(target_file), str(symlink_file))
    except (OSError, NotImplementedError):
        pytest.skip("Symlinks not supported on this platform/environment")

    with pytest.raises(ValueError, match="Path traversal detected"):
        load_config(str(symlink_file))


def test_load_config_nonexistent_outside_base_dir(tmp_path, monkeypatch):
    base_dir = tmp_path / "app_dir"
    base_dir.mkdir()
    monkeypatch.setattr(os, "getcwd", lambda: str(base_dir))

    # Attempt to load non-existent path outside base_dir.
    # Must raise ValueError("Path traversal detected") rather than FileNotFoundError.
    outside_path = str(tmp_path / "nonexistent.json")
    with pytest.raises(ValueError, match="Path traversal detected"):
        load_config(outside_path)


def test_load_config_nonexistent_inside_base_dir(tmp_path, monkeypatch):
    base_dir = tmp_path / "app_dir"
    base_dir.mkdir()
    monkeypatch.setattr(os, "getcwd", lambda: str(base_dir))

    # Attempting to load non-existent path inside base_dir raises FileNotFoundError
    inside_path = str(base_dir / "nonexistent.json")
    with pytest.raises(FileNotFoundError):
        load_config(inside_path)

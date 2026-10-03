import json
import os


def validate_safe_path(path, base_dir=None):
    # SECURITY: reject paths that escape the working directory (CWE-22).
    if base_dir is None:
        base_dir = os.path.realpath(os.getcwd())
    else:
        base_dir = os.path.realpath(base_dir)
    resolved = os.path.realpath(path)
    try:
        if os.path.commonpath([base_dir, resolved]) != base_dir:
            raise ValueError("Path traversal detected")
    except ValueError:
        raise ValueError("Path traversal detected") from None
    return resolved


def load_config(config_path="scripts/config.json"):
    resolved = validate_safe_path(config_path)
    with open(resolved, "r", encoding="utf-8") as f:
        return json.load(f)

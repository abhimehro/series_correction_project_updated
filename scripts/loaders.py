import json
import os


def load_config(config_path="scripts/config.json"):
    # SECURITY: reject paths that escape the working directory (CWE-22).
    base_dir = os.path.abspath(os.getcwd())
    resolved = os.path.abspath(config_path)
    try:
        if os.path.commonpath([base_dir, resolved]) != base_dir:
            raise ValueError("Path traversal detected")
    except ValueError as exc:
        if "Path traversal detected" in str(exc):
            raise
        raise ValueError("Path traversal detected") from exc

    with open(resolved, "r", encoding="utf-8") as f:
        return json.load(f)

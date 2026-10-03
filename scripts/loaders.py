import json
import os


def is_safe_path(target_path, base_dir=None):
    """Check if a target path is contained within the allowed base directory (CWE-22)."""
    if base_dir is None:
        base_dir = os.getcwd()
    resolved_base = os.path.realpath(base_dir)
    resolved_target = os.path.realpath(target_path)
    try:
        return os.path.commonpath([resolved_base, resolved_target]) == resolved_base
    except ValueError:
        return False


def load_config(config_path="scripts/config.json"):
    # SECURITY: reject paths that escape the working directory (CWE-22).
    if not is_safe_path(config_path):
        raise ValueError("Path traversal detected")

    resolved = os.path.realpath(config_path)
    with open(resolved, "r", encoding="utf-8") as f:
        return json.load(f)

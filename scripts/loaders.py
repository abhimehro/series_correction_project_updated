import json
import os


def load_config(config_path="scripts/config.json"):
    # SECURITY: Evaluate directory containment check (CWE-22) BEFORE checking file
    # existence or reading to avoid leaking file existence outside base directory.
    base_dir = os.path.realpath(os.getcwd())
    resolved = os.path.realpath(config_path)
    try:
        if os.path.commonpath([base_dir, resolved]) != base_dir:
            raise ValueError("Path traversal detected")
    except ValueError:
        raise ValueError("Path traversal detected") from None

    with open(resolved, "r", encoding="utf-8") as f:
        return json.load(f)

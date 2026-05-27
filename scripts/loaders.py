import json
import os

import pandas as pd


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


def load_series_data(series_id, config):
    series_cfg = config["series"].get(str(series_id))
    if series_cfg:
        df = pd.read_csv(series_cfg["diagnostic"])
        return df
    raise ValueError("Series {series_id} not defined in config.")

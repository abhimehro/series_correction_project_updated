import json
import os
import pandas as pd


def load_config(config_path="scripts/config.json"):
    base_dir = os.path.abspath(os.getcwd())
    abs_config_path = os.path.abspath(config_path)

    if os.path.commonpath([base_dir, abs_config_path]) != base_dir:
        raise ValueError(f"Invalid config path. Path traversal detected: {config_path}")

    with open(abs_config_path, "r") as f:
        return json.load(f)


def load_series_data(series_id, config):
    series_cfg = config["series"].get(str(series_id))
    if series_cfg:
        df = pd.read_csv(series_cfg["diagnostic"])
        return df
    raise ValueError("Series {series_id} not defined in config.")

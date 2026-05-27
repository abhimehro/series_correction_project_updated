import json
import pandas as pd

def load_config(config_path="scripts/config.json"):
    if ".." in config_path:
        raise ValueError("Path traversal detected")
    with open(config_path, "r") as f:
        return json.load(f)

def load_series_data(series_id, config):
    series_cfg = config["series"].get(str(series_id))
    if series_cfg:
        df = pd.read_csv(series_cfg["diagnostic"])
        return df
    raise ValueError(f"Series {series_id} not defined in config.")


import json
from pathlib import Path
import pandas as pd

def load_config(config_path="scripts/config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

def load_series_data(series_id, config):
    series_cfg = config["series"].get(str(series_id))
    if not series_cfg:
        raise ValueError(f"Series {series_id} not defined in config.")
    df = pd.read_csv(series_cfg["diagnostic"])
    return df

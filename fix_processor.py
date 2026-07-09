import re

filepath = 'scripts/processor.py'
with open(filepath, 'r') as f:
    content = f.read()

search = """def process_data(
    data: pd.DataFrame, config: dict[str, Any] | None = None
) -> pd.DataFrame:"""

replace = """def _setup_config_and_data(data, config):
    merged_config = _merge_config(config)
    log.info("Processing data with configuration: %s", merged_config)

    processed_data = data.copy()
    time_col = merged_config["time_col"]
    processed_data = _validate_and_convert_time_col(processed_data, time_col)

    value_col = _validate_value_col(
        processed_data, merged_config["value_col"], time_col
    )
    merged_config["value_col"] = value_col
    return processed_data, merged_config


def process_data(
    data: pd.DataFrame, config: dict[str, Any] | None = None
) -> pd.DataFrame:"""

if search in content:
    content = content.replace(search, replace)

    # Now replace the body
    body_search = """    merged_config = _merge_config(config)
    log.info("Processing data with configuration: %s", merged_config)

    processed_data = data.copy()
    time_col = merged_config["time_col"]
    processed_data = _validate_and_convert_time_col(processed_data, time_col)

    value_col = _validate_value_col(
        processed_data, merged_config["value_col"], time_col
    )
    merged_config["value_col"] = value_col"""

    body_replace = """    processed_data, merged_config = _setup_config_and_data(data, config)
    time_col = merged_config["time_col"]
    value_col = merged_config["value_col"]"""

    content = content.replace(body_search, body_replace)

    with open(filepath, 'w') as f:
        f.write(content)
    print("Extracted setup logic.")
else:
    print("Search string not found.")

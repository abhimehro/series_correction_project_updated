import re

filepath = 'scripts/processor.py'
with open(filepath, 'r') as f:
    content = f.read()

search = """def process_data(
    data: pd.DataFrame, config: dict[str, Any] | None = None
) -> pd.DataFrame:
    \"\"\"
    Process sensor data to detect and correct discontinuities (gaps, outliers, jumps).

    Applies detection and correction functions sequentially based on configuration.

    Args:
        data: DataFrame containing sensor data.
        config: Configuration dictionary with processing parameters.
    \"\"\"
    merged_config = _merge_config(config)
    log.info("Processing data with configuration: %s", merged_config)

    processed_data = data.copy()
    time_col = merged_config["time_col"]
    processed_data = _validate_and_convert_time_col(processed_data, time_col)

    value_col = _validate_value_col(processed_data, merged_config["value_col"], time_col)
    merged_config["value_col"] = value_col

    window_size = merged_config["window_size"]
    threshold = merged_config["threshold"]
    gap_threshold_factor = merged_config["gap_threshold_factor"]
    gap_method = merged_config["gap_method"]
    outlier_method = merged_config["outlier_method"]

    log.debug("Sorting data by time column: '%s'", time_col)
    processed_data = processed_data.sort_values(by=time_col).reset_index(drop=True)

    processed_data = _process_discontinuity(
        processed_data,
        DiscontinuityConfig(
            step_name="Step 1: Detecting and Correcting Gaps",
            detect_func=detect_gaps,
            correct_func=correct_gaps,
            detect_kwargs={"time_col": time_col, "threshold_factor": gap_threshold_factor},
            correct_kwargs={"time_col": time_col, "value_cols": [value_col], "method": gap_method},
            sort_time_col=time_col,
        ),
    )
    processed_data = _process_discontinuity(
        processed_data,
        DiscontinuityConfig(
            step_name="Step 2: Detecting and Correcting Outliers",
            detect_func=detect_outliers,
            correct_func=correct_outliers,
            detect_kwargs={"value_col": value_col, "window_size": window_size, "threshold": threshold},
            correct_kwargs={"value_col": value_col, "window_size": window_size, "method": outlier_method},
            sort_time_col=None,
        ),
    )
    processed_data = _process_discontinuity(
        processed_data,
        DiscontinuityConfig(
            step_name="Step 3: Detecting and Correcting Jumps",
            detect_func=detect_jumps,
            correct_func=correct_jumps,
            detect_kwargs={"value_col": value_col, "window_size": window_size, "threshold": threshold},
            correct_kwargs={"value_col": value_col, "window_size": window_size},
            sort_time_col=None,
        ),
    )

    log.info("Data processing complete for value column '%s'.", value_col)
    return processed_data"""

replace = """def _setup_processing(data, config):
    merged_config = _merge_config(config)
    log.info("Processing data with configuration: %s", merged_config)

    processed_data = data.copy()
    time_col = merged_config["time_col"]
    processed_data = _validate_and_convert_time_col(processed_data, time_col)

    value_col = _validate_value_col(processed_data, merged_config["value_col"], time_col)
    merged_config["value_col"] = value_col

    log.debug("Sorting data by time column: '%s'", time_col)
    processed_data = processed_data.sort_values(by=time_col).reset_index(drop=True)
    return processed_data, merged_config

def process_data(
    data: pd.DataFrame, config: dict[str, Any] | None = None
) -> pd.DataFrame:
    \"\"\"
    Process sensor data to detect and correct discontinuities (gaps, outliers, jumps).

    Applies detection and correction functions sequentially based on configuration.

    Args:
        data: DataFrame containing sensor data.
        config: Configuration dictionary with processing parameters.
    \"\"\"
    processed_data, cfg = _setup_processing(data, config)
    time_col = cfg["time_col"]
    value_col = cfg["value_col"]

    processed_data = _process_discontinuity(
        processed_data,
        DiscontinuityConfig(
            step_name="Step 1: Detecting and Correcting Gaps",
            detect_func=detect_gaps,
            correct_func=correct_gaps,
            detect_kwargs={"time_col": time_col, "threshold_factor": cfg["gap_threshold_factor"]},
            correct_kwargs={"time_col": time_col, "value_cols": [value_col], "method": cfg["gap_method"]},
            sort_time_col=time_col,
        ),
    )
    processed_data = _process_discontinuity(
        processed_data,
        DiscontinuityConfig(
            step_name="Step 2: Detecting and Correcting Outliers",
            detect_func=detect_outliers,
            correct_func=correct_outliers,
            detect_kwargs={"value_col": value_col, "window_size": cfg["window_size"], "threshold": cfg["threshold"]},
            correct_kwargs={"value_col": value_col, "window_size": cfg["window_size"], "method": cfg["outlier_method"]},
            sort_time_col=None,
        ),
    )
    processed_data = _process_discontinuity(
        processed_data,
        DiscontinuityConfig(
            step_name="Step 3: Detecting and Correcting Jumps",
            detect_func=detect_jumps,
            correct_func=correct_jumps,
            detect_kwargs={"value_col": value_col, "window_size": cfg["window_size"], "threshold": cfg["threshold"]},
            correct_kwargs={"value_col": value_col, "window_size": cfg["window_size"]},
            sort_time_col=None,
        ),
    )

    log.info("Data processing complete for value column '%s'.", value_col)
    return processed_data"""

if search in content:
    content = content.replace(search, replace)
    with open(filepath, 'w') as f:
        f.write(content)
    print("Replaced process_data to fix complex method lint.")
else:
    print("Search string not found.")

import re

with open('scripts/processor.py', 'r') as f:
    content = f.read()

import os
import sys

def extract_helpers(content):
    time_helper_code = """
def _validate_and_convert_time_col(processed_data, time_col):
    if time_col not in processed_data.columns:
        log.warning(
            "Time column '%s' not found in data columns: %s",
            time_col,
            list(processed_data.columns),
        )
        raise ValueError("Time column not found in data columns")

    if not pd.api.types.is_numeric_dtype(processed_data[time_col]):
        try:
            processed_data[time_col] = pd.to_datetime(
                processed_data[time_col], format="mixed"
            )
            processed_data[time_col] = (
                processed_data[time_col] - pd.Timestamp("1970-01-01")
            ) // pd.Timedelta("1s")
            log.info(
                "Converted time column '%s' to numeric (Unix timestamp).", time_col
            )
        except Exception as exc:
            log.exception(
                f"Time column '{time_col}' is not numeric and could not be converted: {exc}"
            )
            raise ValueError(
                "Time column is not numeric and could not be converted"
            ) from None
    return processed_data


def _validate_value_col(processed_data, value_col, time_col):
    if value_col is None:
        numeric_cols = processed_data.select_dtypes(include=np.number).columns
        potential_value_cols = [col for col in numeric_cols if col != time_col]
        if not potential_value_cols:
            log.warning(
                "No numeric value columns found in the data (excluding time column '%s'). Please specify a valid value column in the configuration.",
                time_col,
            )
            raise ValueError("No numeric value columns found in the data")
        value_col = potential_value_cols[0]
        log.info("Auto-detected value column: '%s'", value_col)
    elif value_col not in processed_data.columns:
        log.warning(
            f"Specified value column '{value_col}' not found in data columns: {list(processed_data.columns)}"
        )
        raise ValueError("Specified value column not found in data columns")
    elif not pd.api.types.is_numeric_dtype(processed_data[value_col]):
        log.warning(f"Specified value column '{value_col}' is not numeric.")
        raise ValueError("Specified value column is not numeric.")
    return value_col

"""

    # We need to insert these helpers above `process_data`
    process_data_idx = content.find("def process_data(")
    if process_data_idx == -1:
        print("Could not find process_data")
        return None

    content = content[:process_data_idx] + time_helper_code + content[process_data_idx:]

    # Let's use regex to replace it to handle whitespace differences
    # Find start and end of block to replace
    # Important: since we just modified content, process_data_idx is no longer accurate
    process_data_idx = content.find("def process_data(")

    start_str = '    time_col = merged_config["time_col"]\n    if time_col not in processed_data.columns:'
    end_str = '        raise ValueError("Specified value column is not numeric.")'

    start_idx = content.find(start_str, process_data_idx)
    end_idx = content.find(end_str, start_idx) + len(end_str)

    if start_idx == -1 or end_idx == -1:
        print(f"Could not find replacement block start_idx {start_idx} end_idx {end_idx}")
        return None

    replace_str = """    time_col = merged_config["time_col"]
    processed_data = _validate_and_convert_time_col(processed_data, time_col)

    value_col = merged_config["value_col"]
    value_col = _validate_value_col(processed_data, value_col, time_col)
    merged_config["value_col"] = value_col"""

    content = content[:start_idx] + replace_str + content[end_idx:]
    return content

new_content = extract_helpers(content)
if new_content:
    with open('scripts/processor.py', 'w') as f:
        f.write(new_content)
    print("Done")

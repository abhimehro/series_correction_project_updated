import logging
import os
import re
import tempfile

import pandas as pd

from scripts.spreadsheet_safety import write_csv_safely

DATA_DIR = "../data"
CORRECTED_OUTPUT_DIR = "../corrected_output_refined_shift"
CORRECTION_LOG_PATH = "../correction_log_refined_shift.csv"
YTY_DIFF_CSV_PATH = (
    "../Seatek_Analysis_Summary.xlsx - Year-to-Year Differences.csv"
)

_YEAR_PAIR_REGEX = re.compile(r"(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)")
_FILE_PATTERN = re.compile(r"(S\d+)_Y(\d+)\.txt")


def _is_path_safe(resolved, base_dir):
    temp_dir = os.path.realpath(tempfile.gettempdir())
    try:
        in_base = os.path.commonpath([base_dir, resolved]) == base_dir
        in_temp = os.path.commonpath([temp_dir, resolved]) == temp_dir
        return in_base or in_temp
    except ValueError:
        return False


def _validate_path(path, base_dir=None):
    if base_dir is None:
        base_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
    else:
        base_dir = os.path.realpath(base_dir)
    resolved = os.path.realpath(path)
    if not _is_path_safe(resolved, base_dir):
        raise ValueError("Path traversal detected")
    return resolved


def calculate_non_zero_average(series):
    numeric_series = pd.to_numeric(series, errors="coerce").dropna()
    non_zero = numeric_series[numeric_series != 0]
    return non_zero.mean() if not non_zero.empty else 0.0


def find_sensor_columns(columns):
    return [c for c in columns if c.startswith("Sensor ") and c[7:].isdigit()]


def _melt_and_filter_outliers(df_yty_diff, sensor_cols):
    df_melted = df_yty_diff.melt(
        id_vars=["Year_Pair"],
        value_vars=sensor_cols,
        var_name="Sensor",
        value_name="Difference",
    )
    outliers_df = df_melted[df_melted["Difference"].abs() >= 0.1].copy()
    msg = (
        "No outliers (|Difference| >= 0.1) found."
        if outliers_df.empty
        else f"Successfully loaded {len(outliers_df)} outliers."
    )
    print(msg)
    return outliers_df


def load_identified_outliers(csv_path, base_dir=None):
    _validate_path(csv_path, base_dir=base_dir)
    try:
        df_yty_diff = pd.read_csv(csv_path)
        actual_cols = df_yty_diff.columns.tolist()
        sensor_cols = find_sensor_columns(actual_cols)

        if not sensor_cols:
            print(f"Error: No sensor columns found in {csv_path}.")
            return pd.DataFrame()

        if "Year_Pair" not in actual_cols:
            print(f"Error: 'Year_Pair' column not found in {csv_path}.")
            return pd.DataFrame()

        return _melt_and_filter_outliers(df_yty_diff, sensor_cols)

    except FileNotFoundError:
        print(f"Error: The file '{csv_path}' was not found.")
        return pd.DataFrame()
    except Exception:
        print("An unexpected error occurred while loading outliers.")
        return pd.DataFrame()


def build_raw_file_map(data_dir, base_dir=None):
    _validate_path(data_dir, base_dir=base_dir)
    raw_file_map = {}
    for f in os.listdir(data_dir):
        if f.startswith("S") and "_Y" in f and f.endswith(".txt"):
            m = _FILE_PATTERN.match(f)
            if m:
                series_id, year_num = m.group(1), int(m.group(2))
                raw_file_map.setdefault(series_id, {})[year_num] = os.path.join(data_dir, f)
    return raw_file_map


def load_raw_dataframes(raw_file_map):
    dataframes = {}
    for year_files in raw_file_map.values():
        for file_path in year_files.values():
            if file_path not in dataframes:
                with open(file_path, "r", encoding="utf-8") as f:
                    dataframes[file_path] = pd.read_csv(f, header=None, sep=r"\s+")
    return dataframes


def parse_year_pair(year_pair_str):
    m = _YEAR_PAIR_REGEX.match(year_pair_str)
    if not m:
        return None
    y1_full, y1_yy, y2_full, y2_yy = map(int, m.groups())
    return (y1_yy, y2_yy) if y1_full < y2_full else (y2_yy, y1_yy)


def parse_sensor_index(sensor_name):
    try:
        idx = int(sensor_name.replace("Sensor ", "")) - 1
        return idx if 0 <= idx < 32 else None
    except ValueError:
        return None


def find_year_files(raw_file_map, prev_yy, next_yy, sorted_series_ids=None):
    if sorted_series_ids is None:
        sorted_series_ids = sorted(raw_file_map)
    for series_id in sorted_series_ids:
        year_files = raw_file_map.get(series_id, {})
        if prev_yy in year_files and next_yy in year_files:
            return series_id, year_files[prev_yy], year_files[next_yy]
    return None, None, None


def has_sensor_window(df_prev, df_next, sensor_idx):
    return (
        len(df_prev) >= 5
        and len(df_next) >= 5
        and df_prev.shape[1] > sensor_idx
        and df_next.shape[1] > sensor_idx
    )


def output_file_name(input_file):
    return os.path.basename(input_file).replace(".txt", "_refined_corrected.csv")


def _calculate_and_apply_shift(dfs, metadata, outlier_data):
    df_prev, df_next = dfs
    sensor_idx, next_file, series_id = metadata
    outlier_info, parsed_years = outlier_data
    year_pair_str, sensor_name, orig_diff = outlier_info
    prev_yy, next_yy = parsed_years

    if not has_sensor_window(df_prev, df_next, sensor_idx):
        return None

    prev_avg = calculate_non_zero_average(df_prev.iloc[-5:, sensor_idx])
    next_avg = calculate_non_zero_average(df_next.iloc[:5, sensor_idx])
    shift = prev_avg - next_avg

    df_next[sensor_idx] = pd.to_numeric(df_next[sensor_idx], errors="coerce") + shift
    return {
        "Series": series_id,
        "Year_Pair_Outlier": year_pair_str,
        "Sensor": sensor_name,
        "Original_Difference_Summary": orig_diff,
        "Calculated_Level_Shift": shift,
        "Correction_Type": "Level Shift",
        "File_Corrected": output_file_name(next_file),
        "Rationale": f"Aligned Y{next_yy:02d} head with Y{prev_yy:02d} tail.",
    }


def apply_level_shift_correction(
    outlier_info, raw_file_map, raw_dataframes, sorted_series_ids=None
):
    year_pair_str, sensor_name, orig_diff = outlier_info
    parsed_years = parse_year_pair(year_pair_str)
    sensor_idx = parse_sensor_index(sensor_name)

    if not parsed_years or sensor_idx is None:
        return None

    prev_yy, next_yy = parsed_years
    series_id, prev_file, next_file = find_year_files(
        raw_file_map, prev_yy, next_yy, sorted_series_ids
    )

    if not series_id:
        return None

    try:
        return _calculate_and_apply_shift(
            (raw_dataframes[prev_file], raw_dataframes[next_file]),
            (sensor_idx, next_file, series_id),
            (outlier_info, parsed_years),
        )
    except Exception:
        logging.exception(
            "An unexpected error occurred while processing outlier %s, %s",
            year_pair_str,
            sensor_name,
        )
        print(f"An unexpected error occurred while processing outlier {year_pair_str}, {sensor_name}.")
        return None


def save_corrected_files(applied_corrections, raw_file_map, raw_dataframes, output_dir):
    corrected_names = {c["File_Corrected"] for c in applied_corrections if c is not None}
    for year_files in raw_file_map.values():
        for file_path in year_files.values():
            name = output_file_name(file_path)
            if name in corrected_names:
                write_csv_safely(
                    raw_dataframes[file_path],
                    os.path.join(output_dir, name),
                    index=False,
                    header=False,
                )


def _apply_corrections(outliers_df, raw_file_map, raw_dataframes, applied_corrections):
    sorted_series_ids = sorted(raw_file_map)
    for year_pair, sensor, diff in zip(
        outliers_df["Year_Pair"].to_numpy(),
        outliers_df["Sensor"].to_numpy(),
        outliers_df["Difference"].to_numpy(),
    ):
        result = apply_level_shift_correction(
            (year_pair, sensor, diff), raw_file_map, raw_dataframes, sorted_series_ids
        )
        if result:
            applied_corrections.append(result)


def main():
    outliers_df = load_identified_outliers(YTY_DIFF_CSV_PATH)
    if outliers_df.empty:
        return

    os.makedirs(CORRECTED_OUTPUT_DIR, exist_ok=True)
    print("\n--- Applying Refined Level Shift Corrections ---")

    raw_file_map = build_raw_file_map(DATA_DIR)
    raw_dataframes = load_raw_dataframes(raw_file_map)
    applied_corrections = []

    _apply_corrections(outliers_df, raw_file_map, raw_dataframes, applied_corrections)

    if applied_corrections:
        save_corrected_files(
            applied_corrections, raw_file_map, raw_dataframes, CORRECTED_OUTPUT_DIR
        )
        write_csv_safely(
            pd.DataFrame(applied_corrections), CORRECTION_LOG_PATH, index=False
        )
        print(f"\nCorrection log saved to: {CORRECTION_LOG_PATH}")
    else:
        print("\nNo refined corrections were applied.")

    print("\n--- Refined Level Shift Corrections Complete ---")


if __name__ == "__main__":
    main()

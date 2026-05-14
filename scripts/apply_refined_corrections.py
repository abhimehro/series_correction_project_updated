import glob
import os
import re

import pandas as pd

from scripts.spreadsheet_safety import sanitize_dataframe_for_spreadsheet

# Define directories (adjust paths if your local structure is different)
DATA_DIR = "../data"  # Updated path
# The script will generate corrected files in a new directory
CORRECTED_OUTPUT_DIR = "../corrected_output_refined_shift"  # Updated path
# The script will generate a log file
CORRECTION_LOG_PATH = "../correction_log_refined_shift.csv"  # Updated path

# --- Load Identified Outliers ---
YTY_DIFF_CSV_PATH = (
    "../Seatek_Analysis_Summary.xlsx - Year-to-Year Differences.csv"  # Updated path
)


def calculate_non_zero_average(series):
    """Calculates the average of a pandas Series, excluding zero values, coercing to numeric."""
    numeric_series = pd.to_numeric(series, errors="coerce").dropna()
    non_zero_values = numeric_series[numeric_series != 0]
    if not non_zero_values.empty:
        return non_zero_values.mean()

    return 0.0  # Return 0 if all non-NaN values are zero or series is empty


def find_sensor_columns(columns):
    return [
        col
        for col in columns
        if col.startswith("Sensor ") and col[len("Sensor ") :].isdigit()
    ]


def load_identified_outliers(csv_path):
    """Loads and melts the year-to-year differences CSV to identify outliers."""
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

        df_melted = df_yty_diff.melt(
            id_vars=["Year_Pair"],
            value_vars=sensor_cols,
            var_name="Sensor",
            value_name="Difference",
        )

        # NaN.abs() is NaN and NaN >= 0.1 is False, so NaN rows are already
        # excluded by the abs() filter; no separate dropna is needed.
        outliers_df = df_melted[df_melted["Difference"].abs() >= 0.1].copy()

        if outliers_df.empty:
            print("No outliers (|Difference| >= 0.1) found.")
        else:
            print(f"Successfully loaded {len(outliers_df)} outliers.")

        return outliers_df

    except FileNotFoundError:
        print(f"Error: The file '{csv_path}' was not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while loading outliers: {e}")
        return pd.DataFrame()


def build_raw_file_map(data_dir):
    """Creates a mapping of series and year number to raw data file paths."""
    raw_file_map = {}
    all_raw_files = glob.glob(os.path.join(data_dir, "S*_Y*.txt"))
    for raw_file_path in all_raw_files:
        file_name = os.path.basename(raw_file_path)
        file_match = re.match(r"(S\d+)_Y(\d+)\.txt", file_name)
        if file_match:
            series_id = file_match.group(1)
            year_num = int(file_match.group(2))
            if series_id not in raw_file_map:
                raw_file_map[series_id] = {}
            raw_file_map[series_id][year_num] = raw_file_path
    return raw_file_map


def load_raw_dataframes(raw_file_map):
    """Loads each raw file once so corrections to the same file are preserved."""
    dataframes = {}
    for year_files in raw_file_map.values():
        for file_path in year_files.values():
            if file_path not in dataframes:
                dataframes[file_path] = pd.read_csv(
                    file_path, header=None, sep=r"\s+", engine="python"
                )
    return dataframes


def parse_year_pair(year_pair_str):
    """Parses the Year_Pair string into previous and next year numbers."""
    pair_match = re.match(r"(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)", year_pair_str)
    if not pair_match:
        return None

    y1_full, y1_yy, y2_full, y2_yy = map(int, pair_match.groups())

    if y1_full < y2_full:
        return y1_yy, y2_yy

    return y2_yy, y1_yy


def parse_sensor_index(sensor_name):
    try:
        sensor_idx = int(sensor_name.replace("Sensor ", "")) - 1
    except ValueError:
        return None

    if not 0 <= sensor_idx < 32:
        return None

    return sensor_idx


def find_year_files(raw_file_map, prev_yy, next_yy):
    # Preserve deterministic series preference (S26 before S27) regardless of
    # filesystem/glob ordering.
    for series_id in sorted(raw_file_map):
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


def apply_level_shift_correction(outlier_row, raw_file_map, raw_dataframes):
    """Calculates and applies level shift correction for a single outlier."""
    year_pair_str = outlier_row.Year_Pair
    sensor_name = outlier_row.Sensor
    orig_diff = outlier_row.Difference

    parsed_years = parse_year_pair(year_pair_str)
    if not parsed_years:
        return None

    sensor_idx = parse_sensor_index(sensor_name)
    if sensor_idx is None:
        return None

    prev_yy, next_yy = parsed_years
    series_id, prev_file, next_file = find_year_files(raw_file_map, prev_yy, next_yy)

    if not series_id:
        return None

    try:
        df_prev = raw_dataframes[prev_file]
        df_next = raw_dataframes[next_file]

        if not has_sensor_window(df_prev, df_next, sensor_idx):
            return None

        prev_avg = calculate_non_zero_average(df_prev.iloc[-5:, sensor_idx])
        next_avg = calculate_non_zero_average(df_next.iloc[:5, sensor_idx])
        shift = prev_avg - next_avg

        df_next[sensor_idx] = (
            pd.to_numeric(df_next[sensor_idx], errors="coerce") + shift
        )
        output_name = output_file_name(next_file)

        return {
            "Series": series_id,
            "Year_Pair_Outlier": year_pair_str,
            "Sensor": sensor_name,
            "Original_Difference_Summary": orig_diff,
            "Calculated_Level_Shift": shift,
            "Correction_Type": "Level Shift",
            "File_Corrected": output_name,
            "Rationale": f"Aligned Y{next_yy:02d} head with Y{prev_yy:02d} tail.",
        }

    except Exception as e:
        print(f"Error processing outlier {year_pair_str}, {sensor_name}: {e}")
        return None


def save_corrected_files(applied_corrections, raw_file_map, raw_dataframes, output_dir):
    """Writes each corrected dataframe whose output filename appears in
    ``applied_corrections``. ``None`` entries (e.g. from skipped outliers) are
    ignored so callers can pass unfiltered results safely."""
    corrected_names = {
        correction["File_Corrected"]
        for correction in applied_corrections
        if correction is not None
    }
    for year_files in raw_file_map.values():
        for file_path in year_files.values():
            name = output_file_name(file_path)
            if name in corrected_names:
                output_path = os.path.join(output_dir, name)
                sanitize_dataframe_for_spreadsheet(raw_dataframes[file_path]).to_csv(
                    output_path, index=False, header=False
                )


def main():
    outliers_df = load_identified_outliers(YTY_DIFF_CSV_PATH)
    if outliers_df.empty:
        return

    os.makedirs(CORRECTED_OUTPUT_DIR, exist_ok=True)
    print("\n--- Applying Refined Level Shift Corrections ---")

    raw_file_map = build_raw_file_map(DATA_DIR)
    raw_dataframes = load_raw_dataframes(raw_file_map)
    applied_corrections = []

    for row in outliers_df.itertuples(index=False):
        result = apply_level_shift_correction(row, raw_file_map, raw_dataframes)
        if result:
            applied_corrections.append(result)

    if applied_corrections:
        save_corrected_files(
            applied_corrections, raw_file_map, raw_dataframes, CORRECTED_OUTPUT_DIR
        )
        sanitize_dataframe_for_spreadsheet(pd.DataFrame(applied_corrections)).to_csv(
            CORRECTION_LOG_PATH, index=False
        )
        print(f"\nCorrection log saved to: {CORRECTION_LOG_PATH}")
    else:
        print("\nNo refined corrections were applied.")

    print("\n--- Refined Level Shift Corrections Complete ---")


if __name__ == "__main__":
    main()

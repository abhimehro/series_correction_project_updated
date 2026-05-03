import glob
import os
import re

import pandas as pd

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
    else:
        return 0.0  # Return 0 if all non-NaN values are zero or series is empty


def load_identified_outliers(csv_path):
    """Loads and melts the year-to-year differences CSV to identify outliers."""
    try:
        df_yty_diff = pd.read_csv(csv_path)
        actual_cols = df_yty_diff.columns.tolist()
        sensor_cols = [
            col
            for col in actual_cols
            if col.startswith("Sensor ") and col[len("Sensor ") :].isdigit()
        ]

        if not sensor_cols:
            print(
                f"Error: No sensor columns found in {csv_path}."
            )
            return pd.DataFrame()

        if "Year_Pair" not in actual_cols:
            print(
                f"Error: 'Year_Pair' column not found in {csv_path}."
            )
            return pd.DataFrame()

        df_melted = df_yty_diff.melt(
            id_vars=["Year_Pair"],
            value_vars=sensor_cols,
            var_name="Sensor",
            value_name="Difference",
        )

        outliers_df = (
            df_melted[df_melted["Difference"].abs() >= 0.1]
            .dropna(subset=["Difference"])
            .copy()
        )

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


def parse_year_pair(year_pair_str):
    """Parses the Year_Pair string into year numbers and full years."""
    pair_match = re.match(
        r"(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)", year_pair_str
    )
    if not pair_match:
        return None

    y1_full, y1_yy, y2_full, y2_yy = map(int, pair_match.groups())

    if y1_full < y2_full:
        return {
            "prev_yy": y1_yy, "next_yy": y2_yy,
            "prev_full": y1_full, "next_full": y2_full
        }
    else:
        return {
            "prev_yy": y2_yy, "next_yy": y1_yy,
            "prev_full": y2_full, "next_full": y1_full
        }


def apply_level_shift_correction(
    outlier_row, raw_file_map, output_dir
):
    """Calculates and applies level shift correction for a single outlier."""
    year_pair_str = outlier_row["Year_Pair"]
    sensor_name = outlier_row["Sensor"]
    orig_diff = outlier_row["Difference"]

    parsed_years = parse_year_pair(year_pair_str)
    if not parsed_years:
        return None

    try:
        sensor_idx = int(sensor_name.replace("Sensor ", "")) - 1
        if not 0 <= sensor_idx < 32:
            return None
    except ValueError:
        return None

    prev_yy = parsed_years["prev_yy"]
    next_yy = parsed_years["next_yy"]

    outlier_series_id = None
    prev_file, next_file = None, None

    for s_id in ["S26", "S27"]:
        if (
            s_id in raw_file_map
            and prev_yy in raw_file_map[s_id]
            and next_yy in raw_file_map[s_id]
        ):
            outlier_series_id = s_id
            prev_file = raw_file_map[s_id][prev_yy]
            next_file = raw_file_map[s_id][next_yy]
            break

    if not outlier_series_id:
        return None

    try:
        df_prev = pd.read_csv(prev_file, header=None, sep=r"\s+", engine="python")
        df_next = pd.read_csv(next_file, header=None, sep=r"\s+", engine="python")

        if (
            len(df_prev) < 5 or len(df_next) < 5 or
            df_prev.shape[1] <= sensor_idx or df_next.shape[1] <= sensor_idx
        ):
            return None

        prev_avg = calculate_non_zero_average(df_prev.iloc[-5:, sensor_idx])
        next_avg = calculate_non_zero_average(df_next.iloc[:5, sensor_idx])
        shift = prev_avg - next_avg

        df_corrected = pd.read_csv(next_file, header=None, sep=r"\s+", engine="python")
        df_corrected[sensor_idx] = pd.to_numeric(df_corrected[sensor_idx], errors="coerce") + shift

        output_name = os.path.basename(next_file).replace(".txt", "_refined_corrected.csv")
        output_path = os.path.join(output_dir, output_name)
        df_corrected.to_csv(output_path, index=False, header=False)

        return {
            "Series": outlier_series_id,
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


def main():
    outliers_df = load_identified_outliers(YTY_DIFF_CSV_PATH)
    if outliers_df.empty:
        return

    os.makedirs(CORRECTED_OUTPUT_DIR, exist_ok=True)
    print("\n--- Applying Refined Level Shift Corrections ---")

    raw_file_map = build_raw_file_map(DATA_DIR)
    applied_corrections = []

    for _, row in outliers_df.iterrows():
        result = apply_level_shift_correction(row, raw_file_map, CORRECTED_OUTPUT_DIR)
        if result:
            applied_corrections.append(result)

    if applied_corrections:
        pd.DataFrame(applied_corrections).to_csv(CORRECTION_LOG_PATH, index=False)
        print(f"\nCorrection log saved to: {CORRECTION_LOG_PATH}")
    else:
        print("\nNo refined corrections were applied.")

    print("\n--- Refined Level Shift Corrections Complete ---")


if __name__ == "__main__":
    main()

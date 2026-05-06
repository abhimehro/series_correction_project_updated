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


# --- Function to calculate non-zero average ---
def calculate_non_zero_average(series):
    """Calculates the average of a pandas Series, excluding zero values, coercing to numeric."""
    numeric_series = pd.to_numeric(series, errors="coerce").dropna()
    non_zero_values = numeric_series[numeric_series != 0]
    if not non_zero_values.empty:
        return non_zero_values.mean()
    else:
        return 0.0  # Return 0 if all non-NaN values are zero or series is empty


def load_identified_outliers(csv_path):
    """Loads identified outliers from the specified CSV file."""
    try:
        df_yty_diff = pd.read_csv(csv_path)
        actual_cols = df_yty_diff.columns.tolist()
        sensor_cols = [
            col
            for col in actual_cols
            if col.startswith("Sensor ") and col[len("Sensor ") :].isdigit()
        ]
        id_vars = ["Year_Pair"]

        if not sensor_cols:
            print("Error: Could not find sensor columns in the Year-to-Year Differences CSV.")
            return pd.DataFrame()
        elif "Year_Pair" not in actual_cols:
            print(f"Error: 'Year_Pair' column not found. Available columns: {actual_cols}")
            return pd.DataFrame()
        else:
            df_melted = df_yty_diff.melt(
                id_vars=id_vars,
                value_vars=sensor_cols,
                var_name="Sensor",
                value_name="Difference",
            )
            identified_outliers_df = (
                df_melted[df_melted["Difference"].abs() >= 0.1]
                .dropna(subset=["Difference"])
                .copy()
            )

            if identified_outliers_df.empty:
                print("No outliers (|Difference| >= 0.1) found. No corrections will be applied.")
            else:
                print(f"Successfully loaded {len(identified_outliers_df)} outliers from {csv_path}")

            return identified_outliers_df
    except FileNotFoundError:
        print(f"Error: The file '{csv_path}' was not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while loading outliers: {e}")
        return pd.DataFrame()


def build_raw_file_map(data_dir):
    """Builds a map of raw data files organized by series and year."""
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


def parse_outlier_info(outlier_year_pair_str, outlier_sensor):
    """Parses year pairs and sensor index from outlier string data."""
    pair_match = re.match(r"(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)", outlier_year_pair_str)
    if not pair_match:
        return None, None, None, None

    year1_full, year1_yy, year2_full, year2_yy = map(int, pair_match.groups())

    if year1_full < year2_full:
        prev_year_yy_pair = year1_yy
        next_year_yy_pair = year2_yy
    else:
        prev_year_yy_pair = year2_yy
        next_year_yy_pair = year1_yy

    try:
        sensor_col_index = int(outlier_sensor.replace("Sensor ", "")) - 1
        if not 0 <= sensor_col_index < 32:
            print(f"Skipping outlier with invalid sensor column index for {outlier_sensor}.")
            return None, None, None, None
    except ValueError:
        print(f"Skipping outlier with unparseable sensor number from {outlier_sensor}.")
        return None, None, None, None

    return prev_year_yy_pair, next_year_yy_pair, sensor_col_index, True


def find_raw_files_for_outlier(raw_file_map, prev_year_yy, next_year_yy):
    """Finds the previous and next year raw files for a given outlier."""
    for s_id in ["S26", "S27"]:
        if (
            s_id in raw_file_map
            and prev_year_yy in raw_file_map[s_id]
            and next_year_yy in raw_file_map[s_id]
        ):
            return s_id, raw_file_map[s_id][prev_year_yy], raw_file_map[s_id][next_year_yy]
    return None, None, None


def apply_correction(prev_file, next_file, sensor_col_index, output_dir):
    """Calculates and applies the level shift correction, saving the corrected file."""
    df_prev_raw = pd.read_csv(prev_file, header=None, sep=r"\s+", engine="python")
    df_next_raw = pd.read_csv(next_file, header=None, sep=r"\s+", engine="python")

    if (
        len(df_prev_raw) >= 5
        and len(df_next_raw) >= 5
        and df_prev_raw.shape[1] > sensor_col_index
        and df_next_raw.shape[1] > sensor_col_index
    ):
        prev_tail_avg = calculate_non_zero_average(df_prev_raw.iloc[-5:, sensor_col_index])
        next_head_avg = calculate_non_zero_average(df_next_raw.iloc[:5, sensor_col_index])

        calculated_level_shift = prev_tail_avg - next_head_avg

        df_next_corrected = pd.read_csv(next_file, header=None, sep=r"\s+", engine="python")
        df_next_corrected[sensor_col_index] = (
            pd.to_numeric(df_next_corrected[sensor_col_index], errors="coerce")
            + calculated_level_shift
        )

        output_file_name = os.path.basename(next_file).replace(".txt", "_refined_corrected.csv")
        output_file_path = os.path.join(output_dir, output_file_name)
        df_next_corrected.to_csv(output_file_path, index=False, header=False)

        return calculated_level_shift, output_file_name
    return None, None


def main():
    YTY_DIFF_CSV_PATH = "../Seatek_Analysis_Summary.xlsx - Year-to-Year Differences.csv"
    identified_outliers_df = load_identified_outliers(YTY_DIFF_CSV_PATH)

    if not identified_outliers_df.empty:
        os.makedirs(CORRECTED_OUTPUT_DIR, exist_ok=True)
        print(f"\n--- Applying Refined Level Shift Corrections ---")

        applied_corrections = []
        raw_file_map = build_raw_file_map(DATA_DIR)

        for outlier_row in identified_outliers_df.itertuples(index=False):
            outlier_year_pair_str = outlier_row.Year_Pair
            outlier_sensor = outlier_row.Sensor
            original_difference = outlier_row.Difference

            prev_year_yy, next_year_yy, sensor_col_index, valid = parse_outlier_info(
                outlier_year_pair_str, outlier_sensor
            )

            if not valid:
                continue

            outlier_series_id, prev_year_raw_file, next_year_raw_file = find_raw_files_for_outlier(
                raw_file_map, prev_year_yy, next_year_yy
            )

            if not outlier_series_id:
                print(f"Could not find raw data files for outlier {outlier_year_pair_str}, {outlier_sensor}. Skipping.")
                continue

            try:
                shift, out_name = apply_correction(
                    prev_year_raw_file, next_year_raw_file, sensor_col_index, CORRECTED_OUTPUT_DIR
                )

                if shift is not None:
                    applied_corrections.append({
                        "Series": outlier_series_id,
                        "Year_Pair_Outlier": outlier_year_pair_str,
                        "Sensor": outlier_sensor,
                        "Original_Difference_Summary": original_difference,
                        "Calculated_Level_Shift": shift,
                        "Correction_Type": "Level Shift",
                        "File_Corrected": out_name,
                        "Rationale": f"Aligned first 5 non-zero avg of Y{next_year_yy:02d} with last 5 non-zero avg of Y{prev_year_yy:02d}.",
                    })
                else:
                    print(f"  Not enough data points or incorrect column index ({sensor_col_index}) in raw files for {outlier_year_pair_str}, {outlier_sensor}. Skipping.")

            except FileNotFoundError:
                print(f"  Error: Raw data file not found for {outlier_year_pair_str}, {outlier_sensor}.")
            except Exception as e:
                print(f"  An error occurred while processing outlier {outlier_year_pair_str}, {outlier_sensor}: {e}")

        if applied_corrections:
            pd.DataFrame(applied_corrections).to_csv(CORRECTION_LOG_PATH, index=False)
            print(f"\nCorrection log saved to: {CORRECTION_LOG_PATH}")
        else:
            print("\nNo refined corrections were applied.")

        print("\n--- Refined Level Shift Corrections Complete ---")


if __name__ == '__main__':
    main()

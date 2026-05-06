import argparse
import re

import pandas as pd


def load_data(correction_log_path, updated_averages_csv_path):
    try:
        df_log = pd.read_csv(correction_log_path)
        df_averages = pd.read_csv(updated_averages_csv_path)
        print(f"Successfully loaded correction log from: {correction_log_path}")
        print(f"Successfully loaded updated averages from: {updated_averages_csv_path}")
        return df_log, df_averages
    except FileNotFoundError as e:
        print(f"\nError: Required file not found: {e}.")
        print("Please ensure the required input files are present, or update the file paths.")
        return None, None
    except Exception as e:
        print(f"\nAn error occurred while loading data: {e}")
        return None, None


def build_avg_lookup(df_averages):
    return {
        (row.Series, row.Year_Num_YY): {
            "Beginning_Average": row.Beginning_Average,
            "End_Average": row.End_Average,
        }
        for row in df_averages.itertuples(index=False)
    }


def parse_year_pair(year_pair_outlier_str):
    pair_match = re.match(
        r"(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)", str(year_pair_outlier_str)
    )
    if not pair_match:
        return None, None

    year1_full, year1_yy, year2_full, year2_yy = map(int, pair_match.groups())
    if year1_full < year2_full:
        return year1_yy, year2_yy
    return year2_yy, year1_yy


def process_log_row(row, avg_lookup):
    series = row.Series
    sensor = row.Sensor

    prev_year_yy, next_year_yy = parse_year_pair(row.Year_Pair_Outlier)
    if prev_year_yy is None:
        return None, row.Year_Pair_Outlier

    end_avg_prev_year_corrected = avg_lookup.get(
        (series, prev_year_yy), {}
    ).get("End_Average", "N/A")

    begin_avg_next_year_corrected = avg_lookup.get(
        (series, next_year_yy), {}
    ).get("Beginning_Average", "N/A")

    try:
        orig_diff_val = round(row.Original_Difference_Summary, 3)
    except Exception:
        orig_diff_val = row.Original_Difference_Summary

    try:
        calc_level_shift_val = round(row.Calculated_Level_Shift, 3)
    except Exception:
        calc_level_shift_val = row.Calculated_Level_Shift

    return {
        "Series": series,
        "Year_Pair_YY": f"Y{prev_year_yy:02d} to Y{next_year_yy:02d}",
        "Sensor": sensor,
        "Original_Diff_Summary": orig_diff_val,
        "Calculated_Level_Shift_Applied": calc_level_shift_val,
        "End_Avg_Prev_Year_Corrected": end_avg_prev_year_corrected,
        "Begin_Avg_Next_Year_Corrected": begin_avg_next_year_corrected,
    }, None


def main(correction_log_path, updated_averages_csv_path):
    print("--- Script to Generate Refined Overview Table Data ---")

    df_log, df_averages = load_data(correction_log_path, updated_averages_csv_path)
    if df_log is None or df_averages is None:
        return

    try:
        overview_data = []
        unmatched_year_pairs = []
        avg_lookup = build_avg_lookup(df_averages)

        df_log = df_log.sort_values(by=["Series", "Year_Pair_Outlier", "Sensor"])

        for row in df_log.itertuples(index=False):
            processed_row, unmatched = process_log_row(row, avg_lookup)
            if processed_row:
                overview_data.append(processed_row)
            if unmatched:
                unmatched_year_pairs.append(unmatched)

        if unmatched_year_pairs:
            print(f"\nWARNING: The following Year_Pair_Outlier strings could not be parsed and were skipped:")
            for s in unmatched_year_pairs:
                print(f"  - {s}")

        df_overview = pd.DataFrame(overview_data)

        print("\n--- Content for Refined Overview of Level Shift Strategies Table (CSV Format) ---")
        print(df_overview.to_csv(index=False))
        print("--- End Content for Refined Overview Table ---")

    except Exception as e:
        print(f"\nAn error occurred while generating Overview table content: {e}")

    print("\n--- Script Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Refined Overview Table Data")
    parser.add_argument(
        "--correction_log_path",
        type=str,
        default="series_correction_project_updated/correction_log_refined_shift.csv",
        help="Path to correction_log_refined_shift.csv",
    )
    parser.add_argument(
        "--updated_averages_csv_path",
        type=str,
        default="series_correction_project_updated/updated_beginning_end_averages.csv",
        help="Path to updated_beginning_end_averages.csv",
    )
    args = parser.parse_args()
    main(args.correction_log_path, args.updated_averages_csv_path)

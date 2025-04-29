import argparse
import re

import pandas as pd


def main(correction_log_path, updated_averages_csv_path):
    """
    Generates a refined overview table summarizing level shift strategies applied
    based on provided correction log and updated averages CSV files.

    Parameters:
    - correction_log_path (str): Path to the correction log CSV file.
    - updated_averages_csv_path (str): Path to the updated averages CSV file.
    """

    print("--- Script to Generate Refined Overview Table Data ---")

    try:
        # --- Load the necessary data ---
        df_log = pd.read_csv(correction_log_path)
        df_averages = pd.read_csv(updated_averages_csv_path)

        print(f"Successfully loaded correction log from: {correction_log_path}")
        print(f"Successfully loaded updated averages from: {updated_averages_csv_path}")

        # --- Process data to create the Overview table content ---
        overview_data = []
        unmatched_year_pairs = []

        # Dictionary for quick lookup of averages by (Series, Year_Num_YY)
        avg_lookup = {
            (row['Series'], row['Year_Num_YY']): {
                'Beginning_Average': row['Beginning_Average'],
                'End_Average': row['End_Average']
            }
            for _, row in df_averages.iterrows()
        }

        # Sort the log for deterministic output
        df_log = df_log.sort_values(by=['Series', 'Year_Pair_Outlier', 'Sensor'])

        for _, row in df_log.iterrows():
            series = row['Series']
            year_pair_outlier_str = row['Year_Pair_Outlier']
            sensor = row['Sensor']
            original_difference = row['Original_Difference_Summary']
            calculated_level_shift = row['Calculated_Level_Shift']

            # Parse year pair string
            pair_match = re.match(r'(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)', str(year_pair_outlier_str))
            if pair_match:
                year1_full, year1_yy, year2_full, year2_yy = map(int, pair_match.groups())
                if year1_full < year2_full:
                    prev_year_yy_pair = year1_yy
                    next_year_yy_pair = year2_yy
                else:
                    prev_year_yy_pair = year2_yy
                    next_year_yy_pair = year1_yy

                # Retrieve corrected averages or default to 'N/A'
                end_avg_prev_year_corrected = avg_lookup.get((series, prev_year_yy_pair), {}).get('End_Average', 'N/A')
                begin_avg_next_year_corrected = avg_lookup.get((series, next_year_yy_pair), {}).get('Beginning_Average',
                                                                                                    'N/A')

                # Defensive rounding for numeric values
                try:
                    orig_diff_val = round(original_difference, 3)
                except Exception:
                    orig_diff_val = original_difference
                try:
                    calc_level_shift_val = round(calculated_level_shift, 3)
                except Exception:
                    calc_level_shift_val = calculated_level_shift

                overview_data.append({
                    'Series': series,
                    'Year_Pair_YY': f"Y{prev_year_yy_pair:02d} to Y{next_year_yy_pair:02d}",
                    'Sensor': sensor,
                    'Original_Diff_Summary': orig_diff_val,
                    'Calculated_Level_Shift_Applied': calc_level_shift_val,
                    'End_Avg_Prev_Year_Corrected': end_avg_prev_year_corrected,
                    'Begin_Avg_Next_Year_Corrected': begin_avg_next_year_corrected
                })
            else:
                unmatched_year_pairs.append(year_pair_outlier_str)

        # Warn if any year pairs could not be parsed
        if unmatched_year_pairs:
            print(f"\nWARNING: The following Year_Pair_Outlier strings could not be parsed and were skipped:")
            for s in unmatched_year_pairs:
                print(f"  - {s}")

        # Create DataFrame for overview data
        df_overview = pd.DataFrame(overview_data)

        # Print the overview table content
        print("\n--- Content for Refined Overview of Level Shift Strategies Table (CSV Format) ---")
        print(df_overview.to_csv(index=False))
        print("--- End Content for Refined Overview Table ---")

    except FileNotFoundError as e:
        print(f"\nError: Required file not found: {e}.")
        print("Please ensure the required input files are present, or update the file paths.")
    except Exception as e:
        print(f"\nAn error occurred while generating Overview table content: {e}")
        import traceback
        traceback.print_exc()

    print("\n--- Script Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Refined Overview Table Data")
    parser.add_argument(
        "--correction_log_path",
        type=str,
        default="series_correction_project_updated/correction_log_refined_shift.csv",
        help="Path to correction_log_refined_shift.csv"
    )
    parser.add_argument(
        "--updated_averages_csv_path",
        type=str,
        default="series_correction_project_updated/updated_beginning_end_averages.csv",
        help="Path to updated_beginning_end_averages.csv"
    )
    args = parser.parse_args()
    main(args.correction_log_path, args.updated_averages_csv_path)

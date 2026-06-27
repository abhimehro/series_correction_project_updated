import argparse
import re

import pandas as pd
from scripts.spreadsheet_safety import write_csv_safely


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
            (series, year_num_yy): {
                "Beginning_Average": beg_avg,
                "End_Average": end_avg,
            }
            for series, year_num_yy, beg_avg, end_avg in zip(
                df_averages["Series"].to_numpy(),
                df_averages["Year_Num_YY"].to_numpy(),
                df_averages["Beginning_Average"].to_numpy(),
                df_averages["End_Average"].to_numpy(),
            )
        }

        # Sort the log for deterministic output
        df_log = df_log.sort_values(by=["Series", "Year_Pair_Outlier", "Sensor"])

        def process_outlier_log(s, yps, sen, od, cls):
            pm = re.match(r"(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)", str(yps))
            if pm:
                y1_f, y1_yy, y2_f, y2_yy = map(int, pm.groups())
                py, ny = (y1_yy, y2_yy) if y1_f < y2_f else (y2_yy, y1_yy)
                ea = avg_lookup.get((s, py), {}).get("End_Average", "N/A")
                ba = avg_lookup.get((s, ny), {}).get("Beginning_Average", "N/A")

                try:
                    od_val = round(od, 3)
                except Exception:
                    od_val = od
                try:
                    cls_val = round(cls, 3)
                except Exception:
                    cls_val = cls

                return {
                    "Series": s,
                    "Year_Pair_YY": f"Y{py:02d} to Y{ny:02d}",
                    "Sensor": sen,
                    "Original_Diff_Summary": od_val,
                    "Calculated_Level_Shift_Applied": cls_val,
                    "End_Avg_Prev_Year_Corrected": ea,
                    "Begin_Avg_Next_Year_Corrected": ba,
                }, None
            return None, yps

        for s, yps, sen, od, cls in zip(
            df_log["Series"].to_numpy(),
            df_log["Year_Pair_Outlier"].to_numpy(),
            df_log["Sensor"].to_numpy(),
            df_log["Original_Difference_Summary"].to_numpy(),
            df_log["Calculated_Level_Shift"].to_numpy(),
        ):
            record, unmatched = process_outlier_log(s, yps, sen, od, cls)
            if record:
                overview_data.append(record)
            if unmatched:
                unmatched_year_pairs.append(unmatched)

        # Warn if any year pairs could not be parsed
        if unmatched_year_pairs:
            print(
                "\nWARNING: The following Year_Pair_Outlier strings could not be parsed and were skipped:"
            )
            for s in unmatched_year_pairs:
                print(f"  - {s}")

        # Create DataFrame for overview data
        df_overview = pd.DataFrame(overview_data)

        # Print the overview table content
        print(
            "\n--- Content for Refined Overview of Level Shift Strategies Table (CSV Format) ---"
        )
        print(write_csv_safely(df_overview, index=False))
        print("--- End Content for Refined Overview Table ---")

    except FileNotFoundError as e:
        print(f"\nError: Required file not found: {e}.")
        print(
            "Please ensure the required input files are present, or update the file paths."
        )
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

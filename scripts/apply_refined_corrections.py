import pandas as pd
import os
import glob
import re

# Define directories (adjust paths if your local structure is different)
DATA_DIR = '../data'  # Updated path
# The script will generate corrected files in a new directory
CORRECTED_OUTPUT_DIR = '../corrected_output_refined_shift'  # Updated path
# The script will generate a log file
CORRECTION_LOG_PATH = '../correction_log_refined_shift.csv'  # Updated path

# --- Function to calculate non-zero average ---
def calculate_non_zero_average(series):
    """Calculates the average of a pandas Series, excluding zero values, coercing to numeric."""
    numeric_series = pd.to_numeric(series, errors='coerce').dropna()
    non_zero_values = numeric_series[numeric_series != 0]
    if not non_zero_values.empty:
        return non_zero_values.mean()
    else:
        return 0.0 # Return 0 if all non-NaN values are zero or series is empty

# --- Load Identified Outliers ---
# You will need to ensure the file 'Seatek_Analysis_Summary.xlsx - Year-to-Year Differences.csv'
# is in your project structure, in the location specified by YTY_DIFF_CSV_PATH.
# Based on previous steps, it was likely in the root of the uploaded files or a data/output folder.
# Let's assume it's accessible relative to where you run the script, or adjust the path as needed.
# Assuming the path based on a previous successful load:
YTY_DIFF_CSV_PATH = '../Seatek_Analysis_Summary.xlsx - Year-to-Year Differences.csv'  # Updated path

try:
    df_yty_diff = pd.read_csv(YTY_DIFF_CSV_PATH)
    # Identify the value columns based on the naming convention "Sensor 01" to "Sensor 32"
    actual_cols = df_yty_diff.columns.tolist()
    sensor_cols = [col for col in actual_cols if col.startswith('Sensor ') and col[len('Sensor '):].isdigit()]
    id_vars = ['Year_Pair'] # Corrected ID column name

    if not sensor_cols:
        print("Error: Could not find sensor columns (e.g., 'Sensor 01' to 'Sensor 32') in the Year-to-Year Differences CSV.")
        identified_outliers_df = pd.DataFrame() # Empty df to prevent script from running
    elif 'Year_Pair' not in actual_cols:
         print(f"Error: 'Year_Pair' column not found in the Year-to-Year Differences CSV. Available columns: {actual_cols}")
         identified_outliers_df = pd.DataFrame() # Empty df
    else:
        # Melt the DataFrame
        df_melted = df_yty_diff.melt(id_vars=id_vars,
                                     value_vars=sensor_cols,
                                     var_name='Sensor',
                                     value_name='Difference')

        # Filter for absolute difference >= 0.1, dropping rows with NaN differences
        identified_outliers_df = df_melted[df_melted['Difference'].abs() >= 0.1].dropna(subset=['Difference']).copy()

        if identified_outliers_df.empty:
            print("No outliers (|Difference| >= 0.1) found in the Year-to-Year Differences CSV. No corrections will be applied.")
        else:
             print(f"Successfully loaded {len(identified_outliers_df)} outliers from {YTY_DIFF_CSV_PATH}")

except FileNotFoundError:
    print(f"Error: The file '{YTY_DIFF_CSV_PATH}' was not found. Please ensure it's in the correct location.")
    identified_outliers_df = pd.DataFrame() # Empty df to prevent script from running
except Exception as e:
    print(f"An error occurred while loading or processing the Year-to-Year Differences CSV: {e}")
    identified_outliers_df = pd.DataFrame() # Empty df


# --- Main Correction Logic ---
if not identified_outliers_df.empty:
    # Create the output directory if it doesn't exist
    os.makedirs(CORRECTED_OUTPUT_DIR, exist_ok=True)

    print(f"\n--- Applying Refined Level Shift Corrections ---")

    applied_corrections = []

    # Create a dictionary to easily access raw data file paths by series and year number (YY) for all raw data
    raw_file_map = {}
    all_raw_files = glob.glob(os.path.join(DATA_DIR, "S*_Y*.txt"))
    for raw_file_path in all_raw_files:
         file_name = os.path.basename(raw_file_path)
         file_match = re.match(r'(S\d+)_Y(\d+)\.txt', file_name)
         if file_match:
              series_id = file_match.group(1)
              year_num = int(file_match.group(2))
              if series_id not in raw_file_map:
                  raw_file_map[series_id] = {}
              raw_file_map[series_id][year_num] = raw_file_path


    # Iterate through the identified outliers
    for idx, outlier_row in identified_outliers_df.iterrows():
        outlier_year_pair_str = outlier_row['Year_Pair']
        outlier_sensor = outlier_row['Sensor']
        original_difference = outlier_row['Difference'] # Difference from summary file

        # Parse the Year_Pair string (e.g., "1996 (Y02) to 1995 (Y01)")
        pair_match = re.match(r'(\d+) \(Y(\d+)\) to (\d+) \(Y(\d+)\)', outlier_year_pair_str)
        if not pair_match:
            continue

        year1_full, year1_yy, year2_full, year2_yy = map(int, pair_match.groups())

        # Determine previous and next year numbers (YY) and full years based on sorted full years
        if year1_full < year2_full:
             prev_year_yy_pair = year1_yy
             next_year_yy_pair = year2_yy
             prev_year_full_pair = year1_full
             next_year_full_pair = year2_full
        else: # Handle reversed year pairs
             prev_year_yy_pair = year2_yy
             next_year_yy_pair = year1_yy
             prev_year_full_pair = year2_full
             next_year_full_pair = year1_full


        # Determine sensor column index (Sensor 01 is index 0, ..., Sensor 32 is index 31)
        try:
            sensor_col_index = int(outlier_sensor.replace('Sensor ', '')) - 1
            if not 0 <= sensor_col_index < 32:
                 print(f"Skipping outlier with invalid sensor column index for {outlier_sensor}.")
                 continue
        except ValueError:
             print(f"Skipping outlier with unparseable sensor number from {outlier_sensor}.")
             continue

        # Find the raw data files for the previous and next year numbers (YY)
        outlier_series_id = None
        prev_year_raw_file = None
        next_year_raw_file = None

        # Try to find files for both S26 and S27 for the year pair
        for s_id in ['S26', 'S27']:
            if s_id in raw_file_map and prev_year_yy_pair in raw_file_map[s_id] and next_year_yy_pair in raw_file_map[s_id]:
                 outlier_series_id = s_id
                 prev_year_raw_file = raw_file_map[s_id][prev_year_yy_pair]
                 next_year_raw_file = raw_file_map[s_id][next_year_yy_pair]
                 break # Found the series and files for this outlier

        if not outlier_series_id:
            print(f"Could not find raw data files for outlier {outlier_year_pair_str}, {outlier_sensor}. Skipping correction.")
            continue # Skip this outlier

        # print(f"\nAnalyzing Outlier: {outlier_year_pair_str}, {outlier_sensor}") # Uncomment for detailed processing output

        try:
            # Read raw data for previous and next year
            df_prev_raw = pd.read_csv(prev_year_raw_file, header=None, sep=r'\s+', engine='python')
            df_next_raw = pd.read_csv(next_year_raw_file, header=None, sep=r'\s+', engine='python')

            # Ensure dataframes have enough rows (at least 5) and the sensor column
            if len(df_prev_raw) >= 5 and len(df_next_raw) >= 5 and df_prev_raw.shape[1] > sensor_col_index and df_next_raw.shape[1] > sensor_col_index:
                # Calculate the average of the last 5 non-zero rows of the previous year
                prev_tail_5_avg_non_zero = calculate_non_zero_average(df_prev_raw.iloc[-5:, sensor_col_index])

                # Calculate the average of the first 5 non-zero rows of the next year (from current file)
                next_head_5_avg_non_zero = calculate_non_zero_average(df_next_raw.iloc[:5, sensor_col_index])

                # Calculate the required level shift
                # The goal is to make the next_head_5_avg_non_zero equal to prev_tail_5_avg_non_zero
                # So we need to add (prev_tail_5_avg_non_zero - next_head_5_avg_non_zero) to the next year's data
                calculated_level_shift = prev_tail_5_avg_non_zero - next_head_5_avg_non_zero

                # print(f"  Prev Year ({prev_year_full}) Last 5 Non-Zero Avg ({os.path.basename(prev_year_raw_file)}): {prev_tail_5_avg_non_zero:.3f}") # Uncomment for detailed processing output
                # print(f"  Next Year ({next_year_full}) First 5 Non-Zero Avg ({os.path.basename(next_year_raw_file)}): {next_head_5_avg_non_zero:.3f}") # Uncomment for detailed processing output
                # print(f"  Calculated Level Shift Needed: {calculated_level_shift:.3f}") # Uncomment for detailed processing output
                # print(f"  Original Difference (from summary file): {original_difference:.3f}") # Uncomment for detailed processing output


                # Read the next year's raw data file again to apply correction and save
                # (Reading again to avoid modifying df_next_raw if it's needed elsewhere, though not in this script)
                # We need to be careful here: if the next year file has already been corrected by a previous outlier
                # in this loop, reading the raw file again will discard that correction.
                # A better approach is to load all raw files once, store them, and modify the stored dataframes.
                # For this script, we'll assume idempotency of corrections applied in sequence for simplicity,
                # but in a more complex pipeline, managing state is important.
                df_next_corrected = pd.read_csv(next_year_raw_file, header=None, sep=r'\s+', engine='python')


                # Apply the calculated Level Shift correction to the entire next year's data for this sensor
                # Convert column to numeric, coercing errors to NaN, before correction
                df_next_corrected[sensor_col_index] = pd.to_numeric(df_next_corrected[sensor_col_index], errors='coerce') + calculated_level_shift

                # Log the correction
                applied_corrections.append({
                    'Series': outlier_series_id,
                    'Year_Pair_Outlier': outlier_year_pair_str,
                    'Sensor': outlier_sensor,
                    'Original_Difference_Summary': original_difference,
                    'Calculated_Level_Shift': calculated_level_shift,
                    'Correction_Type': 'Level Shift',
                    'File_Corrected': os.path.basename(next_year_raw_file).replace('.txt', '_refined_corrected.csv'),
                    'Rationale': f'Aligned first 5 non-zero avg of Y{next_year_yy_pair:02d} with last 5 non-zero avg of Y{prev_year_yy_pair:02d}.'
                })
                # print(f"  Applied Level Shift of {calculated_level_shift:.3f} to {outlier_sensor} in {os.path.basename(next_year_raw_file)}") # Uncomment for detailed processing output

                # Save the corrected next year's DataFrame
                # Construct the output filename (e.g., S26_Y01_refined_corrected.csv)
                output_file_name = os.path.basename(next_year_raw_file).replace('.txt', '_refined_corrected.csv')
                output_file_path = os.path.join(CORRECTED_OUTPUT_DIR, output_file_name)

                # Save as standard comma-separated CSV with no header
                df_next_corrected.to_csv(output_file_path, index=False, header=False)

                # print(f"  Saved refined corrected data to: {output_file_path}") # Uncomment for detailed processing output


            else:
                print(f"  Not enough data points (at least 5) or incorrect column index ({sensor_col_index}) in raw files for {outlier_year_pair_str}, {outlier_sensor}. Skipping correction for this outlier.")


        except FileNotFoundError:
            print(f"  Error: Raw data file not found for {outlier_year_pair_str}, {outlier_sensor}.")
        except Exception as e:
            print(f"  An error occurred while processing outlier {outlier_year_pair_str}, {outlier_sensor}: {e}")

    # Save the correction log
    if applied_corrections:
        df_corrections_log = pd.DataFrame(applied_corrections)
        df_corrections_log.to_csv(CORRECTION_LOG_PATH, index=False)
        print(f"\nCorrection log saved to: {CORRECTION_LOG_PATH}")
    else:
        print("\nNo refined corrections were applied.")

    print("\n--- Refined Level Shift Corrections Complete ---")

else:
    print("\nNo outliers loaded from the Year-to-Year Differences CSV. No corrections were applied.")

# --- Script Ends ---


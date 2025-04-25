import os
import glob
import sys
import pandas as pd

# Add project to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

print(f"Current working directory: {os.getcwd()}")

# 1. Search the entire project for any Excel files
print("\n===== SEARCHING FOR ANY EXCEL FILES =====")
all_excel_files = []
for root, dirs, files in os.walk(PROJECT_ROOT):
    for file in files:
        if file.endswith(('.xlsx', '.xls')):
            full_path = os.path.join(root, file)
            all_excel_files.append(full_path)
            print(f"Found: {full_path}")

if not all_excel_files:
    print("No Excel files found anywhere in the project")

# 2. Make sure output directories exist
output_dir = os.path.join(PROJECT_ROOT, "data", "output")
os.makedirs(output_dir, exist_ok=True)
print(f"\nCreated output directory: {output_dir}")

# 3. Process a single file manually to see where it goes
print("\n===== PROCESSING A SINGLE FILE =====")
from scripts.processor import process_data

# Find a raw data file
raw_files = []
data_dir = os.path.join(PROJECT_ROOT, "data")
for file in glob.glob(os.path.join(data_dir, "S*.txt")):
    raw_files.append(file)

if raw_files:
    test_file = raw_files[0]
    print(f"Processing test file: {test_file}")

    # Load the file
    try:
        df = pd.read_csv(
            test_file,
            header=None,
            sep=r"\s+",
            engine="python",
            comment="#",
            skip_blank_lines=True,
        )

        # Apply column names
        if all(isinstance(c, int) for c in df.columns):
            cols = [f"Value{i + 1}" for i in range(len(df.columns))]
            if cols:
                cols[0] = "Time (Seconds)"
            df.columns = cols

        print(f"Loaded data shape: {df.shape}")

        # Process with the corrected threshold
        config = {
            "window_size": 5,
            "threshold": 3.0,
            "gap_threshold_factor": 3.0,
            "gap_method": "time",
            "outlier_method": "median",
            "jump_method": "offset",
            "time_col": "Time (Seconds)",
            "value_col": None
        }

        processed_df = process_data(df, config)

        # Extract series and year info from filename
        filename = os.path.basename(test_file)
        parts = filename.split('_')
        series = parts[0][1:]  # Remove the 'S'
        year_idx = parts[1].split('.')[0]  # Get Y01, Y02, etc.

        # Create a very clear filename
        output_path = os.path.join(output_dir, f"TEST_Series{series}_Year{year_idx}.xlsx")

        # Save with explicit writer
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            processed_df.to_excel(writer, index=False)

        print(f"Explicitly saved file to: {output_path}")
        print(f"Check if file exists: {os.path.exists(output_path)}")

    except Exception as e:
        print(f"Error processing test file: {e}")
else:
    print("No raw data files found to process")

print("\nScript completed. Please check the output directory.")
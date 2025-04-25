import os
import sys
import pandas as pd
import glob

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import your processing functions
from scripts.processor import process_data

# Set up directories
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
NEW_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "fixed_output")
os.makedirs(NEW_OUTPUT_DIR, exist_ok=True)

# Find all raw data files
raw_files = []
for series in [26, 27]:
    pattern = os.path.join(DATA_DIR, f"S{series}_Y*.txt")
    found = glob.glob(pattern)
    raw_files.extend(found)

print(f"Found {len(raw_files)} raw data files")

# Process them one by one
for i, file_path in enumerate(raw_files):
    # Extract series and year index from filename
    filename = os.path.basename(file_path)
    parts = filename.split('_')
    series = parts[0][1:]  # Remove the 'S'
    year_idx = parts[1].split('.')[0]  # Get Y01, Y02, etc.

    try:
        # Load raw data
        df = pd.read_csv(
            file_path,
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

        # Process data with good parameters
        processor_config = {
            "window_size": 5,
            "threshold": 3.0,
            "gap_threshold_factor": 3.0,
            "gap_method": "time",
            "outlier_method": "median",
            "jump_method": "offset",
            "time_col": "Time (Seconds)",
            "value_col": None
        }

        processed_df = process_data(df, processor_config)

        # Save with clear, unambiguous filename
        output_path = os.path.join(NEW_OUTPUT_DIR, f"Series{series}_Year{year_idx}_Processed.xlsx")
        processed_df.to_excel(output_path, index=False)

        print(f"Processed {i + 1}/{len(raw_files)}: {filename} → {os.path.basename(output_path)}")

    except Exception as e:
        print(f"Error processing {filename}: {e}")

print(f"\nAll done! {len(raw_files)} files processed.")
print(f"Check your files in: {NEW_OUTPUT_DIR}")
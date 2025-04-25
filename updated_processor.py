import os
import glob
import pandas as pd
import numpy as np

# Paths
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "final_output")

# Create fresh output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Will save processed files to: {OUTPUT_DIR}")

# Find all data files
all_files = sorted(glob.glob(os.path.join(DATA_DIR, "S*.txt")))
print(f"Found {len(all_files)} files to process")


# Simple outlier detection without using pandas rolling functions
def detect_outliers(values, threshold=3.0):
    """Very simple outlier detection that avoids pandas rolling functions"""
    # Convert to numpy array and handle missing values
    values_array = np.array(values)

    # Get median of non-nan values
    median = np.nanmedian(values_array)

    # Calculate absolute deviation from median
    abs_dev = np.abs(values_array - median)

    # Get median of absolute deviations (MAD)
    mad = np.nanmedian(abs_dev)

    # Avoid division by zero
    mad = max(mad, 0.0001)

    # Calculate z-scores
    z_scores = 0.6745 * abs_dev / mad

    # Flag outliers
    outliers = z_scores > threshold

    # Create corrected values
    corrected = values_array.copy()
    corrected[outliers] = median

    return corrected, outliers


# Process each file
for i, file_path in enumerate(all_files):
    filename = os.path.basename(file_path)

    # Extract series and year info
    parts = filename.split('_')
    series = parts[0][1:]  # Remove the 'S'
    year_idx = parts[1].split('.')[0]  # Get Y01, Y02, etc.

    # Calculate year (assuming Y01 = 1995)
    year = 1994 + int(year_idx[1:])

    print(f"Processing {i + 1}/{len(all_files)}: {filename} (Series {series}, Year {year})")

    try:
        # Read the file as plain text first to examine it
        with open(file_path, 'r') as f:
            lines = f.readlines()

        # Skip comment lines and parse manually
        data_lines = [line.strip() for line in lines if not line.startswith('#')]

        # Parse the data manually to avoid pandas issues
        raw_data = []
        for line in data_lines:
            if line:  # Skip empty lines
                values = line.split()
                if len(values) >= 2:  # Ensure we have at least time and value
                    time_val = float(values[0])
                    sensor_val = float(values[1])
                    raw_data.append((time_val, sensor_val))

        # Convert to simple DataFrame
        df = pd.DataFrame(raw_data, columns=['Time', 'Value'])

        # Apply outlier detection
        df['Processed_Value'], df['Is_Outlier'] = detect_outliers(df['Value'], threshold=3.0)

        # Save to Excel
        out_filename = f"Series{series}_Year{year}_Processed.xlsx"
        out_path = os.path.join(OUTPUT_DIR, out_filename)

        df.to_excel(out_path, index=False)
        print(f"  Saved: {out_filename}")

    except Exception as e:
        print(f"  Error processing {filename}: {e}")

print(f"\nProcessing complete! All files saved to: {OUTPUT_DIR}")
import os
from glob import glob

import numpy as np
from pandas import concat, merge, read_csv, read_excel

from scripts.spreadsheet_safety import write_excel_safely

RAW_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "output")
)
COMPARISON_DIR = os.path.join(OUTPUT_DIR, "comparisons")
os.makedirs(COMPARISON_DIR, exist_ok=True)


def find_matching_raw_file(processed_filename):
    # Assumes processed files are named like 'Year_1995 (Y01)_Data.xlsx' or 'Series26_File01_Processed.xlsx'
    # Attempts to extract series and year index
    import re

    m = re.search(r"Series(\d+)_File(\d+)_Processed", processed_filename)
    if m:
        series = int(m.group(1))
        file_idx = int(m.group(2))
        # Try S{series}_Y{file_idx:02d}.txt
        raw_candidate = f"S{series}_Y{file_idx:02d}.txt"
        raw_path = os.path.join(RAW_DATA_DIR, raw_candidate)
        if os.path.isfile(raw_path):
            return raw_path
    m2 = re.search(r"Year_(\d+) \(Y(\d+)\)_Data", processed_filename)
    if m2:
        yidx = int(m2.group(2))
        # Try to find S??_Y{yidx:02d}.txt
        for f in os.listdir(RAW_DATA_DIR):
            if f.endswith(f"_Y{yidx:02d}.txt"):
                return os.path.join(RAW_DATA_DIR, f)
    return None


def detect_outliers_series(values, window_size=5, threshold=3.0):
    # Simple rolling MAD outlier detection for a pandas Series
    rolling_median = values.rolling(window=window_size, center=True).median().to_numpy()
    # Optimization: Use pure NumPy instead of creating pd.Series inside the tight rolling loop
    # This significantly reduces object creation overhead.
    rolling_mad = (
        values.rolling(window=window_size, center=True)
        .apply(lambda x: np.nanmedian(np.abs(x - np.nanmedian(x))), raw=True)
        .to_numpy()
    )
    mad_scale_factor = 1.4826
    rolling_scaled_mad = rolling_mad * mad_scale_factor
    values_np = values.to_numpy()

    # Calculate absolute differences
    abs_diff = np.abs(values_np - rolling_median)

    # ⚡ Bolt: Vectorize z-score calculation and outlier detection loop using NumPy arrays
    with np.errstate(divide="ignore", invalid="ignore"):
        z_scores = np.where(
            rolling_scaled_mad < 1e-6,
            np.where(abs_diff > 1e-6, np.inf, 0.0),
            abs_diff / rolling_scaled_mad,
        )

    valid_mask = ~(np.isnan(rolling_median) | np.isnan(rolling_scaled_mad))
    outlier_mask = valid_mask & (z_scores > threshold)

    return np.where(outlier_mask)[0].tolist()


def export_comparisons():
    processed_files = glob(os.path.join(OUTPUT_DIR, "*.xlsx"))
    for proc_file in processed_files:
        fname = os.path.basename(proc_file)
        if fname.startswith("Seatek_Analysis_Summary"):  # Skip summary
            continue
        raw_file = find_matching_raw_file(fname)
        if not raw_file:
            print(f"[WARN] No matching raw file for {fname}")
            continue
        # Load raw and processed
        try:
            raw_df = read_csv(
                raw_file,
                sep=r"\s+",
                header=None,
                engine="python",
                comment="#",
                skip_blank_lines=True,
            )
            if all(isinstance(c, int) for c in raw_df.columns):
                cols = [f"Value{i + 1}" for i in range(len(raw_df.columns))]
                if cols:
                    cols[0] = "Time (Seconds)"
                raw_df.columns = cols
        except (IOError, ValueError, FileNotFoundError) as e:
            print(f"[WARN] Could not load raw file {raw_file}: {e}")
            continue
        except Exception as e:
            print(
                f"[WARN] Unexpected error loading raw file {raw_file}: {type(e).__name__}: {e}"
            )
            continue
        try:
            proc_df = read_excel(proc_file)
        except (IOError, ValueError, FileNotFoundError) as e:
            print(f"[WARN] Could not load processed file {proc_file}: {e}")
            continue
        except Exception as e:
            print(
                f"[WARN] Unexpected error loading processed file {proc_file}: {type(e).__name__}: {e}"
            )
            continue
        # Store a reference to proc_df to show it's used in the function
        processed_df = proc_df  # Explicitly show this variable is used

        # Align by time column
        if (
            "Time (Seconds)" in raw_df.columns
            and "Time (Seconds)" in processed_df.columns
        ):
            merged = merge(
                raw_df,
                processed_df,
                on="Time (Seconds)",
                suffixes=("_raw", "_processed"),
                how="outer",
            )
        else:
            merged = concat([raw_df, processed_df], axis=1)
        # Outlier detection on raw data (main value col)
        value_cols = [c for c in raw_df.columns if c.startswith("Value")]
        if value_cols:
            vcol = value_cols[1] if len(value_cols) > 1 else value_cols[0]
            outlier_indices = detect_outliers_series(raw_df[vcol])
            merged["Outlier_Flag"] = False
            merged.loc[merged.index.isin(outlier_indices), "Outlier_Flag"] = True
        # Export
        out_path = os.path.join(
            COMPARISON_DIR, fname.replace(".xlsx", "_comparison.xlsx")
        )
        write_excel_safely(merged, out_path, index=False)
        print(f"[INFO] Exported comparison: {out_path}")


# Initialize these variables at module level to avoid undefined variable warnings
# They will be properly set during execution
raw_df = None
processed_df = None

if __name__ == "__main__":
    export_comparisons()

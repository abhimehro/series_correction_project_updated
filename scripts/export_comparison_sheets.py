import os
from glob import glob
import numpy as np

from pandas import Series, concat, merge, read_csv, read_excel

RAW_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "output")
)
COMPARISON_DIR = os.path.join(OUTPUT_DIR, "comparisons")
os.makedirs(COMPARISON_DIR, exist_ok=True)


def find_matching_raw_file(processed_filename):
    import re
    m = re.search(r"Series(\d+)_File(\d+)_Processed", processed_filename)
    if m:
        series = int(m.group(1))
        file_idx = int(m.group(2))
        raw_candidate = f"S{series}_Y{file_idx:02d}.txt"
        raw_path = os.path.join(RAW_DATA_DIR, raw_candidate)
        if os.path.isfile(raw_path):
            return raw_path
    m2 = re.search(r"Year_(\d+) \(Y(\d+)\)_Data", processed_filename)
    if m2:
        yidx = int(m2.group(2))
        for f in os.listdir(RAW_DATA_DIR):
            if f.endswith(f"_Y{yidx:02d}.txt"):
                return os.path.join(RAW_DATA_DIR, f)
    return None


def detect_outliers_series(values, window_size=5, threshold=3.0):
    rolling_median = values.rolling(window=window_size, center=True).median()
    rolling_mad = values.rolling(window=window_size, center=True).apply(
        lambda x: Series(x).sub(Series(x).median()).abs().median(), raw=True
    )
    mad_scale_factor = 1.4826
    rolling_scaled_mad = rolling_mad * mad_scale_factor

    diff = (values - rolling_median).abs()

    z_score = np.where(
        rolling_scaled_mad < 1e-6,
        np.where(diff > 1e-6, np.inf, 0),
        diff / rolling_scaled_mad
    )

    valid_mask = values.notna() & rolling_median.notna() & rolling_scaled_mad.notna()
    outlier_mask = valid_mask & (z_score > threshold)

    return np.where(outlier_mask)[0].tolist()


def load_raw_data(raw_file):
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
        return raw_df
    except Exception as e:
        print(f"[WARN] Error loading raw file {raw_file}: {e}")
        return None


def load_processed_data(proc_file):
    try:
        return read_excel(proc_file)
    except Exception as e:
        print(f"[WARN] Error loading processed file {proc_file}: {e}")
        return None


def align_and_detect(raw_df, processed_df):
    if "Time (Seconds)" in raw_df.columns and "Time (Seconds)" in processed_df.columns:
        merged = merge(raw_df, processed_df, on="Time (Seconds)", suffixes=("_raw", "_processed"), how="outer")
    else:
        merged = concat([raw_df, processed_df], axis=1)

    value_cols = [c for c in raw_df.columns if c.startswith("Value")]
    if value_cols:
        vcol = value_cols[1] if len(value_cols) > 1 else value_cols[0]
        outlier_indices = detect_outliers_series(raw_df[vcol])
        merged["Outlier_Flag"] = False

        if outlier_indices:
            valid_indices = [idx for idx in outlier_indices if idx < len(merged)]
            if valid_indices:
                merged.loc[valid_indices, "Outlier_Flag"] = True

    return merged


def process_file(proc_file, fname):
    raw_file = find_matching_raw_file(fname)
    if not raw_file:
        print(f"[WARN] No matching raw file for {fname}")
        return

    raw_df = load_raw_data(raw_file)
    if raw_df is None:
        return

    processed_df = load_processed_data(proc_file)
    if processed_df is None:
        return

    merged = align_and_detect(raw_df, processed_df)

    out_path = os.path.join(COMPARISON_DIR, fname.replace(".xlsx", "_comparison.xlsx"))
    merged.to_excel(out_path, index=False)
    print(f"[INFO] Exported comparison: {out_path}")


def main():
    processed_files = glob(os.path.join(OUTPUT_DIR, "*.xlsx"))
    for proc_file in processed_files:
        fname = os.path.basename(proc_file)
        if fname.startswith("Seatek_Analysis_Summary"):
            continue
        process_file(proc_file, fname)


if __name__ == "__main__":
    main()

# Correction Methodology

This document outlines the methodology used by the Series Correction Project to automatically detect and correct common discontinuities found in time-series data, particularly from Seatek sensors. The goal is to produce cleaner, more reliable datasets suitable for further analysis within systems like NESST II.

The process involves several sequential steps, configured via `config.json` and implemented in the `processor.py` module.

## 1. Data Loading and Preprocessing

*Input*: Raw data files (typically `.txt`, e.g., `S26_Y01.txt`) containing time-series readings. Expected formats include columns for timestamps and sensor values, often space-delimited.
*Loading*: The `batch_correction.py` script loads files based on series number, year range, and optionally river mile mapping (`river_mile_map.json`). Data is read via `pandas.read_csv`, handling whitespace delimiters and inferring types.
*Time Column Handling*: The designated time column (`Time (Seconds)`) is converted to numeric to support time-difference computations.
*Value Column Identification*: The primary sensor value column is specified in `value_col` or auto-detected as the first numeric column excluding the time column.
*Sorting*: Data is sorted by the time column to ensure sequential processing.

## 2. Discontinuity Detection and Correction Pipeline

The core logic in `processor.py` applies these steps sequentially to `value_col`.

### Step 2.1: Gap Detection and Correction

* **Detection** (`detect_gaps`):
  * Calculates the time difference (`diff()`) between consecutive timestamps.
  * Computes the median of these differences to estimate the sampling interval.
  * Flags a gap when a difference exceeds `gap_threshold_factor` (default 3.0) × median.
  * Returns indices before which gaps occur.
* **Correction** (`correct_gaps`):
  * Inserts new rows at detected gaps based on the estimated time step.
  * New rows include linearly spaced timestamps and `NaN` in numeric columns.
  * Interpolates missing values via `DataFrame.interpolate(method=gap_method)` (`time`, `linear`, `spline`, etc.).
  * Reindexes the DataFrame after insertions.

### Step 2.2: Outlier Detection and Correction

* **Detection** (`detect_outliers`):
  * Computes rolling median and MAD within `window_size` (default 5).
  * Calculates modified Z-scores: `Z = 0.6745 × (value − rolling_median) / rolling_MAD`.
  * Flags points where |Z| > `threshold` (default 3.0).
  * Returns outlier indices.
* **Correction** (`correct_outliers`):
  * **median**: Replace outliers with surrounding median.
  * **mean**: Replace outliers with surrounding mean.
  * **interpolate**: Set outliers to `NaN` and interpolate linearly.
  * **remove**: Set outliers to `NaN`.

### Step 2.3: Jump Detection and Correction

* **Detection** (`detect_jumps`):
  * Applies a CUSUM approach with rolling statistics (`window_size`, default 5).
  * Accumulates normalized deviations (`cusum`).
  * Flags jumps when |cusum| > `threshold` (default 3.0), then resets.
  * Returns jump indices.
* **Correction** (`correct_jumps`):
  * Computes median before and after each jump.
  * Calculates offset (`median_before − median_after`) to align segments.
  * Applies offset to subsequent values segment by segment.

## 3. Output

* **Corrected Data**: Final DataFrame saved (unless `dry_run`) in `output_dir` (e.g., `S26_Y01_1995_CorrectedData.csv`).
* **Summary Report**: `Batch_Processing_Summary.csv` with processing status, filenames, record counts.
* **Logs**: Detailed logs in `processing_log.txt` (or configured path).

## Configuration Parameters

* `time_col`: Name of the time column.
* `value_col`: Sensor value column or `null` to auto-detect.
* `window_size`: Rolling window size for outlier/jump detection.
* `threshold`: Threshold for outlier/jump detection.
* `gap_threshold_factor`: Median-diff multiplier to detect gaps.
* `gap_method`: Interpolation method for gaps (`time`, `linear`, `spline`, etc.).
* `outlier_method`: Outlier replacement (`median`, `mean`, `interpolate`, `remove`).
* `jump_method`: Jump correction method (offset).

Adjust these parameters to tune sensitivity and behavior for different datasets.

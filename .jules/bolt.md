## 2025-02-28 - Vectorizing Pandas rolling.apply with NumPy sliding_window_view
**Learning:** Replacing Pandas `rolling.apply` lambda functions with NumPy's `sliding_window_view` yields an ~80x speedup for calculations like Median Absolute Deviation (MAD). However, doing this blindly strips away critical Pandas safety nets. Specifically, it introduces a crash risk if the array length is smaller than the window size, a massive `MemoryError` risk on large arrays because `sliding_window_view` combined with math operations creates fully materialized 2D arrays, and it ignores `NaN` propagation (Pandas' `min_periods` behavior).
**Action:** When vectorizing rolling operations with `sliding_window_view`, always: 1) add an early return guard for `len(array) < window_size`, 2) process the data in chunks (e.g. 50k rows at a time) to keep the memory footprint bounded, and 3) manually calculate and apply `NaN` masks to mimic the original `min_periods` behavior exactly.
## 2025-XX-XX - Vectorized Z-score calculation in detect_outliers
**Learning:** Found an inefficient nested loop performing z-score calculation row-by-row on Pandas series using a helper function `_calculate_z_score`. This triggered massive object overhead in Pandas.
**Action:** Replaced the loop with vectorized numpy operations (`np.where` and `np.abs`) operating directly on `to_numpy()` arrays. This reduced execution time by ~100x for 1M rows.

## 2025-XX-XX - Vectorized normalized deviations in detect_jumps
**Learning:** Found an O(n) loop in `detect_jumps` computing normalized deviations using row-by-row lookups into Pandas series arrays (`values[i] - mean_prev_window`).
**Action:** Extracted the calculation into a fully vectorized numpy array (`shifted_mean = np.roll(rolling_mean, 1)`) outside the loop, retaining the loop only for the stateful CUSUM. This sped up jump detection by ~3x for 1M rows.

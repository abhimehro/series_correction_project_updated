## 2025-02-28 - Vectorizing Pandas rolling.apply with NumPy sliding_window_view
**Learning:** Replacing Pandas `rolling.apply` lambda functions with NumPy's `sliding_window_view` yields an ~80x speedup for calculations like Median Absolute Deviation (MAD). However, doing this blindly strips away critical Pandas safety nets. Specifically, it introduces a crash risk if the array length is smaller than the window size, a massive `MemoryError` risk on large arrays because `sliding_window_view` combined with math operations creates fully materialized 2D arrays, and it ignores `NaN` propagation (Pandas' `min_periods` behavior).
**Action:** When vectorizing rolling operations with `sliding_window_view`, always: 1) add an early return guard for `len(array) < window_size`, 2) process the data in chunks (e.g. 50k rows at a time) to keep the memory footprint bounded, and 3) manually calculate and apply `NaN` masks to mimic the original `min_periods` behavior exactly.

## 2025-02-28 - Safely Vectorizing CUSUM Rolling Operations
**Learning:** Pure vectorization of CUSUM-style algorithms (like `detect_jumps`) is not possible due to the stateful reset condition (where `cusum` drops to 0.0 when a threshold is breached). However, extracting all array math (offsets, subtractions, array masking, and divisions) into pre-calculated NumPy operations before the sequential loop reduces per-iteration overhead dramatically, yielding ~66% speedup.
**Action:** When vectorizing stateful loops, extract the non-stateful calculations into vectorized NumPy operations (`np.subtract`, `np.divide`, `np.roll`) before the loop. Use `with np.errstate(invalid="ignore"):` to cleanly suppress `RuntimeWarning`s caused by initial `NaN` values in the vectorized arrays.

## 2025-02-28 - Vectorizing Symmetric Windows with sliding_window_view
**Learning:** When vectorizing custom symmetric window logic (e.g. grabbing `radius` elements on both sides of a given index) using `sliding_window_view`, using the original `window_size` for `window_shape` can introduce a mathematical bug for even window sizes because Pandas `rolling(center=True)` drops the rightmost element (e.g. radius 2 left, 1 right). `sliding_window_view` cannot mimic this asymmetric behavior inherently.
**Action:** When migrating iterative window algorithms to `sliding_window_view`, calculate padding carefully (`pad_width = window_size // 2`) and set `window_shape = pad_width * 2 + 1` to ensure symmetric bounds matching the original explicit iteration logic.

## 2025-02-28 - Vectorizing Accumulating Offsets
**Learning:** In jump correction logic (`offsets[j] += diff`), simply translating this to `offsets[valid_indices] = diffs` causes a regression when `valid_indices` contains duplicates, effectively overwriting previous offsets rather than accumulating them.
**Action:** Use `np.add.at(offsets, indices, diffs)` to safely apply operations (like addition) over an array using a list of potentially repeating indices.

## 2025-03-01 - DataFrame-wide pd.to_numeric Optimization
**Learning:** In Pandas 2+, `pd.to_numeric(errors='ignore')` no longer works on entire DataFrames. Using a Python `for` loop to iteratively assign converted columns back to the DataFrame (`df[col] = pd.to_numeric(df[col])`) is slow due to DataFrame fragmentation and assignment overhead.
**Action:** When applying `to_numeric` across an entire DataFrame, use a dictionary comprehension to reconstruct the DataFrame directly: `pd.DataFrame({col: safe_numeric_func(df[col]) for col in df.columns})`. This provides a significant speedup by avoiding iterative assignment.

## $(date +%Y-%m-%d) - Optimizing Pandas iteration
**Learning:** `df.itertuples()` creates namedtuple objects per row, which introduces high object overhead in large datasets.
**Action:** Replace `itertuples()` with `zip(*[df[col].to_numpy() for col in required_columns])` to leverage NumPy arrays for a ~5x performance improvement when full vectorization isn't possible. Remember to update downstream function signatures to accept unpacked values instead of the namedtuple object.

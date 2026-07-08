
## 2025-06-30 - Optimize file discovery with os.listdir vs glob
**Learning:** Using `glob.glob` inside a loop for file discovery on a flat directory causes repeated O(N) directory scans which dramatically slows down initialization for large datasets or many matching series.
**Action:** When finding multiple files in a single flat directory based on varied criteria (like different series IDs), use a single `os.listdir` call to read the directory contents into memory once, then filter the resulting list with string operations and regex, reducing file I/O operations and providing up to a ~35x speedup in discovery.
## 2025-07-02 - Vectorize list comprehension window extraction
**Learning:** Using Python list comprehensions to extract multiple sliding windows from a NumPy array (e.g. `[arr[i:i+w] for i in valid_jumps]`) has significant Python iteration overhead.
**Action:** When extracting multiple offset windows from a sequence, use `numpy.lib.stride_tricks.sliding_window_view(arr, window_shape=w)` to create a memory-efficient view and then index into it directly (e.g., `windows[indices]`). This replaces Python loops with fast C-level operations and provides a substantial (~3x) speedup.

## 2025-03-05 - Optimize redundant directory listings
**Learning:** Calling `os.listdir()` repeatedly inside loops (like searching for matches file by file) causes significant I/O overhead.
**Action:** When searching a directory that remains static during execution, cache the `os.listdir()` results in memory (e.g., using a function attribute or module-level variable) on the first call to avoid redundant I/O operations and substantially improve performance.
## 2025-07-04 - Vectorize Pandas Time-Series Differences
**Learning:** Using Pandas `.diff()` and `.median()` along with boolean index filtering incurs significant overhead from Series instantiation, block manager operations, and index alignment.
**Action:** When calculating element-wise differences and filtering based on the median in performance-critical code, extract the underlying NumPy array (`.to_numpy()`) and use `np.diff()`, `np.median()`, and `np.where()`. This avoids Pandas overhead and provides a substantial (~35%) speedup. Since `np.diff` returns an array of length N-1, remember to adjust positional indices (e.g., `+ 1`) to map back to the original array correctly.
## 2025-07-08 - Reuse precomputed rolling arrays in windowed calculations
**Learning:** Calculating statistics on sliding window chunks redundantly (like calculating the median inside a loop chunk using `np.nanmedian`) introduces significant overhead when the same rolling statistic was already calculated over the full dataset upstream.
**Action:** When calculating statistics on sliding window chunks, do not redundantly recalculate values (like medians) using `np.nanmedian(chunk_windows, axis=1)` if a full `rolling_median` (or similar rolling statistic) has already been precomputed for the entire array. Reuse the existing array by slicing it (e.g., `rolling_median[start_idx + pad : end_idx + pad, np.newaxis]`) to avoid redundant computation and drastically improve performance (e.g., ~45% reduction in MAD calculation time).


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

## 2025-07-06 - Optimize redundant directory listings
**Learning:** Calling `os.listdir()` repeatedly inside loops or repeatedly called functions (like searching for matches file by file) causes significant I/O overhead.
**Action:** When searching a directory that remains static during execution, cache the `os.listdir()` results in memory. For clean state encapsulation, attach the cache to the function object itself (e.g., `func._cache`) on the first call to avoid redundant I/O operations and substantially improve performance without polluting the global namespace.

## 2025-10-24 - Use C engine for pd.read_csv
**Learning:** Pandas `pd.read_csv` using `engine="python"` is significantly slower (~10x) than the default C engine. The C engine natively supports regex separators like `sep=r"\s+"` and is much more efficient.
**Action:** Remove `engine="python"` when parsing files with `pd.read_csv` unless there is a specific feature that explicitly requires the Python engine.

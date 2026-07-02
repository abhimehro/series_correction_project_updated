
## 2025-06-30 - Optimize file discovery with os.listdir vs glob
**Learning:** Using `glob.glob` inside a loop for file discovery on a flat directory causes repeated O(N) directory scans which dramatically slows down initialization for large datasets or many matching series.
**Action:** When finding multiple files in a single flat directory based on varied criteria (like different series IDs), use a single `os.listdir` call to read the directory contents into memory once, then filter the resulting list with string operations and regex, reducing file I/O operations and providing up to a ~35x speedup in discovery.
## 2025-07-02 - Vectorize list comprehension window extraction
**Learning:** Using Python list comprehensions to extract multiple sliding windows from a NumPy array (e.g. `[arr[i:i+w] for i in valid_jumps]`) has significant Python iteration overhead.
**Action:** When extracting multiple offset windows from a sequence, use `numpy.lib.stride_tricks.sliding_window_view(arr, window_shape=w)` to create a memory-efficient view and then index into it directly (e.g., `windows[indices]`). This replaces Python loops with fast C-level operations and provides a substantial (~3x) speedup.


## 2025-06-30 - Optimize file discovery with os.listdir vs glob
**Learning:** Using `glob.glob` inside a loop for file discovery on a flat directory causes repeated O(N) directory scans which dramatically slows down initialization for large datasets or many matching series.
**Action:** When finding multiple files in a single flat directory based on varied criteria (like different series IDs), use a single `os.listdir` call to read the directory contents into memory once, then filter the resulting list with string operations and regex, reducing file I/O operations and providing up to a ~35x speedup in discovery.
## 2025-06-30 - Optimize window extraction with sliding_window_view
**Learning:** Using Python list comprehensions to extract multiple small sliding windows from a NumPy array (e.g. `[arr[i:i+w] for i in indices]`) has massive overhead due to repeated object creation and Python-level looping.
**Action:** When extracting multiple sliding windows centered or offset from arbitrary indices, use `numpy.lib.stride_tricks.sliding_window_view(arr, window_shape=w)` to create a fast, memory-efficient view of all possible windows, then index directly into it using `windows[indices]`. This provides an order-of-magnitude speedup (~12x) for window extraction.

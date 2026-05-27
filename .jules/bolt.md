## 2024-05-18 - Pandas iteration optimization
**Learning:** Iterating over Pandas DataFrames with `iterrows()` is a performance bottleneck due to Series creation overhead.
**Action:** Replace `iterrows()` with `itertuples(index=False)` and use attribute access for significant speedup without loss of readability.

## 2024-05-18 - Pandas Object Creation in rolling.apply
**Learning:** Creating pandas objects (like `pd.Series`) inside tightly grouped or rolling loops (e.g. `rolling.apply(lambda x: pd.Series(x)...)`) causes massive overhead due to repeated object instantiations.
**Action:** Pass `raw=True` to `apply`/`rolling.apply` and replace pandas operations with pure NumPy equivalents (like `np.nanmedian`) operating directly on the provided array `x` to eliminate object creation overhead while preserving logic.

## 2024-05-18 - Redundant Pandas Series wrapping
**Learning:** Wrapping slices returned from `iloc` or `loc` in a `pd.Series()` creates unnecessary overhead, and doing so with `pd.Series(list(slice))` is even worse.
**Action:** Call aggregate methods like `.median()` or `.mean()` directly on the returned slice (which is already a Series) to reduce object instantiation time.
## 2024-05-18 - Pandas iloc iteration overhead
**Learning:** Using `.iloc[i]` to fetch values inside a large for loop causes significant performance bottleneck due to Pandas API overhead.
**Action:** Extract the underlying arrays using `.to_numpy()` before the loop and use standard array indexing (`[i]`) inside the loop.

## 2024-05-22 - [Pandas .iloc within loops is a significant bottleneck]
**Learning:** Iterating over a Pandas Series row-by-row using a loop and indexing with `.iloc[]` is extremely slow. In `detect_outliers_series`, replacing a `for` loop that used `.iloc[]` with a fully vectorized NumPy implementation using `np.where` and mathematical masks reduced processing time for a 100k element series from ~2.1s to ~0.003s.
**Action:** When performing element-wise conditional logic on Pandas Series or DataFrames, extract the underlying data via `.to_numpy()` and vectorize operations using NumPy's conditional masks (`np.where`, `&`, `|`) instead of explicitly iterating in Python.
## 2024-06-25 - Avoid Quadratic pd.concat in DataFrame Iteration
**Learning:** Using `pd.concat` inside a loop that slices a large DataFrame at each iteration creates quadratic time complexity. This was occurring in `correct_gaps` during missing data point interpolation.
**Action:** Accumulate row dictionaries in a simple Python list (`all_new_rows`) within the loop. Convert this list to a DataFrame once outside the loop and apply `pd.concat` in one single operation at the end.
## 2024-06-13 - [Pandas Loop Bottleneck]
**Learning:** Creating empty Pandas DataFrame rows incrementally using a dictionary loop is highly inefficient. Benchmarks show `pd.DataFrame(np.nan, ...)` with column assignment is ~180x faster because it bypasses python loops and object overhead.
**Action:** Always pre-allocate entire DataFrame blocks or use vectorized creation (`pd.DataFrame(np.nan)`) before concatenating when generating placeholder rows, rather than building them row-by-row via dictionary appending.
## 2024-05-27 - [Pandas Datetime Numpy Fallbacks]
**Learning:** When optimizing away `.iloc` lookups for pandas timestamp columns, extracting columns with `.to_numpy(dtype=object)` creates massive memory regressions because it boxes millions of elements instead of keeping native C arrays. However, native `np.datetime64` arrays fail with `np.linspace`.
**Action:** Extract using native `.to_numpy()` to keep memory/performance high. For interpolation bound values extracted from this array, cast them to `pd.Timestamp()` and generate sequences using `pd.date_range()` instead of `np.linspace()`.

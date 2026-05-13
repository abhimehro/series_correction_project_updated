## 2024-05-18 - Pandas iteration optimization
**Learning:** Iterating over Pandas DataFrames with `iterrows()` is a performance bottleneck due to Series creation overhead.
**Action:** Replace `iterrows()` with `itertuples(index=False)` and use attribute access for significant speedup without loss of readability.

## 2024-05-18 - Pandas Object Creation in rolling.apply
**Learning:** Creating pandas objects (like `pd.Series`) inside tightly grouped or rolling loops (e.g. `rolling.apply(lambda x: pd.Series(x)...)`) causes massive overhead due to repeated object instantiations.
**Action:** Pass `raw=True` to `apply`/`rolling.apply` and replace pandas operations with pure NumPy equivalents (like `np.nanmedian`) operating directly on the provided array `x` to eliminate object creation overhead while preserving logic.
## 2024-05-13 - [Pandas .iloc Loop Bottlenecks]
**Learning:** Using `.iloc[i]` within large `for` loops in Pandas Series processing code causes massive overhead due to bounds-checking and object boxing. This was a major bottleneck in `detect_jumps` and `detect_outliers`.
**Action:** Always convert Series to raw NumPy arrays via `.to_numpy()` before doing element-wise loops, which increases performance by up to ~20x.

## 2024-05-18 - Pandas iteration optimization
**Learning:** Iterating over Pandas DataFrames with `iterrows()` is a performance bottleneck due to Series creation overhead.
**Action:** Replace `iterrows()` with `itertuples(index=False)` and use attribute access for significant speedup without loss of readability.

## 2024-05-18 - Pandas Object Creation in rolling.apply
**Learning:** Creating pandas objects (like `pd.Series`) inside tightly grouped or rolling loops (e.g. `rolling.apply(lambda x: pd.Series(x)...)`) causes massive overhead due to repeated object instantiations.
**Action:** Pass `raw=True` to `apply`/`rolling.apply` and replace pandas operations with pure NumPy equivalents (like `np.nanmedian`) operating directly on the provided array `x` to eliminate object creation overhead while preserving logic.

## 2024-05-18 - Redundant Pandas Series wrapping
**Learning:** Wrapping slices returned from `iloc` or `loc` in a `pd.Series()` creates unnecessary overhead, and doing so with `pd.Series(list(slice))` is even worse.
**Action:** Call aggregate methods like `.median()` or `.mean()` directly on the returned slice (which is already a Series) to reduce object instantiation time.

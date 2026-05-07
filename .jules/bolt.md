## 2024-05-18 - Pandas iteration optimization
**Learning:** Iterating over Pandas DataFrames with `iterrows()` is a performance bottleneck due to Series creation overhead.
**Action:** Replace `iterrows()` with `itertuples(index=False)` and use attribute access for significant speedup without loss of readability.
## 2024-06-25 - Pandas rolling apply lambda optimization
**Learning:** Creating Pandas objects (like `pd.Series`) inside `rolling.apply` lambda functions causes significant performance overhead.
**Action:** Use `raw=True` in the apply method and replace Pandas operations with pure NumPy equivalents (e.g., `np.nanmedian`) operating directly on the passed NumPy array.

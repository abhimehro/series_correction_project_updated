## 2024-05-18 - Pandas iteration optimization
**Learning:** Iterating over Pandas DataFrames with `iterrows()` is a performance bottleneck due to Series creation overhead.
**Action:** Replace `iterrows()` with `itertuples(index=False)` and use attribute access for significant speedup without loss of readability.

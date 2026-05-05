## 2024-05-18 - DataFrame Iteration Performance
**Learning:** Using `iterrows()` in Pandas DataFrames can be a significant bottleneck due to type inference and Series creation overhead on each iteration.
**Action:** Always prefer `itertuples(index=False)` and attribute access over `iterrows()` when iterating over DataFrames in this repository for better performance.

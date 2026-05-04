## 2024-05-18 - DataFrame Iteration Optimization
**Learning:** In this codebase's architecture, iterating over Pandas DataFrames using `iterrows()` is a performance anti-pattern because it creates a Series object for each row, adding significant overhead.
**Action:** Always prefer `itertuples(index=False)` combined with attribute access (e.g., `row.ColumnName`) instead of `iterrows()` when row-by-row DataFrame iteration is required.

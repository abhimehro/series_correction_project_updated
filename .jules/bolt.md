## 2024-06-25 - Avoid Quadratic pd.concat in DataFrame Iteration
**Learning:** Using `pd.concat` inside a loop that slices a large DataFrame at each iteration creates quadratic time complexity. This was occurring in `correct_gaps` during missing data point interpolation.
**Action:** Accumulate row dictionaries in a simple Python list (`all_new_rows`) within the loop. Convert this list to a DataFrame once outside the loop and apply `pd.concat` in one single operation at the end.

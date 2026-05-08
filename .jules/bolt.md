## 2024-05-24 - Pandas Series Instantiation Overhead in Rolling Apply

**Learning:** Creating `pandas.Series` objects inside `rolling.apply` lambda functions causes significant performance overhead in tight loops.
**Action:** Use `raw=True` in the `apply()` method to pass numpy arrays instead, and replace the inner pandas operations with pure `numpy` equivalents (like `np.nanmedian`) for faster computation.

## 2024-05-24 - Redundant Pandas Series Wrapping
**Learning:** Found an anti-pattern in `scripts/processor.py` where pandas operations (like `.iloc`, `.loc`) return Series, but are then needlessly wrapped in `pd.Series()` (sometimes even via `list()`) before calling `.median()` or `.mean()`. This causes unnecessary object creation and memory allocation.
**Action:** Always call aggregate methods directly on pandas Series slices without re-wrapping or converting to native Python types like lists.

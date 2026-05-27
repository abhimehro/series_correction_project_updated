## 2024-05-27 - [Pandas Datetime Numpy Fallbacks]
 **Learning:** When optimizing away `.iloc` lookups for pandas timestamp columns, extracting columns with `.to_numpy(dtype=object)` creates massive memory regressions because it boxes millions of elements instead of keeping native C arrays. However, native `np.datetime64` arrays fail with `np.linspace`.
 **Action:** Extract using native `.to_numpy()` to keep memory/performance high. For interpolation bound values extracted from this array, cast them to `pd.Timestamp()` and generate sequences using `pd.date_range()` instead of `np.linspace()`.

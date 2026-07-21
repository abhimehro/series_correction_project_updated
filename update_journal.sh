mkdir -p .jules
if [ ! -f .jules/bolt.md ]; then
  touch .jules/bolt.md
fi

cat << 'INNER_EOF' >> .jules/bolt.md

## 2025-05-16 - Avoid np.nanmedian overhead when NaNs are explicitly invalidated
**Learning:** `np.nanmedian` creates masks and copies internally to ignore `NaN` values, which introduces significant overhead (~3x slower than `np.median`). When processing `sliding_window_view` chunks where any window containing a `NaN` is subsequently explicitly assigned `np.nan` anyway, this overhead is entirely unnecessary.
**Action:** Replace `np.nanmedian` with `np.median` when calculating window statistics if windows containing `NaNs` will be discarded or explicitly masked afterward. Keep `np.nanmedian` only for operations where NaNs are intentionally injected as placeholders for specific elements (like outliers) and need to be actively ignored during the calculation.
INNER_EOF

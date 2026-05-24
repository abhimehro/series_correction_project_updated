========== ELIR ==========
PURPOSE: Removed unused `load_series_data` function and `pandas` import from `scripts/loaders.py`.
SECURITY: Reduces attack surface slightly by removing unused imports, though primarily a code hygiene task.
FAILS IF: N/A - Function was entirely dead code.
VERIFY: Confirm the CI pipeline passes.
MAINTAIN: Avoid adding one-off loader functions without ensuring they are integrated into the main application execution path.

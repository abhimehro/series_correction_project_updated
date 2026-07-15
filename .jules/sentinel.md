## 2025-02-24 - Exception Data Leakage
**Vulnerability:** Raw exception data strings being exposed to users through `ValueError` and `ProcessingError` instances in CLI scripts.
**Learning:** Returning `Exception` objects directly wrapped in standard Python errors can leak sensitive internal paths, logic, and configurations to external consumers.
**Prevention:** Catch the specific exception, securely log its details via `logging.exception(...)`, and return a static, generic error message (e.g. `raise ValueError("Invalid format") from e`) instead.
## 2025-02-24 - Python Exception Chaining Data Leakage
**Vulnerability:** Attempting to hide exception details by simply removing `from exc` fails due to Python 3's implicit exception chaining.
**Learning:** In Python 3, exceptions raised inside an `except` block implicitly chain the previous exception to `__context__`, leaking the original stack trace unless explicitly overridden.
**Prevention:** Always use `raise CustomException(...) from None` to fully suppress the original exception's context and prevent internal detail leakage to end-users.
## 2025-02-24 - Path Traversal Bypass
**Vulnerability:** A symlink path traversal bypass in `scripts/loaders.py` caused by using `os.path.abspath` instead of `os.path.realpath`.
**Learning:** `os.path.abspath` standardizes path representation but does not resolve symbolic links, allowing an attacker to bypass `os.path.commonpath` checks if they control symlinks within the allowed directory.
**Prevention:** Always use `os.path.realpath` to fully resolve symlinks before verifying directory confinement using `os.path.commonpath`.
## 2025-02-25 - Pandas Categorical Type CSV/Formula Injection
**Vulnerability:** Malicious formulas were not escaped in Pandas `category` columns during spreadsheet sanitization.
**Learning:** `dataframe.select_dtypes(include=["object", "string"])` misses `category` types. A standard `.map()` on a category column throws an error or silently fails; one must use `.cat.rename_categories()` to safely escape categorical values without breaking their properties.
**Prevention:** Always include `category` in `select_dtypes` for sanitization, and handle `CategoricalDtype` uniquely using its built-in `.cat` accessor methods.
## 2024-06-12 - Prevent CSV/Formula Injection via unescaped DataFrame exports
**Vulnerability:** Calling `to_csv()` directly on Pandas DataFrames containing untrusted string or categorical columns allows malicious actors to inject Excel formulas (e.g., values starting with `=`, `+`, `-`, `@`) which execute when the CSV is opened in spreadsheet software.
**Learning:** While `to_excel()` was explicitly protected by `write_excel_safely` in this codebase, exports to CSV via `to_csv()` were overlooked in scripts like `apply_refined_corrections.py` and `generate_overview_table.py`.
**Prevention:** Always apply the existing `sanitize_dataframe_for_spreadsheet()` utility to DataFrames before any export (CSV or Excel) that might be opened in spreadsheet software. Created a centralized `write_csv_safely` utility alongside `write_excel_safely` to enforce this consistently.

## 2024-05-18 - Fix Raw Exception Data Exposure
**Vulnerability:** Raw exception texts exposing sensitive underlying system details were directly printed out using `print(f"...: {e}")` strings during failure scenarios in scripts handling file corrections.
**Learning:** Printing or returning untrusted error exception details (`Exception as e`) to users/logs can leak stack information, filesystem paths, or internal logic.
**Prevention:** Catch generalized exceptions without capturing `as e` when only a notification is needed, and use generic fixed string messages like "An unexpected error occurred" instead of interpolating the raw exception text.
## 2025-02-28 - [Information Exposure in CLI output]
**Vulnerability:** Scripts were exposing raw Python exceptions (e.g., `print(f"Error processing {filename}: {e}")`) in user-facing CLI output.
**Learning:** Exception details can expose internal system paths, dependencies, and state information (CWE-209). This is especially risky in CLI tools where the output is directly accessible by the user.
**Prevention:** Use generic warning messages when handling errors and outputting them to the terminal. If detailed error traces are needed, write them to structured, secured log files instead of standard output.

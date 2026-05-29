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

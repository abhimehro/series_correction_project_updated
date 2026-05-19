## 2025-02-24 - Exception Data Leakage
**Vulnerability:** Raw exception data strings being exposed to users through `ValueError` and `ProcessingError` instances in CLI scripts.
**Learning:** Returning `Exception` objects directly wrapped in standard Python errors can leak sensitive internal paths, logic, and configurations to external consumers.
**Prevention:** Catch the specific exception, securely log its details via `logging.exception(...)`, and return a static, generic error message (e.g. `raise ValueError("Invalid format") from e`) instead.

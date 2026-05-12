## 2024-05-12 - Prevented exception detail leakage in output
**Vulnerability:** Generic exceptions exposed their details directly in the output data's `Status` field (e.g., `status = f"Failed (Unexpected Error: {exc})"`).
**Learning:** Missing error handling can expose internal data structures or file paths to end users.
**Prevention:** Use generic error messages for output statuses and securely log actual exception details internally using `log.error`.

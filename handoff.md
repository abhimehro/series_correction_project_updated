========== ELIR ==========
PURPOSE: Implemented secure authentication functions using PBKDF2 HMAC SHA-256 for password hashing and `hmac.compare_digest` for safe comparison, alongside mitigating an infinite loop bug in `_is_json_array` by capping character reads at 1024 to prevent Denial of Service (DoS) from malicious whitespace-only files.
SECURITY: The authentication implementation prevents timing attacks via `hmac.compare_digest`, uses strong key derivation (PBKDF2 HMAC SHA256) with a random 16-byte salt, and safely fails on missing attributes. The `max_reads` in `_is_json_array` prevents infinite read loops that could cause memory/CPU exhaustion.
FAILS IF: An upstream system relies on passing an array with over 1024 leading whitespaces before the opening bracket `[`.
VERIFY: Code review should confirm `hmac.compare_digest` usage, verify PBKDF2 parameters meet organization guidelines (e.g. 100,000 iterations is a reasonable baseline but could be tuned), and review `tests/test_dummy_todos.py` coverage.
MAINTAIN: Be careful modifying loop invariants in `_is_json_array` - the character limit protects against maliciously crafted payloads.

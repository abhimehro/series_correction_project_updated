## 2025-02-24 - Updated PBKDF2 Iteration Count
**Vulnerability:** Weak PBKDF2 iteration count (100,000) used for password hashing.
**Learning:** Found in `dummy_todos.py`, utilizing 100k iterations instead of OWASP's recommended 600,000 for SHA-256. This was a straightforward hardcoded update, but it's important to remember that such updates in production break existing user hashes unless a migration path is introduced.
**Prevention:** Always use established security recommendations for cryptographic parameters and consider the upgrade path for existing data.

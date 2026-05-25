# 🛡️ Sentinel: [High] Fix Potential Path Traversal in Configuration Loading

## 🎯 What
The `load_config` function in `scripts/loaders.py` previously accepted arbitrary strings as paths for reading the JSON config file without verifying whether the path escapes the current working directory.

## ⚠️ Risk
An attacker who can influence the `config_path` argument could potentially perform a Path Traversal attack (e.g. `../../../../etc/passwd`), reading sensitive files on the host filesystem that the application process has access to. Given that the scripts batch processes input data, a manipulated config file path argument could lead to arbitrary file disclosure.

## 🛡️ Solution
The `load_config` function now strictly validates `config_path`. It computes the absolute path of the current working directory and the provided config path. Using `os.path.commonpath`, it ensures that the resolved config path resides entirely within the working directory. If it escapes, it raises a `ValueError` securely avoiding unauthorized file access.

========== ELIR ==========
PURPOSE: Fix potential path traversal vulnerability in config loading by validating that the resolved config path remains within the base working directory.
SECURITY: Path Traversal (CWE-22). Prevents an attacker from reading arbitrary files outside the application's intended directory.
FAILS IF: The application is intentionally configured to load a config from a directory outside the current working directory (which is an unsafe pattern).
VERIFY: Ensure `os.path.commonpath` works correctly across expected operating environments and that valid config paths load normally.
MAINTAIN: Be cautious not to introduce logic that allows path segments containing `..` to bypass the check without being resolved to absolute paths first.

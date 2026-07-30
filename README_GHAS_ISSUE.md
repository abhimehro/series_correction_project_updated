# GitHub Advanced Security Error

The CI failure encountered on check run `github-advanced-security` with message `Execution failed: CAPIError: 400 The requested model is not supported` is caused by GitHub's internal infrastructure.

Specifically, the automatically injected GitHub Copilot Advanced Security agent attempts to use `sweagent-capi:claude-opus-4.6`, which results in a `400` error from the internal Copilot API.

This failure is external to the repository and does not relate to the codebase or workflow files. It is an infrastructure issue that GitHub needs to resolve.

No code changes are required or possible in this repository to fix this issue.

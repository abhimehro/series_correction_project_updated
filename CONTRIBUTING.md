# Contributing

Thanks for helping keep this Seatek series-correction CLI maintainable.

## Setup

Use `python3` (not `python`). From the repo root:

```bash
pip3 install -r scripts/requirements-dev.txt && pip3 install -e .
python3 -m pytest scripts/tests/ -v
flake8 scripts/ --max-line-length=100
```

See `AGENTS.md` for the full command table and caveats.

## Pull requests

- Prefer small, focused changes. Do not commit `.env`, credentials, or
  `*.egg-info/` from editable installs.
- Follow `.github/PULL_REQUEST_TEMPLATE.md`.
- Runtime pins live in `scripts/requirements.txt`. Do not bump `pandas` to 3.x
  without an explicit hold/spike decision (issue #382).

## Code of Conduct

This project follows [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md).

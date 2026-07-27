# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

CLI tool to detect and correct discontinuities (jumps, gaps, outliers) in Seatek sensor time-series data. Outputs corrected Excel files. No frontend, no database, no Docker required. See `README.md` for full details.

### Key commands

| Task | Command |
| -------------------- | ----------------------------------------------------------------- |
| Install deps | `pip3 install --user --no-deps -e .` (see caveats below) |
| Run tests | `python3 -m pytest scripts/tests/ -v` |
| Run tests + coverage | `python3 -m pytest --cov=scripts scripts/tests/ -v` |
| Lint | `flake8 scripts/ --max-line-length=100` |
| CLI help | `seatek-correction --help` |
| CLI dry-run | `seatek-correction --series 26 --river-miles 54.0 53.0 --years 1995 1996 --dry-run` |
| Batch processing | `python3 scripts/manual_batch_run.py` |

### Non-obvious caveats

- **`pandas<2.0` constraint is incompatible with Python 3.12**: The pinned `pandas<2.0` in `scripts/requirements.txt` cannot build from source on Python 3.12+. Install dependencies individually (without the upper bound) then install the package with `pip3 install --user --no-deps -e .`.
- **Mocks**: `scripts/tests/test_batch_correction.py` uses `unittest.mock` (stdlib). No third-party `mock` package is required.
- **Test status**: With the documented dependency install (which resolves to a current pandas, e.g. 3.x), `python3 -m pytest scripts/tests/ -v` collects **75** tests (including `test_batch_correction.py`).
- **Test path**: Primary suite is under `scripts/tests/`, not root `tests/` (root `tests/` only covers scaffolding helpers).
- **Data files**: `.txt` sensor data files in `data/` are committed for Series 26 and 27. Output goes to `data/output/`.
- **CI note**: The `Python Tests` GitHub Actions workflow (`.github/workflows/python-tests.yml`) is currently `disabled_manually` on the remote — local pytest remains the gate until it is re-enabled.
- Use `python3` (not `python`) as the command.

### CodeScene review/salvage trigger

During automated PR review or salvage sessions, if CodeScene is red on a PR, post:

```bash
/cs-agent skill:fix-code-health-degradations
```

Then wait for that run to complete before final defer/salvage disposition.

## Learned User Preferences

- Do not commit local editable-install build metadata when finishing unrelated sessions.

## Learned Workspace Facts

- Prefer not staging `seatek_series_correction.egg-info/` or other editable-install metadata; keep `*.egg-info/` gitignored.

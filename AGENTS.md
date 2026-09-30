# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

CLI tool to detect and correct discontinuities (jumps, gaps, outliers) in Seatek
sensor time-series data. Outputs corrected Excel files. No frontend, no
database, no Docker required. See `README.md` for full details.

### Key commands

| Task                 | Command                                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Install deps         | `python3.12 -m venv --clear .venv && .venv/bin/pip install -r scripts/requirements-dev.txt && .venv/bin/pip install -e .` |
| Run tests            | `.venv/bin/python -m pytest scripts/tests/ -v`                                                                            |
| Run tests + coverage | `.venv/bin/python -m pytest --cov=scripts scripts/tests/ -v`                                                              |
| Lint                 | `.venv/bin/python -m flake8 scripts/` (see `setup.cfg`)                                                                   |
| CLI help             | `.venv/bin/seatek-correction --help`                                                                                      |
| CLI dry-run          | `.venv/bin/seatek-correction --series 26 --river-miles 54.0 53.0 --years 1995 1996 --dry-run`                             |
| Batch processing     | `.venv/bin/python scripts/manual_batch_run.py`                                                                            |

<!-- gitnexus:end -->

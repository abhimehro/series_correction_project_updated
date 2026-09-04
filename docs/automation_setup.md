# Automation Setup

How to run the correction CLI on a schedule or in CI-like local loops.

## CLI (preferred)

Install once, then use the packaged entry point:

```bash
pip3 install -r scripts/requirements-dev.txt && pip3 install -e .
seatek-correction --help
seatek-correction --series 26 --river-miles 54.0 53.0 --years 1995 1996 --dry-run
```

Batch everything listed in `scripts/config.json`:

```bash
python3 scripts/manual_batch_run.py
```

Outputs land in `data/output/`. Comparison sheets:

```bash
python3 scripts/export_comparison_sheets.py
```

## Environment

| Item         | Value                                                            |
| ------------ | ---------------------------------------------------------------- |
| Python       | 3.10 or 3.11 (CI matrix in `.github/workflows/python-tests.yml`) |
| Runtime pins | `scripts/requirements.txt` (`pandas==2.3.3`, numpy, openpyxl)    |
| Config       | `scripts/config.json` — do not mutate it from one-shot helpers   |

`scripts/run_analysis.py` (removed) used to rewrite the config threshold as a
side effect. Pass thresholds through `BatchConfig` / CLI flags instead.

## Scheduling

This repo has no launchd/cron installer. If you need a local timer, wrap
`python3 scripts/manual_batch_run.py` in your own scheduler and keep
`scripts/config.json` under version control.

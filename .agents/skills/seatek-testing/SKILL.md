---
name: seatek-testing
description: How to install and test the seatek-series-correction package using the pyenv Python versions in this workspace.
---

# Testing Seatek Series Correction

## Environment

- Pyenv root: `$HOME/.pyenv`
- Required Python versions are installed under `~/.pyenv/versions` (3.9.21,
  3.10.16, 3.11.11, 3.12.8).
- Use `3.10` or `3.11` for this project. The GitHub Actions matrix tests these
  versions and the exact wheel pins (`pandas==2.3.3`, `numpy==2.2.6`) are built
  for them. `setup.py` declares `python_requires=">=3.10"`, so later versions may
  work but are not continuously validated.

## One-time shell setup

```bash
export PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
export PATH="$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH"
```

## Dependency layout

- `scripts/requirements.txt` contains **runtime** dependencies only:
  `pandas==2.3.3`, `numpy==2.2.6`, `openpyxl==3.1.5`.
- `scripts/requirements-dev.txt` includes runtime deps plus exact dev/test/lint
  pins: `pytest`, `pytest-cov`, `pytest-mock`, `black`, `flake8`, `pylint`.
- `setup.py` reads `scripts/requirements.txt` for `install_requires`, so
  `pip install .` or `pip install -e .` installs **only** the runtime set.

## Install in an isolated venv

For end-to-end validation matching CI:

```bash
PYENV_VERSION=3.10.16 python3 -m venv /tmp/seatek-venv
source /tmp/seatek-venv/bin/activate
pip install --upgrade pip
pip install -r scripts/requirements-dev.txt
pip install -e .
```

For a runtime-only install:

```bash
PYENV_VERSION=3.10.16 python3 -m venv /tmp/seatek-runtime-venv
source /tmp/seatek-runtime-venv/bin/activate
pip install --upgrade pip
pip install -r scripts/requirements.txt
pip install -e .
```

## Run tests

```bash
python3 -m pytest scripts/tests/ -v
```

Expected result: 114 passed.

## Run lint / format checks

```bash
python3 -m black --check scripts
python3 -m flake8 scripts/ --max-line-length=100
```

## Verify the CLI

```bash
seatek-correction --help
```

## Verify runtime metadata

After installing the package, `pip show seatek-series-correction` should report:

```
Requires: numpy, openpyxl, pandas
```

It must not list dev tools such as `pytest`, `black`, `flake8`, `pylint`,
`click`, `filelock`, `psutil`, or `ijson`.

## Verify `python_requires` rejection on Python 3.9

`pip install .` from a source tree on Python 3.9 may fail with a
dependency-resolution error rather than a clean `python_requires` message. To
see the explicit `python_requires` rejection, build a wheel on 3.10 and install
it with `--no-deps` under 3.9:

```bash
PYENV_VERSION=3.10.16 python3 -m venv /tmp/build-venv
/tmp/build-venv/bin/pip install wheel build
/tmp/build-venv/bin/python3 -m build -w -o /tmp/dist
PYENV_VERSION=3.9.21 python3 -m venv /tmp/py39-venv
/tmp/py39-venv/bin/pip install --no-deps /tmp/dist/seatek_series_correction-*.whl
```

Expected output contains:
`Package 'seatek-series-correction' requires a different Python: 3.9.x not in '>=3.10'`.

## Notes

- Tests use `unittest.mock` (stdlib). The third-party `mock` package is not
  required.
- Do not commit `*.egg-info/` or `build/`; both are already in `.gitignore`.
- Do not commit local editable-install build metadata when finishing unrelated
  sessions.

## Devin Secrets Needed

None.

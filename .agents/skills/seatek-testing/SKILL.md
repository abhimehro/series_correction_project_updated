---
name: seatek-testing
description: How to install and test the seatek-series-correction package using the pyenv Python versions in this workspace.
---

# Testing Seatek Series Correction

## Environment

- Pyenv root: `/home/ubuntu/.pyenv`
- Required Python versions are installed under `~/.pyenv/versions` (3.9.21, 3.10.16, 3.11.11, 3.12.8).
- The system `python3` is 3.12 and cannot install `pandas<2.0`; use `3.10` or `3.11` for this project.

## One-time shell setup

```bash
export PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
export PATH="$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH"
```

## Install in an isolated venv

```bash
PYENV_VERSION=3.10.16 python -m venv /tmp/seatek-venv
/tmp/seatek-venv/bin/pip install -r scripts/requirements.txt
/tmp/seatek-venv/bin/pip install .
```

## Run tests

```bash
/tmp/seatek-venv/bin/python -m pytest scripts/tests/ -v
```

## Verify `python_requires` rejection on Python 3.9

`pip install .` from a source tree on Python 3.9 may fail with a dependency-resolution error (`filelock>=3.20.3` requires Python >=3.10) rather than a clean `python_requires` message. To see the explicit `python_requires` rejection, build a wheel on 3.10 and install it with `--no-deps` under 3.9:

```bash
PYENV_VERSION=3.10.16 python -m venv /tmp/build-venv
/tmp/build-venv/bin/pip install wheel
/tmp/build-venv/bin/python setup.py bdist_wheel -d /tmp/dist
PYENV_VERSION=3.9.21 python -m venv /tmp/py39-venv
/tmp/py39-venv/bin/pip install --no-deps /tmp/dist/seatek_series_correction-*.whl
```

Expected output contains: `Package 'seatek-correction' requires a different Python: 3.9.x not in '>=3.10'`.

## Notes

- `scripts/requirements.txt` includes pytest, black, flake8, pylint, etc. `setup.py` treats all of them as `install_requires`, so `pip install .` installs dev tools into the environment.
- Tests use `unittest.mock`; the third-party `mock` package is not required.
- Do not commit `*.egg-info/` or `build/`; both are already in `.gitignore`.

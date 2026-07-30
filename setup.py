"""
Setup script for the Seatek Series Correction Project.

Typical usage
-------------
• Install runtime only:           python -m pip install .
• Install editable (development):  python -m pip install -r scripts/requirements-dev.txt -e .
• Build distribution:             python -m build
• Show help / commands:           python setup.py --help
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

from setuptools import find_packages, setup


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def parse_requirements(filename: str = "scripts/requirements.txt") -> list[str]:
    """
    Read a pip-style runtime requirements file.

    Skips blank/comment lines and strips inline comments so each line is a
    valid PEP 508 requirement. Returns an empty list (with a warning) if the
    file does not exist.
    """
    req_path = Path(filename)

    if not req_path.exists():
        warnings.warn(
            f"Requirements file '{filename}' not found; install_requires will be empty."
        )
        return []

    reqs: list[str] = []
    with req_path.open(encoding="utf-8") as f:
        for raw in f:
            ln = raw.strip()
            if not ln or ln.startswith("#"):
                continue
            if ln.startswith("-r "):
                # Skip include directives; this parser is only used for the
                # runtime requirements file, which must not reference others.
                continue
            # Strip inline comments (e.g. "package==1.0  # reason")
            if " #" in ln:
                ln = ln.split(" #", 1)[0].strip()
            reqs.append(ln)
    return reqs


def read_long_description(filename: str = "README.md") -> str:
    """Return the project README contents or an empty string with warning."""
    readme_path = Path(filename)
    if not readme_path.exists():
        warnings.warn(f"README '{filename}' not found; long_description empty.")
        return ""
    return readme_path.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Project metadata
# --------------------------------------------------------------------------- #
NAME = "seatek-series-correction"
VERSION = "0.1.1"  # Bumped from 0.1.0
DESCRIPTION = (
    "Tools to detect and correct discontinuities in Seatek sensor data series."
)
LONG_DESCRIPTION = read_long_description()
LONG_DESCRIPTION_CONTENT_TYPE = "text/markdown"
AUTHOR = "Abhi Mehrotra"
AUTHOR_EMAIL = "AbhiMhrtr@pm.me"
URL = "https://github.com/abhimehro/series_correction_project_updated"
LICENSE = "MIT"

# --------------------------------------------------------------------------- #
# Package discovery / requirements
# --------------------------------------------------------------------------- #
PACKAGES = find_packages(where=".", include=["scripts", "scripts.*"])
PYTHON_REQUIRES = ">=3.10"

INSTALL_REQUIRES = parse_requirements("scripts/requirements.txt")

# Validate core deps are listed
required_core = {"pandas", "numpy", "openpyxl"}
missing_core = required_core - {
    ln.split("==")[0].split("<")[0].split(">")[0] for ln in INSTALL_REQUIRES
}
if missing_core:
    warnings.warn(
        f"Core dependencies {', '.join(sorted(missing_core))} are missing from "
        "requirements.txt"
    )


# --------------------------------------------------------------------------- #
# Setup invocation
# --------------------------------------------------------------------------- #
def main() -> None:
    """
    When called with no arguments, show friendly guidance instead of raising
    'error: no commands supplied'.
    """
    if len(sys.argv) == 1:
        print(
            "setup.py is a build script. Supply a command or use pip, e.g.:\n"
            "  python -m pip install -e .           # editable install\n"
            "  python -m pip install .              # normal install\n"
            "  python -m build                      # build wheel/sdist (needs 'build')\n"
            "  python setup.py --help               # show all commands"
        )
        sys.exit(0)
    setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        long_description_content_type=LONG_DESCRIPTION_CONTENT_TYPE,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        python_requires=PYTHON_REQUIRES,
        url=URL,
        license=LICENSE,
        packages=PACKAGES,
        include_package_data=True,
        install_requires=INSTALL_REQUIRES,
        entry_points={
            "console_scripts": [
                "seatek-correction=scripts.series_correction_cli:main",
            ],
        },
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Science/Research",
            # Removed deprecated license classifier per setuptools warning.
            "Natural Language :: English",
            "Operating System :: OS Independent",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Topic :: Scientific/Engineering",
            "Topic :: Scientific/Engineering :: Information Analysis",
            "Typing :: Typed",
        ],
        keywords="seatek sensor timeseries data correction environmental science",
    )


if __name__ == "__main__":
    main()

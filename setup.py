"""
Setup script for the Seatek Series Correction Project.

Typical usage
-------------
• Install editable (development):  python -m pip install -e .
• Install normally:               python -m pip install .
• Build distribution:             python -m build
• Show help / commands:           python setup.py --help
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

from setuptools import setup, find_packages


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def parse_requirements(filename: str = "scripts/requirements.txt") -> list[str]:
    """
    Read a pip‐style requirements file.

    Returns an empty list (with a warning) if the file does not exist.
    """
    req_path = Path(filename)

    if not req_path.exists():
        warnings.warn(
            f"Requirements file '{filename}' not found; install_requires will be empty."
        )
        return []

    with req_path.open(encoding="utf-8") as f:
        line_iter = (line.strip() for line in f)
        return [ln for ln in line_iter if ln and not ln.startswith("#")]


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
DESCRIPTION = "Tools to detect and correct discontinuities in Seatek sensor data series."
LONG_DESCRIPTION = read_long_description()
LONG_DESCRIPTION_CONTENT_TYPE = "text/markdown"
AUTHOR = "Abhi Mehrotra"
AUTHOR_EMAIL = "AbhiMhrtr@pm.me"
URL = "https://github.com/yourusername/series-correction-project"
LICENSE = "MIT"

# --------------------------------------------------------------------------- #
# Package discovery / requirements
# --------------------------------------------------------------------------- #
PACKAGES = find_packages(where=".", include=["scripts", "scripts.*"])
PYTHON_REQUIRES = ">=3.8"

INSTALL_REQUIRES = parse_requirements()

# Validate core deps are listed
required_core = {"pandas", "numpy", "click", "openpyxl"}
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
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
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

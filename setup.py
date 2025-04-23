"""
Setup script for the Seatek Series Correction Project.

This script uses setuptools to package the project, allowing it to be
installed via pip and enabling the use of its command-line interface.
"""

# Function to read the requirements file
from setuptools import setup, find_packages


def parse_requirements(filename="scripts/requirements.txt"):
    """Load requirements from a pip requirements file."""
    lineiter = (line.strip() for line in open(filename))
    return [line for line in lineiter if line and not line.startswith("#")]


# Function to read the long description from README.md
def read_long_description(filename="README.md"):
    """Read the README file for the long description."""
    try:
        with open(filename, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        print(f"Warning: {filename} not found. Long description will be empty.")
        return None


# Project metadata
NAME = "seatek-series-correction"
VERSION = "0.1.0"  # Initial version
DESCRIPTION = (
    "Tools to detect and correct discontinuities in Seatek sensor data series."
)
LONG_DESCRIPTION = read_long_description()
LONG_DESCRIPTION_CONTENT_TYPE = "text/markdown"
AUTHOR = "Abhi Mehrotra"
AUTHOR_EMAIL = "AbhiMhrtr@pm.me"
URL = "https://github.com/yourusername/series-correction-project"  # TODO: Replace
LICENSE = "MIT"

# Define where the source code lives (relative to setup.py)
PACKAGES = find_packages(where=".", include=["scripts", "scripts.*"])

# Specify Python version requirement
PYTHON_REQUIRES = ">=3.8"

# Get installation requirements from requirements.txt
INSTALL_REQUIRES = parse_requirements()
required = {"pandas", "numpy", "click", "openpyxl"}
missing = required - set(
    line.split("==")[0].split("<")[0].split(">")[0] for line in INSTALL_REQUIRES
)
if missing:
    print(
        f"Warning: Core dependencies {missing} might be missing from requirements.txt"
    )

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
        "License :: OSI Approved :: MIT License",
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

print(f"\nSetup complete for {NAME} version {VERSION}.")
print("To install this package locally for development, run:")
print("  pip install -e .")
print("To install the command-line tool, run:")
print("  pip install .")
print("Then you can use the command: seatek-correction --help")

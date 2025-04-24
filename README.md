# Series Correction Project (Seatek Sensor Data)

[![Python Tests](https://github.com/yourusername/series-correction-project/actions/workflows/python-tests.yml/badge.svg)](https://github.com/yourusername/series-correction-project/actions/workflows/python-tests.yml) [![Code Coverage](https://codecov.io/gh/yourusername/series-correction-project/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/series-correction-project) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This project provides tools to automatically detect and correct discontinuities (such as jumps, gaps, or outliers) commonly found in time-series data from Seatek sensors. The goal is to produce cleaner, more reliable datasets for further analysis, suitable for integration with systems like NESST II.

## Table of Contents

- [Problem Statement](#problem-statement)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Data Format](#data-format)
  - [Input Data Requirements](#input-data-requirements)
  - [Sample Data](#sample-data)
- [Methodology](#methodology)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)
- [Future Improvements](#future-improvements)
- [Workflow for Efficient Batch Processing, QA, and Visualization Updates (2025)](#workflow-for-efficient-batch-processing-qa-and-visualization-updates-2025)

## Problem Statement

Seatek sensors, like many environmental or industrial sensors, can produce time-series data with various imperfections:

- **Jumps/Shifts:** Sudden, persistent changes in the baseline value.
- **Gaps:** Missing data points or periods.
- **Outliers:** Spurious, isolated spikes or drops.
- **Drift:** Slow, gradual changes unrelated to the measured phenomenon.

Manually identifying and correcting these discontinuities is time-consuming, subjective, and prone to errors. This project aims to automate this process, providing a consistent and efficient way to improve data quality.

## Features

- **Discontinuity Detection:** Implements algorithms (`processor.py`) to automatically identify potential jumps, gaps, and outliers in Seatek sensor time-series data.
- **Discontinuity Correction:** Offers methods (`processor.py`) to correct detected discontinuities:
  - Interpolation for gaps (e.g., 'time', 'linear').
  - Offset correction for jumps.
  - Replacement for outliers (e.g., 'median', 'mean', 'interpolate').
- **Configurable Parameters:** Allows tuning of detection sensitivity (e.g., thresholds, window sizes) and selection of correction methods via configuration files (`config.json`).
- **Batch Processing:** Capable of processing multiple data files (`S<series>_Y<index>.txt`) efficiently based on series, year range, and river mile criteria (`batch_correction.py`).
- **Command-Line Interface:** Provides a CLI tool (`seatek-correction`) for easy execution (`series_correction_cli.py`).
- **Reporting:** Generates a summary CSV file (`Batch_Processing_Summary.csv`) and detailed logs (`processing_log.txt`).
- **Testing & CI:** Includes unit tests (`tests/`) and a GitHub Actions workflow (`.github/workflows/python-tests.yml`) for automated testing.

## Installation

**Prerequisites:**

- Python >= 3.8
- `pip` (Python package installer)
- `git` (for cloning the repository)
- (Optional: `virtualenv` or `conda` for environment management)

**Steps:**

1. **Clone the repository:**

   ```bash
   # TODO: Replace with your actual repository URL
   git clone [https://github.com/yourusername/series-correction-project.git](https://github.com/yourusername/series-correction-project.git)
   cd series-correction-project
   ```

2. **Create and activate a virtual environment (Recommended):**

   ```bash
   # Using venv
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`

   # Or using conda
   # conda create --name seatek_corr python=3.9
   # conda activate seatek_corr
   ```

3. **Install the package and its dependencies:**
   This command uses `setup.py` to install the project, including the CLI tool and required libraries from `requirements.txt`.

   ```bash
   pip install -e .
   ```

   _(The `-e` flag installs the project in "editable" mode, meaning changes to the source code are reflected immediately without needing to reinstall.)_

## Usage

The primary way to use the tool is via the `seatek-correction` command-line interface.

**Command-Line Arguments:**

```bash
seatek-correction --help
Output:
usage: seatek-correction [-h] [--series SERIES] --river-miles RIVER_MILES RIVER_MILES --years YEARS YEARS [--dry-run] [--config CONFIG_PATH] [--output OUTPUT_DIR] [--log LOG_FILE]

Run series correction batch processing on sensor data.

options:
  -h, --help            show this help message and exit
  --series SERIES       Series number to process, or 'all' for all available series. (default: all)
  --river-miles RIVER_MILES RIVER_MILES
                        Upstream and downstream river mile markers (e.g., 54.0 53.0). (required)
  --years YEARS YEARS   Start and end years of data to process (e.g., 1995 2014). (required)
  --dry-run             If set, process data without saving output files.
  --config CONFIG_PATH  Path to the configuration JSON file. (default: scripts/config.json)
  --output OUTPUT_DIR   Directory to save output files (default: data/output/).
  --log LOG_FILE        Path to the log file (default: processing_log.txt).
```

**Example: Process Series 26 data between river miles 53.0 and 54.0 for the years 1995 to 2014, using a custom configuration and saving output to ./corrected_output/.**

```bash
# This command processes data for a specific series (26) within the given river mile range and years.
# It uses a custom config file and directs output to a specific folder.
seatek-correction --series 26 --river-miles 54.0 53.0 --years 1995 2014 --config custom_config.json --output ./corrected_output/
```

**Example (Dry Run for All Series): Perform a dry run (no files saved) for all series associated with river miles 53.0-54.0 between 1995-1996, logging to a specific file.**

```bash
# This command simulates processing for all applicable series in the range without saving output files.
# It's useful for checking which files would be processed and verifying logs.
seatek-correction --series all --river-miles 54.0 53.0 --years 1995 1996 --dry-run --log dry_run.log
```

## Configuration

The processing behavior is controlled by a JSON configuration file (default: `scripts/config.json`). See `config.json` for the structure. Key sections include:

- **series** (Optional): Mapping series numbers to specific diagnostic or raw data file paths (less used now as batch processing relies on file patterns).
- **defaults / processor_config**: Parameters passed to the `processor.py` module:
  - `window_size`: Rolling window size (default: 5).
  - `threshold`: Sensitivity threshold for jumps/outliers (default: 3.0).
  - `gap_threshold_factor`: Multiplier for gap detection (default: 3.0).
  - `gap_method`: Gap interpolation method ('time', 'linear', etc.).
  - `outlier_method`: Outlier correction method ('median', 'mean', 'interpolate', 'remove').
  - `time_col`: Name of the time column (default: "Time (Seconds)").
  - `value_col`: Name of the value column (default: null for auto-detect).
- **RAW_DATA_DIR** (Optional): Path to the directory containing input .txt files (defaults to `./data/`).
- **RIVER_MILE_MAP_PATH** (Optional): Path to the JSON file mapping sensors to river miles (defaults to `scripts/river_mile_map.json`).

## Data Format

### Input Data Requirements

- **Location**: Input files should reside in the directory specified by `RAW_DATA_DIR` in the config, or the `./data/` directory by default.
- **Filename Pattern**: Files must follow the pattern `S<series>_Y<index>.txt`, where `<series>` is the series number (e.g., 26) and `<index>` is a zero-padded sequential index representing the year within the processing range (e.g., Y01 for the first year, Y02 for the second, etc.).
- **File Format**: Text files (.txt).
- **Content**: Space or tab-delimited columns. Must contain at least a time column and one numeric sensor value column. Header rows are typically absent or should be handled by pandas.read_csv (e.g., using comment='#'). Data should be roughly chronological.

**Example File (data/S26_Y01.txt)**

```
# Example data for Series 26, Year 1 (e.g., 1995)
# Time(Seconds) Value
100             23.5
102             23.6
104             23.5
108             23.7  # Potential small gap here
110             23.8
112             50.0  # Potential outlier
114             23.9
116             30.1  # Potential jump start
118             30.2
120             30.0
```

### Sample Data

**Status: Required**

This repository does not include sample `S<series>_Y<index>.txt` files due to data sensitivity or size. To run the processing and tests effectively, you must provide your own representative (anonymized if necessary) data files and place them in the `./data/` directory.

**Action Required**: Create files like `data/S26_Y01.txt`, `data/S27_Y01.txt`, etc., using your Seatek data, ensuring they match the required filename pattern and content format described above.

**Importance**: Without these files in the `./data/` directory, the batch processing script will not find any input, and end-to-end testing cannot be performed.

## Methodology

The correction process involves sequentially detecting and correcting gaps, outliers, and jumps using statistical methods within rolling windows. For detailed steps and algorithms, please refer to:

`docs/correction_methodology.md`

## Project Structure

```
series-correction-project/
│
├── data/                     # Input data directory (MUST ADD SAMPLE S<series>_Y<index>.txt FILES HERE)
│   └── S26_Y01.txt           # Example required input file format
│
├── output/                   # Default directory for corrected data and reports
│   └── S26_Y01_1995_CorrectedData.csv # Example output format
│   └── Batch_Processing_Summary.csv   # Example summary output
│
├── scripts/                  # Source code package
│   ├── __init__.py
│   ├── batch_correction.py
│   ├── loaders.py
│   ├── processor.py
│   ├── series_correction_cli.py
│   ├── config.json           # Default configuration
│   ├── river_mile_map.json   # Default mapping
│   └── requirements.txt
│
├── tests/                    # Unit and integration tests
│   ├── __init__.py
│   └── test_batch_correction.py # Example test file
│   # (Add other test files like test_processor.py here)
│
├── docs/                     # Documentation files
│   ├── correction_methodology.md
│   └── automation_setup.md
│
├── .github/                  # GitHub Actions Workflows
│   └── workflows/
│       └── python-tests.yml
│
├── .gitignore
├── LICENSE
├── README.md                 # This file
└── setup.py                  # Installation script
```

## Testing

This project uses pytest for testing.

1. Ensure development dependencies are installed (included in `pip install -e .` if requirements.txt includes them, or use a separate requirements-dev.txt).

   ```bash
   pip install pytest pytest-cov pytest-mock
   ```

2. Navigate to the project's root directory.

3. Run tests:

   ```bash
   pytest
   ```

4. Run tests with coverage:

   ```bash
   pytest --cov=scripts tests/
   ```

(Note: Comprehensive testing, especially integration tests, requires the presence of sample data files in the `data/` directory.)

## Contributing

Contributions are welcome! Please follow standard GitHub Fork & Pull Request workflows. Ensure code includes tests, passes linting/formatting checks, and updates documentation where necessary.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contact

For questions or support, please open an issue on the GitHub repository:

<https://github.com/yourusername/series-correction-project/issues>

Alternatively, contact Abhi Mehrotra <AbhiMhrtr@pm.me>

## Acknowledgements

- **Libraries**: pandas, numpy, click
- **Testing**: pytest
- **Guidance**: Audit report and best practices.
- **Support**: Baton Rouge Community College (BRCC), Louisiana State University (LSU), LSU Center for River Studies.

## Future Improvements

Based on the audit report and potential enhancements:

- **Sample Data**: Add standardized, anonymized sample data files.
- **Integration Tests**: Develop tests that run the full pipeline on sample data.
- **Visualization**: Implement a visualization.py module (e.g., using Matplotlib/Seaborn) to plot raw vs. corrected data, highlighting changes. Add a --plot flag to the CLI.
- **Data Validation**: Add more explicit data validation checks during loading (e.g., expected columns, value ranges).
- **Configuration**: Refactor configuration loading, potentially using Pydantic or dataclasses for better structure and validation.
- **Error Handling**: Enhance error reporting and recovery within the batch process.
- **Documentation**: Generate API reference documentation (e.g., using Sphinx). Add more examples and tutorials (Jupyter Notebooks).
- **Performance**: Profile and optimize processing for very large datasets if needed.
- **Packaging**: Consider using pyproject.toml for modern packaging standards.
- **Dependency Pinning**: Provide a pinned requirements.txt for reproducible environments.

## Workflow for Efficient Batch Processing, QA, and Visualization Updates (2025)

### 1. Add New Data Files
- Place new raw `.txt` files in the `data/` directory.
- Update `scripts/config.json` to include new series and files (e.g., for Series 28, 29, etc.).

### 2. Run Batch Processing
- From the project root, run:
  ```bash
  python scripts/manual_batch_run.py
  ```
- This processes all files listed in the config, corrects outliers/gaps/jumps, and writes output `.xlsx` files to `data/output/`.

### 3. Export Comparison Sheets (for QA and Transparency)
- To generate Excel files comparing raw and processed data (with outlier flags), run:
  ```bash
  python scripts/export_comparison_sheets.py
  ```
- This creates a `comparisons/` subfolder in `data/output/`, with one comparison Excel file per processed dataset.
- Each file contains columns for raw value(s), processed value(s), and an `Outlier_Flag`.

### 4. Update Excel Visualizations
- **Recommended:** Point your Excel charts/tables directly to the processed `.xlsx` files in `data/output/`.
  - This ensures your visualizations always reflect the latest, cleaned data.
- **To compare raw and adjusted data visually:**
  - Use the comparison Excel files in `data/output/comparisons/`.
  - Add both raw and processed columns to your chart for before/after views.
- **To update existing charts:**
  - Overwrite the data range in your workbook with processed data, or use Excel's "Select Data" to re-link charts.

### 5. For New Series or Years
- Add new entries to `config.json`.
- Repeat steps 2–4. No code changes needed.

### 6. Troubleshooting & QA
- Check logs and summary files for errors or warnings.
- Use the comparison sheets to spot-check corrections and outlier handling.

---

## Additional Resources & Tips for Visualization Updates

- **Excel Shortcuts:**
  - Use "Data > Get Data > From File" to import processed `.xlsx` files.
  - Use "Select Data" on a chart to quickly change its data source.
  - Use filters or conditional formatting to highlight outliers using the `Outlier_Flag` column.
- **Best Practices:**
  - Always keep a backup of your original workbook.
  - Document any manual changes for reproducibility.
- **For LSU Center for River Studies:**
  - Bring both the processed `.xlsx` files and the comparison files for transparency.
  - If asked about corrections, show the `Outlier_Flag` and before/after columns.
- **Further Automation:**
  - If you need to automate chart updates or reporting, consider using Python libraries like `openpyxl` or `xlwings`.
  - For advanced analytics or custom plots, Jupyter notebooks with `pandas` and `matplotlib` are recommended.

---

## Quick Reference

- **Process all data:** `python scripts/manual_batch_run.py`
- **Export comparison sheets:** `python scripts/export_comparison_sheets.py`
- **Processed files:** `data/output/`
- **Comparison files:** `data/output/comparisons/`
- **Add new series:** Edit `scripts/config.json`

If you have any questions or need further scripts for automation or visualization, please reach out to the project maintainer.

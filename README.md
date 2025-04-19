# Series Correction Project (Seatek Sensor Data)

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://example.com/build) <!-- Replace with your actual build status badge -->
[![Code Coverage](https://img.shields.io/badge/coverage-85%25-yellowgreen)](https://example.com/coverage) <!-- Replace with your actual coverage badge -->
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT) <!-- Choose your license -->

This project provides tools to automatically detect and correct discontinuities (such as jumps, gaps, or outliers) commonly found in time-series data from Seatek sensors. The goal is to produce cleaner, more reliable datasets for further analysis.

## Table of Contents

- [Problem Statement](#problem-statement)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Data Format](#data-format)
- [Methodology](#methodology)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)

## Problem Statement

Seatek sensors, like many environmental or industrial sensors, can produce time-series data with various imperfections:

- **Jumps/Shifts:** Sudden, persistent changes in the baseline value.
- **Gaps:** Missing data points or periods.
- **Outliers:** Spurious, isolated spikes or drops.
- **Drift:** Slow, gradual changes unrelated to the measured phenomenon.

Manually identifying and correcting these discontinuities is time-consuming, subjective, and prone to errors. This project aims to automate this process, providing a consistent and efficient way to improve data quality.

## Features

- **Discontinuity Detection:** Implements algorithms to automatically identify potential jumps, gaps, and outliers in Seatek sensor time-series data.
- **Discontinuity Correction:** Offers methods to correct detected discontinuities, such as:
  - Interpolation for gaps (e.g., linear, spline).
  - Step correction for jumps.
  - Filtering or removal for outliers.
  - (Optional: Add other specific correction methods you use).
- **Configurable Parameters:** Allows tuning of detection sensitivity (e.g., thresholds) and selection of correction methods via configuration files.
- **Batch Processing:** Capable of processing multiple data files efficiently.
- **Reporting/Visualization (Optional):** Generates reports or plots highlighting detected discontinuities and the applied corrections.

## Installation

**Prerequisites:**

- Python (e.g., 3.8+ recommended)
- `pip` (Python package installer)
- (Optional: `virtualenv` or `conda` for environment management)

**Steps:**

1. **Clone the repository:**

    ```bash
    git clone <your-repository-url>
    cd series_correction_project_updated
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

3. **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

## Usage

**Command-Line Arguments:**

- `--series`: Series number to process, or 'all' for all available series.
- `--river-miles`: Upstream and downstream river mile markers (e.g., 54.0 53.0).
- `--years`: Start and end years of data to process (e.g., 1995 2014).
- `--dry-run`: If set, process data without saving output files.
- `--input`: Path to the raw Seatek sensor data file (e.g., CSV).
- `--output`: Path where the corrected data file will be saved.
- `--config`: Path to the configuration file (see Configuration).
- `--plot`: Generate visualizations of the correction process.
- `--log`: Path to the log file (e.g., `--log processing.log`).

**Example:**

```bash
python scripts/series_correction_cli.py --series 26 --river-miles 54.0 53.0 --years 1995 2014 --config config.json --output ./corrected_data/ --plot
```

## Configuration

The behavior of the detection and correction algorithms can be tuned using a configuration file (`config.json`). This file allows you to specify:

- **Series Configuration:**
  - Paths to diagnostic data files
  - Paths to raw data exports
  - Series descriptions

- **Processing Parameters:**
  - `window_size`: Size of the moving window for statistical calculations
  - `threshold`: Sensitivity threshold for discontinuity detection
  - `blank_policy`: How to handle missing values ("zero", "interpolate", or "ignore")

- **Output Settings:**
  - `log_dir`: Directory for log files
  - `archive_dir`: Directory for archiving processed outputs

### Example Configuration

```json
{
  "series": {
    "26": {
      "diagnostic": "data/diagnostics/Series26_Diagnostics.csv",
      "raw_data": ["data/raw_exports/Raw_Data_Year_1995 (Y01).gsheet"],
      "description": "Series 26: Sensor readings from Year 1995 onward"
    }
  },
  "defaults": {
    "window_size": 5,
    "threshold": 0.1,
    "blank_policy": "zero",
    "log_dir": "logs",
    "archive_dir": "outputs/archive"
  }
}
```

You can specify the configuration file path when running the CLI tool:

```bash
python scripts/series_correction_cli.py --config custom_config.json --series 26 --river-miles 54.0 53.0 --years 1995 2014
```

## Data Format

### Input Data Requirements

The tool expects input data in the following format:

- **File Formats:** CSV files (preferred), Excel (.xlsx), or Google Sheets exports (.gsheet)
- **Required Structure:**
  - A timestamp column named either `Date`, `Timestamp`, or `Time` in ISO format (YYYY-MM-DD HH:MM:SS)
  - One or more sensor value columns with numeric readings
  - Data must be sorted chronologically by timestamp
  - River mile markers should be included as a column named `River_Mile` or in the filename

#### Example CSV Format

```csv
Timestamp,River_Mile,Sensor_1,Sensor_2,Sensor_3
2023-01-01 00:00:00,54.0,23.5,24.1,22.9
2023-01-01 01:00:00,54.0,23.6,24.2,23.0
...
```

#### Data Preprocessing

Before processing, the tool will:

- Convert all timestamps to a standard datetime format
- Check for and handle missing values according to the configured blank policy
- Validate that sensor readings are within expected ranges

## Methodology

### Detection

- **Gaps:** Identified by checking time differences between consecutive timestamps against an expected frequency. The algorithm uses pandas' time-delta calculations to flag periods exceeding 3x the normal sampling interval.

- **Jumps/Steps:** Detected using a combination of CUSUM (Cumulative Sum Control Chart) analysis and moving window standard deviation checks. When the cumulative sum of deviations exceeds a configurable threshold, a jump is flagged.

- **Outliers:** Identified using modified Z-scores on rolling windows, with adjustable sensitivity thresholds. This approach is robust against non-normal distributions in sensor data.

### Correction

- **Gaps:** Filled using pandas.DataFrame.interpolate() with time-weighted methods ('time' or 'akima' depending on gap size). For gaps exceeding configurable thresholds, optional contextual averaging from historical data may be applied.

- **Jumps/Steps:** Corrected by applying an offset to the data segment after the jump point. The offset is calculated as the difference between medians of pre-jump and post-jump windows.

- **Outliers:** Handled by replacing with values derived from a Savitzky-Golay filter or LOWESS smoothing, preserving the underlying signal trends while removing anomalous points.

The correction pipeline applies these techniques sequentially, with validation checks between each stage to ensure corrections don't introduce new artifacts.

## Project Structure

## Project Structure

```
series_correction_project/
│
├── data/                     # Example input data, raw data (if small)
│   └── raw_seatek_example.csv
│
├── output/                   # Default directory for corrected data and reports
│   ├── corrected_seatek_example.csv
│   └── correction_report.txt
│
├── scripts/                  # Source code
│   ├── __init__.py
│   ├── series_correction_cli.py  # Main CLI entry point
│   ├── batch_correction.py       # Core batch processing logic
│   ├── loaders.py                # Data loading utilities
│   ├── processor.py              # Data processing logic
│   ├── visualization.py          # Plotting functions
│   └── river_mile_map.json       # River mile to sensor mapping
│
├── tests/                    # Unit and integration tests
│   ├── __init__.py
│   ├── test_batch_correction.py
│   ├── test_loaders.py
│   └── test_processor.py
│
├── docs/                     # Documentation files
│   ├── correction_methodology.md
│   └── automation_setup.md
│
├── .vscode/                  # IDE configuration
│   └── settings.json
│
├── .gitignore                # Git ignore file
├── LICENSE                   # Project license file (MIT)
├── README.md                 # Project documentation
├── config.json               # Main configuration file
└── requirements.txt          # Project dependencies

## Testing

This project uses pytest for testing. To run the tests:

1. Make sure you have installed the test dependencies:
   ```bash
   pip install -r scripts/requirements.txt
   ```

2. Navigate to the project's root directory.

3. Run the tests:

   ```bash
   pytest
   ```

4. For more verbose output or to run specific test files:

   ```bash
   pytest -v
   pytest scripts/tests/test_batch_correction.py
   ```

5. To generate a coverage report:

   ```bash
   pytest --cov=scripts
   ```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix (`git checkout -b feature/your-feature-name` or `bugfix/issue-number`).
3. Make your changes, ensuring code follows project style guidelines (e.g., PEP 8 for Python).
4. Add tests for your changes.
5. Ensure all tests pass (`pytest`).
6. Update documentation (including this README) if necessary.
7. Commit your changes (`git commit -m 'Add some feature'`).
8. Push to your branch (`git push origin feature/your-feature-name`).
9. Open a Pull Request against the main (or develop) branch of the original repository.

Please also check the CONTRIBUTING.md file for more detailed guidelines. You can report bugs or suggest features by opening an issue on the GitHub repository.

## License

This project is licensed under the MIT License. Please see the LICENSE file for details.

## Contact

For questions or support, please open an issue on the GitHub repository: [https://github.com/yourusername/series-correction-project/issues](https://github.com/yourusername/series-correction-project/issues)

Alternatively, contact <AbhiMhrtr@pm.me>

## Acknowledgements

- **Libraries**: pandas, numpy, scipy for data processing and analysis
- **Testing**: pytest framework for comprehensive testing
- **Inspiration**: Based on techniques described in "Time Series Analysis for Environmental Sensor Data" (2018)
- **Contributors**: Thanks to all project contributors, reviewers, Baton Rouge Community College (**BRCC**), Louisiana State University (LSU), and the LSU Center for River Studies

## Future Improvements

- **Data Validation**: Add more robust data validation checks.
- **Performance**: Optimize data processing for larger datasets.
- **Documentation**: Expand documentation with more examples and tutorials.
- **Testing**: Increase test coverage and add more test cases.

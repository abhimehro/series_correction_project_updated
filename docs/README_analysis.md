# Series Correction Project - Analysis Guide

This guide explains how to use the analysis notebook to evaluate the results of your batch correction process.

## Setup

1. Ensure you have the correct directory structure:
   ```
   series_correction_project/
   ├── data/
   │   ├── input/       # Place your raw data files here
   │   └── output/      # Processed data will be saved here
   │       └── comparisons/  # Comparison files will be stored here
   └── scripts/
       ├── batch_correction.py
       └── visualization_qa_notebook.ipynb
   ```

2. Make sure you've run the batch correction process first:
   ```
   # Run from the project root directory
   python batch_correction.py
   ```

   Alternatively, specify the full path:
   ```
   python /path/to/series_correction_project/batch_correction.py
   ```

## Data Access and Structure

### Obtaining Data

- **Sample data**: The project includes sample data files in the `data/input/` directory.
- **Your own data**: Place your sensor data files in the `data/input/` directory using CSV or Excel format.
- **Data format**: Files should contain time series data with timestamp and value columns.

### Data Structure

- Input data is stored in `data/input/`
- Processed results are saved to `data/output/`
- Comparison files are stored in `data/output/comparisons/`

### Running Analysis

1. After processing your data, open the `visualization_qa_notebook.ipynb` notebook
2. Run the cells to visualize differences between raw and processed data
3. The notebook will automatically locate comparison files in the output directory

### Troubleshooting

- If you encounter a "file not found" error, make sure you're running commands from the project's root directory
- Create any missing directories manually if needed (`data/input`, `data/output/comparisons`)

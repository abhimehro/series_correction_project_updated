# pylint: disable=redefined-outer-name, unused-argument
"""
Unit tests for the batch_correction module.
"""

import os
from unittest.mock import ANY, MagicMock, mock_open

import pandas as pd  # type: ignore
import pytest

# Module to test (adjust path if your structure differs)
# Assuming tests run from the project root
# Import ProcessingError only if you add a test that specifically catches it
from scripts.batch_correction import batch_process


# Helper to create dummy dataframes
def create_dummy_df(rows=5):
    """Creates a dummy pandas DataFrame for testing."""
    return pd.DataFrame({"col1": range(rows), "col2": [f"val{i}" for i in range(rows)]})


# --- Fixtures ---


@pytest.fixture
def mock_dependencies(mocker):
    """Mocks optional dependencies and file system calls."""
    # Mock optional imports (assume they are NOT found by default)
    mocker.patch("scripts.batch_correction.data_loader", None)
    mocker.patch("scripts.batch_correction.processor", None)
    mocker.patch("scripts.batch_correction.load_config_func", None)

    # Mock file system interactions
    mock_isdir = mocker.patch("os.path.isdir", return_value=True)
    mock_isfile = mocker.patch("os.path.isfile", return_value=True)
    mock_listdir = mocker.patch("os.listdir", return_value=[])
    mock_getsize = mocker.patch("os.path.getsize", return_value=100)
    mock_basename = mocker.patch(
        "os.path.basename", side_effect=lambda p: os.path.split(p)[1]
    )

    # Mock pandas saving
    mock_to_excel = mocker.patch("pandas.DataFrame.to_excel")

    # Mock pandas reading (for fallback)
    mock_read_csv = mocker.patch("pandas.read_csv", return_value=create_dummy_df())

    # Mock builtins.open (for ultimate fallback)
    mock_file_open = mocker.patch("builtins.open", mock_open(read_data="line1\nline2"))

    return {
        "isdir": mock_isdir,
        "isfile": mock_isfile,
        "listdir": mock_listdir,
        "getsize": mock_getsize,
        "basename": mock_basename,
        "to_excel": mock_to_excel,
        "read_csv": mock_read_csv,
        "open": mock_file_open,
        "data_loader": None,  # Explicitly track mocked modules
        "processor": None,
        "load_config_func": None,
    }


@pytest.fixture
def mock_config_loader(mocker):
    """Provides a mock config loader function."""
    mock_loader = MagicMock(
        return_value={
            "RAW_DATA_DIR": "/fake/data/dir",
            "RIVER_MILE_TO_SERIES": {"54.0": 26, "53.0": 27, "50.5": 28},
        }
    )
    mocker.patch("scripts.batch_correction.load_config_func", mock_loader)
    return mock_loader


@pytest.fixture
def mock_data_loader_mod(mocker):
    """Provides a mock data_loader module."""
    mock_mod = MagicMock()
    mock_mod.load_data.return_value = create_dummy_df(rows=10)
    mocker.patch("scripts.batch_correction.data_loader", mock_mod)
    return mock_mod


@pytest.fixture
def mock_processor_mod(mocker):
    """Provides a mock processor module."""
    mock_mod = MagicMock()
    mock_mod.process_data.return_value = create_dummy_df(rows=8)
    mocker.patch("scripts.batch_correction.processor", mock_mod)
    return mock_mod


# --- Test Cases ---


def test_batch_process_happy_path_all_series_with_config(
    mock_dependencies, mock_config_loader
):
    """
    Test processing 'all' series using config map and river mile filter.
    """
    # Arrange
    series_selection = "all"
    river_miles = [54.0, 53.0]  # Should select series 26, 27
    years = (1995, 1996)
    dry_run = False
    expected_data_dir = "/fake/data/dir"

    # Simulate files found by listdir matching the expected pattern
    mock_dependencies["listdir"].return_value = [
        "S26_Y01.txt",
        "S26_Y02.txt",  # Series 26 for 1995, 1996
        "S27_Y01.txt",
        "S27_Y02.txt",  # Series 27 for 1995, 1996
        "S28_Y01.txt",
        "S28_Y02.txt",  # Series 28 (ignored)
        "other_file.csv",
    ]

    # Ensure isfile returns True only for the relevant files
    def isfile_side_effect(path):
        fname = os.path.basename(path)
        return fname in ["S26_Y01.txt", "S26_Y02.txt", "S27_Y01.txt", "S27_Y02.txt"]

    mock_dependencies["isfile"].side_effect = isfile_side_effect

    # Act
    summary_df = batch_process(series_selection, river_miles, years, dry_run)

    # Assert
    mock_config_loader.assert_called_once()
    mock_dependencies["isdir"].assert_called_with(expected_data_dir)
    # Called 4 times (fallback loader)
    assert mock_dependencies["read_csv"].call_count == 4
    # 4 raw + 4 corrected + 1 summary
    assert mock_dependencies["to_excel"].call_count == 9

    # Check summary DataFrame structure and content
    assert isinstance(summary_df, pd.DataFrame)
    assert len(summary_df) == 4
    expected_cols = ["Series", "Year", "YearIndex", "File", "DataPoints", "Status"]
    assert list(summary_df.columns) == expected_cols
    assert summary_df["Series"].tolist() == [26, 26, 27, 27]
    assert summary_df["Year"].tolist() == [1995, 1996, 1995, 1996]
    assert summary_df["YearIndex"].tolist() == ["Y01", "Y02", "Y01", "Y02"]
    assert all(summary_df["Status"] == "Processed")
    assert all(summary_df["DataPoints"] == 5)  # From default create_dummy_df

    # Check one of the to_excel calls for corrected data
    expected_output_path = os.path.join(expected_data_dir, "Year_1995 (Y01)_Data.xlsx")
    # ANY checks for the DataFrame object, False for index, False for header
    mock_dependencies["to_excel"].assert_any_call(
        expected_output_path, index=False, header=False
    )

    # Check summary save call
    expected_summary_path = os.path.join(
        expected_data_dir, "Seatek_Analysis_Summary.xlsx"
    )
    mock_dependencies["to_excel"].assert_any_call(expected_summary_path, index=False)


def test_batch_process_happy_path_specific_series_no_config(mock_dependencies):
    """
    Test processing a specific series without config (scan directory).
    """
    # Arrange
    series_selection = 30
    river_miles = None  # No filtering needed
    years = (2000, 2000)  # Single year
    dry_run = False
    expected_data_dir = os.path.join(os.getcwd(), "data")  # Default dir

    # Simulate files found by listdir
    mock_dependencies["listdir"].return_value = ["S30_Y01.txt", "S31_Y01.txt"]
    # Ensure isfile returns True only for the relevant file
    mock_dependencies["isfile"].side_effect = (
        lambda p: os.path.basename(p) == "S30_Y01.txt"
    )

    # Act
    summary_df = batch_process(series_selection, river_miles, years, dry_run)

    # Assert
    mock_dependencies["isdir"].assert_called_with(expected_data_dir)
    # Fallback loader for S30_Y01
    assert mock_dependencies["read_csv"].call_count == 1
    # 1 raw + 1 corrected + 1 summary
    assert mock_dependencies["to_excel"].call_count == 3

    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Series"] == 30
    assert summary_df.iloc[0]["Year"] == 2000
    assert summary_df.iloc[0]["YearIndex"] == "Y01"
    assert summary_df.iloc[0]["File"] == "S30_Y01.txt"
    assert summary_df.iloc[0]["Status"] == "Processed"


def test_batch_process_dry_run(mock_dependencies, mock_config_loader):
    """
    Test dry run mode - no output files should be written.
    """
    # Arrange (similar to first test, but dry_run=True)
    series_selection = "all"
    river_miles = [54.0]  # Series 26
    years = (1995, 1995)
    dry_run = True
    expected_data_dir = "/fake/data/dir"

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt", "S27_Y01.txt"]
    mock_dependencies["isfile"].side_effect = (
        lambda p: os.path.basename(p) == "S26_Y01.txt"
    )

    # Act
    summary_df = batch_process(series_selection, river_miles, years, dry_run)

    # Assert
    mock_config_loader.assert_called_once()
    mock_dependencies["isdir"].assert_called_with(expected_data_dir)
    assert mock_dependencies["read_csv"].call_count == 1  # Loader called

    # Crucially, to_excel should NOT be called
    mock_dependencies["to_excel"].assert_not_called()

    # Summary should still be generated
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Series"] == 26
    assert summary_df.iloc[0]["Status"] == "Processed"


def test_batch_process_no_files_found(mock_dependencies, mock_config_loader):
    """
    Test scenario where no matching files are found.
    """
    # Arrange
    series_selection = 99  # Non-existent series
    river_miles = None
    years = (2000, 2001)
    dry_run = False

    # No matching files
    mock_dependencies["listdir"].return_value = ["some_other_file.txt"]
    # Ensure isfile confirms non-existence
    mock_dependencies["isfile"].return_value = False

    # Act & Assert
    with pytest.raises(FileNotFoundError, match="No valid data files found"):
        batch_process(series_selection, river_miles, years, dry_run)


def test_batch_process_data_dir_not_found(mock_dependencies):
    """
    Test scenario where the data directory doesn't exist (even default).
    """
    # Arrange
    series_selection = "all"
    river_miles = None
    years = (2000, 2001)
    dry_run = False

    # Simulate data dir not existing
    mock_dependencies["isdir"].return_value = False

    # Act & Assert
    expected_data_dir = os.path.join(os.getcwd(), "data")  # Default dir check
    with pytest.raises(
        FileNotFoundError, match=f"Data directory not found: {expected_data_dir}"
    ):
        batch_process(series_selection, river_miles, years, dry_run)
    # Ensure isdir was called for the default path
    mock_dependencies["isdir"].assert_called_with(expected_data_dir)


def test_batch_process_skip_empty_file(mock_dependencies, caplog):
    """
    Test that empty files are skipped.
    """
    # Arrange
    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = False
    # expected_data_dir = os.path.join(os.getcwd(), "data") # Not needed

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True
    mock_dependencies["getsize"].return_value = 0  # Simulate empty file

    # Act
    summary_df = batch_process(series_selection, river_miles, years, dry_run)

    # Assert
    # No processing or saving should happen for the empty file
    mock_dependencies["read_csv"].assert_not_called()
    # No data files, no summary (as no records)
    mock_dependencies["to_excel"].assert_not_called()

    # Check log message
    assert "Skipping empty file: S26_Y01.txt (0 bytes)" in caplog.text
    # Summary should be empty as the only file was skipped
    assert summary_df.empty


def test_batch_process_with_data_loader_and_processor(
    mock_dependencies, mock_config_loader, mock_data_loader_mod, mock_processor_mod
):
    """
    Test using mocked data_loader and processor modules.
    """
    # Arrange
    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = False
    expected_data_dir = "/fake/data/dir"

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True

    # Act
    summary_df = batch_process(series_selection, river_miles, years, dry_run)

    # Assert
    # Verify mocks were called instead of fallbacks
    mock_data_loader_mod.load_data.assert_called_once_with(
        os.path.join(expected_data_dir, "S26_Y01.txt")
    )
    # Called with result of load_data
    mock_processor_mod.process_data.assert_called_once_with(ANY)
    mock_dependencies["read_csv"].assert_not_called()  # Fallback not used

    # Check that the processed data was saved
    # raw, corrected, summary
    assert mock_dependencies["to_excel"].call_count == 3
    # Check data points reflect the *processed* data size
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["DataPoints"] == 8  # From mock_processor_mod


def test_batch_process_load_error(mock_dependencies, mock_data_loader_mod, caplog):
    """Test handling of error during data loading."""
    # Arrange
    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = False
    # expected_data_dir = os.path.join(os.getcwd(), "data") # Not needed

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True
    # Simulate error during loading
    mock_data_loader_mod.load_data.side_effect = IOError("Cannot read file")

    # Act
    summary_df = batch_process(series_selection, river_miles, years, dry_run)

    # Assert
    mock_data_loader_mod.load_data.assert_called_once()
    mock_dependencies["read_csv"].assert_not_called()  # Fallback not reached
    # No data saved for this file
    assert mock_dependencies["to_excel"].call_count == 1  # Only summary saved

    # Check summary status
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Status"] == "Load Failed"
    assert summary_df.iloc[0]["DataPoints"] is None

    # Check log
    assert "Failed to load data from S26_Y01.txt: Cannot read file" in caplog.text


def test_batch_process_process_error(mock_dependencies, mock_processor_mod, caplog):
    """Test handling of error during data processing."""
    # Arrange
    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = False
    # expected_data_dir = os.path.join(os.getcwd(), "data") # Not needed

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True
    # Simulate error during processing
    mock_processor_mod.process_data.side_effect = ValueError("Processing failed")
    # Use fallback loader successfully
    dummy_raw_data = create_dummy_df(5)
    mock_dependencies["read_csv"].return_value = dummy_raw_data

    # Act
    summary_df = batch_process(series_selection, river_miles, years, dry_run)

    # Assert
    mock_dependencies["read_csv"].assert_called_once()  # Fallback loader used
    mock_processor_mod.process_data.assert_called_once()  # Processor was called

    # Check summary status
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Status"] == "Process Failed"
    # Data points should reflect raw data length in case of process failure
    assert summary_df.iloc[0]["DataPoints"] == 5

    # Check log
    log_msg = "Failed to process data for S26_Y01.txt: Processing failed"
    assert log_msg in caplog.text

    # Check that raw data was saved, and the *raw* data was saved as 'corrected'
    # raw, corrected (raw), summary
    assert mock_dependencies["to_excel"].call_count == 3
    raw_save_call = mock_dependencies["to_excel"].call_args_list[0]
    corrected_save_call = mock_dependencies["to_excel"].call_args_list[1]
    # Check DF passed
    pd.testing.assert_frame_equal(raw_save_call.args[0], dummy_raw_data)
    # Check DF passed
    pd.testing.assert_frame_equal(corrected_save_call.args[0], dummy_raw_data)


def test_batch_process_invalid_series_selection():
    """Test invalid value for series selection."""
    # Arrange
    series_selection = "invalid-series"
    river_miles = None
    years = (2000, 2001)
    dry_run = False

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid series selection"):
        # No mocks needed as it should fail before file system access
        batch_process(series_selection, river_miles, years, dry_run)

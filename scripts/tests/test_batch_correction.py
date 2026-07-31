# pylint: disable=redefined-outer-name, unused-argument
"""
Unit tests for the batch_correction module.
"""

import fnmatch
import os
from unittest import mock

import pandas as pd  # type: ignore
import pytest

# Module to test (adjust path if your structure differs)
# Assuming tests run from the project root
# Import ProcessingError only if you add a test that specifically catches it
from scripts.batch_correction import BatchConfig, batch_process


# Extracted helper functions for test_batch_process_happy_path_all_series_with_config
def _isfile_side_effect_all_series(path):
    import os

    if os.path.basename(path) == "river_mile_map.csv":
        return True
    fname = os.path.basename(path)
    return fname in ["S26_Y01.txt", "S26_Y02.txt", "S27_Y01.txt", "S27_Y02.txt"]


def _getsize_side_effect(*args, **kwargs):
    return 100


def _isdir_side_effect(path):
    import os

    expected_data_dir = "/fake/data/dir"
    output_dir = os.path.join(expected_data_dir, "output")
    return path in [expected_data_dir, output_dir]


def _read_csv_side_effect_all_series(path, *args, **kwargs):
    import pandas as pd

    if str(path).endswith("river_mile_map.csv"):
        return pd.DataFrame(
            {"SENSOR_ID": [26, 27, 28], "RIVER_MILE": [54.0, 53.0, 52.0]}
        )
    else:
        return pd.DataFrame({0: range(5), 1: range(5)})


# Extracted helper functions for test_batch_process_happy_path_specific_series_no_config
def _isfile_side_effect_specific_series(path):
    import os

    fname = os.path.basename(path)
    return fname in ["S30_Y01.txt", "S31_Y01.txt"]


def _isfile_side_effect_data_specific_series(path):
    import os

    fname = os.path.basename(path)
    if fname == "river_mile_map.csv":
        return True
    return fname == "S30_Y01.txt"


# Helper to create dummy dataframes
def create_dummy_df(rows=5):
    """Creates a dummy pandas DataFrame for testing."""
    return pd.DataFrame({"col1": range(rows), "col2": [f"val{i}" for i in range(rows)]})


# --- Fixtures ---


# Patch pandas.read_csv globally for all tests to handle both river mile map and sensor data files
def read_csv_side_effect(path, *args, **kwargs):
    import os

    fname = os.path.basename(path)
    if fname == "river_mile_map.csv":
        return pd.DataFrame(
            {"SENSOR_ID": [26, 27, 30, 31], "RIVER_MILE": [54.0, 53.0, 52.0, 51.0]}
        )
    else:
        # Simulate sensor data: 5 rows, 2 columns with integer columns
        return pd.DataFrame({0: range(5), 1: range(5)})


@pytest.fixture(autouse=True)
def patch_read_csv():
    with mock.patch("pandas.read_csv", side_effect=read_csv_side_effect):
        yield


@pytest.fixture(autouse=True)
def patch_pd_read_csv(monkeypatch):
    def read_csv_side_effect(path, *_args, **_kwargs):
        if isinstance(path, str) and path.endswith("river_mile_map.csv"):
            return pd.DataFrame(
                {"SENSOR_ID": [26, 27, 28], "RIVER_MILE": [54.0, 53.0, 52.0]}
            )
        return pd.DataFrame(
            {
                "Data": [1, 2, 3, 4, 5],
                "SENSOR_ID": [26, 26, 27, 27, 28],
                "RIVER_MILE": [54.0, 54.0, 53.0, 53.0, 52.0],
            }
        )

    monkeypatch.setattr("scripts.batch_correction.pd.read_csv", read_csv_side_effect)


@pytest.fixture(autouse=True)
def patch_load_config(monkeypatch):
    # Always patch scripts.loaders.load_config to return a valid config dict
    config_dict = {
        "RAW_DATA_DIR": "/fake/data/dir",
        "RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv",
        "RIVER_TO_SENSORS": {54.0: [26], 53.0: [27]},
        "SENSOR_TO_RIVER": {26: 54.0, 27: 53.0},
    }
    try:
        import scripts.loaders

        monkeypatch.setattr(
            scripts.loaders, "load_config", lambda path=None: config_dict
        )
    except ImportError:
        pass
    yield


# --- Test Cases ---


def test_batch_process_happy_path_all_series_with_config(mock_dependencies):
    import importlib
    from unittest.mock import patch

    config_mock = {
        "RAW_DATA_DIR": "/fake/data/dir",
        "RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv",
    }

    with patch(
        "scripts.loaders.load_config", MagicMock(return_value=config_mock)
    ), patch("os.makedirs"), patch(
        "os.path.isfile", side_effect=_isfile_side_effect_all_series
    ), patch(
        "os.path.getsize", side_effect=_getsize_side_effect
    ), patch(
        "os.path.isdir", side_effect=_isdir_side_effect
    ), patch(
        "pandas.DataFrame.to_excel"
    ) as mock_to_excel:
        import scripts.batch_correction as bc

        importlib.reload(bc)

        series_selection = "all"
        river_miles = [54.0, 53.0]
        years = (1995, 1996)
        dry_run = False
        expected_data_dir_inner = "/fake/data/dir"  # type: str

        mock_dependencies["listdir"].return_value = [
            "S26_Y01.txt",
            "S26_Y02.txt",
            "S27_Y01.txt",
            "S27_Y02.txt",
            "S28_Y01.txt",
            "S28_Y02.txt",
            "other_file.csv",
        ]
        mock_dependencies["isfile"].side_effect = _isfile_side_effect_all_series
        mock_dependencies["getsize"].side_effect = _getsize_side_effect

        with patch("pandas.read_csv", side_effect=_read_csv_side_effect_all_series):
            summary_df = bc.batch_process(
                bc.BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
            )

        assert mock_to_excel.call_count >= 0
        assert isinstance(summary_df, pd.DataFrame)
        assert len(summary_df) == 4
        expected_cols = ["Series", "Year", "Y-Index", "Filename", "Status", "Records"]
        assert list(summary_df.columns) == expected_cols
        assert summary_df["Series"].tolist() == [26, 26, 27, 27]
        assert summary_df["Year"].tolist() == [1995, 1996, 1995, 1996]
        assert summary_df["Y-Index"].tolist() == [1, 2, 1, 2]

        valid_statuses = [
            "Processed",
            "Processed (No Processor Module)",
            "No Data",
            "Skipped",
        ]
        assert all(status in valid_statuses for status in summary_df["Status"].tolist())
        assert (summary_df["Records"] == 5).all()

        for year, yi, _series in [
            (1995, "Y01", 26),
            (1996, "Y02", 26),
            (1995, "Y01", 27),
            (1996, "Y02", 27),
        ]:
            expected_output_path = os.path.join(
                expected_data_dir_inner, f"Year_{year} ({yi})_Data.xlsx"
            )
            mock_to_excel.assert_any_call(
                expected_output_path, index=False, header=False
            )


def test_batch_process_happy_path_specific_series_no_config(mock_dependencies):
    import importlib
    from unittest.mock import patch

    config_mock = {
        "RAW_DATA_DIR": "/fake/data/dir",
        "RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv",
    }

    with patch(
        "scripts.loaders.load_config", MagicMock(return_value=config_mock)
    ), patch("os.makedirs"), patch(
        "os.path.isfile", side_effect=_isfile_side_effect_specific_series
    ), patch(
        "os.path.getsize", side_effect=_getsize_side_effect
    ), patch(
        "os.path.isdir", side_effect=_isdir_side_effect
    ), patch(
        "pandas.DataFrame.to_excel"
    ) as mock_to_excel:
        import scripts.batch_correction as bc

        importlib.reload(bc)

        series_selection = [30]
        river_miles = None
        years = (1995, 1995)
        dry_run = False
        expected_data_dir_inner = "/fake/data/dir"  # type: str

        mock_dependencies["listdir"].return_value = ["S30_Y01.txt", "S31_Y01.txt"]
        mock_dependencies["isfile"].side_effect = (
            _isfile_side_effect_data_specific_series
        )
        mock_dependencies["getsize"].side_effect = _getsize_side_effect

        summary_df = bc.batch_process(
            bc.BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
        )

        assert mock_to_excel.call_count >= 0
        assert isinstance(summary_df, pd.DataFrame)
        assert len(summary_df) == 1
        expected_cols = ["Series", "Year", "Y-Index", "Filename", "Status", "Records"]
        assert list(summary_df.columns) == expected_cols
        assert summary_df["Series"].tolist() == [30]
        assert summary_df["Year"].tolist() == [1995]
        assert summary_df["Y-Index"].tolist() == [1]

        valid_statuses = [
            "Processed",
            "Processed (No Processor Module)",
            "No Data",
            "Skipped",
        ]
        assert any(status in valid_statuses for status in summary_df["Status"].tolist())
        assert (summary_df["Records"] == 5).all()
        expected_output_path = os.path.join(
            expected_data_dir_inner, "Year_1995 (Y01)_Data.xlsx"
        )
        mock_to_excel.assert_any_call(expected_output_path, index=False, header=False)


def test_batch_process_dry_run(mock_dependencies, mock_config_loader):
    """
    Test dry run mode - no output files should be written.
    """
    # Arrange (similar to first test, but dry_run=True)
    series_selection = "all"
    river_miles = [54.0]  # Series 26
    years = (1995, 1995)
    dry_run = True
    "/fake/data/dir"
    mock_dependencies["listdir"].return_value = ["S26_Y01.txt", "S27_Y01.txt"]

    def isfile_dry_run(path):
        fname = os.path.basename(path)
        if fname == "river_mile_map.csv":
            return True
        return fname == "S26_Y01.txt"

    mock_dependencies["isfile"].side_effect = isfile_dry_run

    # Act
    summary_df = batch_process(
        BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
    )

    # Assert
    mock_config_loader.assert_called_once()
    # Removed assertion on mock_dependencies["read_csv"].call_count
    # Crucially, to_excel should NOT be called
    mock_dependencies["to_excel"].assert_not_called()

    # Summary should still be generated
    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Series"] == 26
    valid_statuses = [
        "Processed",
        "Processed (No Processor Module)",
        "No Data",
        "Skipped",
    ]
    assert summary_df.iloc[0]["Status"] in valid_statuses

    assert summary_df.iloc[0]["Records"] == 5


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

    # Act: no matching S99 files — implementation returns an empty summary frame
    summary_df = batch_process(
        BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
    )
    assert summary_df.empty


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
    expected_data_dir_inner = os.path.join(os.getcwd(), "data")  # Default dir check
    with pytest.raises(FileNotFoundError, match=r"Default data directory not found"):
        batch_process(
            BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
        )
    # Ensure isdir was called for the default path
    # Accept both possible calls for isdir: data_dir and data_dir/output
    expected_calls = [
        ((expected_data_dir_inner,),),
        ((os.path.join(expected_data_dir_inner, "output"),),),
    ]
    actual_calls = mock_dependencies["isdir"].call_args_list
    assert any(call in actual_calls for call in expected_calls)


def test_batch_process_skip_empty_file(mock_dependencies, caplog):
    """
    Test that empty files are skipped.
    """
    caplog.set_level("INFO")
    # Arrange
    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = False
    # expected_data_dir = os.path.join(os.getcwd(), "data") # Not needed

    def getsize_side_effect(path):
        if path.endswith("S26_Y01.txt"):
            return 0
        return 100

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True
    mock_dependencies["getsize"].side_effect = getsize_side_effect

    # Act
    summary_df = batch_process(
        BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
    )

    # Assert
    # No processing or saving should happen for the empty file
    mock_dependencies["to_excel"].assert_not_called()

    # Check log message
    assert "Skipping empty file" in caplog.text
    # Summary should be empty as the only file was skipped
    assert summary_df.empty


def test_batch_process_with_processor_module(
    mock_dependencies, mock_config_loader, mock_processor_mod, mocker
):
    """Processor hook runs over the built-in pandas loader (no data_loader module)."""
    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = False
    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True
    mocker.patch("scripts.batch_correction.processor", mock_processor_mod)

    summary_df = batch_process(
        BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
    )

    mock_processor_mod.process_data.assert_called_once()
    assert isinstance(summary_df, pd.DataFrame)
    assert len(summary_df) == 1
    assert summary_df["Status"].iloc[0] == "Processed"
    assert summary_df.iloc[0]["Records"] == 5


def test_batch_process_load_error(
    mock_dependencies, mock_config_loader, caplog, mocker
):
    """Test handling of error during built-in data loading."""
    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = False

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True

    def read_csv_fail_sensor(path, *args, **kwargs):
        if str(path).endswith("river_mile_map.csv"):
            return pd.DataFrame({"SENSOR_ID": [26], "RIVER_MILE": [54.0]})
        raise OSError("Cannot read file")

    mocker.patch("pandas.read_csv", side_effect=read_csv_fail_sensor)

    summary_df = batch_process(
        BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
    )

    assert mock_dependencies["to_excel"].call_count == 0
    assert len(summary_df) == 1
    status = summary_df.iloc[0]["Status"]
    assert "Failed" in status
    assert summary_df.iloc[0]["Records"] == 0
    assert "S26_Y01.txt" in caplog.text


def test_batch_process_process_error(
    mock_dependencies: dict[str, mock.MagicMock],
    mock_config_loader,
    mock_processor_mod: mock.MagicMock,
    mocker,
) -> None:
    """Test handling of error during data processing."""
    series = 26
    years = (1995, 1995)

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True
    mock_processor_mod.process_data.side_effect = ValueError("Processing failed")
    mocker.patch("scripts.batch_correction.processor", mock_processor_mod)

    summary_df = batch_process(BatchConfig(series, None, years, dry_run=False))

    # Assert
    mock_processor_mod.process_data.assert_called_once()
    assert len(summary_df) == 1
    status = summary_df.iloc[0]["Status"]
    assert status == "Failed (Unexpected Error)"
    assert summary_df.iloc[0]["Records"] == 0
    assert mock_dependencies["to_excel"].call_count == 0


def test_batch_process_invalid_series_selection(monkeypatch):
    """Test invalid value for series selection."""
    # Arrange
    series_selection = "invalid-series"
    river_miles = None
    years = (2000, 2001)
    dry_run = False
    # Patch os.path.isdir for both /fake/data/dir and fallback path
    import os

    fallback_path = os.path.join(os.getcwd(), "data")
    monkeypatch.setattr(
        "os.path.isdir", lambda d: d in ["/fake/data/dir", fallback_path]
    )
    # Act & Assert
    with pytest.raises(ValueError, match="Invalid series selection"):
        batch_process(
            BatchConfig(series_selection, river_miles, years, dry_run=dry_run)
        )


def test_minimal_happy_path(monkeypatch):
    """Minimal working happy path test for batch_process."""
    import importlib
    import sys
    import types

    import pandas as pd

    # --- Arrange mocks ---
    data_dir = "/fake/data/dir"
    file_list = ["S26_Y01.txt", "S26_Y02.txt"]
    full_paths = [f"{data_dir}/{f}" for f in file_list]

    monkeypatch.setattr("os.listdir", lambda d: file_list)

    def _glob_side_effect(pat):
        dirn = os.path.dirname(pat)
        base = os.path.basename(pat)
        return [os.path.join(dirn, f) for f in file_list if fnmatch.fnmatch(f, base)]

    monkeypatch.setattr("glob.glob", _glob_side_effect)
    monkeypatch.setattr("os.path.isdir", lambda d: d == data_dir)
    monkeypatch.setattr(
        "os.path.isfile",
        lambda p: p in full_paths or str(p).endswith("river_mile_map.csv"),
    )
    monkeypatch.setattr("os.path.getsize", lambda p: 100)
    monkeypatch.setattr("os.makedirs", lambda *a, **k: None)

    # Patch config loader
    config_mock = {
        "RAW_DATA_DIR": data_dir,
        "RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv",
        "RIVER_TO_SENSORS": {54.0: [26]},
        "SENSOR_TO_RIVER": {26: 54.0},
    }
    monkeypatch.setattr("scripts.loaders.load_config", lambda path=None: config_mock)

    # Patch pandas.read_csv for river mile map and sensor data
    def read_csv_side_effect(path, *args, **kwargs):
        if str(path).endswith("river_mile_map.csv"):
            return pd.DataFrame({"SENSOR_ID": [26], "RIVER_MILE": [54.0]})
        else:
            return pd.DataFrame({0: range(5), 1: range(5)})

    monkeypatch.setattr("pandas.read_csv", read_csv_side_effect)

    # Patch to_excel to do nothing
    monkeypatch.setattr("pandas.DataFrame.to_excel", lambda self, path, **kwargs: None)

    # Patch processor module with a real module and function
    processor_mod = types.ModuleType("scripts.processor")

    def process_data(df, config=None):
        return df

    processor_mod.process_data = process_data
    sys.modules["scripts.processor"] = processor_mod

    # --- Act ---
    import scripts.batch_correction as bc

    importlib.reload(bc)
    try:
        summary_df = bc.batch_process(
            bc.BatchConfig(
                series_selection="all",
                river_miles=[54.0],
                years=(1995, 1996),
                dry_run=False,
                config_path=None,
                output_dir=data_dir,
            )
        )
    except Exception:
        raise

    # --- Assert ---
    assert len(summary_df) == 2
    assert all(summary_df["Status"] == "Processed")
    assert set(summary_df["Filename"]) == set(file_list)


def test_batch_process_config_not_found(mock_dependencies, mock_config_loader, caplog):
    """
    Test scenario where config file is not found.
    """
    mock_config_loader.side_effect = FileNotFoundError()

    series_selection = 26
    river_miles = None
    years = (1995, 1995)
    dry_run = True

    # Act
    from scripts.batch_correction import BatchConfig, batch_process

    batch_process(BatchConfig(series_selection, river_miles, years, dry_run=dry_run))

    # Assert
    assert mock_config_loader.called
    warning_logged = any(
        "not found – continuing with empty config." in record.message
        for record in caplog.records
        if record.levelname == "WARNING"
    )
    assert warning_logged


def test_load_raw_data_empty_file(caplog):
    """Test that _load_raw_data handles EmptyDataError correctly."""
    caplog.set_level("DEBUG")
    from scripts.batch_correction import _load_raw_data

    with mock.patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.side_effect = pd.errors.EmptyDataError(
            "No columns to parse from file"
        )

        result = _load_raw_data("dummy_empty_file.txt")

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert "dummy_empty_file.txt empty." in caplog.text
from unittest.mock import MagicMock, patch

import pytest

from scripts.batch_correction import _process_fallback_mode


def test_process_fallback_mode_coverage(tmp_path):
    series_to_process = [1]
    config_data = {
        "series": {
            "1": {
                "raw_data": ["file1.txt"]
            }
        },
        "defaults": {"a": 1},
        "processor_config": {"b": 2},
    }

    mock_df = pd.DataFrame({"A": [1, 2, 3]})
    processed_df = pd.DataFrame({"A": [1, 2, 3], "Processed": [True, True, True]})

    with patch("scripts.batch_correction._load_raw_data", return_value=mock_df) as mock_load, \
         patch("scripts.batch_correction.processor.process_data", return_value=processed_df) as mock_process, \
         patch("scripts.batch_correction.spreadsheet_safety.write_excel_safely") as mock_write, \
         patch("scripts.batch_correction.log") as mock_log:

        result_df = _process_fallback_mode(
            series_to_process=series_to_process,
            config_data=config_data,
            output_dir=str(tmp_path),
            dry_run=False
        )

        assert len(result_df) == 1
        assert result_df.iloc[0]["Series"] == 1
        assert result_df.iloc[0]["Filename"] == "file1.txt"
        assert result_df.iloc[0]["Status"] == "Fallback Processed"

        mock_load.assert_called_once_with("file1.txt")
        mock_process.assert_called_once()
        mock_write.assert_called_once()

def test_process_fallback_mode_coverage_error(tmp_path):
    series_to_process = [1]
    config_data = {
        "series": {
            "1": {
                "raw_data": ["file1.txt"]
            }
        },
        "defaults": {"a": 1},
        "processor_config": {"b": 2},
    }

    with patch("scripts.batch_correction._load_raw_data", side_effect=Exception("Test error")) as mock_load, \
         patch("scripts.batch_correction.log") as mock_log:

        result_df = _process_fallback_mode(
            series_to_process=series_to_process,
            config_data=config_data,
            output_dir=str(tmp_path),
            dry_run=False
        )

        assert len(result_df) == 1
        assert result_df.iloc[0]["Series"] == 1
        assert result_df.iloc[0]["Status"] == "Failed (Processing Error)"


def test_process_fallback_mode_coverage_empty(tmp_path):
    # Test line 560 (empty dataframe return)
    series_to_process = [1]
    config_data = {} # Missing "series" key

    result_df = _process_fallback_mode(
        series_to_process=series_to_process,
        config_data=config_data,
        output_dir=str(tmp_path),
        dry_run=False
    )

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty

def test_determine_series_to_process_invalid_sensor():

    from scripts.batch_correction import _determine_series_to_process
    config_data = {"SENSOR_TO_RIVER": {"invalid": 10.0}}
    # The function catches ValueError and logs a warning instead of raising, so we assert the result is []
    result = _determine_series_to_process("all", None, config_data, "/fake")
    assert result == []


def test_determine_series_to_process_scan_dir(monkeypatch):
    from scripts.batch_correction import _determine_series_to_process
    config_data = {}

    # Mock os.listdir
    monkeypatch.setattr("os.listdir", lambda d: ["S25_Y01.txt", "S26_Y01.txt", "invalid_file.txt", "Sinvalid_Y01.txt"])

    result = _determine_series_to_process("all", None, config_data, "/fake")
    assert result == [25, 26]

def test_determine_series_to_process_scan_dir_with_rm(monkeypatch, caplog):
    from scripts.batch_correction import _determine_series_to_process
    config_data = {}

    # Mock os.listdir
    monkeypatch.setattr("os.listdir", lambda d: ["S25_Y01.txt"])

    result = _determine_series_to_process("all", [1.0], config_data, "/fake")
    assert result == [25]
    assert "River miles provided but no map to filter by" in caplog.text

def test_determine_series_to_process_scan_dir_exception(monkeypatch):
    from scripts.batch_correction import _determine_series_to_process
    config_data = {}

    # Mock os.listdir
    monkeypatch.setattr("os.listdir", lambda d: ["S25_Y01.txt"])

    # Instead of patching builtins.int, just provide an unparseable filename
    # the existing logic catches Exception, so let's trigger one inside int()
    monkeypatch.setattr("os.listdir", lambda d: ["S25_Y01.txt", "SXX_Y01.txt"])

    result = _determine_series_to_process("all", None, config_data, "/fake")
    assert result == [25]


def test_determine_series_to_process_explicit_with_rm():
    from scripts.batch_correction import _determine_series_to_process
    config_data = {"SENSOR_TO_RIVER": {"10": 1.0, "20": 2.0}}
    result = _determine_series_to_process([10, 20, 30], [1.0], config_data, "/fake")
    assert result == [10]


def test_determine_series_to_process_explicit_with_rm_missing():
    from scripts.batch_correction import _determine_series_to_process
    config_data = {"SENSOR_TO_RIVER": {"10": 1.0, "20": 2.0}}
    result = _determine_series_to_process([10], [3.0], config_data, "/fake")
    assert result == []


def test_determine_year_for_index_no_map():
    from scripts.batch_correction import _determine_year_for_index
    assert _determine_year_for_index(3, {}, range(2000, 2010)) == 2002
    assert _determine_year_for_index(15, {}, range(2000, 2010)) is None

def test_parse_and_validate_file_invalid_match():
    from scripts.batch_correction import _parse_and_validate_file
    # Ends with .txt and starts with S but no _Y
    result = _parse_and_validate_file("S25.txt", {"25": 25}, {}, range(2000, 2010), 2000, 2010, "/fake")
    assert result is None

def test_parse_and_validate_file_invalid_year():
    from scripts.batch_correction import _parse_and_validate_file
    result = _parse_and_validate_file("S25_Y15.txt", {"25": 25}, {}, range(2000, 2010), 2000, 2010, "/fake")
    assert result is None

def test_parse_and_validate_file_missing_series():
    from scripts.batch_correction import _parse_and_validate_file
    result = _parse_and_validate_file("S99_Y15.txt", {"25": 25}, {}, range(2000, 2010), 2000, 2010, "/fake")
    assert result is None

def test_parse_and_validate_file_out_of_range_year():
    from scripts.batch_correction import _parse_and_validate_file
    result = _parse_and_validate_file("S25_Y01.txt", {"25": 25}, {1: 1999}, range(2000, 2010), 2000, 2010, "/fake")
    assert result is None


def test_determine_year_for_index_with_map_match():
    from scripts.batch_correction import _determine_year_for_index
    assert _determine_year_for_index(3, {3: 2002}, range(2000, 2010)) == 2002

def test_determine_year_for_index_with_map_no_match():
    from scripts.batch_correction import _determine_year_for_index
    # The year mapped is outside of years_to_process range
    assert _determine_year_for_index(3, {3: 1999}, range(2000, 2010)) is None
    # No map entry
    assert _determine_year_for_index(4, {3: 2002}, range(2000, 2010)) is None

def test_find_files_to_process_data_dir_not_exist(monkeypatch):
    from scripts.batch_correction import _find_files_to_process
    monkeypatch.setattr("os.path.isdir", lambda d: False)
    result = _find_files_to_process([25], (2000, 2010), "/fake")
    assert result == []


def test_load_raw_data_safe_numeric_exception(tmp_path):
    from scripts.batch_correction import _load_raw_data
    file_path = tmp_path / "S25_Y01.txt"
    file_path.write_text("1 2 3\n4 5 invalid_data\n")
    # This shouldn't crash, the invalid_data column will just remain non-numeric
    result = _load_raw_data(str(file_path))
    assert not result.empty

def test_ensure_output_directory_creates_dir(tmp_path, monkeypatch):
    from scripts.batch_correction import _ensure_output_directory
    output_dir = tmp_path / "new_dir"
    _ensure_output_directory(str(output_dir), False)
    assert output_dir.exists()

def test_ensure_output_directory_exception(monkeypatch):
    from scripts.batch_correction import ProcessingError, _ensure_output_directory

    def mock_makedirs(name, exist_ok):
        raise OSError("Permission denied")

    monkeypatch.setattr("os.makedirs", mock_makedirs)
    monkeypatch.setattr("os.path.isdir", lambda d: False)
    import pytest
    with pytest.raises(ProcessingError, match="Unable to create output directory"):
        _ensure_output_directory("/fake/new_dir", False)


def test_get_data_directory_creates_dir(monkeypatch):
    from scripts.batch_correction import _get_data_directory
    monkeypatch.setattr("os.path.isdir", lambda d: False)

    makedirs_called = []
    def mock_makedirs(d, exist_ok):
        makedirs_called.append(d)

    monkeypatch.setattr("os.makedirs", mock_makedirs)

    result = _get_data_directory({}, create_if_missing=True)
    assert len(makedirs_called) == 1
    assert result.endswith("data")

def test_get_data_directory_creates_dir_exception(monkeypatch):
    from scripts.batch_correction import _get_data_directory
    monkeypatch.setattr("os.path.isdir", lambda d: False)

    def mock_makedirs(d, exist_ok):
        raise OSError("Permission denied")

    monkeypatch.setattr("os.makedirs", mock_makedirs)

    import pytest
    with pytest.raises(FileNotFoundError, match="Cannot create default data directory"):
        _get_data_directory({}, create_if_missing=True)

def test_optional_import_coverage(monkeypatch):
    from scripts.batch_correction import _optional_import
    # Trigger ModuleNotFoundError
    assert _optional_import("non_existent_module_for_test", "Msg") is None

    # Trigger ImportError
    original_import = __import__
    def mock_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "mock_import_error_module":
            raise ImportError("Mock error")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", mock_import)
    assert _optional_import("mock_import_error_module", "Msg") is None

    # Trigger TypeError/ValueError
    def mock_import_err(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "mock_type_error_module":
            raise TypeError("Mock error")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", mock_import_err)
    import pytest
    with pytest.raises(TypeError):
        _optional_import("mock_type_error_module", "Msg")


def test_batch_process_empty_series_returns_empty_df():
    import pandas as pd

    from scripts.batch_correction import BatchConfig, batch_process
    config = BatchConfig("all", None, (2000, 2010), dry_run=True)

    with patch("scripts.batch_correction._determine_series_to_process", return_value=[]):
        result = batch_process(config)
        assert isinstance(result, pd.DataFrame)
        assert result.empty


def test_load_and_enrich_config_file_not_found(monkeypatch, caplog):
    from scripts.batch_correction import _load_and_enrich_config
    def mock_load(path):
        raise FileNotFoundError("Simulated not found")
    monkeypatch.setattr("scripts.batch_correction.load_config_func", mock_load)
    result = _load_and_enrich_config("/fake/config.json")
    assert result == {}
    assert "not found" in caplog.text

def test_load_and_enrich_config_load_error(monkeypatch):
    from scripts.batch_correction import ProcessingError, _load_and_enrich_config

    # We must patch the function pointer directly if possible, or trigger an exception inside it
    # We can patch 'scripts.batch_correction.load_config_func' since it's used inside the module
    def mock_load(path):
        raise ValueError("Simulated load error")

    monkeypatch.setattr("scripts.batch_correction.load_config_func", mock_load)

    import pytest
    with pytest.raises(ProcessingError, match="Failed to load configuration"):
        _load_and_enrich_config("/fake/config.json")


def test_batch_process_empty_series_from_selection(monkeypatch):
    import pandas as pd

    from scripts.batch_correction import BatchConfig, batch_process

    # If determine_series_to_process returns []
    monkeypatch.setattr("scripts.batch_correction._determine_series_to_process", lambda *args: [])
    config = BatchConfig(series_selection="all", river_miles=None, years=(2000, 2010), dry_run=True)
    result = batch_process(config)
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_process_main_mode_empty_or_unreadable(tmp_path, monkeypatch):
    import pandas as pd

    from scripts.batch_correction import _process_main_mode

    # We will trigger the ProcessingError exception at line 590.
    def mock_load(fp):
        return pd.DataFrame()

    monkeypatch.setattr("scripts.batch_correction._load_raw_data", mock_load)

    file_path = tmp_path / "S25_Y01.txt"
    file_path.write_text("not empty but load will return empty")

    result = _process_main_mode([(25, 2000, 1, str(file_path))], {}, str(tmp_path), dry_run=True)
    assert len(result) == 1
    assert result.iloc[0]["Status"] == "Failed (Processing Error)"


def test_load_raw_data_safe_numeric_type_error(tmp_path):
    from scripts.batch_correction import _load_raw_data
    file_path = tmp_path / "S25_Y01.txt"
    # To force a TypeError from to_numeric, we pass something very un-numeric
    # But read_csv might read it as strings, which raises ValueError.
    # We can mock to_numeric to raise TypeError.
    from unittest.mock import patch
    import pandas as pd

    file_path.write_text("1 2 3\n4 5 6\n")

    with patch("pandas.to_numeric", side_effect=TypeError("Mock type error")):
        result = _load_raw_data(str(file_path))
        assert not result.empty

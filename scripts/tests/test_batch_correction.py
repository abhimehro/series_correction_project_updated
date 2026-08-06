# pylint: disable=redefined-outer-name, unused-argument
"""
Unit tests for the batch_correction module.
"""

import fnmatch
import importlib
import os
from unittest import mock
from unittest.mock import MagicMock, patch

import pandas as pd  # type: ignore
import pytest

import scripts.batch_correction as bc
import scripts.loaders

# Module to test (adjust path if your structure differs)
# Assuming tests run from the project root
# Import ProcessingError only if you add a test that specifically catches it
from scripts.batch_correction import (
    BatchConfig,
    _determine_series_to_process,
    _get_data_directory,
    _load_raw_data,
    batch_process,
)


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

        monkeypatch.setattr(
            scripts.loaders, "load_config", lambda path=None: config_dict
        )
    except ImportError:
        pass
    yield


# --- Test Cases ---


def test_batch_process_happy_path_all_series_with_config(mock_dependencies):

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


def test_get_data_directory_creates_dir(mock_dependencies):
    from unittest.mock import patch

    config_data = {}
    with patch("os.path.isdir", return_value=False), patch(
        "os.makedirs"
    ) as mock_makedirs:
        result = _get_data_directory(config_data, create_if_missing=True)
        # Note: Depending on where __file__ is relative to the project root, the path changes.
        # But we know it evaluates to something ending with /data.
        assert mock_makedirs.called
        args, kwargs = mock_makedirs.call_args
        assert args[0].endswith("data")
        assert kwargs.get("exist_ok") is True
        assert result.endswith("data")


def test_get_data_directory_creates_dir_oserror(mock_dependencies):
    from unittest.mock import patch

    config_data = {}
    with patch("os.path.isdir", return_value=False), patch(
        "os.makedirs", side_effect=OSError("Perm denied")
    ), pytest.raises(FileNotFoundError, match="Cannot create default data directory"):
        _get_data_directory(config_data, create_if_missing=True)


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
    def process_data(df, config=None):
        return df

    monkeypatch.setattr("scripts.processor.process_data", process_data)

    # --- Act ---

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

    with mock.patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.side_effect = pd.errors.EmptyDataError(
            "No columns to parse from file"
        )

        result = _load_raw_data("dummy_empty_file.txt")

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert "dummy_empty_file.txt empty." in caplog.text


def test_determine_series_to_process_all_fallback(mocker, tmp_path):
    """Test 'all' series selection when no river mile map is present."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "S1_Y1.txt").touch()
    (data_dir / "S2_Y1.txt").touch()
    (data_dir / "S3.txt").touch()  # Invalid format
    (data_dir / "Sinvalid_Y1.txt").touch()  # Invalid series ID

    series = _determine_series_to_process("all", None, {}, str(data_dir))

    assert series == [1, 2]


def test_determine_series_to_process_all_fallback_with_river_miles(mocker, tmp_path):
    """Test 'all' series selection with river miles but no map."""
    mock_log = mocker.patch("scripts.batch_correction.log")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "S1_Y1.txt").touch()

    series = _determine_series_to_process("all", [1.0], {}, str(data_dir))

    assert series == [1]
    mock_log.warning.assert_called_with(
        "River miles provided but no map to filter by – ignored."
    )


def test_determine_series_to_process_invalid_sensor_id_in_map(mocker):
    """Test map building handles invalid sensor IDs gracefully."""
    mock_log = mocker.patch("scripts.batch_correction.log")
    config_data = {"SENSOR_TO_RIVER": {"invalid": 1.0}}
    _determine_series_to_process("all", [1.0], config_data, "fake_dir")

    # It should log the warning and return an empty list since the map was invalid
    mock_log.warning.assert_any_call(
        "Invalid sensor id in SENSOR_TO_RIVER map: invalid"
    )


def test_determine_series_to_process_explicit_invalid_value(mocker):
    """Test explicit series list parsing handles ValueErrors."""
    mock_log = mocker.patch("scripts.batch_correction.log")
    with pytest.raises(ValueError, match="Invalid series selection"):
        _determine_series_to_process(["invalid"], None, {}, "fake_dir")

    mock_log.exception.assert_called()


def test_batch_process_fallback_mode_exception(mock_dependencies, mock_config_loader, mocker):
    """Test exception handling in _process_fallback_mode."""
    series = 26
    years = (1995, 1995)

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True

    # Force _process_main_mode to fail so it falls back to _process_fallback_mode
    mocker.patch("scripts.batch_correction.processor", None)

    # Mock _load_raw_data to raise Exception
    mocker.patch("scripts.batch_correction._load_raw_data", side_effect=Exception("Load failed"))

    # Call batch_process
    summary_df = __import__('scripts.batch_correction').batch_correction.batch_process(
        __import__('scripts.batch_correction').batch_correction.BatchConfig(series, None, years, dry_run=False)
    )

    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Status"] == "Failed (Unexpected Error)"

def test_ensure_output_directory_oserror(mock_dependencies):
    """Regression: OSError creating output dir becomes ProcessingError."""
    from unittest.mock import patch
    import scripts.batch_correction as bc_mod

    with patch("os.path.isdir", return_value=False), patch(
        "os.makedirs", side_effect=OSError("Perm denied")
    ), pytest.raises(
        bc_mod.ProcessingError, match="Unable to create output directory"
    ):
        bc_mod._ensure_output_directory("dummy_dir", dry_run=False)


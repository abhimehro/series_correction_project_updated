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


# --- Fixtures ---


@pytest.fixture(autouse=True)
def mock_read_csv():
    """Mock pandas.read_csv to handle river mile map and sensor data files."""
    def read_csv_side_effect(path, *args, **kwargs):
        fname = os.path.basename(str(path))
        if fname == "river_mile_map.csv":
            return pd.DataFrame(
                {"SENSOR_ID": [26, 27, 28], "RIVER_MILE": [54.0, 53.0, 52.0]}
            )
        else:
            # Simulate sensor data: 5 rows, 2 columns
            return pd.DataFrame({0: range(5), 1: range(5)})

    with mock.patch("pandas.read_csv", side_effect=read_csv_side_effect):
        yield


@pytest.fixture
def mock_config():
    """Standard config for most tests."""
    return {
        "RAW_DATA_DIR": "/fake/data/dir",
        "RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv",
        "RIVER_TO_SENSORS": {54.0: [26], 53.0: [27]},
        "SENSOR_TO_RIVER": {26: 54.0, 27: 53.0},
    }


# --- Helper functions for specific test scenarios ---

def _isfile_side_effect_all_series(path):
    if os.path.basename(path) == "river_mile_map.csv":
        return True
    fname = os.path.basename(path)
    return fname in ["S26_Y01.txt", "S26_Y02.txt", "S27_Y01.txt", "S27_Y02.txt"]


def _isdir_side_effect(path):
    expected_data_dir = "/fake/data/dir"
    output_dir = os.path.join(expected_data_dir, "output")
    return path in [expected_data_dir, output_dir]


def _getsize_side_effect(*args, **kwargs):
    return 100


def _read_csv_side_effect_all_series(path, *args, **kwargs):
    if str(path).endswith("river_mile_map.csv"):
        return pd.DataFrame(
            {"SENSOR_ID": [26, 27, 28], "RIVER_MILE": [54.0, 53.0, 52.0]}
        )
    else:
        return pd.DataFrame({0: range(5), 1: range(5)})


def _isfile_side_effect_specific_series(path):
    fname = os.path.basename(path)
    return fname in ["S30_Y01.txt", "S31_Y01.txt"]


def _isfile_side_effect_data_specific_series(path):
    fname = os.path.basename(path)
    if fname == "river_mile_map.csv":
        return True
    return fname == "S30_Y01.txt"


# --- Test Cases ---


class TestBatchProcessHappyPath:
    """Tests for successful batch processing scenarios."""

    def test_all_series_with_config(self, mock_dependencies, mock_config_loader, mocker):
        expected_data_dir = "/fake/data/dir"

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
        mock_dependencies["isdir"].side_effect = _isdir_side_effect
        mock_dependencies["getsize"].side_effect = _getsize_side_effect

        summary_df = bc.batch_process(
            bc.BatchConfig("all", [54.0, 53.0], (1995, 1996), dry_run=False)
        )

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

    def test_specific_series_no_config(self, mock_dependencies, mock_config_loader, mocker):
        expected_data_dir = "/fake/data/dir"

        mock_dependencies["listdir"].return_value = ["S30_Y01.txt", "S31_Y01.txt"]
        mock_dependencies["isfile"].side_effect = _isfile_side_effect_data_specific_series
        mock_dependencies["isdir"].side_effect = _isdir_side_effect
        mock_dependencies["getsize"].side_effect = _getsize_side_effect

        summary_df = bc.batch_process(
            bc.BatchConfig([30], None, (1995, 1995), dry_run=False)
        )

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

    def test_dry_run(self, mock_dependencies, mock_config_loader, mocker):
        """Test dry run mode - no output files should be written."""
        mock_dependencies["listdir"].return_value = ["S26_Y01.txt", "S27_Y01.txt"]

        def isfile_dry_run(path):
            fname = os.path.basename(path)
            if fname == "river_mile_map.csv":
                return True
            return fname == "S26_Y01.txt"

        mock_dependencies["isfile"].side_effect = isfile_dry_run

        summary_df = batch_process(
            BatchConfig("all", [54.0], (1995, 1995), dry_run=True)
        )

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

    def test_no_files_found(self, mock_dependencies, mock_config_loader):
        """Test scenario where no matching files are found."""
        mock_dependencies["listdir"].return_value = ["some_other_file.txt"]
        mock_dependencies["isfile"].return_value = False

        summary_df = batch_process(
            BatchConfig(99, None, (2000, 2001), dry_run=False)
        )
        assert summary_df.empty

    def test_skip_empty_file(self, mock_dependencies, mock_config_loader, caplog):
        """Test that empty files are skipped."""
        caplog.set_level("INFO")

        def getsize_side_effect(path):
            if path.endswith("S26_Y01.txt"):
                return 0
            return 100

        mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
        mock_dependencies["isfile"].return_value = True
        mock_dependencies["getsize"].side_effect = getsize_side_effect

        summary_df = batch_process(
            BatchConfig(26, None, (1995, 1995), dry_run=False)
        )

        mock_dependencies["to_excel"].assert_not_called()
        assert "Skipping empty file" in caplog.text
        assert summary_df.empty

    def test_with_processor_module(
        self, mock_dependencies, mock_config_loader, mock_processor_mod, mocker
    ):
        """Processor hook runs over the built-in pandas loader (no data_loader module)."""
        mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
        mock_dependencies["isfile"].return_value = True

        summary_df = batch_process(
            BatchConfig(26, None, (1995, 1995), dry_run=False)
        )

        mock_processor_mod.process_data.assert_called_once()
        assert isinstance(summary_df, pd.DataFrame)
        assert len(summary_df) == 1
        assert summary_df["Status"].iloc[0] == "Processed"
        assert summary_df.iloc[0]["Records"] == 5

    def test_load_error(
        self, mock_dependencies, mock_config_loader, caplog, mocker
    ):
        """Test handling of error during built-in data loading."""
        mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
        mock_dependencies["isfile"].return_value = True

        def read_csv_fail_sensor(path, *args, **kwargs):
            if str(path).endswith("river_mile_map.csv"):
                return pd.DataFrame({"SENSOR_ID": [26], "RIVER_MILE": [54.0]})
            raise OSError("Cannot read file")

        mocker.patch("pandas.read_csv", side_effect=read_csv_fail_sensor)

        summary_df = batch_process(
            BatchConfig(26, None, (1995, 1995), dry_run=False)
        )

        assert mock_dependencies["to_excel"].call_count == 0
        assert len(summary_df) == 1
        status = summary_df.iloc[0]["Status"]
        assert "Failed" in status
        assert summary_df.iloc[0]["Records"] == 0
        assert "S26_Y01.txt" in caplog.text

    def test_process_error(
        self, mock_dependencies, mock_config_loader, mock_processor_mod, mocker
    ):
        """Test handling of error during data processing."""
        mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
        mock_dependencies["isfile"].return_value = True
        mock_processor_mod.process_data.side_effect = ValueError("Processing failed")

        summary_df = batch_process(BatchConfig(26, None, (1995, 1995), dry_run=False))

        mock_processor_mod.process_data.assert_called_once()
        assert len(summary_df) == 1
        status = summary_df.iloc[0]["Status"]
        assert status == "Failed (Unexpected Error)"
        assert summary_df.iloc[0]["Records"] == 0
        assert mock_dependencies["to_excel"].call_count == 0

    def test_invalid_series_selection(self, monkeypatch):
        """Test invalid value for series selection."""
        fallback_path = os.path.join(os.getcwd(), "data")
        monkeypatch.setattr(
            "os.path.isdir", lambda d: d in ["/fake/data/dir", fallback_path]
        )
        with pytest.raises(ValueError, match="Invalid series selection"):
            batch_process(
                BatchConfig("invalid-series", None, (2000, 2001), dry_run=False)
            )

    def test_minimal_happy_path(self, monkeypatch):
        """Minimal working happy path test for batch_process."""
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

        config_mock = {
            "RAW_DATA_DIR": data_dir,
            "RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv",
            "RIVER_TO_SENSORS": {54.0: [26]},
            "SENSOR_TO_RIVER": {26: 54.0},
        }
        monkeypatch.setattr("scripts.loaders.load_config", lambda path=None: config_mock)

        def read_csv_side_effect(path, *args, **kwargs):
            if str(path).endswith("river_mile_map.csv"):
                return pd.DataFrame({"SENSOR_ID": [26], "RIVER_MILE": [54.0]})
            else:
                return pd.DataFrame({0: range(5), 1: range(5)})

        monkeypatch.setattr("pandas.read_csv", read_csv_side_effect)
        monkeypatch.setattr("pandas.DataFrame.to_excel", lambda self, path, **kwargs: None)

        def process_data(df, config=None):
            return df

        monkeypatch.setattr("scripts.processor.process_data", process_data)

        importlib.reload(bc)
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

        assert len(summary_df) == 2
        assert all(summary_df["Status"] == "Processed")
        assert set(summary_df["Filename"]) == set(file_list)

    def test_config_not_found(self, mock_dependencies, mock_config_loader, caplog):
        """Test scenario where config file is not found."""
        mock_config_loader.side_effect = FileNotFoundError()

        batch_process(BatchConfig(26, None, (1995, 1995), dry_run=True))

        assert mock_config_loader.called
        warning_logged = any(
            "not found – continuing with empty config." in record.message
            for record in caplog.records
            if record.levelname == "WARNING"
        )
        assert warning_logged

    def test_fallback_mode_exception(self, mock_dependencies, mock_config_loader, mocker):
        """Test exception handling in _process_fallback_mode."""
        mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
        mock_dependencies["isfile"].return_value = True

        mocker.patch("scripts.batch_correction.processor", None)
        mocker.patch("scripts.batch_correction._load_raw_data", side_effect=Exception("Load failed"))

        summary_df = bc.batch_process(
            bc.BatchConfig(26, None, (1995, 1995), dry_run=False)
        )

        assert len(summary_df) == 1
        assert summary_df.iloc[0]["Status"] == "Failed (Unexpected Error)"


class TestGetDataDirectory:
    """Tests for _get_data_directory function."""

    def test_creates_dir(self):
        from unittest.mock import patch

        config_data = {}
        with patch("os.path.isdir", return_value=False), patch(
            "os.makedirs"
        ) as mock_makedirs:
            result = _get_data_directory(config_data, create_if_missing=True)
            assert mock_makedirs.called
            args, kwargs = mock_makedirs.call_args
            assert args[0].endswith("data")
            assert kwargs.get("exist_ok") is True
            assert result.endswith("data")

    def test_creates_dir_oserror(self):
        from unittest.mock import patch

        config_data = {}
        with patch("os.path.isdir", return_value=False), patch(
            "os.makedirs", side_effect=OSError("Perm denied")
        ), pytest.raises(FileNotFoundError, match="Cannot create default data directory"):
            _get_data_directory(config_data, create_if_missing=True)


class TestLoadRawData:
    """Tests for _load_raw_data function."""

    def test_empty_file(self, caplog):
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


class TestDetermineSeriesToProcess:
    """Tests for _determine_series_to_process function."""

    def test_all_fallback(self, mocker, tmp_path):
        """Test 'all' series selection when no river mile map is present."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "S1_Y1.txt").touch()
        (data_dir / "S2_Y1.txt").touch()
        (data_dir / "S3.txt").touch()  # Invalid format
        (data_dir / "Sinvalid_Y1.txt").touch()  # Invalid series ID

        series = _determine_series_to_process("all", None, {}, str(data_dir))

        assert series == [1, 2]

    def test_all_fallback_with_river_miles(self, mocker, tmp_path):
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

    def test_invalid_sensor_id_in_map(self, mocker):
        """Test map building handles invalid sensor IDs gracefully."""
        mock_log = mocker.patch("scripts.batch_correction.log")
        config_data = {"SENSOR_TO_RIVER": {"invalid": 1.0}}
        _determine_series_to_process("all", [1.0], config_data, "fake_dir")

        mock_log.warning.assert_any_call(
            "Invalid sensor id in SENSOR_TO_RIVER map: invalid"
        )

    def test_explicit_invalid_value(self, mocker):
        """Test explicit series list parsing handles ValueErrors."""
        mock_log = mocker.patch("scripts.batch_correction.log")
        with pytest.raises(ValueError, match="Invalid series selection"):
            _determine_series_to_process(["invalid"], None, {}, "fake_dir")

        mock_log.exception.assert_called()


class TestEnsureOutputDirectory:
    """Tests for _ensure_output_directory function."""

    def test_oserror(self, mock_dependencies):
        """Regression: OSError creating output dir becomes ProcessingError."""
        from unittest.mock import patch

        with patch("os.path.isdir", return_value=False), patch(
            "os.makedirs", side_effect=OSError("Perm denied")
        ), pytest.raises(
            bc.ProcessingError, match="Unable to create output directory"
        ):
            bc._ensure_output_directory("dummy_dir", dry_run=False)

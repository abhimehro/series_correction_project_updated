# pylint: disable=redefined-outer-name, unused-argument
"""
Unit tests for batch_correction fallback mode and year-index helpers.
"""

from unittest.mock import MagicMock, patch

import pandas as pd  # type: ignore
import pytest

from scripts.batch_correction import (
    _determine_series_to_process,
    _determine_year_for_index,
    _find_files_to_process,
    _process_fallback_mode,
    _resolve_base_year,
)


# Shared patcher bodies live in conftest; these wrappers keep them autouse
# for this module only.
@pytest.fixture(autouse=True)
def patch_pd_read_csv(pd_read_csv_patcher):
    yield


@pytest.fixture(autouse=True)
def patch_load_config(load_config_patcher):
    yield


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


def test_batch_process_fallback_mode_exception(
    mock_dependencies, mock_config_loader, mocker
):
    """Test exception handling in _process_fallback_mode."""
    series = 26
    years = (1995, 1995)

    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True

    # Force _process_main_mode to fail so it falls back to _process_fallback_mode
    mocker.patch("scripts.batch_correction.processor", None)

    # Mock _load_raw_data to raise Exception
    mocker.patch(
        "scripts.batch_correction._load_raw_data", side_effect=Exception("Load failed")
    )

    # Call batch_process
    summary_df = __import__("scripts.batch_correction").batch_correction.batch_process(
        __import__("scripts.batch_correction").batch_correction.BatchConfig(
            series, None, years, dry_run=False
        )
    )

    assert len(summary_df) == 1
    assert summary_df.iloc[0]["Status"] == "Failed (Unexpected Error)"


def test_year_index():
    """Map default and explicit year indexes."""
    assert _determine_year_for_index(1, {}, 1995) == 1995
    assert _determine_year_for_index(6, {}, 1995) == 2000
    assert _determine_year_for_index(3, {3: 2001}, 1995) == 2001
    assert _determine_year_for_index(4, {3: 2001}, 1995) is None


def test_files_default_year(tmp_path):
    """Find files using the default Y01 base year."""
    for y_index in (1, 6, 7, 8):
        (tmp_path / f"S26_Y{y_index:02d}.txt").touch()

    files = _find_files_to_process([26], (2000, 2001), str(tmp_path), {})

    assert files == [
        (26, 2000, 6, str(tmp_path / "S26_Y06.txt")),
        (26, 2001, 7, str(tmp_path / "S26_Y07.txt")),
    ]


def test_files_config_year(tmp_path):
    """Find files using a configured base year."""
    for y_index in (1, 2):
        (tmp_path / f"S26_Y{y_index:02d}.txt").touch()

    files = _find_files_to_process(
        [26], (2000, 2000), str(tmp_path), {"base_year": 2000}
    )

    assert files == [(26, 2000, 1, str(tmp_path / "S26_Y01.txt"))]


def test_resolve_year_map():
    """Use the default base year when an index map is configured."""
    assert _resolve_base_year({"year_index_map": {"1995": 1}}, {"1995": 1}) == 1995


def test_resolve_year_default_warning(caplog):
    """Warn when fallback processing uses the default base year."""
    assert _resolve_base_year({}, {}) == 1995
    assert "No 'year_index_map' or 'base_year' in config" in caplog.text


def test_resolve_year_no_config():
    """Use the default base year when configuration is unavailable."""
    assert _resolve_base_year(None, {}) == 1995


def _run_fallback(
    tmp_path, raw_files, dry_run=False, process_side_effect=None, years=(1900, 2100)
):
    """Run _process_fallback_mode with a stub processor and data loader."""
    mock_processor = MagicMock()
    if process_side_effect is not None:
        mock_processor.process_data.side_effect = process_side_effect
    else:
        mock_processor.process_data.return_value = pd.DataFrame({"value": [1]})

    with patch("scripts.batch_correction.processor", mock_processor), patch(
        "scripts.batch_correction._load_raw_data",
        return_value=pd.DataFrame({"value": [1]}),
    ), patch("scripts.batch_correction.spreadsheet_safety.write_excel_safely") as write:
        summary = _process_fallback_mode(
            [26],
            {"series": {"26": {"raw_data": raw_files}}},
            str(tmp_path),
            dry_run,
            years,
        )
    return summary, write


def test_fallback_y_indices(tmp_path):
    """Use filename indexes for fallback summaries and output files."""
    summary, write = _run_fallback(tmp_path, ["data/S26_Y06.txt", "data/S26_Y08.txt"])

    assert summary["Y-Index"].tolist() == [6, 8]
    assert [call.args[1] for call in write.call_args_list] == [
        str(tmp_path / "Series26_File06_Processed.xlsx"),
        str(tmp_path / "Series26_File08_Processed.xlsx"),
    ]


def test_fallback_name_collision(tmp_path):
    """Separate positional and Y-index output names for mixed inputs."""
    summary, write = _run_fallback(tmp_path, ["data/custom.txt", "data/S26_Y01.txt"])

    y_indexes = summary["Y-Index"].tolist()
    assert pd.isna(y_indexes[0]) and y_indexes[1] == 1
    assert [call.args[1] for call in write.call_args_list] == [
        str(tmp_path / "Series26_Pos01_Processed.xlsx"),
        str(tmp_path / "Series26_File01_Processed.xlsx"),
    ]


def test_fallback_dry_run(tmp_path):
    """Avoid writing processed fallback files during a dry run."""
    summary, write = _run_fallback(tmp_path, ["data/S26_Y01.txt"], dry_run=True)

    assert summary["Y-Index"].tolist() == [1]
    write.assert_not_called()


def test_fallback_empty_data(tmp_path):
    """Skip fallback inputs whose loader returns no rows."""
    with patch("scripts.batch_correction.processor", MagicMock()), patch(
        "scripts.batch_correction._load_raw_data", return_value=pd.DataFrame()
    ):
        summary = _process_fallback_mode(
            [26],
            {"series": {"26": {"raw_data": ["data/S26_Y01.txt"]}}},
            str(tmp_path),
            False,
            (1995, 1995),
        )

    assert summary.empty


def test_fallback_error_index(tmp_path):
    """Keep filename indexes in fallback failure records."""
    summary, _ = _run_fallback(
        tmp_path,
        ["data/S26_Y08.txt"],
        process_side_effect=ValueError("processing failed"),
    )

    assert summary["Y-Index"].tolist() == [8]


def test_fallback_filters_by_requested_year(tmp_path):
    """Y-indexed fallback inputs outside the requested years are skipped."""
    summary, write = _run_fallback(
        tmp_path,
        ["data/S26_Y01.txt", "data/S26_Y02.txt", "data/S26_Y03.txt"],
        years=(1996, 1996),
    )

    assert summary["Y-Index"].tolist() == [2]
    assert [call.args[1] for call in write.call_args_list] == [
        str(tmp_path / "Series26_File02_Processed.xlsx")
    ]


def test_fallback_keeps_unindexed_files_when_filtering_years(tmp_path):
    """Nonconventional names have no derivable year and stay unfiltered."""
    summary, _ = _run_fallback(tmp_path, ["data/custom.txt"], years=(1996, 1996))

    assert summary["Filename"].tolist() == ["custom.txt"]

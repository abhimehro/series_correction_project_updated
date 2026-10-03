import logging
from unittest.mock import MagicMock, patch

from scripts.apply_refined_corrections import (
    load_raw_dataframes,
    load_identified_outliers,
    apply_level_shift_correction,
)


def test_load_raw_dataframes_handles_and_logs_exception(caplog):
    raw_file_map = {"S26": {1: "non_existent_file_path.txt"}}
    with caplog.at_level(logging.ERROR):
        result = load_raw_dataframes(raw_file_map)
    assert result == {}
    assert "Failed to load raw data file non_existent_file_path.txt" in caplog.text


def test_load_identified_outliers_handles_and_logs_exception(caplog):
    with caplog.at_level(logging.ERROR):
        with patch("pandas.read_csv", side_effect=ValueError("Corrupted CSV")):
            df = load_identified_outliers("invalid_path.csv")
    assert df.empty
    assert (
        "An unexpected error occurred while loading outliers from invalid_path.csv"
        in caplog.text
    )


def test_apply_level_shift_correction_handles_missing_raw_dataframe():
    raw_file_map = {"S26": {1: "file1.txt", 2: "file2.txt"}}
    raw_dataframes = {}  # Empty, file missing from raw_dataframes
    outlier_info = ("1995 (Y01) to 1996 (Y02)", "Sensor 1", 0.5)

    result = apply_level_shift_correction(outlier_info, raw_file_map, raw_dataframes)
    assert result is None


def test_apply_level_shift_correction_logs_unexpected_exception(caplog):
    raw_file_map = {"S26": {1: "file1.txt", 2: "file2.txt"}}
    mock_df1 = MagicMock()
    mock_df2 = MagicMock()
    raw_dataframes = {"file1.txt": mock_df1, "file2.txt": mock_df2}
    outlier_info = ("1995 (Y01) to 1996 (Y02)", "Sensor 1", 0.5)

    with caplog.at_level(logging.ERROR):
        with patch(
            "scripts.apply_refined_corrections._calculate_and_apply_shift",
            side_effect=RuntimeError("Unexpected error"),
        ):
            result = apply_level_shift_correction(
                outlier_info, raw_file_map, raw_dataframes
            )

    assert result is None
    assert "An unexpected error occurred while processing outlier" in caplog.text

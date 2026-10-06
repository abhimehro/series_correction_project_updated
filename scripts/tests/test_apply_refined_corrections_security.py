import logging
from unittest.mock import patch

from scripts.apply_refined_corrections import (
    apply_level_shift_correction,
    load_identified_outliers,
)


def test_apply_level_shift_correction_exception_logs_exception(caplog, capsys):
    """Verify that apply_level_shift_correction logs the exception details via log.exception while stdout remains safe."""
    outlier_info = ("1995 (Y01) to 1996 (Y02)", "Sensor 1", 0.5)
    raw_file_map = {"S26": {1: "S26_Y01.txt", 2: "S26_Y02.txt"}}
    # Passing a raw_dataframes dictionary where getting item raises an unexpected exception
    raw_dataframes = {}

    with patch.dict(raw_dataframes, {}, clear=True):
        # We can mock _calculate_and_apply_shift or cause an exception during lookup
        with patch(
            "scripts.apply_refined_corrections._calculate_and_apply_shift",
            side_effect=Exception("Internal processing memory error"),
        ):
            # Fill raw_dataframes so it reaches _calculate_and_apply_shift
            valid_dfs = {"S26_Y01.txt": "dummy1", "S26_Y02.txt": "dummy2"}
            with caplog.at_level(logging.ERROR):
                result = apply_level_shift_correction(
                    outlier_info, raw_file_map, valid_dfs
                )

    assert result is None
    captured = capsys.readouterr()
    assert (
        "An unexpected error occurred while processing outlier 1995 (Y01) to 1996 (Y02), Sensor 1."
        in captured.out
    )
    assert "Internal processing memory error" not in captured.out

    (record,) = caplog.records
    assert record.name == "scripts.apply_refined_corrections"
    assert record.levelno == logging.ERROR
    assert (
        "An unexpected error occurred while processing outlier" in record.getMessage()
    )
    assert record.exc_info is not None
    assert str(record.exc_info[1]) == "Internal processing memory error"


@patch("scripts.apply_refined_corrections.pd.read_csv")
def test_load_identified_outliers_logs_exception(mock_read_csv, caplog, capsys):
    """Verify that load_identified_outliers logs exception details via log.exception."""
    error = Exception("Sensitive filesystem access exception")
    mock_read_csv.side_effect = error

    with caplog.at_level(logging.ERROR):
        df = load_identified_outliers("dummy_path.csv")

    assert df.empty
    output = capsys.readouterr().out
    assert "An unexpected error occurred while loading outliers." in output
    assert "Sensitive filesystem access exception" not in output

    (record,) = caplog.records
    assert record.name == "scripts.apply_refined_corrections"
    assert record.levelno == logging.ERROR
    assert "An unexpected error occurred while loading outliers" in record.getMessage()
    assert record.exc_info is not None
    assert record.exc_info[1] is error

"""Security tests for scripts/export_comparison_sheets.py logging observability (CWE-209)."""

import logging
from unittest.mock import patch

from scripts.export_comparison_sheets import load_processed_file, load_raw_file


@patch("scripts.export_comparison_sheets.read_csv")
def test_load_raw_file_unexpected_exception_logs_exception(
    mock_read_csv, caplog, capsys
):
    """Verify that an unexpected exception during raw file loading triggers log.exception."""
    error = RuntimeError("Raw file processing failure")
    mock_read_csv.side_effect = error

    with caplog.at_level(logging.ERROR):
        result = load_raw_file("dummy_raw.txt")

    assert result is None
    matched = [
        r
        for r in caplog.records
        if "Unexpected error loading raw file dummy_raw.txt" in r.getMessage()
    ]
    assert matched, "expected unexpected error to be logged"
    for record in matched:
        assert record.exc_info is not None, "log.exception must attach a traceback"
        assert record.exc_info[1] is error

    captured = capsys.readouterr()
    assert "[WARN] Unexpected error loading raw file dummy_raw.txt" in captured.out
    assert str(error) not in captured.out


@patch("scripts.export_comparison_sheets.read_excel")
def test_load_processed_file_unexpected_exception_logs_exception(
    mock_read_excel, caplog, capsys
):
    """Verify that an unexpected exception during processed file loading triggers log.exception."""
    error = RuntimeError("Excel workbook parsing failure")
    mock_read_excel.side_effect = error

    with caplog.at_level(logging.ERROR):
        result = load_processed_file("dummy_proc.xlsx")

    assert result is None
    matched = [
        r
        for r in caplog.records
        if "Unexpected error loading processed file dummy_proc.xlsx" in r.getMessage()
    ]
    assert matched, "expected unexpected error to be logged"
    for record in matched:
        assert record.exc_info is not None, "log.exception must attach a traceback"
        assert record.exc_info[1] is error

    captured = capsys.readouterr()
    assert (
        "[WARN] Unexpected error loading processed file dummy_proc.xlsx" in captured.out
    )
    assert str(error) not in captured.out

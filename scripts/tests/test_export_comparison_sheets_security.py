"""Security tests for scripts/export_comparison_sheets.py logging observability (CWE-209)."""

from unittest.mock import patch

from scripts.export_comparison_sheets import load_processed_file, load_raw_file


@patch("scripts.export_comparison_sheets.read_csv")
@patch("scripts.export_comparison_sheets.log.exception")
def test_load_raw_file_generic_exception_logs_exception(
    mock_log_exception, mock_read_csv, capsys
):
    """Verify that a generic exception during raw file loading triggers log.exception."""
    mock_read_csv.side_effect = RuntimeError("Raw file parse failure")

    result = load_raw_file("dummy_raw.txt")

    assert result is None
    mock_log_exception.assert_called_once()
    assert "dummy_raw.txt" in mock_log_exception.call_args[0][1]

    captured = capsys.readouterr()
    assert "[WARN] Unexpected error loading raw file dummy_raw.txt" in captured.out
    assert "RuntimeError" not in captured.out
    assert "Raw file parse failure" not in captured.out


@patch("scripts.export_comparison_sheets.read_excel")
@patch("scripts.export_comparison_sheets.log.exception")
def test_load_processed_file_generic_exception_logs_exception(
    mock_log_exception, mock_read_excel, capsys
):
    """Verify that a generic exception during processed file loading triggers log.exception."""
    mock_read_excel.side_effect = RuntimeError("Excel corrupt")

    result = load_processed_file("dummy_proc.xlsx")

    assert result is None
    mock_log_exception.assert_called_once()
    assert "dummy_proc.xlsx" in mock_log_exception.call_args[0][1]

    captured = capsys.readouterr()
    assert (
        "[WARN] Unexpected error loading processed file dummy_proc.xlsx" in captured.out
    )
    assert "RuntimeError" not in captured.out
    assert "Excel corrupt" not in captured.out

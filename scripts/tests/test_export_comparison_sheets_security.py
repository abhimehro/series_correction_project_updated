from unittest.mock import patch

from scripts.export_comparison_sheets import load_processed_file, load_raw_file


@patch("scripts.export_comparison_sheets.read_csv")
@patch("scripts.export_comparison_sheets.log.exception")
def test_load_raw_file_unexpected_exception_logs_exception(
    mock_log_exception, mock_read_csv, capsys
):
    """Verify that unexpected exception during load_raw_file logs exception and prints warning."""
    mock_read_csv.side_effect = RuntimeError("Unexpected raw file error")

    result = load_raw_file("dummy_raw.txt")

    assert result is None
    mock_log_exception.assert_called_once_with(
        "Unexpected error loading raw file %s", "dummy_raw.txt"
    )
    captured = capsys.readouterr()
    assert "[WARN] Unexpected error loading raw file dummy_raw.txt" in captured.out


@patch("scripts.export_comparison_sheets.read_excel")
@patch("scripts.export_comparison_sheets.log.exception")
def test_load_processed_file_unexpected_exception_logs_exception(
    mock_log_exception, mock_read_excel, capsys
):
    """Verify that unexpected exception during load_processed_file logs exception and prints warning."""
    mock_read_excel.side_effect = RuntimeError("Unexpected processed file error")

    result = load_processed_file("dummy_proc.xlsx")

    assert result is None
    mock_log_exception.assert_called_once_with(
        "Unexpected error loading processed file %s", "dummy_proc.xlsx"
    )
    captured = capsys.readouterr()
    assert (
        "[WARN] Unexpected error loading processed file dummy_proc.xlsx" in captured.out
    )

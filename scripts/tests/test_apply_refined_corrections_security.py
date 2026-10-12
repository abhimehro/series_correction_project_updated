"""Security tests for scripts/apply_refined_corrections.py logging observability (CWE-209)."""

from unittest.mock import patch

from scripts.apply_refined_corrections import load_identified_outliers


@patch("scripts.apply_refined_corrections.pd.read_csv")
@patch("scripts.apply_refined_corrections.logging.exception")
def test_load_identified_outliers_generic_exception_logs_exception(
    mock_log_exception, mock_read_csv, capsys
):
    """Verify that a generic exception during outlier loading triggers logging.exception."""
    mock_read_csv.side_effect = RuntimeError("Database read failure")

    result = load_identified_outliers("dummy_path.csv")

    assert result.empty
    mock_log_exception.assert_called_once()
    assert "dummy_path.csv" in mock_log_exception.call_args[0][1]

    captured = capsys.readouterr()
    assert "An unexpected error occurred while loading outliers." in captured.out
    assert "RuntimeError" not in captured.out
    assert "Database read failure" not in captured.out

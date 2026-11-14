"""Security unit tests for export_comparison_sheets logging and exception handling."""

from unittest.mock import patch

from scripts.export_comparison_sheets import load_processed_file, load_raw_file


def test_load_raw_file_oserror_logs_warning(caplog):
    """Verify load_raw_file logs a warning without leaking stack trace on expected OSError."""
    with caplog.at_level("WARNING"):
        res = load_raw_file("nonexistent_file.txt")
        assert res is None
        assert "Could not load raw file" in caplog.text


def test_load_raw_file_generic_exception_logs_exception(caplog):
    """Verify load_raw_file logs full exception traceback for unexpected errors."""
    with patch(
        "scripts.export_comparison_sheets.read_csv",
        side_effect=RuntimeError("unexpected DB fail"),
    ):
        with caplog.at_level("ERROR"):
            res = load_raw_file("dummy.txt")
            assert res is None
            assert "Unexpected error loading raw file" in caplog.text
            assert "RuntimeError: unexpected DB fail" in caplog.text


def test_load_processed_file_oserror_logs_warning(caplog):
    """Verify load_processed_file logs a warning on OSError/ValueError."""
    with caplog.at_level("WARNING"):
        res = load_processed_file("nonexistent_file.xlsx")
        assert res is None
        assert "Could not load processed file" in caplog.text


def test_load_processed_file_generic_exception_logs_exception(caplog):
    """Verify load_processed_file logs full exception traceback for unexpected errors."""
    with patch(
        "scripts.export_comparison_sheets.read_excel",
        side_effect=KeyError("Corrupted sheet"),
    ):
        with caplog.at_level("ERROR"):
            res = load_processed_file("corrupted.xlsx")
            assert res is None
            assert "Unexpected error loading processed file" in caplog.text
            assert "KeyError: 'Corrupted sheet'" in caplog.text

import logging
from unittest.mock import patch

from scripts.generate_overview_table import main


@patch("scripts.generate_overview_table.pd.read_csv")
def test_main_generic_exception_logs_exception(mock_read_csv, caplog, capsys):
    """Tests that generic exceptions are logged with log.exception while preserving generic output."""
    mock_read_csv.side_effect = Exception(
        "Sensitive internal database connection error"
    )

    with caplog.at_level(logging.ERROR):
        main("dummy_log.csv", "dummy_avg.csv")

    captured = capsys.readouterr()
    output = captured.out

    # Verify generic user output
    assert "An error occurred while generating Overview table content." in output

    # Verify the exception was logged together with its traceback
    matched = [
        record
        for record in caplog.records
        if "Error generating Overview table content" in record.getMessage()
    ]
    assert matched, "expected the failure to be logged"
    for record in matched:
        assert record.exc_info is not None, "log.exception must attach a traceback"
        assert "Sensitive internal database connection error" in str(record.exc_info[1])

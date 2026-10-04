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

    # Verify exception was captured in log
    assert any(
        "Error generating Overview table content" in record.message
        for record in caplog.records
    )
    assert any(
        "Sensitive internal database connection error" in record.message
        or (
            record.exc_info
            and "Sensitive internal database connection error"
            in str(record.exc_info[1])
        )
        for record in caplog.records
    )

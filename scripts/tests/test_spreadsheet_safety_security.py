"""Security tests for spreadsheet formula injection protection (CWE-1236)."""

import csv
import pandas as pd
import pytest
from openpyxl import load_workbook

from scripts.spreadsheet_safety import (
    escape_spreadsheet_formula,
    sanitize_dataframe_for_spreadsheet,
    write_csv_safely,
    write_excel_safely,
)


@pytest.mark.parametrize(
    "payload,expected",
    [
        ("|cmd", "'|cmd"),
        ("%1+1", "'%1+1"),
        ("\t|calc", "'\t|calc"),
        ("  %test", "'  %test"),
        ("\uff5ccmd", "'\uff5ccmd"),
        ("\uff05cmd", "'\uff05cmd"),
    ],
)
def test_escape_spreadsheet_formula_pipe_and_percent(payload, expected):
    """Test that pipe and percent formula/DDE initiators are neutralized."""
    assert escape_spreadsheet_formula(payload) == expected


def test_sanitize_dataframe_pipe_and_percent():
    """Verify DataFrame cell and column sanitization for pipe and percent payloads."""
    df = pd.DataFrame({"|col": ["%payload", "safe"]})
    sanitized = sanitize_dataframe_for_spreadsheet(df)
    assert list(sanitized.columns) == ["'|col"]
    assert sanitized["'|col"].iloc[0] == "'%payload"
    assert sanitized["'|col"].iloc[1] == "safe"


def test_write_csv_safely_pipe_and_percent(tmp_path):
    """Verify write_csv_safely neutralizes pipe and percent payloads."""
    df = pd.DataFrame({"val": ["|cmd", "%cmd", "\uff5ccmd", "\uff05cmd"]})
    out_path = tmp_path / "out.csv"
    write_csv_safely(df, out_path, index=False)

    with open(out_path, newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["val"]
    for row in rows[1:]:
        assert row[0].startswith("'")


def test_write_excel_safely_pipe_and_percent(tmp_path):
    """Verify write_excel_safely neutralizes pipe and percent payloads."""
    df = pd.DataFrame({"val": ["|cmd", "%cmd", "\uff5ccmd", "\uff05cmd"]})
    out_path = tmp_path / "out.xlsx"
    write_excel_safely(df, out_path, index=False)

    workbook = load_workbook(out_path, data_only=False)
    for row in workbook.active.iter_rows(min_row=2, max_col=1):
        cell = row[0]
        assert cell.data_type == "s"
        assert cell.value.startswith("'")

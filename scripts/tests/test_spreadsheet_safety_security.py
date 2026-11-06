"""Security unit tests for spreadsheet safety enhancements (CWE-1236)."""

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
        ("|cmd|'/C calc'!A0", "'|cmd|'/C calc'!A0"),
        ("%0A=cmd", "'%0A=cmd"),
        ("%0D+cmd", "'%0D+cmd"),
        ("\t|cmd", "'\t|cmd"),
        ("  %calc", "'  %calc"),
        ("\r\n|calc", "'\r\n|calc"),
        ("｜cmd", "'｜cmd"),  # full-width vertical bar (\uff5c)
        ("％cmd", "'％cmd"),  # full-width percent sign (\uff05)
    ],
)
def test_escape_spreadsheet_formula_pipe_and_percent(payload, expected):
    """Ensure pipe and percent command injection payloads are neutralized."""
    assert escape_spreadsheet_formula(payload) == expected


def test_sanitize_dataframe_neutralizes_pipe_and_percent():
    """Ensure DataFrames with pipe and percent payloads are neutralized."""
    df = pd.DataFrame(
        {
            "|header": ["|cmd /C calc", "%0A=1+1"],
            "%header": ["safe", "｜calc"],
        },
        index=pd.Index(["|idx", "%idx"], name="|name"),
    )
    sanitized = sanitize_dataframe_for_spreadsheet(df)
    assert list(sanitized.columns) == ["'|header", "'%header"]
    assert list(sanitized.index) == ["'|idx", "'%idx"]
    assert sanitized.index.name == "'|name"
    assert sanitized["'|header"].tolist() == ["'|cmd /C calc", "'%0A=1+1"]
    assert sanitized["'%header"].tolist() == ["safe", "'｜calc"]


def test_write_csv_safely_neutralizes_pipe_and_percent(tmp_path):
    """Ensure CSV exports neutralize pipe and percent formula prefixes."""
    payloads = ["|cmd", "%test", "  |calc", "\t%calc", "safe"]
    df = pd.DataFrame({"value": payloads})
    out_path = tmp_path / "test_out.csv"
    write_csv_safely(df, out_path, index=False)

    with open(out_path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["value"]
    for row in rows[1:]:
        val = row[0]
        assert not val.startswith(("|", "%", " |", " %", "\t|", "\t%"))
        assert len(row) == 1


def test_write_excel_safely_neutralizes_pipe_and_percent(tmp_path):
    """Ensure Excel exports neutralize pipe and percent formula prefixes."""
    payloads = ["|cmd", "%test", "  |calc", "\t%calc", "safe"]
    df = pd.DataFrame({"value": payloads})
    out_path = tmp_path / "test_out.xlsx"
    write_excel_safely(df, out_path, index=False)

    wb = load_workbook(out_path, data_only=False)
    for row in wb.active.iter_rows(min_row=2, max_col=1):
        cell = row[0]
        assert cell.data_type == "s"
        assert not str(cell.value).startswith(("|", "%"))

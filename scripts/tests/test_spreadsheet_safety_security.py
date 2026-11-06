"""Security tests for spreadsheet formula injection protection (CWE-1236).

Verifies neutralization of pipe (|), percent (%), and their full-width
Unicode equivalents (\\uff5c, \\uff05).
"""

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
        ("  |cmd", "'  |cmd"),
        ("\t|cmd", "'\t|cmd"),
        ("%cmd", "'%cmd"),
        ("  %cmd", "'  %cmd"),
        ("\uff5ccmd", "'\uff5ccmd"),
        ("\uff05cmd", "'\uff05cmd"),
        ("'|cmd", "'|cmd"),
        ("'%cmd", "'%cmd"),
        ("normal|text", "normal|text"),
        ("100%", "100%"),
    ],
)
def test_escape_spreadsheet_formula_pipe_and_percent(payload, expected):
    """Ensure pipe and percent formula initiators are neutralized."""
    assert escape_spreadsheet_formula(payload) == expected


def test_sanitize_dataframe_pipe_and_percent():
    """Ensure pipe and percent prefixes are neutralized across columns and rows."""
    df = pd.DataFrame(
        {"|col": ["%val", "\uff5cval"], "%col2": ["\uff05val", "safe"]},
        index=pd.Index(["|idx1", "%idx2"], name="|idx_name"),
    )
    sanitized = sanitize_dataframe_for_spreadsheet(df)

    assert list(sanitized.columns) == ["'|col", "'%col2"]
    assert list(sanitized.index) == ["'|idx1", "'%idx2"]
    assert sanitized.index.name == "'|idx_name"
    assert sanitized["'|col"].tolist() == ["'%val", "'\uff5cval"]
    assert sanitized["'%col2"].tolist() == ["'\uff05val", "safe"]


def test_write_excel_safely_pipe_and_percent(tmp_path):
    """Ensure Excel export neutralizes pipe and percent payloads."""
    payloads = ["|cmd", "%cmd", "\uff5ccmd", "\uff05cmd"]
    df = pd.DataFrame({"cmd": payloads})
    out_path = tmp_path / "test_sec.xlsx"

    write_excel_safely(df, out_path, index=False)

    wb = load_workbook(out_path, data_only=False)
    for row in wb.active.iter_rows(min_row=2, max_col=1):
        val = row[0].value
        assert val.startswith("'")
        assert not val.startswith(("|", "%", "\uff5c", "\uff05"))


def test_write_csv_safely_pipe_and_percent(tmp_path):
    """Ensure CSV export neutralizes pipe and percent payloads."""
    payloads = ["|cmd", "%cmd", "\uff5ccmd", "\uff05cmd"]
    df = pd.DataFrame({"cmd": payloads})
    out_path = tmp_path / "test_sec.csv"

    write_csv_safely(df, out_path, index=False)

    with open(out_path, newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["cmd"]
    for row in rows[1:]:
        assert row[0].startswith("'")

"""Security tests for spreadsheet formula injection protection (CWE-1236)."""

import pandas as pd
import pytest

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
        ("%test", "'%test"),
        ("｜cmd", "'｜cmd"),
        ("％cmd", "'％cmd"),
        ("  |cmd", "'  |cmd"),
        ("  %test", "'  %test"),
        ("pipe|inside", "pipe|inside"),
        ("percent%inside", "percent%inside"),
    ],
)
def test_escape_spreadsheet_formula_pipe_and_percent(payload, expected):
    """Test that pipe and percent formula/DDE initiators are neutralized."""
    assert escape_spreadsheet_formula(payload) == expected


def test_sanitize_dataframe_escapes_pipe_and_percent():
    """Verify DataFrame cell and column sanitization for pipe and percent payloads."""
    df = pd.DataFrame(
        {
            "|col": ["|cmd", "%test"],
            "normal": ["｜cmd", "％cmd"],
        }
    )
    sanitized = sanitize_dataframe_for_spreadsheet(df)
    assert list(sanitized.columns) == ["'|col", "normal"]
    assert sanitized["'|col"].tolist() == ["'|cmd", "'%test"]
    assert sanitized["normal"].tolist() == ["'｜cmd", "'％cmd"]


def test_write_csv_safely_pipe_and_percent():
    """Verify write_csv_safely neutralizes pipe and percent payloads."""
    df = pd.DataFrame({"cmd": ["|1+1", "%2+2"]})
    output = write_csv_safely(df, index=False)
    lines = [line.strip() for line in output.strip().splitlines()]
    assert lines[0] == "cmd"
    assert lines[1] == "'|1+1"
    assert lines[2] == "'%2+2"


def test_write_excel_safely_pipe_and_percent(tmp_path):
    """Verify write_excel_safely neutralizes pipe and percent payloads."""
    from openpyxl import load_workbook

    df = pd.DataFrame({"cmd": ["|1+1", "%2+2"]})
    out_path = tmp_path / "out_security.xlsx"
    write_excel_safely(df, out_path, index=False)

    wb = load_workbook(out_path, data_only=False)
    ws = wb.active
    assert ws["A2"].value == "'|1+1"
    assert ws["A3"].value == "'%2+2"

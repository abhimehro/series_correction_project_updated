import pytest

from scripts.spreadsheet_safety import escape_spreadsheet_formula


@pytest.mark.parametrize(
    "payload,expected",
    [
        ("|cmd", "'|cmd"),
        ("%cmd", "'%cmd"),
        ("  |cmd", "'  |cmd"),
        ("  %cmd", "'  %cmd"),
        ("\t|cmd", "'\t|cmd"),
        ("\t%cmd", "'\t%cmd"),
        ("\uff5ccmd", "'\uff5ccmd"),
        ("\uff05cmd", "'\uff05cmd"),
        ("safe|test", "safe|test"),
        ("100%", "100%"),
    ],
)
def test_escape_pipe_and_percent_formula_prefixes(payload, expected):
    """Ensure pipe and percent prefixes (and full-width variants) are neutralized."""
    assert escape_spreadsheet_formula(payload) == expected

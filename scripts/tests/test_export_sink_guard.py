"""
Sink guard: production code must route spreadsheet/CSV exports through
scripts.spreadsheet_safety.  Any direct DataFrame.to_csv/to_excel, csv.writer,
ExcelWriter, xlsxwriter, or unreviewed openpyxl cell writes must fail CI.
"""

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "venv",
    "env",
    "__pycache__",
    "build",
    "dist",
    ".pytest_cache",
    "*.egg-info",
    "scripts/tests",
    "tests",
}

# Files outside tests that are allowed to mention restricted libraries because
# they have been reviewed and do not write attacker-controlled cell values.
ALLOWED_OPENPYXL_FILES = {
    "generate_summary.py",  # loads workbook, styles/chart/saves; no cell.value writes
    "setup.py",  # dependency declaration only
}

SINK_PATTERNS = {
    "direct_to_csv": re.compile(r"\.\s*to_csv\s*\("),
    "direct_to_excel": re.compile(r"\.\s*to_excel\s*\("),
    "csv_writer": re.compile(r"\bcsv\s*\.\s*(?:writer|DictWriter)\s*\("),
    "xlsxwriter": re.compile(r"\bxlsxwriter\b"),
    "excel_writer": re.compile(r"\b(?:pd|pandas)\s*\.\s*ExcelWriter\s*\("),
    "openpyxl": re.compile(r"\bopenpyxl\b"),
    "manual_delimiter_write": re.compile(
        r"\.\s*write\s*\([^)]*(?:,\s*(?:[^,)]+,\s*)*[^,)]|sep\s*=|delimiter\s*=|join\s*\(\s*['\"][,;\t])"
    ),
}


def _is_excluded(path: Path) -> bool:
    parts = set(path.parts)
    return any(exclude in parts for exclude in EXCLUDE_DIRS) or path.name.endswith(
        "_project_code.txt"
    )


def _is_cell_value_target(target: ast.expr) -> bool:
    """Return True if the assignment target writes to a worksheet/cell value."""
    return isinstance(target, ast.Subscript) or (
        isinstance(target, ast.Attribute) and target.attr == "value"
    )


def _has_cell_value_write(file_path: Path) -> bool:
    """Return True if the file assigns to a worksheet/cell value."""
    tree = ast.parse(file_path.read_text(encoding="utf-8"))
    return any(
        _is_cell_value_target(target)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
    )


def _find_python_files():
    for path in REPO_ROOT.rglob("*.py"):
        if _is_excluded(path):
            continue
        yield path


def _is_allowed_openpyxl(file_path: Path) -> bool:
    return file_path.name in ALLOWED_OPENPYXL_FILES


def _check_openpyxl(file_path: Path, rel_str: str) -> list[str]:
    """Return violations for openpyxl usage, or an empty list if allowed."""
    if not _is_allowed_openpyxl(file_path):
        return [f"{rel_str}: unauthorized openpyxl usage"]
    if file_path.name == "generate_summary.py" and _has_cell_value_write(file_path):
        return [f"{rel_str}: openpyxl cell value write detected"]
    return []


def _format_violation(rel_str: str, sink_name: str) -> str:
    if sink_name in ("direct_to_csv", "direct_to_excel"):
        return f"{rel_str}: unauthorized direct {sink_name} call"
    if sink_name == "manual_delimiter_write":
        return f"{rel_str}: possible manual delimiter-based write"
    return f"{rel_str}: unauthorized {sink_name} usage"


def _scan_file(file_path: Path) -> list[str]:
    """Return all sink violations found in a single Python file."""
    text = file_path.read_text(encoding="utf-8")
    rel = file_path.relative_to(REPO_ROOT)
    rel_str = str(rel)

    # The safety module is the sole authorized gateway.
    if rel_str == "scripts/spreadsheet_safety.py":
        return []

    violations = []
    for sink_name, pattern in SINK_PATTERNS.items():
        if not pattern.search(text):
            continue
        if sink_name == "openpyxl":
            violations.extend(_check_openpyxl(file_path, rel_str))
            continue
        violations.append(_format_violation(rel_str, sink_name))
    return violations


def test_no_unauthorized_spreadsheet_sinks():
    violations = []
    for file_path in _find_python_files():
        violations.extend(_scan_file(file_path))

    assert not violations, "Unauthorized spreadsheet/CSV sinks found:\n" + "\n".join(
        violations
    )

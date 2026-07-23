from unittest.mock import patch

import pandas as pd
from openpyxl import load_workbook

import generate_summary


def test_generate_summary_main(tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    summary_file = output_dir / "Summary_Report.xlsx"

    df1 = pd.DataFrame({"Processed_Value": [1.0, 2.0, 3.0], "Is_Outlier": [0, 1, 0]})
    df2 = pd.DataFrame({"Processed_Value": [4.0, 5.0, 6.0], "Is_Outlier": [1, 1, 0]})

    file1 = output_dir / "a_Processed.xlsx"
    file2 = output_dir / "b_Processed.xlsx"

    df1.to_excel(file1, index=False)
    df2.to_excel(file2, index=False)

    with patch("generate_summary.OUTPUT_DIR", str(output_dir)), patch(
        "generate_summary.SUMMARY_FILE", str(summary_file)
    ):
        generate_summary.main()

    assert summary_file.exists()

    summary_df = pd.read_excel(summary_file)
    assert len(summary_df) == 2

    assert summary_df.loc[0, "File"] == "a_Processed.xlsx"
    assert summary_df.loc[0, "Mean_Processed_Value"] == 2.0
    assert summary_df.loc[0, "Median_Processed_Value"] == 2.0
    assert summary_df.loc[0, "Outlier_Count"] == 1

    assert summary_df.loc[1, "File"] == "b_Processed.xlsx"
    assert summary_df.loc[1, "Mean_Processed_Value"] == 5.0
    assert summary_df.loc[1, "Median_Processed_Value"] == 5.0
    assert summary_df.loc[1, "Outlier_Count"] == 2

    wb = load_workbook(summary_file)
    ws = wb.active
    assert len(ws._charts) == 1
    assert ws._charts[0].title.tx.rich.p[0].r[0].t == "Outlier Count per File"


def test_generate_summary_main_no_files(tmp_path, capsys):
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    summary_file = output_dir / "Summary_Report.xlsx"

    with patch("generate_summary.OUTPUT_DIR", str(output_dir)), patch(
        "generate_summary.SUMMARY_FILE", str(summary_file)
    ):
        generate_summary.main()

    captured = capsys.readouterr()
    assert f"No processed files found in {output_dir!s}" in captured.out
    assert not summary_file.exists()


def test_generate_summary_main_with_exception(tmp_path, capsys):
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    summary_file = output_dir / "Summary_Report.xlsx"

    file1 = output_dir / "corrupted_Processed.xlsx"
    file1.write_text("Not an excel file")

    with patch("generate_summary.OUTPUT_DIR", str(output_dir)), patch(
        "generate_summary.SUMMARY_FILE", str(summary_file)
    ):
        generate_summary.main()

    captured = capsys.readouterr()
    assert "Error processing corrupted_Processed.xlsx:" in captured.out

    assert summary_file.exists()
    summary_df = pd.read_excel(summary_file)
    assert len(summary_df) == 0

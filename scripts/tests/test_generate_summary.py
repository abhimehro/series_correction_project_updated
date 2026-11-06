import os
import shutil
import subprocess
import sys
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
        result = generate_summary.main()

    assert result == 0
    assert summary_file.exists()

    summary_df = pd.read_excel(summary_file)
    assert len(summary_df) == 2

    assert summary_df.loc[0, "File"] == "a_Processed.xlsx"
    assert summary_df.loc[0, "Mean_Processed_Value"] == 2.0
    assert summary_df.loc[0, "Median_Processed_Value"] == 2.0
    assert summary_df.loc[0, "Outlier_Count"] == 1
    assert summary_df.loc[0, "Status"] == "OK"
    assert pd.isna(summary_df.loc[0, "Error"])

    assert summary_df.loc[1, "File"] == "b_Processed.xlsx"
    assert summary_df.loc[1, "Mean_Processed_Value"] == 5.0
    assert summary_df.loc[1, "Median_Processed_Value"] == 5.0
    assert summary_df.loc[1, "Outlier_Count"] == 2
    assert summary_df.loc[1, "Status"] == "OK"
    assert pd.isna(summary_df.loc[1, "Error"])

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
        result = generate_summary.main()

    captured = capsys.readouterr()
    assert result == 0
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
        result = generate_summary.main()

    captured = capsys.readouterr()
    assert result == 1
    assert (
        "Error processing corrupted_Processed.xlsx: An unexpected error occurred."
        in captured.out
    )
    assert "1 of 1 file(s) failed" in captured.err

    assert summary_file.exists()
    summary_df = pd.read_excel(summary_file)
    assert len(summary_df) == 1
    assert summary_df.loc[0, "File"] == "corrupted_Processed.xlsx"
    assert summary_df.loc[0, "Status"] == "Failed"
    assert summary_df.loc[0, "Error"] == "Unexpected error (see log)"
    assert pd.isna(summary_df.loc[0, "Mean_Processed_Value"])
    assert pd.isna(summary_df.loc[0, "Median_Processed_Value"])
    assert pd.isna(summary_df.loc[0, "Outlier_Count"])


def test_generate_summary_main_with_missing_column(tmp_path, capsys):
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    summary_file = output_dir / "Summary_Report.xlsx"

    good_df = pd.DataFrame({"Processed_Value": [1.0], "Is_Outlier": [0]})
    bad_df = pd.DataFrame({"Processed_Value": [2.0]})
    good_df.to_excel(output_dir / "good_Processed.xlsx", index=False)
    bad_df.to_excel(output_dir / "bad_Processed.xlsx", index=False)

    with patch("generate_summary.OUTPUT_DIR", str(output_dir)), patch(
        "generate_summary.SUMMARY_FILE", str(summary_file)
    ):
        result = generate_summary.main()

    captured = capsys.readouterr()
    assert result == 1
    assert "1 of 2 file(s) failed" in captured.err

    summary_df = pd.read_excel(summary_file)
    assert len(summary_df) == 2
    bad_row = summary_df.loc[summary_df["File"] == "bad_Processed.xlsx"].iloc[0]
    good_row = summary_df.loc[summary_df["File"] == "good_Processed.xlsx"].iloc[0]
    assert bad_row["Status"] == "Failed"
    assert bad_row["Error"] == "Missing required columns: Is_Outlier"
    assert good_row["Status"] == "OK"
    assert pd.isna(good_row["Error"])

    wb = load_workbook(summary_file)
    assert len(wb.active._charts) == 1


def test_generate_summary_main_exit_code_in_isolated_directory(tmp_path):
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    shutil.copyfile(
        os.path.join(project_root, "generate_summary.py"),
        tmp_path / "generate_summary.py",
    )
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "corrupted_Processed.xlsx").write_text("Not an excel file")

    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        [project_root, environment.get("PYTHONPATH", "")]
    )
    result = subprocess.run(
        [sys.executable, "generate_summary.py"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "1 of 1 file(s) failed" in result.stderr


def test_generate_summary_escapes_malicious_filename(tmp_path):
    """A processed file whose name looks like a formula must be stored as text."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    summary_file = output_dir / "Summary_Report.xlsx"

    payload_name = "=SUM(1,1)_Processed.xlsx"
    malicious_file = output_dir / payload_name
    df = pd.DataFrame({"Processed_Value": [1.0, 2.0, 3.0], "Is_Outlier": [0, 1, 0]})
    df.to_excel(malicious_file, index=False)

    with patch("generate_summary.OUTPUT_DIR", str(output_dir)), patch(
        "generate_summary.SUMMARY_FILE", str(summary_file)
    ):
        result = generate_summary.main()

    assert result == 0
    assert summary_file.exists()
    wb = load_workbook(summary_file, data_only=False)
    file_cell = wb.active["A2"]
    assert file_cell.data_type == "s"
    assert file_cell.value == "'" + payload_name

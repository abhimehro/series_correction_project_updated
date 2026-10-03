from unittest import mock

import pandas as pd

from scripts.apply_refined_corrections import apply_level_shift_correction


def test_apply_level_shift_correction_logs_exception():
    outlier_info = ("1995 (Y01) to 1996 (Y02)", "Sensor 1", 0.5)
    raw_file_map = {"S01": {1: "file1.txt", 2: "file2.txt"}}
    raw_dfs = {"file1.txt": pd.DataFrame(), "file2.txt": pd.DataFrame()}

    with mock.patch(
        "scripts.apply_refined_corrections._calculate_and_apply_shift",
        side_effect=RuntimeError("Test error"),
    ), mock.patch("logging.exception") as mock_log_exception:
        result = apply_level_shift_correction(
            outlier_info, raw_file_map, raw_dfs, sorted_series_ids=["S01"]
        )

        assert result is None
        mock_log_exception.assert_called_once_with(
            "An unexpected error occurred while processing outlier %s, %s",
            "1995 (Y01) to 1996 (Y02)",
            "Sensor 1",
        )

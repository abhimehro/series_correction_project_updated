from unittest.mock import patch

import pytest


def test_load_and_enrich_config_file_not_found(monkeypatch, caplog):
    from scripts.batch_correction import _load_and_enrich_config

    def mock_load(path):
        raise FileNotFoundError("Simulated not found")

    monkeypatch.setattr("scripts.batch_correction.load_config_func", mock_load)
    result = _load_and_enrich_config("/fake/config.json")
    assert result == {}
    assert "not found" in caplog.text


def test_load_and_enrich_config_load_error(monkeypatch):
    from scripts.batch_correction import ProcessingError, _load_and_enrich_config

    # We must patch the function pointer directly if possible, or trigger an exception inside it
    # We can patch 'scripts.batch_correction.load_config_func' since it's used inside the module
    def mock_load(path):
        raise ValueError("Simulated load error")

    monkeypatch.setattr("scripts.batch_correction.load_config_func", mock_load)

    with pytest.raises(ProcessingError, match="Failed to load configuration"):
        _load_and_enrich_config("/fake/config.json")


def test_ensure_output_directory_creates_dir(tmp_path, monkeypatch):
    from scripts.batch_correction import _ensure_output_directory

    output_dir = tmp_path / "new_dir"
    _ensure_output_directory(str(output_dir), False)
    assert output_dir.exists()


def test_ensure_output_directory_exception(monkeypatch):
    from scripts.batch_correction import ProcessingError, _ensure_output_directory

    def mock_makedirs(name, exist_ok):
        raise OSError("Permission denied")

    monkeypatch.setattr("os.makedirs", mock_makedirs)
    monkeypatch.setattr("os.path.isdir", lambda d: False)

    with pytest.raises(ProcessingError, match="Unable to create output directory"):
        _ensure_output_directory("/fake/new_dir", False)


def test_get_data_directory_creates_dir(monkeypatch):
    from scripts.batch_correction import _get_data_directory

    monkeypatch.setattr("os.path.isdir", lambda d: False)

    makedirs_called = []

    def mock_makedirs(d, exist_ok):
        makedirs_called.append(d)

    monkeypatch.setattr("os.makedirs", mock_makedirs)

    result = _get_data_directory({}, create_if_missing=True)
    assert len(makedirs_called) == 1
    assert result.endswith("data")


def test_get_data_directory_creates_dir_exception(monkeypatch):
    from scripts.batch_correction import _get_data_directory

    monkeypatch.setattr("os.path.isdir", lambda d: False)

    def mock_makedirs(d, exist_ok):
        raise OSError("Permission denied")

    monkeypatch.setattr("os.makedirs", mock_makedirs)

    with pytest.raises(FileNotFoundError, match="Cannot create default data directory"):
        _get_data_directory({}, create_if_missing=True)


def test_optional_import_coverage(monkeypatch):
    from scripts.batch_correction import _optional_import

    # Trigger ModuleNotFoundError
    assert _optional_import("non_existent_module_for_test", "Msg") is None

    # Trigger ImportError
    original_import = __import__

    def mock_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "mock_import_error_module":
            raise ImportError("Mock error")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", mock_import)
    assert _optional_import("mock_import_error_module", "Msg") is None

    # Trigger TypeError/ValueError
    def mock_import_err(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "mock_type_error_module":
            raise TypeError("Mock error")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", mock_import_err)

    with pytest.raises(TypeError):
        _optional_import("mock_type_error_module", "Msg")


def test_process_main_mode_empty_or_unreadable(tmp_path, monkeypatch):
    import pandas as pd

    from scripts.batch_correction import _process_main_mode

    # We will trigger the ProcessingError exception at line 590.
    def mock_load(fp):
        return pd.DataFrame()

    monkeypatch.setattr("scripts.batch_correction._load_raw_data", mock_load)

    file_path = tmp_path / "S25_Y01.txt"
    file_path.write_text("not empty but load will return empty")

    result = _process_main_mode(
        [(25, 2000, 1, str(file_path))], {}, str(tmp_path), dry_run=True
    )
    assert len(result) == 1
    assert result.iloc[0]["Status"] == "Failed (Processing Error)"


def test_load_raw_data_safe_numeric_type_error(tmp_path):
    from scripts.batch_correction import _load_raw_data

    file_path = tmp_path / "S25_Y01.txt"
    # To force a TypeError from to_numeric, we pass something very un-numeric
    # But read_csv might read it as strings, which raises ValueError.
    # We can mock to_numeric to raise TypeError.
    from unittest.mock import patch

    file_path.write_text("1 2 3\n4 5 6\n")

    with patch("pandas.to_numeric", side_effect=TypeError("Mock type error")):
        result = _load_raw_data(str(file_path))
        assert not result.empty


def test_batch_process_empty_series_returns_empty_df():
    import pandas as pd

    from scripts.batch_correction import BatchConfig, batch_process

    config = BatchConfig("all", None, (2000, 2010), dry_run=True)

    with patch(
        "scripts.batch_correction._determine_series_to_process", return_value=[]
    ):
        result = batch_process(config)
        assert isinstance(result, pd.DataFrame)
        assert result.empty


def test_batch_process_empty_series_from_selection(monkeypatch):
    import pandas as pd

    from scripts.batch_correction import BatchConfig, batch_process

    # If determine_series_to_process returns []
    monkeypatch.setattr(
        "scripts.batch_correction._determine_series_to_process", lambda *args: []
    )
    config = BatchConfig(
        series_selection="all", river_miles=None, years=(2000, 2010), dry_run=True
    )
    result = batch_process(config)
    assert isinstance(result, pd.DataFrame)
    assert result.empty

def test_process_fallback_mode_coverage_error(tmp_path):
    from unittest.mock import patch
    import pandas as pd
    from scripts.batch_correction import _process_fallback_mode

    series_to_process = [1]
    config_data = {
        "series": {"1": {"raw_data": ["file1.txt"]}},
        "defaults": {"a": 1},
        "processor_config": {"b": 2},
    }

    with patch(
        "scripts.batch_correction._load_raw_data", side_effect=Exception("Test error")
    ) as mock_load, patch("scripts.batch_correction.log") as mock_log:

        result_df = _process_fallback_mode(
            series_to_process=series_to_process,
            config_data=config_data,
            output_dir=str(tmp_path),
            dry_run=False,
        )

        assert len(result_df) == 1
        assert result_df.iloc[0]["Series"] == 1
        assert result_df.iloc[0]["Status"] == "Failed (Processing Error)"


def test_process_fallback_mode_coverage_empty(tmp_path):
    import pandas as pd
    from scripts.batch_correction import _process_fallback_mode

    # Test line 560 (empty dataframe return)
    series_to_process = [1]
    config_data = {}  # Missing "series" key

    result_df = _process_fallback_mode(
        series_to_process=series_to_process,
        config_data=config_data,
        output_dir=str(tmp_path),
        dry_run=False,
    )

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty

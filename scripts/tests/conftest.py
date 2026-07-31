# conftest.py to ensure project root is on sys.path for imports
import os
import sys

# Add project root to path for pytest
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from unittest import mock

import pandas as pd
import pytest


@pytest.fixture
def mock_dependencies(mocker):
    """Mocks optional dependencies and file system calls for batch tests."""
    mocker.patch("scripts.batch_correction.data_loader", None)
    mocker.patch("scripts.batch_correction.processor", None)

    mock_isdir = mocker.patch("os.path.isdir", return_value=True)
    mock_isfile = mocker.patch("os.path.isfile", return_value=True)
    mock_listdir = mocker.patch("os.listdir", return_value=[])

    mock_getsize = mocker.patch("os.path.getsize", return_value=100)
    mock_basename = mocker.patch(
        "os.path.basename", side_effect=lambda p: os.path.split(p)[1]
    )

    mock_to_excel = mocker.patch("pandas.DataFrame.to_excel")
    mock_to_csv = mocker.patch("pandas.DataFrame.to_csv")

    from scripts import spreadsheet_safety as _ss

    real_write_excel_safely = _ss.write_excel_safely
    mock_write_excel_safely = mocker.patch(
        "scripts.spreadsheet_safety.write_excel_safely"
    )
    mock_write_excel_safely.side_effect = real_write_excel_safely

    mock_file_open = mocker.patch(
        "builtins.open", mock.mock_open(read_data="line1\nline2")
    )
    mock_file_open.side_effect = None

    return {
        "isdir": mock_isdir,
        "isfile": mock_isfile,
        "listdir": mock_listdir,
        "getsize": mock_getsize,
        "basename": mock_basename,
        "to_excel": mock_to_excel,
        "to_csv": mock_to_csv,
        "write_excel_safely": mock_write_excel_safely,
        "open": mock_file_open,
        "data_loader": None,
        "processor": None,
    }


@pytest.fixture
def mock_config_loader(mocker):
    """Provides a mock config loader function for batch tests."""
    mock_loader = mock.MagicMock(
        return_value={
            "RAW_DATA_DIR": "/fake/data/dir",
            "RIVER_MILE_TO_SERIES": {"54.0": 26, "53.0": 27, "50.5": 28},
            "RIVER_MILE_MAP_PATH": "scripts/river_mile_map.csv",
        }
    )
    mocker.patch("scripts.batch_correction.load_config_func", mock_loader)
    return mock_loader


@pytest.fixture
def mock_data_loader_mod(mocker):
    """Provides a mock data_loader module for batch tests."""
    mock_mod = mock.MagicMock()
    mock_mod.load_data.return_value = pd.DataFrame({0: range(5), 1: range(5)})
    mocker.patch("scripts.batch_correction.data_loader", mock_mod)
    return mock_mod


@pytest.fixture
def mock_processor_mod(mocker):
    """Provides a mock processor module for batch tests."""
    mock_mod = mock.MagicMock()
    mock_mod.process_data.return_value = pd.DataFrame({0: range(5), 1: range(5)})
    mocker.patch("scripts.batch_correction.processor", mock_mod)
    return mock_mod

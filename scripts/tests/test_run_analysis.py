import pytest
import os
import json
from unittest.mock import patch, mock_open

# We import the module to test
from scripts import run_analysis

@pytest.fixture
def mock_config():
    return {
        "defaults": {
            "threshold": 2.5
        }
    }

def test_run_analysis_success(mocker, mock_config, capsys):
    # Mock os.makedirs
    mocker.patch('os.makedirs')

    # Mock open for reading and writing config
    mocker.patch('builtins.open', mock_open(read_data=json.dumps(mock_config)))

    # Mock the json load/dump to not fail on mock_open
    mocker.patch('json.load', return_value=mock_config)
    mocker.patch('json.dump')

    # Mock batch_process to succeed
    mocker.patch('scripts.run_analysis.batch_process', return_value=[{"file": "test.csv"}])

    # Run main
    run_analysis.main()

    # Assert expected prints
    captured = capsys.readouterr()
    assert "Updated threshold to 3.0 for better outlier detection" in captured.out
    assert "Successfully processed 1 files" in captured.out
    assert "Processing complete! Check the output directory for Excel files." in captured.out

def test_run_analysis_batch_process_error(mocker, mock_config, capsys):
    # Mock os.makedirs
    mocker.patch('os.makedirs')

    # Mock open for reading and writing config
    mocker.patch('builtins.open', mock_open(read_data=json.dumps(mock_config)))

    mocker.patch('json.load', return_value=mock_config)
    mocker.patch('json.dump')

    # Mock batch_process to raise Exception
    mocker.patch('scripts.run_analysis.batch_process', side_effect=Exception("Simulated processing error"))

    # Run main
    run_analysis.main()

    # Assert expected prints for error path
    captured = capsys.readouterr()
    assert "Error during processing: Simulated processing error" in captured.out
    assert "Processing complete! Check the output directory for Excel files." in captured.out

def test_run_analysis_config_update_error(mocker, capsys):
    # Mock os.makedirs
    mocker.patch('os.makedirs')

    # Mock open to raise exception
    mocker.patch('builtins.open', side_effect=IOError("Simulated IO error"))

    # Mock batch_process
    mocker.patch('scripts.run_analysis.batch_process', return_value=[])

    # Run main
    run_analysis.main()

    # Assert expected prints for config error path
    captured = capsys.readouterr()
    assert "Error updating config: Simulated IO error" in captured.out
    assert "Will continue with existing config" in captured.out
    assert "Successfully processed 0 files" in captured.out

import sys

import pytest
import pandas as pd

import scripts.series_correction_cli as cli

_ARGV = [
    "prog",
    "--series",
    "all",
    "--river-miles",
    "1.0",
    "2.0",
    "--years",
    "2010",
    "2015",
]


def _run_main_with_summary(monkeypatch, summary_df):
    """Run cli.main() with batch_process stubbed to return summary_df."""
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(cli, "batch_process", lambda config: summary_df)
    monkeypatch.setattr(sys, "argv", _ARGV)
    return cli.main()


def _run_main_with_batch_error(monkeypatch, exc):
    """Run cli.main() with batch_process stubbed to raise exc."""
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: None)

    def fake_batch(config):
        raise exc

    monkeypatch.setattr(cli, "batch_process", fake_batch)
    monkeypatch.setattr(sys, "argv", _ARGV)
    return cli.main()


def test_main_happy_path(monkeypatch):
    # Stub logging.basicConfig to avoid writing to file
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: None)
    # Stub batch_process to capture its inputs
    called = {}

    def fake_batch(config):
        called["series"] = config.series_selection
        called["river_miles"] = config.river_miles
        called["years"] = config.years
        called["dry_run"] = config.dry_run

    monkeypatch.setattr(cli, "batch_process", fake_batch)
    # Set CLI args
    test_args = [
        "prog",
        "--series",
        "2",
        "--river-miles",
        "54.0",
        "53.0",
        "--years",
        "1995",
        "2000",
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    # Execute main without error
    cli.main()
    assert called == {
        "series": "2",
        "river_miles": [54.0, 53.0],
        "years": [1995, 2000],
        "dry_run": False,
    }


def test_main_with_dry_run_flag(monkeypatch):
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: None)
    # Capture dry_run flag
    called = {}

    def fake_batch(config):
        called["dry_run"] = config.dry_run

    monkeypatch.setattr(
        cli,
        "batch_process",
        fake_batch,
    )
    test_args = [
        "prog",
        "--series",
        "all",
        "--river-miles",
        "10.0",
        "20.0",
        "--years",
        "2000",
        "2005",
        "--dry-run",
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    cli.main()
    assert called["dry_run"] is True


def test_main_missing_required_args(monkeypatch):
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: None)
    # Missing required --river-miles and --years
    monkeypatch.setattr(sys, "argv", ["prog"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    # Argparse uses exit code 2 for argument errors
    assert excinfo.value.code == 2


def test_main_batch_process_exception(monkeypatch):
    with pytest.raises(SystemExit) as excinfo:
        _run_main_with_batch_error(monkeypatch, RuntimeError("test error"))
    assert excinfo.value.code == 1


def test_main_batch_process_value_error(monkeypatch):
    with pytest.raises(SystemExit) as excinfo:
        _run_main_with_batch_error(monkeypatch, ValueError("test value error"))
    assert excinfo.value.code == 1


def test_main_exits_on_failed_file(monkeypatch):
    """Exit nonzero when a file failed."""
    with pytest.raises(SystemExit) as excinfo:
        _run_main_with_summary(
            monkeypatch, pd.DataFrame({"Status": ["Failed (Processing Error)"]})
        )

    assert excinfo.value.code == 1


def test_main_ok_when_all_succeed(monkeypatch):
    """Return normally when all files succeed."""
    assert (
        _run_main_with_summary(
            monkeypatch, pd.DataFrame({"Status": ["Processed", "Processed"]})
        )
        is None
    )

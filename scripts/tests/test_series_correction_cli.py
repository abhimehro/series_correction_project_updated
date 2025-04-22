import sys
import pytest

import scripts.series_correction_cli as cli


def test_main_happy_path(monkeypatch):
    # Stub logging.basicConfig to avoid writing to file
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: None)
    # Stub batch_process to capture its inputs
    called = {}
    def fake_batch(series, river_miles, years, dry_run):
        called["series"] = series
        called["river_miles"] = river_miles
        called["years"] = years
        called["dry_run"] = dry_run
    monkeypatch.setattr(cli, "batch_process", fake_batch)
    # Set CLI args
    test_args = [
        "prog",
        "--series", "2",
        "--river-miles", "54.0", "53.0",
        "--years", "1995", "2000",
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
    monkeypatch.setattr(cli, "batch_process", lambda s, r, y, dr: called.setdefault("dry_run", dr))
    test_args = [
        "prog",
        "--series", "all",
        "--river-miles", "10.0", "20.0",
        "--years", "2000", "2005",
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
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kwargs: None)
    # Simulate an error in batch_process
    def fake_batch(*args, **kwargs):
        raise Exception("test error")
    monkeypatch.setattr(cli, "batch_process", fake_batch)
    test_args = [
        "prog",
        "--series", "all",
        "--river-miles", "1.0", "2.0",
        "--years", "2010", "2015",
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1

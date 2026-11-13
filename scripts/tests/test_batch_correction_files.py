"""Boundary tests for batch_correction file discovery, year mapping, and parsing."""

# Covers the filename -> year mapping and forgiving text loader that replaced
# the retired updated_processor script: sequential Y-index -> year mapping,
# explicit year_index_map overrides, malformed filenames, and ragged files.

import os

import pandas as pd  # type: ignore
import pytest  # type: ignore

import scripts.batch_correction as bc
from scripts.batch_correction import (
    _determine_year_for_index,
    _find_files_to_process,
    _load_raw_data,
    _parse_and_validate_file,
)

YEARS = (1995, 2014)
YEARS_TO_PROCESS = range(YEARS[0], YEARS[1] + 1)
SERIES_MAP = {"26": 26}


# ---------------------------------------------------------------------------
# _determine_year_for_index
# ---------------------------------------------------------------------------


def test_year_index_maps_first_and_last_year():
    """Sequential fallback: Y01 -> first year, Y20 -> last year."""
    assert _determine_year_for_index(1, {}, YEARS_TO_PROCESS) == 1995
    assert _determine_year_for_index(20, {}, YEARS_TO_PROCESS) == 2014


def test_year_index_out_of_range_returns_none():
    """Y-indices beyond the requested range are not mapped."""
    assert _determine_year_for_index(21, {}, YEARS_TO_PROCESS) is None
    assert _determine_year_for_index(100, {}, YEARS_TO_PROCESS) is None


def test_year_index_zero_returns_none():
    """Y00 is not a valid index; it must not wrap to the last year."""
    assert _determine_year_for_index(0, {}, YEARS_TO_PROCESS) is None


def test_year_index_explicit_map():
    """Explicit year_index_map ({idx: year} reverse map) beats the fallback."""
    reverse_map = {1: 1995, 20: 1996}
    assert _determine_year_for_index(1, reverse_map, YEARS_TO_PROCESS) == 1995
    assert _determine_year_for_index(20, reverse_map, YEARS_TO_PROCESS) == 1996
    assert _determine_year_for_index(2, reverse_map, YEARS_TO_PROCESS) is None


# ---------------------------------------------------------------------------
# _parse_and_validate_file
# ---------------------------------------------------------------------------


def test_parse_valid_filename():
    """A conforming name yields (series, year, y_index, path)."""
    result = _parse_and_validate_file(
        "S26_Y01.txt", SERIES_MAP, {}, YEARS_TO_PROCESS, *YEARS, "/data"
    )
    assert result == (26, 1995, 1, os.path.join("/data", "S26_Y01.txt"))


def test_parse_filename_outside_requested_series():
    """A well-formed name for an unrequested series is skipped."""
    result = _parse_and_validate_file(
        "S27_Y01.txt", SERIES_MAP, {}, YEARS_TO_PROCESS, *YEARS, "/data"
    )
    assert result is None


@pytest.mark.parametrize(
    "file_name",
    [
        "readme.txt",
        "notes.csv",
        "S26_Year1.txt",
        "S26_Y.txt",
        "S26_Y01.csv",
        "S26_Y01.TXT",
        "S26_Y01.txt.bak",
        "S26_Y01_extra.txt",
        "XS26_Y01.txt",
        "s26_y01.txt",
        "S26__Y01.txt",
    ],
)
def test_parse_malformed_filenames_return_none(file_name):
    """Non-conforming filenames are skipped, not parsed or raising."""
    result = _parse_and_validate_file(
        file_name, SERIES_MAP, {}, YEARS_TO_PROCESS, *YEARS, "/data"
    )
    assert result is None


# ---------------------------------------------------------------------------
# _find_files_to_process
# ---------------------------------------------------------------------------


def _touch(path):
    path.write_text("0.0 1.0\n")


def test_find_files_maps_year_indices(tmp_path):
    """End-to-end: Y01 -> 1995 and Y20 -> 2014 for a 1995-2014 run."""
    data_dir = str(tmp_path)
    _touch(tmp_path / "S26_Y01.txt")
    _touch(tmp_path / "S26_Y20.txt")
    _touch(tmp_path / "S26_Y21.txt")  # beyond range -> skipped
    _touch(tmp_path / "S26_Y00.txt")  # invalid index -> skipped
    _touch(tmp_path / "S26_Year1.txt")  # malformed -> skipped
    _touch(tmp_path / "S27_Y01.txt")  # unrequested series -> skipped
    _touch(tmp_path / "notes.txt")  # malformed -> skipped

    found = _find_files_to_process([26], YEARS, data_dir, {})

    assert [(series, year, yi) for series, year, yi, _ in found] == [
        (26, 1995, 1),
        (26, 2014, 20),
    ]


def test_find_files_honors_explicit_year_index_map(tmp_path):
    """config_data['year_index_map'] ({year: index}) overrides sequencing."""
    data_dir = str(tmp_path)
    _touch(tmp_path / "S26_Y01.txt")
    _touch(tmp_path / "S26_Y20.txt")

    config = {"year_index_map": {"1995": 1, "1996": 20}}
    found = _find_files_to_process([26], (1995, 1996), data_dir, config)

    assert [(series, year, yi) for series, year, yi, _ in found] == [
        (26, 1995, 1),
        (26, 1996, 20),
    ]


def test_find_files_missing_directory_returns_empty(tmp_path):
    """A missing data directory yields no files, not an error."""
    found = _find_files_to_process([26], YEARS, str(tmp_path / "nope"), {})
    assert found == []


# ---------------------------------------------------------------------------
# _load_raw_data
# ---------------------------------------------------------------------------


def test_load_raw_data_short_rows_are_counted_not_dropped(tmp_path):
    """Rows with fewer fields than the first row are NaN-padded, not dropped."""
    path = tmp_path / "S26_Y01.txt"
    path.write_text("# comment\n0.0 1.0\n1.0\n\n2.0 2.0\n")

    df = _load_raw_data(str(path))

    assert len(df) == 3
    assert df["Time (Seconds)"].tolist() == [0.0, 1.0, 2.0]
    assert pd.isna(df.iloc[1, 1])  # short row's missing value is NaN, not dropped


def test_load_raw_data_comments_and_blanks_skipped(tmp_path):
    """Comment and blank lines are skipped; data rows are kept."""
    path = tmp_path / "S26_Y01.txt"
    path.write_text("# header comment\n\n0.0 1.0\n# inline section\n1.0 2.0\n")

    df = _load_raw_data(str(path))

    assert len(df) == 2


def test_load_raw_data_comments_only_file_is_empty(tmp_path):
    """A file with only comments and blanks loads as an empty DataFrame."""
    path = tmp_path / "S26_Y01.txt"
    path.write_text("# only comments\n\n# nothing else\n")

    df = _load_raw_data(str(path))

    assert df.empty


def test_load_raw_data_overwide_rows_raise_processing_error(tmp_path):
    """Rows with more fields than the first row fail the whole file load."""
    path = tmp_path / "S26_Y01.txt"
    path.write_text("0.0 1.0\n1.0 2.0 3.0\n")

    # Reference via the module: test_batch_correction.py reloads it, which
    # rebinds ProcessingError to a new class object.
    with pytest.raises(bc.ProcessingError, match="Failed to load data from file"):
        _load_raw_data(str(path))


def test_load_raw_data_missing_file_raises_processing_error(tmp_path):
    """A nonexistent file raises ProcessingError."""
    with pytest.raises(bc.ProcessingError, match="Failed to load data from file"):
        _load_raw_data(str(tmp_path / "does_not_exist.txt"))

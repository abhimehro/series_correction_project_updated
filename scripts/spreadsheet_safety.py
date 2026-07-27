from __future__ import annotations

import re
from typing import Any, Tuple

import pandas as pd

# Formula initiators: =, +, -, @ and their full-width Unicode equivalents.
# These may be hidden by leading whitespace/control characters.
FORMULA_PREFIX_RE = re.compile(
    r"^[\s]*[" r"=\+\-@\uff1d\uff0b\uff0d\uff20]",
    re.UNICODE,
)

# Prefix that forces spreadsheet consumers to treat a cell as text.
NEUTRALIZE_PREFIX = "'"

# Characters that openpyxl (and Excel) do not allow in sheet titles.
_INVALID_SHEET_NAME_RE = re.compile(r"[\\*?:/\[\]]")


def _label_has_null_byte(value: Any) -> bool:
    return isinstance(value, str) and "\x00" in value


def _is_sanitizable_dtype(dtype: Any) -> bool:
    """Return True for dtypes that can contain string labels needing escaping."""
    return (
        pd.api.types.is_object_dtype(dtype)
        or pd.api.types.is_string_dtype(dtype)
        or isinstance(dtype, pd.CategoricalDtype)
    )


def escape_spreadsheet_formula(value: Any) -> Any:
    """Idempotent cell encoder.

    For strings, prefix once with an apostrophe if the first logical character
    (after leading whitespace) is a formula initiator.  Preserves non-strings
    and missing values and never double-prefixes already-neutralized values.
    """
    if not isinstance(value, str):
        return value
    if value.startswith(NEUTRALIZE_PREFIX):
        return value
    if FORMULA_PREFIX_RE.match(value):
        return NEUTRALIZE_PREFIX + value
    return value


def _sanitize_label(value: Any, context: str = "") -> Any:
    """Escape a single label and reject embedded null bytes."""
    if _label_has_null_byte(value):
        msg = "Null byte found in spreadsheet export"
        if context:
            msg += f" ({context})"
        msg += f": {value!r}"
        raise ValueError(msg)
    return escape_spreadsheet_formula(value)


def _find_null_byte_in_index(index: pd.Index) -> Tuple[str, Any] | None:
    """Return (location, offending_value) for the first null byte in an Index."""
    if isinstance(index, pd.MultiIndex):
        for level_index, level in enumerate(index.levels):
            bad = _find_null_byte_in_index(level)
            if bad is not None:
                location, value = bad
                return (f"MultiIndex level {level_index} {location}", value)
        for name_index, name in enumerate(index.names):
            if _label_has_null_byte(name):
                return (f"MultiIndex name {name_index}", name)
        return None

    if isinstance(index, pd.CategoricalIndex):
        for label in index.categories:
            if _label_has_null_byte(label):
                return ("CategoricalIndex category", label)
    elif _is_sanitizable_dtype(index.dtype):
        for label in index:
            if _label_has_null_byte(label):
                return ("index label", label)

    if _label_has_null_byte(index.name):
        return ("index name", index.name)
    return None


def _sanitize_index(index: pd.Index) -> pd.Index:
    """Escape formula initiators and null bytes in Index/MultiIndex labels and names."""
    bad = _find_null_byte_in_index(index)
    if bad is not None:
        location, value = bad
        raise ValueError(f"Null byte in {location}: {value!r}")

    if isinstance(index, pd.MultiIndex):
        new_levels = [_sanitize_index(level) for level in index.levels]
        new_index = index.set_levels(new_levels)
        new_names = tuple(
            _sanitize_label(name, "MultiIndex name") if name is not None else None
            for name in new_index.names
        )
        return new_index.set_names(new_names)

    if isinstance(index, pd.CategoricalIndex):
        new_categories = [
            _sanitize_label(cat, "CategoricalIndex category")
            for cat in index.categories
        ]
        if len(set(new_categories)) == len(new_categories):
            new_values = index.rename_categories(new_categories)
        else:
            new_values = index.astype(object).map(escape_spreadsheet_formula)
    elif _is_sanitizable_dtype(index.dtype):
        new_values = index.map(escape_spreadsheet_formula)
    else:
        new_values = index

    if index.name is not None:
        new_name = _sanitize_label(index.name, "index name")
        if new_name != new_values.name:
            return new_values.set_names(new_name)
    return new_values


def _find_null_byte_in_series(series: pd.Series) -> Any:
    """Return the first offending value containing a null byte, or None."""
    if isinstance(series.dtype, pd.CategoricalDtype):
        for cat in series.cat.categories:
            if _label_has_null_byte(cat):
                return cat
        return None

    if isinstance(series.dtype, pd.StringDtype):
        mask = series.str.contains("\x00", regex=False, na=False)
        if mask.any():
            return series[mask].iloc[0]
        return None

    # object dtype (or strings stored as object)
    if series.dtype == object or pd.api.types.is_object_dtype(series.dtype):
        mask = series.apply(lambda x: _label_has_null_byte(x))
        if mask.any():
            return series[mask].iloc[0]
    return None


def _sanitize_object_series(series: pd.Series, column: Any) -> pd.Series:
    """Escape a Series while handling categorical collisions and null bytes."""
    bad = _find_null_byte_in_series(series)
    if bad is not None:
        raise ValueError(f"Null byte in column {column!r}: {bad!r}")

    if isinstance(series.dtype, pd.CategoricalDtype):
        old_categories = list(series.cat.categories)
        new_categories = [escape_spreadsheet_formula(cat) for cat in old_categories]
        if len(set(new_categories)) == len(new_categories):
            return series.cat.rename_categories(new_categories)
        # Collision fallback: keep values in object form, preserving NaN/order.
        return series.astype(object).map(escape_spreadsheet_formula)

    return series.map(escape_spreadsheet_formula)


def sanitize_dataframe_for_spreadsheet(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of a DataFrame with spreadsheet-injection payloads neutralized.

    Sanitizes:
    - column labels (single and MultiIndex)
    - row index labels (single and MultiIndex)
    - object/string/categorical cell values

    Raises ValueError if any exported cell/label contains a null byte.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Expected a pandas DataFrame")

    sanitized = dataframe.copy()
    sanitized.columns = _sanitize_index(sanitized.columns)
    sanitized.index = _sanitize_index(sanitized.index)

    object_like_columns = sanitized.select_dtypes(
        include=["object", "string", "category"]
    ).columns

    for column in object_like_columns:
        sanitized[column] = _sanitize_object_series(sanitized[column], column)

    return sanitized


def _validate_sheet_name(sheet_name: Any) -> None:
    """Ensure a sheet name is legal for the openpyxl/Excel target engine."""
    if sheet_name is None:
        return
    if not isinstance(sheet_name, str):
        raise ValueError(f"sheet_name must be a string, got {type(sheet_name).__name__}")
    if not sheet_name:
        raise ValueError("sheet_name cannot be empty")
    if len(sheet_name) > 31:
        raise ValueError(f"sheet_name exceeds 31 characters: {sheet_name!r}")
    if _INVALID_SHEET_NAME_RE.search(sheet_name):
        raise ValueError(f"sheet_name contains invalid characters: {sheet_name!r}")


def _sanitize_writer_kwargs(kwargs: dict, *, excel: bool = False) -> dict:
    """Sanitize user-supplied header/index_label/sheet_name arguments."""
    sanitized = dict(kwargs)

    header = sanitized.get("header")
    if header is not None and not isinstance(header, bool):
        if isinstance(header, (list, tuple)):
            sanitized["header"] = [
                _sanitize_label(item, "header") for item in header
            ]
        else:
            sanitized["header"] = _sanitize_label(header, "header")

    index_label = sanitized.get("index_label")
    if index_label is not None and not isinstance(index_label, bool):
        if isinstance(index_label, (list, tuple)):
            sanitized["index_label"] = tuple(
                _sanitize_label(item, "index_label") for item in index_label
            )
        else:
            sanitized["index_label"] = _sanitize_label(index_label, "index_label")

    if excel:
        _validate_sheet_name(sanitized.get("sheet_name"))

    return sanitized


def write_excel_safely(dataframe: pd.DataFrame, *args, **kwargs) -> Any:
    """Export a DataFrame to Excel with formula-injection protection."""
    kwargs = _sanitize_writer_kwargs(kwargs, excel=True)
    return sanitize_dataframe_for_spreadsheet(dataframe).to_excel(*args, **kwargs)


def write_csv_safely(dataframe: pd.DataFrame, *args, **kwargs) -> Any:
    """Export a DataFrame to CSV with formula-injection protection."""
    kwargs = _sanitize_writer_kwargs(kwargs, excel=False)
    return sanitize_dataframe_for_spreadsheet(dataframe).to_csv(*args, **kwargs)

from __future__ import annotations

from typing import Any

import pandas as pd

FORMULA_PREFIXES = ("=", "+", "-", "@")


def escape_spreadsheet_formula(value: Any) -> Any:
    if isinstance(value, str) and value.startswith(FORMULA_PREFIXES):
        return "'" + value
    return value


def sanitize_dataframe_for_spreadsheet(dataframe: pd.DataFrame) -> pd.DataFrame:
    object_columns = dataframe.select_dtypes(include=["object", "string"]).columns
    if object_columns.empty:
        return dataframe

    sanitized = dataframe.copy()
    for column in object_columns:
        sanitized[column] = sanitized[column].map(escape_spreadsheet_formula)
    return sanitized

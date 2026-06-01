from __future__ import annotations

import re
from typing import Any

import pandas as pd

# Leading whitespace may precede formula triggers in exported cells.
FORMULA_PREFIX_RE = re.compile(r"^[\t\r\n ]*[=+\-@]")


def escape_spreadsheet_formula(value: Any) -> Any:
    if isinstance(value, str) and FORMULA_PREFIX_RE.match(value):
        return "'" + value
    return value


def sanitize_dataframe_for_spreadsheet(dataframe: pd.DataFrame) -> pd.DataFrame:
    object_columns = dataframe.select_dtypes(include=["object", "string", "category"]).columns
    if object_columns.empty:
        return dataframe

    sanitized = dataframe.copy()
    for column in object_columns:
        if isinstance(sanitized[column].dtype, pd.CategoricalDtype):
            new_categories = [escape_spreadsheet_formula(cat) for cat in sanitized[column].cat.categories]
            sanitized[column] = sanitized[column].cat.rename_categories(new_categories)
        else:
            sanitized[column] = sanitized[column].map(escape_spreadsheet_formula)
    return sanitized


def write_excel_safely(dataframe: pd.DataFrame, *args, **kwargs) -> None:
    sanitize_dataframe_for_spreadsheet(dataframe).to_excel(*args, **kwargs)

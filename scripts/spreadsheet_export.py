from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


FORMULA_PREFIX_RE = re.compile(r"^[\t\r\n ]*[=+\-@]")


def neutralize_formula_text(value: Any) -> Any:
    if isinstance(value, str) and FORMULA_PREFIX_RE.match(value):
        return "'" + value
    return value


def neutralize_formulas(dataframe: pd.DataFrame) -> pd.DataFrame:
    result = dataframe.copy()
    object_columns = result.select_dtypes(include=["object", "string"]).columns
    for column in object_columns:
        result[column] = result[column].map(neutralize_formula_text)
    return result


def safe_to_excel(
    dataframe: pd.DataFrame,
    excel_writer: str | Path | pd.ExcelWriter,
    *args: Any,
    **kwargs: Any,
) -> None:
    neutralize_formulas(dataframe).to_excel(excel_writer, *args, **kwargs)

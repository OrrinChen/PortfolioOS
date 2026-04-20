"""Shared file-loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml

from portfolio_os.data.import_profiles import ImportFileType, ImportProfile, apply_import_profile
from portfolio_os.domain.errors import InputValidationError

BOOLEAN_TRUE = {"true", "1", "yes"}
BOOLEAN_FALSE = {"false", "0", "no"}


def normalize_ticker(value: object) -> str:
    """Normalize ticker strings."""

    text = str(value).strip()
    if not text:
        raise InputValidationError("Ticker values cannot be empty.")
    return text


def parse_bool(value: object, field_name: str) -> bool:
    """Parse flexible CSV boolean values."""

    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in BOOLEAN_TRUE:
        return True
    if text in BOOLEAN_FALSE:
        return False
    raise InputValidationError(
        f"Unsupported boolean value {value!r} for field {field_name}. "
        "Allowed values: true/false/1/0/yes/no."
    )


def read_csv(path: str | Path) -> pd.DataFrame:
    """Read a CSV file into a DataFrame."""

    return pd.read_csv(Path(path), converters={"ticker": lambda value: str(value).strip()})


def read_input_frame(
    path: str | Path,
    *,
    input_type: ImportFileType,
    import_profile: ImportProfile | None = None,
) -> pd.DataFrame:
    """Read one input CSV and apply an optional import profile."""

    frame = read_csv(path)
    return apply_import_profile(
        frame,
        input_type=input_type,
        source_name=str(path),
        profile=import_profile,
    )


def read_yaml(path: str | Path) -> dict:
    """Read a YAML mapping."""

    with Path(path).open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise InputValidationError(f"Expected a YAML mapping in {path}.")
    return data


def ensure_columns(frame: pd.DataFrame, required_columns: Iterable[str], source_name: str) -> None:
    """Validate that required columns exist."""

    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise InputValidationError(
            f"Missing required columns in {source_name}: {', '.join(missing)}"
        )


def ensure_unique_tickers(frame: pd.DataFrame, source_name: str) -> None:
    """Reject duplicated tickers."""

    duplicated = frame[frame["ticker"].duplicated()]["ticker"].tolist()
    if duplicated:
        raise InputValidationError(
            f"Duplicate ticker(s) in {source_name}: {', '.join(sorted(set(duplicated)))}"
        )


def ensure_non_negative(frame: pd.DataFrame, column: str, source_name: str) -> None:
    """Fail if a numeric column contains negative values."""

    if frame[column].isna().any():
        raise InputValidationError(f"{source_name} contains missing values in {column}.")
    if (pd.to_numeric(frame[column], errors="raise") < 0).any():
        raise InputValidationError(f"{source_name} contains negative values in {column}.")


def ensure_positive(frame: pd.DataFrame, column: str, source_name: str) -> None:
    """Fail if a numeric column contains non-positive values."""

    if frame[column].isna().any():
        raise InputValidationError(f"{source_name} contains missing values in {column}.")
    if (pd.to_numeric(frame[column], errors="raise") <= 0).any():
        raise InputValidationError(f"{source_name} contains non-positive values in {column}.")


def ensure_optional_positive(frame: pd.DataFrame, column: str, source_name: str) -> None:
    """Fail if an optional numeric column is present and contains non-positive values."""

    if column not in frame.columns:
        return
    populated = frame[column].notna()
    if not populated.any():
        return
    if (pd.to_numeric(frame.loc[populated, column], errors="raise") <= 0).any():
        raise InputValidationError(f"{source_name} contains non-positive values in {column}.")

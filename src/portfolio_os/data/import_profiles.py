"""Declarative import-profile support for pilot file mappings."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from portfolio_os.domain.errors import InputValidationError

ImportFileType = Literal["holdings", "target", "market", "reference"]


class ImportFileRule(BaseModel):
    """Declarative mapping rule for one input file type."""

    model_config = ConfigDict(extra="forbid")

    columns: dict[str, str] = Field(default_factory=dict)
    defaults: dict[str, Any] = Field(default_factory=dict)
    boolean_values: dict[str, dict[str, Any]] = Field(default_factory=dict)
    numeric_scales: dict[str, float] = Field(default_factory=dict)
    required_fields: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_required_fields(self) -> "ImportFileRule":
        """Require each required field to have either a mapping or a default."""

        missing = [
            field
            for field in self.required_fields
            if field not in self.columns and field not in self.defaults
        ]
        if missing:
            raise ValueError(
                "Import profile required_fields must appear in columns or defaults: "
                + ", ".join(missing)
            )
        return self


class ImportProfile(BaseModel):
    """Import-profile document covering supported PortfolioOS file types."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    holdings: ImportFileRule | None = None
    target: ImportFileRule | None = None
    market: ImportFileRule | None = None
    reference: ImportFileRule | None = None


def _normalize_mapped_ticker(value: Any) -> Any:
    """Normalize ticker-like mapped values while preserving 6-digit codes."""

    if pd.isna(value):
        return value
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit() and len(text) < 6:
        return text.zfill(6)
    return text


def load_import_profile(path: str | Path) -> ImportProfile:
    """Load and validate an import profile."""

    with Path(path).open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise InputValidationError(f"Expected a YAML mapping in import profile {path}.")
    try:
        return ImportProfile.model_validate(payload)
    except ValidationError as exc:
        raise InputValidationError(f"Invalid import profile: {exc}") from exc


def apply_import_profile(
    frame: pd.DataFrame,
    *,
    input_type: ImportFileType,
    source_name: str,
    profile: ImportProfile | None,
) -> pd.DataFrame:
    """Apply a declarative import profile to a raw input frame."""

    if profile is None:
        return frame
    rule = getattr(profile, input_type)
    if rule is None:
        return frame

    transformed = frame.copy()
    for internal_name, external_name in rule.columns.items():
        if external_name in frame.columns:
            if internal_name == "ticker":
                transformed[internal_name] = frame[external_name].map(_normalize_mapped_ticker)
            else:
                transformed[internal_name] = frame[external_name]
        elif internal_name not in transformed.columns and internal_name in rule.required_fields and internal_name not in rule.defaults:
            raise InputValidationError(
                f"Import profile {profile.name!r} expects source column {external_name!r} "
                f"for required field {internal_name!r} in {source_name}."
            )

    for column, default_value in rule.defaults.items():
        if column not in transformed.columns:
            transformed[column] = default_value
        else:
            transformed[column] = transformed[column].fillna(default_value)

    for column, mapping in rule.boolean_values.items():
        if column not in transformed.columns:
            continue
        normalized_map = {
            str(key).strip().lower(): value
            for key, value in mapping.items()
        }
        transformed[column] = transformed[column].map(
            lambda value, values=normalized_map: values.get(str(value).strip().lower(), value)
        )

    for column, scale in rule.numeric_scales.items():
        if column not in transformed.columns:
            continue
        transformed[column] = pd.to_numeric(transformed[column], errors="raise") * float(scale)

    missing_required = [field for field in rule.required_fields if field not in transformed.columns]
    if missing_required:
        raise InputValidationError(
            f"Mapped {input_type} input is missing required field(s): {', '.join(missing_required)}"
        )
    return transformed

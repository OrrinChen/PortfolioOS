"""Signal implementation contract for Q1 alpha triage."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class SignalContract(BaseModel):
    """Contract a signal implementation must satisfy before evaluation."""

    signal_name: str
    input_fields: list[str] = Field(min_length=1)
    output_column: str
    valid_universe: str
    timestamp_column: str
    no_future_data_required: bool
    implementation_path: str | None = None

    @field_validator("signal_name", "output_column", "valid_universe", "timestamp_column")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank identifiers and descriptions."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("input_fields")
    @classmethod
    def require_non_empty_input_fields(cls, values: list[str]) -> list[str]:
        """Reject blank input field names."""

        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("input_fields cannot contain blank values")
        return cleaned

    @field_validator("no_future_data_required")
    @classmethod
    def require_no_future_data(cls, value: bool) -> bool:
        """Require timestamp-safe signal implementations."""

        if value is not True:
            raise ValueError("no_future_data_required must be true")
        return value

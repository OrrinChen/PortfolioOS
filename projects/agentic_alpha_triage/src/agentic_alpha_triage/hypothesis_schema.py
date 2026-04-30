"""Hypothesis schema for Q1 alpha triage."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


SignalDirection = Literal["positive", "negative", "long", "short", "long_short"]


class Hypothesis(BaseModel):
    """Strict alpha hypothesis record before any signal implementation."""

    hypothesis_id: str
    title: str
    description: str
    economic_rationale: str
    required_data: list[str] = Field(min_length=1)
    signal_direction: SignalDirection
    expected_horizon: str
    timestamp_assumptions: list[str] = Field(min_length=1)
    risk_notes: str

    @field_validator(
        "hypothesis_id",
        "title",
        "description",
        "economic_rationale",
        "expected_horizon",
        "risk_notes",
    )
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank free-text fields."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("required_data", "timestamp_assumptions")
    @classmethod
    def require_non_empty_items(cls, values: list[str]) -> list[str]:
        """Reject blank list entries."""

        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("list entries cannot be blank")
        return cleaned

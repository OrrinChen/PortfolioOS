"""Evaluation contract for leakage-safe alpha triage."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class EvaluationContract(BaseModel):
    """Contract for a leakage-safe event or signal evaluation."""

    event_available_timestamp: str
    anchor_trade_date: str
    entry_rule: str
    holding_windows: list[str] = Field(min_length=1)
    benchmark: str
    cost_assumptions: dict[str, Any] = Field(default_factory=dict)
    placebo_tests_required: bool
    leakage_tests_required: bool

    @field_validator("event_available_timestamp", "anchor_trade_date", "entry_rule", "benchmark")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank evaluation fields."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("holding_windows")
    @classmethod
    def require_non_empty_windows(cls, values: list[str]) -> list[str]:
        """Reject blank holding-window labels."""

        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("holding_windows cannot contain blank values")
        return cleaned

    @field_validator("cost_assumptions")
    @classmethod
    def require_cost_assumptions(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Require explicit cost assumptions."""

        if not value:
            raise ValueError("cost_assumptions must be explicit")
        return value

    @field_validator("placebo_tests_required")
    @classmethod
    def require_placebo_tests(cls, value: bool) -> bool:
        """Require placebo tests for Q1 promotion."""

        if value is not True:
            raise ValueError("placebo_tests_required must be true")
        return value

    @field_validator("leakage_tests_required")
    @classmethod
    def require_leakage_tests(cls, value: bool) -> bool:
        """Require leakage tests for Q1 promotion."""

        if value is not True:
            raise ValueError("leakage_tests_required must be true")
        return value

"""Evaluator fixture schema for Q1 leakage-safe examples."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class EvaluatorFixture(BaseModel):
    """A static evaluator example used to document Q1 evaluation expectations."""

    fixture_id: str
    hypothesis_id: str
    hypothesis_path: str
    signal_name: str
    signal_contract_path: str
    evaluation_contract_path: str
    description: str
    event_source: str
    required_input_columns: list[str] = Field(min_length=1)
    feature_columns: list[str] = Field(min_length=1)
    event_available_timestamp_column: str
    anchor_trade_date_column: str
    label_column: str
    leakage_checks: list[str] = Field(min_length=1)
    placebo_tests: list[str] = Field(min_length=1)
    cost_assumption_keys: list[str] = Field(min_length=1)
    uses_future_data_as_feature: bool
    entry_after_event_available: bool

    @field_validator(
        "fixture_id",
        "hypothesis_id",
        "hypothesis_path",
        "signal_name",
        "signal_contract_path",
        "evaluation_contract_path",
        "description",
        "event_source",
        "event_available_timestamp_column",
        "anchor_trade_date_column",
        "label_column",
    )
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank text fields."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator(
        "required_input_columns",
        "feature_columns",
        "leakage_checks",
        "placebo_tests",
        "cost_assumption_keys",
    )
    @classmethod
    def require_non_empty_items(cls, values: list[str]) -> list[str]:
        """Reject blank list entries."""

        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("list entries cannot be blank")
        return cleaned

    @field_validator("uses_future_data_as_feature")
    @classmethod
    def reject_future_features(cls, value: bool) -> bool:
        """Q1 evaluator fixtures cannot use future labels as model inputs."""

        if value is not False:
            raise ValueError("uses_future_data_as_feature must be false")
        return value

    @field_validator("entry_after_event_available")
    @classmethod
    def require_safe_entry_anchor(cls, value: bool) -> bool:
        """Require entries to occur after event availability."""

        if value is not True:
            raise ValueError("entry_after_event_available must be true")
        return value

    @model_validator(mode="after")
    def require_label_not_in_features(self) -> "EvaluatorFixture":
        """Reject direct label leakage through feature columns."""

        if self.label_column in set(self.feature_columns):
            raise ValueError("label_column cannot appear in feature_columns")
        return self


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _require_referenced_paths(path: Path, fixture: EvaluatorFixture) -> None:
    for relative_path in (
        fixture.hypothesis_path,
        fixture.signal_contract_path,
        fixture.evaluation_contract_path,
    ):
        resolved = (path.parent / relative_path).resolve()
        if not resolved.exists():
            raise ValueError(f"Referenced example path does not exist: {relative_path}")


def load_evaluator_fixture(path: str | Path) -> EvaluatorFixture:
    """Load and validate one evaluator fixture YAML file."""

    resolved_path = Path(path)
    fixture = EvaluatorFixture.model_validate(_load_yaml_mapping(resolved_path))
    _require_referenced_paths(resolved_path, fixture)
    return fixture


def load_evaluator_fixtures(fixtures_dir: str | Path) -> list[EvaluatorFixture]:
    """Load all evaluator fixtures in a directory."""

    resolved_dir = Path(fixtures_dir)
    if not resolved_dir.exists():
        raise ValueError(f"Evaluator fixture directory does not exist: {resolved_dir}")
    if not resolved_dir.is_dir():
        raise ValueError(f"Evaluator fixture path is not a directory: {resolved_dir}")

    paths = sorted(resolved_dir.glob("*.yaml"))
    if not paths:
        raise ValueError(f"No evaluator fixture YAML files found in {resolved_dir}")

    return [load_evaluator_fixture(path) for path in paths]

"""Local dry-run evaluator planner for Q1 fixtures."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

from agentic_alpha_triage.evaluator_contract import EvaluationContract
from agentic_alpha_triage.evaluator_fixture import EvaluatorFixture, load_evaluator_fixture
from agentic_alpha_triage.event_registry_schema import (
    EventRegistryExample,
    load_event_registry_examples,
)
from agentic_alpha_triage.hypothesis_schema import Hypothesis
from agentic_alpha_triage.signal_contract import SignalContract


PlannerStatus = Literal["ready_for_local_evaluation", "rejected"]


class EvaluatorPlan(BaseModel):
    """Non-executing Q1 evaluation plan assembled from local contract artifacts."""

    plan_id: str
    fixture_id: str
    hypothesis_id: str
    signal_name: str
    event_registry_ids: list[str] = Field(default_factory=list)
    required_input_columns: list[str] = Field(default_factory=list)
    feature_columns: list[str] = Field(default_factory=list)
    output_column: str
    label_column: str
    holding_windows: list[str] = Field(default_factory=list)
    benchmark: str
    cost_assumptions: dict[str, Any] = Field(default_factory=dict)
    leakage_checks: list[str] = Field(default_factory=list)
    placebo_tests: list[str] = Field(default_factory=list)
    status: PlannerStatus
    rejection_reasons: list[str] = Field(default_factory=list)


def build_evaluator_plan(
    fixture_path: str | Path,
    *,
    event_registry_dir: str | Path,
) -> EvaluatorPlan:
    """Build one local dry-run evaluator plan from schema-backed Q1 examples."""

    resolved_fixture_path = Path(fixture_path)
    fixture = load_evaluator_fixture(resolved_fixture_path)
    base_dir = resolved_fixture_path.parent
    hypothesis = _load_model(base_dir / fixture.hypothesis_path, Hypothesis)
    signal = _load_model(base_dir / fixture.signal_contract_path, SignalContract)
    evaluation = _load_model(base_dir / fixture.evaluation_contract_path, EvaluationContract)
    event_registries = load_event_registry_examples(event_registry_dir)

    _validate_fixture_family(
        fixture=fixture,
        hypothesis=hypothesis,
        signal=signal,
        evaluation=evaluation,
        event_registries=event_registries,
    )

    return EvaluatorPlan(
        plan_id=f"PLAN-{fixture.fixture_id}",
        fixture_id=fixture.fixture_id,
        hypothesis_id=fixture.hypothesis_id,
        signal_name=fixture.signal_name,
        event_registry_ids=[registry.registry_id for registry in event_registries],
        required_input_columns=fixture.required_input_columns,
        feature_columns=fixture.feature_columns,
        output_column=signal.output_column,
        label_column=fixture.label_column,
        holding_windows=evaluation.holding_windows,
        benchmark=evaluation.benchmark,
        cost_assumptions=evaluation.cost_assumptions,
        leakage_checks=fixture.leakage_checks,
        placebo_tests=fixture.placebo_tests,
        status="ready_for_local_evaluation",
        rejection_reasons=[],
    )


def _validate_fixture_family(
    *,
    fixture: EvaluatorFixture,
    hypothesis: Hypothesis,
    signal: SignalContract,
    evaluation: EvaluationContract,
    event_registries: list[EventRegistryExample],
) -> None:
    if fixture.hypothesis_id != hypothesis.hypothesis_id:
        raise ValueError("Evaluator fixture hypothesis_id does not match referenced hypothesis.")
    if fixture.signal_name != signal.signal_name:
        raise ValueError("Evaluator fixture signal_name does not match referenced signal contract.")
    if not event_registries:
        raise ValueError("At least one event registry example is required to build an evaluator plan.")
    mismatched_registries = [
        registry.registry_id
        for registry in event_registries
        if registry.hypothesis_id != fixture.hypothesis_id
    ]
    if mismatched_registries:
        raise ValueError(
            "Event registry hypothesis_id does not match evaluator fixture: "
            + ", ".join(mismatched_registries)
        )
    if signal.timestamp_column not in fixture.required_input_columns:
        raise ValueError("Signal timestamp_column must be included in evaluator required_input_columns.")
    if evaluation.event_available_timestamp != fixture.event_available_timestamp_column:
        raise ValueError(
            "Evaluation event_available_timestamp must match evaluator event_available_timestamp_column."
        )
    if signal.output_column in fixture.feature_columns:
        raise ValueError("Signal output_column cannot appear in evaluator feature_columns.")
    if signal.output_column not in fixture.required_input_columns:
        raise ValueError("Signal output_column must be present in evaluator required_input_columns.")
    missing_cost_keys = [
        cost_key for cost_key in fixture.cost_assumption_keys if cost_key not in evaluation.cost_assumptions
    ]
    if missing_cost_keys:
        raise ValueError("Evaluation cost_assumptions missing fixture keys: " + ", ".join(missing_cost_keys))
    if not evaluation.placebo_tests_required:
        raise ValueError("Evaluation contract must require placebo tests.")
    if not evaluation.leakage_tests_required:
        raise ValueError("Evaluation contract must require leakage tests.")


def _load_model(path: Path, model_cls: type[BaseModel]) -> Any:
    with path.resolve().open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return model_cls.model_validate(payload)

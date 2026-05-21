"""SUE execution-survival attribution contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


SUE_EXECUTION_SURVIVAL_ATTRIBUTION_SCHEMA_VERSION = "sue_execution_survival_attribution.v1"

SueDecisionLabel = Literal[
    "sue_q2_observed_survives",
    "sue_q2_observed_cost_failure",
    "sue_q2_observed_constraint_failure",
    "sue_q2_projection_too_sparse",
    "sue_q2_injection_unavailable",
    "sue_q2_fixture_unavailable",
    "sue_q2_inconclusive",
]
LayerStatus = Literal["passed", "observed", "unavailable", "failed", "inconclusive"]


class SueLayerAttribution(BaseModel):
    """One interpreted attribution layer for SUE Q2 survival."""

    model_config = ConfigDict(extra="forbid")

    layer_name: str
    status: LayerStatus
    details: str
    observed_rows: int = 0
    unavailable_rows: int = 0
    mean_gross_return: float | None = None
    mean_net_return: float | None = None
    mean_turnover: float | None = None
    mean_cost_drag: float | None = None
    mean_gross_to_net_retention: float | None = None


class SueExecutionSurvivalAttribution(BaseModel):
    """Decision record for Phase 51 SUE execution-survival attribution."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_execution_survival_attribution.v1"] = (
        SUE_EXECUTION_SURVIVAL_ATTRIBUTION_SCHEMA_VERSION
    )
    run_id: str
    decision_label: SueDecisionLabel
    primary_stop_layer: str
    phase52_revision_marginal_value_should_proceed: bool
    phase52_recommendation: str
    alpha_failure_detected: bool
    execution_failure_detected: bool
    projection_sparsity_detected: bool
    optimizer_failure_detected: bool
    production_approval_claimed: bool = False
    local_only: bool = True
    layer_attribution: list[SueLayerAttribution] = Field(default_factory=list)
    what_this_proves: list[str] = Field(default_factory=list)
    what_this_does_not_prove: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)

"""Revision marginal-value gate contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


REVISION_MARGINAL_VALUE_INPUT_SCHEMA_VERSION = "revision_marginal_value_input.v1"
REVISION_TEST_RESULT_SCHEMA_VERSION = "revision_marginal_value_test_result.v1"
REVISION_MARGINAL_METRICS_SCHEMA_VERSION = "revision_marginal_metrics.v1"
REVISION_MARGINAL_THRESHOLDS_SCHEMA_VERSION = "revision_marginal_thresholds.v1"
REVISION_MARGINAL_VALUE_SUMMARY_SCHEMA_VERSION = "revision_marginal_value_summary.v1"
REVISION_OVERLAP_ROW_SCHEMA_VERSION = "revision_overlap_row.v1"
REVISION_MARGINAL_VALUE_RESULT_SCHEMA_VERSION = "revision_marginal_value_result.v1"

RevisionGateDecision = Literal[
    "revision_promote_to_composite_eval",
    "revision_real_but_no_marginal_value",
    "revision_needs_more_evidence",
    "revision_reject_due_to_pit_or_horizon",
]
RevisionTestName = Literal[
    "sue_only_baseline",
    "revision_only_shadow_branch",
    "sue_revision_equal_composite",
    "sue_revision_confidence_weighted_composite",
    "sue_residualized_against_revision",
    "revision_residualized_against_sue",
    "event_overlap_coverage_overlap",
    "cost_aware_marginal_contribution",
]
RevisionTestStatus = Literal["passed", "failed", "unavailable"]
RevisionHorizonType = Literal["to_next_announcement", "event_window", "rebalance_period", "fixed_horizon"]


class RevisionTestResult(BaseModel):
    """One required Phase 52 marginal-value diagnostic result."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["revision_marginal_value_test_result.v1"] = REVISION_TEST_RESULT_SCHEMA_VERSION
    test_name: RevisionTestName
    status: RevisionTestStatus
    metric_name: str
    metric_value: float | None = None
    details: str


class RevisionMarginalMetrics(BaseModel):
    """Metrics used by the SUE-adjusted marginal-value threshold."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["revision_marginal_metrics.v1"] = REVISION_MARGINAL_METRICS_SCHEMA_VERSION
    marginal_rank_ic_t: float
    marginal_alpha_only_t: float
    sue_adjusted_net_improvement: float
    cost_aware_net_improvement: float
    turnover_delta: float
    gross_to_net_retention: float
    event_overlap_ratio: float
    coverage_overlap_ratio: float


class RevisionMarginalThresholds(BaseModel):
    """Thresholds required before revision can advance to composite evaluation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["revision_marginal_thresholds.v1"] = REVISION_MARGINAL_THRESHOLDS_SCHEMA_VERSION
    min_marginal_rank_ic_t: float = 2.0
    min_marginal_alpha_only_t: float = 2.0
    min_sue_adjusted_net_improvement: float = 0.001
    min_cost_aware_net_improvement: float = 0.001
    min_gross_to_net_retention: float = 0.50
    max_event_overlap_ratio: float = 0.75
    max_coverage_overlap_ratio: float = 0.85


class RevisionMarginalValueInput(BaseModel):
    """Input contract for the Phase 52 local revision marginal-value gate."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["revision_marginal_value_input.v1"] = REVISION_MARGINAL_VALUE_INPUT_SCHEMA_VERSION
    run_id: str
    pit_source: str
    revision_data_source: str
    horizon_type: RevisionHorizonType
    proof_type: str
    required_test_results: list[RevisionTestResult] = Field(default_factory=list)
    marginal_metrics: RevisionMarginalMetrics
    marginal_thresholds: RevisionMarginalThresholds = Field(default_factory=RevisionMarginalThresholds)
    no_network: bool = True
    no_broker: bool = True
    local_only: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "RevisionMarginalValueInput":
        """Require local-only operation for Phase 52."""

        if not self.no_network:
            raise ValueError("RevisionMarginalValueInput requires no_network=true")
        if not self.no_broker:
            raise ValueError("RevisionMarginalValueInput requires no_broker=true")
        return self


class RevisionMarginalValueSummary(BaseModel):
    """Compact summary for the Phase 52 gate decision."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["revision_marginal_value_summary.v1"] = REVISION_MARGINAL_VALUE_SUMMARY_SCHEMA_VERSION
    run_id: str
    gate_decision: RevisionGateDecision
    pit_source_required: Literal["WRDS"] = "WRDS"
    pit_source_accepted: bool
    fmp_estimate_history_rejected: bool
    required_tests_passed: bool
    feature_importance_rejected: bool
    beats_sue_adjusted_marginal_threshold: bool
    composite_promotion_allowed: bool
    production_approval_claimed: bool = False


class RevisionOverlapRow(BaseModel):
    """One overlap diagnostic row for SUE versus revision."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["revision_overlap_row.v1"] = REVISION_OVERLAP_ROW_SCHEMA_VERSION
    metric: Literal["event_overlap_ratio", "coverage_overlap_ratio"]
    value: float
    threshold: float
    passed: bool


class RevisionMarginalValueResult(BaseModel):
    """Top-level Phase 52 result."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["revision_marginal_value_result.v1"] = REVISION_MARGINAL_VALUE_RESULT_SCHEMA_VERSION
    run_id: str
    summary: RevisionMarginalValueSummary
    required_test_results: list[RevisionTestResult] = Field(default_factory=list)
    marginal_metrics: RevisionMarginalMetrics
    marginal_thresholds: RevisionMarginalThresholds
    overlap_rows: list[RevisionOverlapRow] = Field(default_factory=list)
    decision_reasons: list[str] = Field(default_factory=list)
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True
    local_only: bool = True

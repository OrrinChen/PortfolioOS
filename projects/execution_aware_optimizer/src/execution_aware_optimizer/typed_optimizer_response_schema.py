"""Typed optimizer response acceptance contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


TYPED_OPTIMIZER_RESPONSE_INPUT_SCHEMA_VERSION = "typed_optimizer_response_input.v1"
TYPED_OPTIMIZER_RESPONSE_ROW_SCHEMA_VERSION = "typed_optimizer_response_row.v1"
TYPED_OPTIMIZER_RESPONSE_SUMMARY_SCHEMA_VERSION = "typed_optimizer_response_summary.v1"
TYPED_OPTIMIZER_RESPONSE_RESULT_SCHEMA_VERSION = "typed_optimizer_response_result.v1"

ResponseStatus = Literal["observed", "unavailable", "rejected"]
ViewState = Literal["active_view", "zero_alpha", "no_view"]


class TypedOptimizerResponseInput(BaseModel):
    """Input contract for the Phase 49 local optimizer response suite."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["typed_optimizer_response_input.v1"] = TYPED_OPTIMIZER_RESPONSE_INPUT_SCHEMA_VERSION
    run_id: str
    q2_input_contract_v2_path: Path
    expected_return_panel_path: Path
    projection_manifest_path: Path
    local_backtest_manifest_path: Path
    allow_portfolioos_run: bool = False
    rebalance_date: str | None = None
    base_expected_return_unit: float = 0.01
    no_network: bool = True
    no_broker: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "TypedOptimizerResponseInput":
        if not self.no_network:
            raise ValueError("TypedOptimizerResponseInput requires no_network=true")
        if not self.no_broker:
            raise ValueError("TypedOptimizerResponseInput requires no_broker=true")
        if self.base_expected_return_unit <= 0.0:
            raise ValueError("base_expected_return_unit must be positive")
        return self


class TypedOptimizerResponseRow(BaseModel):
    """One optimizer response diagnostic row for a typed expected-return panel."""

    schema_version: Literal["typed_optimizer_response_row.v1"] = TYPED_OPTIMIZER_RESPONSE_ROW_SCHEMA_VERSION
    panel_name: str
    view_state: ViewState
    expected_return_scale: float
    expected_return_sign: int
    optimizer_status: str
    alpha_reward_share: float
    rank_alignment: float
    top_minus_bottom_weight_delta: float
    gross_traded_notional: float
    continuous_gross_traded_notional: float
    repair_retention: float | None
    expected_return_used_share: float
    active_name_count: int
    zero_alpha_distinct_from_no_view: bool = False


class TypedOptimizerResponseSummary(BaseModel):
    """Compact Phase 49 acceptance summary."""

    schema_version: Literal["typed_optimizer_response_summary.v1"] = (
        TYPED_OPTIMIZER_RESPONSE_SUMMARY_SCHEMA_VERSION
    )
    run_id: str
    response_status: ResponseStatus
    optimizer_status: str
    panel_count: int
    positive_rank_alignment_passed: bool = False
    scaled_alpha_reward_monotone: bool = False
    sign_flip_reverses_ordering: bool = False
    no_view_distinct_from_zero_alpha: bool = False
    repair_retention_reported: bool = False
    unavailable_reason: str | None = None
    rejection_reasons: list[str] = Field(default_factory=list)


class TypedOptimizerResponseResult(BaseModel):
    """Top-level result for the typed optimizer response suite."""

    schema_version: Literal["typed_optimizer_response_result.v1"] = TYPED_OPTIMIZER_RESPONSE_RESULT_SCHEMA_VERSION
    run_id: str
    response_status: ResponseStatus
    response_rows: list[TypedOptimizerResponseRow] = Field(default_factory=list)
    summary: TypedOptimizerResponseSummary
    source_config_hash: str
    input_artifact_hashes: dict[str, str] = Field(default_factory=dict)
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True

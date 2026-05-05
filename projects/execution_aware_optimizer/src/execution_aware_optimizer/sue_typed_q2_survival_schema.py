"""SUE typed Q2 survival contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


SUE_TYPED_Q2_SURVIVAL_INPUT_SCHEMA_VERSION = "sue_typed_q2_survival_input.v1"
SUE_TYPED_Q2_SURVIVAL_ROW_SCHEMA_VERSION = "sue_typed_q2_survival_row.v1"
SUE_TYPED_Q2_SURVIVAL_SUMMARY_SCHEMA_VERSION = "sue_typed_q2_survival_summary.v1"
SUE_TYPED_Q2_SURVIVAL_RESULT_SCHEMA_VERSION = "sue_typed_q2_survival_result.v1"

SurvivalStatus = Literal["observed", "partially_observed", "unavailable", "rejected"]
SurvivalRowStatus = Literal["observed", "unavailable", "rejected"]


class SueTypedQ2SurvivalInput(BaseModel):
    """Input contract for the local SUE typed Q2 survival matrix."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_typed_q2_survival_input.v1"] = SUE_TYPED_Q2_SURVIVAL_INPUT_SCHEMA_VERSION
    run_id: str
    q2_input_contract_v2_path: Path
    expected_return_panel_path: Path
    projection_manifest_path: Path
    local_backtest_manifest_path: Path
    adapter_config_path: Path
    allow_portfolioos_run: bool = False
    local_rebalance_date: str | None = None
    no_network: bool = True
    no_broker: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "SueTypedQ2SurvivalInput":
        if not self.no_network:
            raise ValueError("SueTypedQ2SurvivalInput requires no_network=true")
        if not self.no_broker:
            raise ValueError("SueTypedQ2SurvivalInput requires no_broker=true")
        return self


class SueTypedQ2SurvivalRow(BaseModel):
    """One SUE typed Q2 survival matrix row."""

    schema_version: Literal["sue_typed_q2_survival_row.v1"] = SUE_TYPED_Q2_SURVIVAL_ROW_SCHEMA_VERSION
    scenario_id: str
    alpha_family: Literal["SUE"] = "SUE"
    projection_policy: Literal["event_window_decay"] = "event_window_decay"
    abstain_policy: Literal["explicit_abstain"] = "explicit_abstain"
    layer: str
    date: str | None = None
    status: SurvivalRowStatus
    active_rebalance_count: int
    active_name_count: int
    expected_return_used_share: float
    gross_return: float | None = None
    net_return: float | None = None
    turnover: float | None = None
    cost_drag: float | None = None
    gross_to_net_retention: float | None = None
    repair_retention: float | None = None
    unavailable_reason: str | None = None
    source_config_hash: str


class SueTypedQ2SurvivalSummary(BaseModel):
    """Compact status summary for Phase 50 artifacts."""

    schema_version: Literal["sue_typed_q2_survival_summary.v1"] = SUE_TYPED_Q2_SURVIVAL_SUMMARY_SCHEMA_VERSION
    run_id: str
    survival_status: SurvivalStatus
    sue_status: Literal["integration_benchmark_q2_candidate"] = "integration_benchmark_q2_candidate"
    injection_status: str
    expected_return_reached_optimizer_input: bool
    optimizer_rebalance_date: str | None = None
    active_rebalance_count: int
    active_name_count: int
    expected_return_used_share: float
    q2_observed_rows: int
    q2_unavailable_rows: int
    production_approval_claimed: bool = False
    unavailable_reason: str | None = None
    rejection_reasons: list[str] = Field(default_factory=list)


class SueTypedQ2SurvivalResult(BaseModel):
    """Top-level Phase 50 result."""

    schema_version: Literal["sue_typed_q2_survival_result.v1"] = SUE_TYPED_Q2_SURVIVAL_RESULT_SCHEMA_VERSION
    run_id: str
    survival_status: SurvivalStatus
    injection_status: str
    expected_return_reached_optimizer_input: bool
    optimizer_rebalance_date: str | None = None
    original_projection_dates: list[str] = Field(default_factory=list)
    local_rebalance_date: str | None = None
    active_rebalance_count: int = 0
    active_name_count: int = 0
    expected_return_used_share: float = 0.0
    q2_observed_rows: int = 0
    q2_unavailable_rows: int = 0
    matrix_rows: list[SueTypedQ2SurvivalRow] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    unavailable_reason: str | None = None
    source_config_hash: str
    input_artifact_hashes: dict[str, str] = Field(default_factory=dict)
    production_approval_claimed: bool = False
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True

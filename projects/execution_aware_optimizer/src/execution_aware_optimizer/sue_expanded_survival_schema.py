"""Expanded deterministic SUE typed-Q2 candidate contracts."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from statistics import median
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from execution_aware_optimizer.sue_typed_q2_survival_schema import SueTypedQ2SurvivalRow


EXPANDED_SUE_EVENT_ROW_SCHEMA_VERSION = "sue_expanded_event_row.v1"
EXPANDED_SUE_FIXTURE_CONFIG_SCHEMA_VERSION = "sue_expanded_fixture_config.v1"
SUE_EXPANDED_TYPED_Q2_INPUT_SCHEMA_VERSION = "sue_expanded_typed_q2_input.v1"
SUE_EXPANDED_TYPED_Q2_SUMMARY_SCHEMA_VERSION = "sue_expanded_typed_q2_summary.v1"
SUE_EXPANDED_TYPED_Q2_RESULT_SCHEMA_VERSION = "sue_expanded_typed_q2_result.v1"

EvidenceMode = Literal["deterministic_fixture", "real_historical"]
ExpandedSurvivalStatus = Literal["observed", "partially_observed", "unavailable", "rejected"]


class ExpandedSueEventRow(BaseModel):
    """One deterministic SUE event-name row with PIT timestamps."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_expanded_event_row.v1"] = EXPANDED_SUE_EVENT_ROW_SCHEMA_VERSION
    event_id: str
    symbol: str
    event_timestamp: datetime
    event_available_timestamp: datetime
    tradable_timestamp: datetime
    rebalance_date: date
    sue_score: float
    expected_return: float
    evidence_mode: EvidenceMode = "deterministic_fixture"

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        text = str(value).strip().upper()
        if not text:
            raise ValueError("symbol cannot be blank")
        return text

    @model_validator(mode="after")
    def validate_pit_order(self) -> "ExpandedSueEventRow":
        if self.event_available_timestamp > self.tradable_timestamp:
            raise ValueError("event_available_timestamp must be <= tradable_timestamp")
        if self.event_timestamp > self.event_available_timestamp:
            raise ValueError("event_timestamp must be <= event_available_timestamp")
        if self.tradable_timestamp.date() > self.rebalance_date:
            raise ValueError("tradable_timestamp date must be <= rebalance_date")
        if self.expected_return == 0.0:
            raise ValueError("expanded SUE event rows cannot encode missing coverage as zero alpha")
        return self


class SueExpandedFixtureConfig(BaseModel):
    """Deterministic fixture generator configuration for expanded SUE."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_expanded_fixture_config.v1"] = EXPANDED_SUE_FIXTURE_CONFIG_SCHEMA_VERSION
    fixture_id: str
    evidence_mode: EvidenceMode = "deterministic_fixture"
    rebalance_dates: list[date] = Field(min_length=12)
    universe_symbols: list[str] = Field(min_length=5)
    active_symbols_per_date: int = Field(ge=5)
    risk_horizon_days: int = Field(default=21, gt=0)
    cost_bps: float = Field(default=5.0, ge=0.0)
    confidence_score: float = Field(default=0.72, ge=0.0, le=1.0)
    base_expected_return: float = Field(default=0.004, gt=0.0)

    @field_validator("universe_symbols")
    @classmethod
    def normalize_symbols(cls, value: list[str]) -> list[str]:
        cleaned = [str(item).strip().upper() for item in value if str(item).strip()]
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("universe_symbols must be unique")
        return cleaned

    @model_validator(mode="after")
    def validate_active_width(self) -> "SueExpandedFixtureConfig":
        if self.active_symbols_per_date > len(self.universe_symbols):
            raise ValueError("active_symbols_per_date cannot exceed universe size")
        return self


class SueExpandedTypedQ2SurvivalInput(BaseModel):
    """Input contract for Phase 56A expanded SUE local survival."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_expanded_typed_q2_input.v1"] = SUE_EXPANDED_TYPED_Q2_INPUT_SCHEMA_VERSION
    run_id: str
    fixture_config_path: Path
    local_backtest_manifest_path: Path
    adapter_config_path: Path
    allow_portfolioos_run: bool = False
    local_rebalance_date: str | None = None
    no_network: bool = True
    no_broker: bool = True

    @model_validator(mode="after")
    def require_local_safety_flags(self) -> "SueExpandedTypedQ2SurvivalInput":
        if not self.no_network:
            raise ValueError("SueExpandedTypedQ2SurvivalInput requires no_network=true")
        if not self.no_broker:
            raise ValueError("SueExpandedTypedQ2SurvivalInput requires no_broker=true")
        return self


class SueExpandedTypedQ2SurvivalSummary(BaseModel):
    """Compact expanded SUE candidate summary."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_expanded_typed_q2_summary.v1"] = SUE_EXPANDED_TYPED_Q2_SUMMARY_SCHEMA_VERSION
    run_id: str
    evidence_mode: EvidenceMode
    survival_status: ExpandedSurvivalStatus
    injection_status: str
    expected_return_reached_optimizer_input: bool
    event_count: int
    rebalance_date_count: int
    active_rebalance_count: int
    active_name_count: int
    median_active_names_per_active_date: float
    expected_return_used_share: float
    abstain_count: int
    coverage_loss_count: int
    q2_observed_rows: int
    q2_unavailable_rows: int
    production_approval_claimed: bool = False


class SueExpandedTypedQ2SurvivalResult(BaseModel):
    """Top-level Phase 56A expanded SUE result."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_expanded_typed_q2_result.v1"] = SUE_EXPANDED_TYPED_Q2_RESULT_SCHEMA_VERSION
    run_id: str
    evidence_mode: EvidenceMode
    survival_status: ExpandedSurvivalStatus
    injection_status: str
    expected_return_reached_optimizer_input: bool
    event_count: int
    rebalance_date_count: int
    active_rebalance_count: int
    active_name_count: int
    active_names_by_rebalance_date: dict[str, int] = Field(default_factory=dict)
    expected_return_used_share: float
    abstain_count: int
    coverage_loss_count: int
    q2_observed_rows: int
    q2_unavailable_rows: int
    q2_matrix_rows: list[SueTypedQ2SurvivalRow] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    unavailable_reason: str | None = None
    source_config_hash: str
    input_artifact_hashes: dict[str, str] = Field(default_factory=dict)
    production_approval_claimed: bool = False
    no_live_data_confirmed: bool = True
    no_orders_confirmed: bool = True
    no_broker_confirmed: bool = True

    @property
    def median_active_names_per_active_date(self) -> float:
        values = [count for count in self.active_names_by_rebalance_date.values() if count > 0]
        return float(median(values)) if values else 0.0

    @property
    def summary(self) -> SueExpandedTypedQ2SurvivalSummary:
        return SueExpandedTypedQ2SurvivalSummary(
            run_id=self.run_id,
            evidence_mode=self.evidence_mode,
            survival_status=self.survival_status,
            injection_status=self.injection_status,
            expected_return_reached_optimizer_input=self.expected_return_reached_optimizer_input,
            event_count=self.event_count,
            rebalance_date_count=self.rebalance_date_count,
            active_rebalance_count=self.active_rebalance_count,
            active_name_count=self.active_name_count,
            median_active_names_per_active_date=self.median_active_names_per_active_date,
            expected_return_used_share=self.expected_return_used_share,
            abstain_count=self.abstain_count,
            coverage_loss_count=self.coverage_loss_count,
            q2_observed_rows=self.q2_observed_rows,
            q2_unavailable_rows=self.q2_unavailable_rows,
            production_approval_claimed=self.production_approval_claimed,
        )

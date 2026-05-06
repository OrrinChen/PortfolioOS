from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


FactorStatus = Literal["enabled", "reference", "disabled"]
DataTier = Literal["tier_1_price", "tier_1_price_volume", "tier_2_fundamental", "tier_3_event", "tier_3_estimates"]


class DataRequirements(BaseModel):
    required_fields: list[str]
    optional_fields: list[str] = Field(default_factory=list)


class PITContract(BaseModel):
    signal_timestamp_rule: str
    visibility_timestamp_rule: str
    tradable_timestamp_rule: str
    reporting_lag_days: int = Field(ge=0)
    reject_if_missing_visibility: bool = True


class SignalDefinition(BaseModel):
    lookback_days: int | None = Field(default=None, ge=0)
    skip_days: int = Field(default=0, ge=0)
    transform: str
    winsorize: bool = True
    winsorize_limits: tuple[float, float] = (0.01, 0.99)


class Horizon(BaseModel):
    horizon_type: str
    holding_days: int = Field(gt=0)
    rebalance_frequency: str


class Coverage(BaseModel):
    min_assets: int = Field(gt=0)
    min_history_days: int = Field(ge=0)
    missing_policy: Literal["explicit_abstain"]


class Neutralization(BaseModel):
    beta: str
    sector: str
    size: str
    liquidity: str = "report"


class CostSensitivity(BaseModel):
    expected_turnover: Literal["low", "medium", "high"]
    capacity_risk: Literal["low", "medium", "high"]


class NonClaims(BaseModel):
    production_approval: bool = False
    live_trading: bool = False
    security_orders: bool = False
    direct_q2_entry: bool = False

    @model_validator(mode="after")
    def require_all_false(self) -> "NonClaims":
        if any(self.model_dump().values()):
            raise ValueError("all non-claim flags must be false")
        return self


class FactorSpec(BaseModel):
    schema_version: Literal["factor_spec.v1"]
    factor_id: str
    family_id: str
    display_name: str
    mechanism: str
    mechanism_type: str
    data_tier: DataTier
    status: FactorStatus
    disabled_reason: str | None = None
    pit_source_required: str | None = None
    data_requirements: DataRequirements
    pit_contract: PITContract
    signal_definition: SignalDefinition
    horizon: Horizon
    coverage: Coverage
    neutralization: Neutralization
    cost_sensitivity: CostSensitivity
    known_failure_modes: list[str]
    no_view_is_not_zero_alpha: bool
    non_claims: NonClaims

    @model_validator(mode="after")
    def validate_factor_contract(self) -> "FactorSpec":
        if self.no_view_is_not_zero_alpha is not True:
            raise ValueError("no_view_is_not_zero_alpha must be true")
        if self.data_tier == "tier_2_fundamental" and self.pit_contract.reporting_lag_days < 45:
            raise ValueError("fundamental factors require a reporting lag of at least 45 days")
        if self.factor_id == "analyst_revision_disabled":
            if self.status != "disabled":
                raise ValueError("analyst revision must remain disabled without a PIT source")
            if self.disabled_reason != "missing_pit_estimate_source":
                raise ValueError("disabled analyst revision must state the missing PIT source")
            if not self.pit_source_required:
                raise ValueError("disabled analyst revision must declare the required PIT source")
        if self.status == "disabled" and not self.disabled_reason:
            raise ValueError("disabled specs require disabled_reason")
        return self


"""Configuration loading and validation helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from portfolio_os.domain.models import ConstraintSnapshot, PortfolioState


class ProjectConfig(BaseModel):
    """Static project metadata."""

    name: str
    disclaimer: str


class TradingConfig(BaseModel):
    """Trading-specific configuration."""

    market: str = "cn"
    lot_size: int = 100
    allow_fractional_shares_in_optimizer: bool = True

    @property
    def normalized_market(self) -> str:
        """Return normalized market label."""

        raw = str(self.market).strip().lower()
        if raw in {"us", "usa", "us_equity", "us_stock"}:
            return "us"
        return "cn"

    @property
    def is_us_market(self) -> bool:
        """Return whether current config targets US equities."""

        return self.normalized_market == "us"


class FeeConfig(BaseModel):
    """Simple fee model parameters."""

    commission_rate: float
    transfer_fee_rate: float
    stamp_duty_rate: float


class SlippageConfig(BaseModel):
    """Simple market-impact parameters."""

    k: float
    alpha: float = Field(default=0.6, ge=0.0)


class ObjectiveWeights(BaseModel):
    """Optimizer objective weights for legacy and risk-aware modes."""

    risk_term: float = 1.0
    tracking_error: float = 1.0
    transaction_cost: float = 1.0
    alpha_weight: float = 0.0

    # Legacy objective fields retained for backward compatibility.
    target_deviation: float | None = None
    transaction_fee: float | None = None
    turnover_penalty: float | None = None
    slippage_penalty: float | None = None


class SolverConfig(BaseModel):
    """CVXPY solver configuration."""

    name: str = "SCS"
    max_iters: int = 5000
    eps: float = 1e-4


class ReportingConfig(BaseModel):
    """Reporting preferences."""

    top_weight_changes: int = 5
    top_findings: int = 10


class IndustryBound(BaseModel):
    """Industry lower and upper limits."""

    min: float | None = None
    max: float | None = None


class DoubleTenConfig(BaseModel):
    """Simplified double-ten configuration."""

    enabled: bool = True
    single_fund_limit: float | None = None
    manager_aggregate_limit: float | None = None
    remediation_days: int = 10


class SingleNameGuardrailConfig(BaseModel):
    """Optional single-name guardrail used inside the optimizer."""

    enabled: bool = True
    buffer_lot_multiplier: float = 1.0
    buffer_min_weight: float = 5e-4


class SeverityPolicyConfig(BaseModel):
    """Template-level severity defaults."""

    blocked_trade: str = "WARNING"
    unresolved_risk: str = "BREACH"
    manager_aggregate: str = "WARNING"
    remediation_note: str = "INFO"


class ReportLabelsConfig(BaseModel):
    """Lightweight labels used by reports and exports."""

    mandate_type: str
    audience: str
    strategy_tag: str


class BlockedTradePolicyConfig(BaseModel):
    """Blocked-trade handling policy for reporting and export readiness."""

    treat_as_blocking: bool = True
    cleared_if_removed: bool = True
    export_requires_blocking_checks_cleared: bool = True


class FactorBoundConfig(BaseModel):
    """Optional absolute and active bounds for one factor exposure."""

    abs_min: float | None = None
    abs_max: float | None = None
    active_min: float | None = None
    active_max: float | None = None


class NoTradeZoneConfig(BaseModel):
    """Dead-zone policy keyed off current-vs-target weight deviation."""

    enabled: bool = False
    weight_threshold: float = Field(default=0.003, ge=0.0)


class ConstraintConfig(BaseModel):
    """Constraint template loaded from YAML."""

    single_name_max_weight: float
    industry_bounds: dict[str, IndustryBound] = Field(default_factory=dict)
    max_turnover: float
    min_order_notional: float
    participation_limit: float
    cash_non_negative: bool = True
    double_ten: DoubleTenConfig = Field(default_factory=DoubleTenConfig)
    single_name_guardrail: SingleNameGuardrailConfig = Field(default_factory=SingleNameGuardrailConfig)
    factor_bounds: dict[str, FactorBoundConfig] = Field(default_factory=dict)
    no_trade_zone: NoTradeZoneConfig = Field(default_factory=NoTradeZoneConfig)
    severity_policy: SeverityPolicyConfig = Field(default_factory=SeverityPolicyConfig)
    report_labels: ReportLabelsConfig
    blocked_trade_policy: BlockedTradePolicyConfig = Field(default_factory=BlockedTradePolicyConfig)


class ExecutionProfile(BaseModel):
    """Execution profile settings."""

    urgency: str = "low"
    slice_ratio: float = 0.25
    max_child_orders: int = 4
    backtest_fixed_half_spread_bps: float = 5.0


class RiskModelConfig(BaseModel):
    """Risk-model inputs and covariance estimator settings."""

    enabled: bool = False
    integration_mode: Literal["replace", "augment"] = "replace"
    estimator: Literal["sample", "ledoit_wolf"] = "ledoit_wolf"
    returns_path: str | None = None
    factor_exposure_path: str | None = None
    lookback_days: int = Field(default=252, gt=0)
    min_history_days: int = Field(default=120, gt=0)
    annualization_factor: float = Field(default=252.0, gt=0.0)
    diagonal_jitter: float = Field(default=1e-8, ge=0.0)

    @model_validator(mode="after")
    def validate_required_paths_when_enabled(self) -> "RiskModelConfig":
        """Require explicit risk input files when risk mode is enabled."""

        if not self.enabled:
            return self
        missing: list[str] = []
        if not self.returns_path:
            missing.append("risk_model.returns_path")
        if not self.factor_exposure_path:
            missing.append("risk_model.factor_exposure_path")
        if missing:
            raise ValueError("Missing required risk-model path(s): " + ", ".join(missing))
        if self.min_history_days > self.lookback_days:
            raise ValueError("risk_model.min_history_days cannot exceed risk_model.lookback_days")
        return self


class AppConfig(BaseModel):
    """Merged runtime configuration."""

    project: ProjectConfig
    trading: TradingConfig
    fees: FeeConfig
    slippage: SlippageConfig
    objective_weights: ObjectiveWeights
    solver: SolverConfig
    reporting: ReportingConfig
    constraints: ConstraintConfig
    execution: ExecutionProfile
    risk_model: RiskModelConfig = Field(default_factory=RiskModelConfig)
    portfolio_state: PortfolioState

    @property
    def effective_single_name_limit(self) -> float:
        """Return the strictest single-name limit in force."""

        generic_limit = self.constraints.single_name_max_weight
        double_ten_limit = self.constraints.double_ten.single_fund_limit
        if self.trading.is_us_market:
            return generic_limit
        if self.constraints.double_ten.enabled and double_ten_limit is not None:
            return min(generic_limit, double_ten_limit)
        return generic_limit

    def build_constraint_snapshot(self, source_path: Path | None = None) -> ConstraintSnapshot:
        """Create a serializable snapshot of resolved constraints."""

        return ConstraintSnapshot(
            source_path=str(source_path) if source_path is not None else None,
            values=self.constraints.model_dump(mode="json"),
            effective_single_name_limit=self.effective_single_name_limit,
        )


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    """Load a YAML file into a dictionary."""

    with Path(path).open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    return data


def load_app_config(
    *,
    default_path: str | Path,
    constraints_path: str | Path,
    execution_path: str | Path,
    portfolio_state: PortfolioState,
) -> AppConfig:
    """Load and validate the merged application configuration."""

    default_payload = load_yaml_file(default_path)
    constraints_payload = load_yaml_file(constraints_path)
    execution_payload = load_yaml_file(execution_path)
    merged = {
        **default_payload,
        "constraints": constraints_payload,
        "execution": execution_payload,
        "portfolio_state": portfolio_state.model_dump(mode="json"),
    }
    config = AppConfig.model_validate(merged)
    config_base_dir = Path(default_path).resolve().parent
    if config.risk_model.returns_path:
        returns_path = Path(config.risk_model.returns_path)
        if not returns_path.is_absolute():
            config.risk_model.returns_path = str((config_base_dir / returns_path).resolve())
    if config.risk_model.factor_exposure_path:
        factor_path = Path(config.risk_model.factor_exposure_path)
        if not factor_path.is_absolute():
            config.risk_model.factor_exposure_path = str((config_base_dir / factor_path).resolve())
    return config

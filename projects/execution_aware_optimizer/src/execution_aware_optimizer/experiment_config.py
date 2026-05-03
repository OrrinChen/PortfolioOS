"""Config schema for the execution-aware optimizer project."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator

from portfolio_os.domain.errors import InputValidationError


ALPHA_DECAY_LADDER_LAYERS: tuple[str, ...] = (
    "raw_top_alpha_equal_weight",
    "risk_controlled",
    "sector_constrained",
    "position_constrained",
    "turnover_constrained",
    "liquidity_constrained",
    "full_execution_aware_cost_adjusted",
)

LayerName = Literal[
    "raw_top_alpha_equal_weight",
    "risk_controlled",
    "sector_constrained",
    "position_constrained",
    "turnover_constrained",
    "liquidity_constrained",
    "full_execution_aware_cost_adjusted",
]


class AlphaInputConfig(BaseModel):
    """Alpha input adapter configuration."""

    path: str | None = None
    rank_normalize_by_date: bool = False
    winsorize_quantile: float | None = None


class PortfolioOSAdapterConfig(BaseModel):
    """Explicit controls for optional PortfolioOS API integration."""

    backtest_manifest: str | None = None
    allow_portfolioos_run: bool = False
    transaction_cost_objective_mode: Literal["nav_fraction", "raw_currency"] = "nav_fraction"
    cost_assumption_bps: float | None = None
    output_dir: str = "projects/execution_aware_optimizer/reports"


class LayerConfig(BaseModel):
    """One alpha-decay ladder layer."""

    layer_name: LayerName
    enabled: bool = True
    description: str | None = None
    constraint_overrides: dict[str, Any] = Field(default_factory=dict)


def default_ladder_layers() -> list[LayerConfig]:
    """Return the canonical Q2 layer sequence."""

    return [LayerConfig(layer_name=layer_name) for layer_name in ALPHA_DECAY_LADDER_LAYERS]


class ExperimentConfig(BaseModel):
    """Top-level experiment configuration."""

    experiment_name: str = "execution_aware_optimizer"
    alpha_input: AlphaInputConfig = Field(default_factory=AlphaInputConfig)
    portfolioos: PortfolioOSAdapterConfig = Field(default_factory=PortfolioOSAdapterConfig)
    layers: list[LayerConfig] = Field(default_factory=default_ladder_layers)
    cost_sensitivity_bps: list[int] = Field(default_factory=lambda: [0, 5, 10, 25, 50])
    execution_matrix: dict[str, Any] = Field(default_factory=dict)
    report_path: str = "projects/execution_aware_optimizer/reports/execution_aware_optimizer_report.md"

    @field_validator("cost_sensitivity_bps")
    @classmethod
    def validate_cost_sensitivity_bps(cls, values: list[int]) -> list[int]:
        """Require non-negative basis-point assumptions."""

        if not values:
            raise ValueError("cost_sensitivity_bps must include at least one cost level.")
        invalid = [value for value in values if int(value) < 0]
        if invalid:
            raise ValueError("cost_sensitivity_bps cannot include negative values.")
        return [int(value) for value in values]

    @field_validator("layers")
    @classmethod
    def validate_layers(cls, values: list[LayerConfig]) -> list[LayerConfig]:
        """Reject empty layer lists."""

        if not values:
            raise ValueError("layers must include at least one layer.")
        return values


def load_experiment_config(path: str | Path) -> ExperimentConfig:
    """Load an experiment YAML config."""

    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise InputValidationError(f"Expected YAML mapping in {config_path}.")
    return ExperimentConfig.model_validate(payload)

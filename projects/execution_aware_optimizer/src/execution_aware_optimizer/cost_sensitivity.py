"""Cost-sensitivity planning adapters for Q2."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel

from execution_aware_optimizer.experiment_config import ExperimentConfig


class CostSensitivityScenario(BaseModel):
    """One non-mutating cost-assumption scenario."""

    cost_bps: int
    config: ExperimentConfig
    portfolioos_overrides: dict[str, float | str]


def _cost_bps_label(cost_bps: int) -> str:
    """Return a stable path label for one cost level."""

    return f"cost_{int(cost_bps)}bps"


def build_portfolioos_cost_overrides(
    *,
    cost_bps: int,
    transaction_cost_objective_mode: str,
) -> dict[str, float | str]:
    """Build explicit PortfolioOS config overrides for one cost assumption.

    The overrides are returned as data, not applied globally. A future execution
    adapter can write these into a derived manifest/config inside a run-specific
    output directory.
    """

    bps = float(cost_bps)
    return {
        "fees.commission_rate": bps / 10000.0,
        "execution.backtest_fixed_half_spread_bps": bps,
        "objective_weights.transaction_cost_objective_mode": transaction_cost_objective_mode,
    }


def build_cost_sensitivity_scenarios(config: ExperimentConfig) -> list[CostSensitivityScenario]:
    """Clone the base experiment config once per cost level."""

    scenarios: list[CostSensitivityScenario] = []
    base_output_dir = Path(config.portfolioos.output_dir)
    for raw_cost_bps in config.cost_sensitivity_bps:
        cost_bps = int(raw_cost_bps)
        scenario_config = config.model_copy(deep=True)
        scenario_config.portfolioos.cost_assumption_bps = float(cost_bps)
        scenario_config.portfolioos.output_dir = str(base_output_dir / _cost_bps_label(cost_bps))
        scenarios.append(
            CostSensitivityScenario(
                cost_bps=cost_bps,
                config=scenario_config,
                portfolioos_overrides=build_portfolioos_cost_overrides(
                    cost_bps=cost_bps,
                    transaction_cost_objective_mode=config.portfolioos.transaction_cost_objective_mode,
                ),
            )
        )
    return scenarios

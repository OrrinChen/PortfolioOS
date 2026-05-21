"""Baseline strategy helpers for the historical backtest loop."""

from __future__ import annotations

import pandas as pd

from portfolio_os.optimizer.rebalancer import RebalanceRun
from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.simulation.benchmarks import naive_target_rebalance
from portfolio_os.utils.config import AppConfig


SUPPORTED_BASELINES = {
    "naive_pro_rata",
    "buy_and_hold",
    "alpha_only_top_quintile",
    "cost_unaware_rebalance",
}


def run_naive_pro_rata(
    universe,
    config: AppConfig,
    *,
    input_findings=None,
) -> RebalanceRun:
    """Run the direct pro-rata target baseline with a stable public name."""

    run = naive_target_rebalance(
        universe,
        config,
        input_findings=input_findings,
    )
    run.strategy_name = "naive_pro_rata"
    return run


def run_alpha_only_top_quintile(
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    alpha_target_weights: dict[str, float],
    input_findings=None,
) -> RebalanceRun:
    """Run the top-quintile equal-weight alpha baseline through the naive rebalance path."""

    alpha_universe = universe.copy()
    alpha_universe["target_weight"] = (
        alpha_universe["ticker"].astype(str).map(alpha_target_weights).fillna(0.0).astype(float)
    )
    run = naive_target_rebalance(
        alpha_universe,
        config,
        input_findings=input_findings,
    )
    run.strategy_name = "alpha_only_top_quintile"
    return run


def run_cost_unaware_rebalance(
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    input_findings=None,
) -> RebalanceRun:
    """Run the optimizer with cost terms disabled in the objective only.

    Realized backtest fills still pay the execution profile's commission and
    spread costs. This baseline isolates the effect of cost-aware optimization
    from the accounting of transaction costs after trades are chosen.
    """

    config_copy = config.model_copy(deep=True)
    config_copy.objective_weights.transaction_cost = 0.0
    config_copy.objective_weights.transaction_fee = 0.0
    config_copy.objective_weights.turnover_penalty = 0.0
    config_copy.objective_weights.slippage_penalty = 0.0
    run = run_rebalance(
        universe,
        config_copy,
        input_findings=input_findings,
    )
    run.strategy_name = "cost_unaware_rebalance"
    return run

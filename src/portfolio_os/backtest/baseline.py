"""Baseline strategy helpers for the historical backtest loop."""

from __future__ import annotations

from portfolio_os.optimizer.rebalancer import RebalanceRun
from portfolio_os.simulation.benchmarks import naive_target_rebalance
from portfolio_os.utils.config import AppConfig


SUPPORTED_BASELINES = {"naive_pro_rata", "buy_and_hold"}


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

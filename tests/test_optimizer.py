from __future__ import annotations

import numpy as np

from portfolio_os.optimizer.rebalancer import run_rebalance
from portfolio_os.simulation.benchmarks import run_benchmark_suite


def test_optimizer_generates_non_empty_trade_list(sample_context: dict) -> None:
    universe = sample_context["universe"]
    config = sample_context["config"]
    rebalance_run = run_rebalance(universe, config)

    prices = universe["estimated_price"].to_numpy(dtype=float)
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    target_weights = universe["target_weight"].to_numpy(dtype=float)
    pre_trade_nav = rebalance_run.pre_trade_nav
    current_deviation = float(np.sum(np.abs(current_quantities * prices / pre_trade_nav - target_weights)))
    repaired_deviation = float(np.sum(np.abs(rebalance_run.post_trade_quantities * prices / pre_trade_nav - target_weights)))

    assert rebalance_run.orders
    assert repaired_deviation < current_deviation
    assert rebalance_run.basket.gross_traded_notional / pre_trade_nav <= config.constraints.max_turnover + 1e-6


def test_portfolio_os_cost_is_not_higher_than_naive(sample_context: dict) -> None:
    suite = run_benchmark_suite(sample_context["universe"], sample_context["config"])
    metrics_by_name = {metric.strategy_name: metric for metric in suite.comparison.strategies}

    assert (
        metrics_by_name["portfolio_os_rebalance"].estimated_total_cost
        <= metrics_by_name["naive_target_rebalance"].estimated_total_cost + 1e-6
    )


def test_portfolio_os_blocked_trades_are_not_higher_than_naive(sample_context: dict) -> None:
    suite = run_benchmark_suite(sample_context["universe"], sample_context["config"])
    metrics_by_name = {metric.strategy_name: metric for metric in suite.comparison.strategies}

    assert (
        metrics_by_name["portfolio_os_rebalance"].blocked_trade_count
        <= metrics_by_name["naive_target_rebalance"].blocked_trade_count
    )

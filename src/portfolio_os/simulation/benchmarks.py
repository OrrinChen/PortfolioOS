"""Static benchmark strategies and comparison rendering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from portfolio_os.domain.models import BenchmarkComparison, BenchmarkMetrics, TradeInstruction
from portfolio_os.execution.basket import build_basket
from portfolio_os.execution.order import build_orders
from portfolio_os.explain.trade_reason import build_benchmark_explanation
from portfolio_os.optimizer.rebalancer import RebalanceRun, run_rebalance
from portfolio_os.optimizer.repair import repair_instructions
from portfolio_os.risk.model import RiskModelContext, build_risk_model_context
from portfolio_os.simulation.backtest import cash_after_orders, evaluate_strategy_metrics, signed_quantities_from_orders
from portfolio_os.storage.snapshots import file_metadata
from portfolio_os.compliance.pretrade import collect_data_quality_findings, run_pretrade_checks
from portfolio_os.constraints.base import compute_post_trade_quantities
from portfolio_os.utils.config import AppConfig


@dataclass
class BenchmarkSuiteResult:
    """Benchmark runs, metrics, and rendered summaries."""

    runs: dict[str, RebalanceRun]
    comparison: BenchmarkComparison


def _build_rebalance_run_from_instructions(
    *,
    strategy_name: str,
    universe: pd.DataFrame,
    instructions: list[TradeInstruction],
    config: AppConfig,
    pre_trade_nav: float,
    risk_context: RiskModelContext | None = None,
    input_findings=None,
) -> RebalanceRun:
    """Build a rebalance-style result from explicit trade instructions."""

    repaired_instructions, repair_findings = repair_instructions(
        instructions,
        universe,
        config,
        pre_trade_nav=pre_trade_nav,
    )
    orders = build_orders(repaired_instructions, universe, config)
    basket = build_basket(orders)
    findings = run_pretrade_checks(
        universe,
        orders,
        config,
        pre_trade_nav=pre_trade_nav,
        extra_findings=[*(input_findings or []), *repair_findings],
    )
    signed_quantities = signed_quantities_from_orders(universe, orders)
    post_trade_quantities = compute_post_trade_quantities(universe, signed_quantities)
    cash_after = cash_after_orders(universe, signed_quantities, config)
    return RebalanceRun(
        strategy_name=strategy_name,
        universe=universe.copy(),
        optimization_result=None,
        risk_context=risk_context,
        repaired_instructions=repaired_instructions,
        orders=orders,
        basket=basket,
        findings=findings,
        post_trade_quantities=post_trade_quantities,
        cash_before=config.portfolio_state.available_cash,
        cash_after=cash_after,
        pre_trade_nav=pre_trade_nav,
    )


def naive_target_rebalance(
    universe: pd.DataFrame,
    config: AppConfig,
    risk_context: RiskModelContext | None = None,
    *,
    input_findings=None,
) -> RebalanceRun:
    """Directly chase target weights, then apply basic executable repair."""

    prices = universe["estimated_price"].to_numpy(dtype=float)
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    target_weights = universe["target_weight"].to_numpy(dtype=float)
    pre_trade_nav = float(np.sum(current_quantities * prices) + config.portfolio_state.available_cash)
    target_quantities = np.divide(
        target_weights * pre_trade_nav,
        prices,
        out=np.zeros_like(target_weights, dtype=float),
        where=prices > 0,
    )
    desired_trades = target_quantities - current_quantities
    instructions = [
        TradeInstruction(
            ticker=str(ticker),
            quantity=float(quantity),
            estimated_price=float(price),
            current_weight=float(current_weight),
            target_weight=float(target_weight),
            reason_tags=["naive_target"],
        )
        for ticker, quantity, price, current_weight, target_weight in zip(
            universe["ticker"],
            desired_trades,
            prices,
            universe["current_weight"],
            target_weights,
            strict=True,
        )
    ]
    return _build_rebalance_run_from_instructions(
        strategy_name="naive_target_rebalance",
        universe=universe,
        instructions=instructions,
        config=config,
        pre_trade_nav=pre_trade_nav,
        risk_context=risk_context,
        input_findings=input_findings,
    )


def cost_unaware_rebalance(
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    input_findings=None,
) -> RebalanceRun:
    """Run the optimizer with target deviation only and no cost terms."""

    config_copy = config.model_copy(deep=True)
    config_copy.objective_weights.transaction_cost = 0.0
    config_copy.objective_weights.transaction_fee = 0.0
    config_copy.objective_weights.turnover_penalty = 0.0
    config_copy.objective_weights.slippage_penalty = 0.0
    config_copy.constraints.single_name_max_weight = 1.0
    config_copy.constraints.industry_bounds = {}
    config_copy.constraints.max_turnover = 1.0
    config_copy.constraints.double_ten.enabled = False
    run = run_rebalance(universe, config_copy, input_findings=input_findings)
    run.strategy_name = "cost_unaware_rebalance"
    return run


def portfolio_os_rebalance(
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    input_findings=None,
) -> RebalanceRun:
    """Run the default PortfolioOS cost-aware strategy."""

    run = run_rebalance(universe, config, input_findings=input_findings)
    run.strategy_name = "portfolio_os_rebalance"
    return run


def metrics_from_rebalance_run(run: RebalanceRun, config: AppConfig) -> BenchmarkMetrics:
    """Evaluate one rebalance run with unified benchmark metrics."""

    return evaluate_strategy_metrics(
        strategy_name=run.strategy_name,
        universe=run.universe,
        orders=run.orders,
        findings=run.findings,
        config=config,
        pre_trade_nav=run.pre_trade_nav,
        cash_before=run.cash_before,
        cash_after=run.cash_after,
        risk_context=run.risk_context,
    )


def build_comparison_summary(metrics: list[BenchmarkMetrics]) -> dict[str, Any]:
    """Build a compact comparison summary for demo output."""

    metrics_by_name = {metric.strategy_name: metric for metric in metrics}
    portfolio_os_metrics = metrics_by_name["portfolio_os_rebalance"]
    naive_metrics = metrics_by_name["naive_target_rebalance"]
    cost_unaware_metrics = metrics_by_name["cost_unaware_rebalance"]
    cost_savings_vs_naive = naive_metrics.estimated_total_cost - portfolio_os_metrics.estimated_total_cost
    cost_savings_vs_cost_unaware = (
        cost_unaware_metrics.estimated_total_cost - portfolio_os_metrics.estimated_total_cost
    )
    turnover_reduction_vs_naive = naive_metrics.turnover - portfolio_os_metrics.turnover
    blocked_trade_reduction_vs_naive = (
        naive_metrics.blocked_trade_count - portfolio_os_metrics.blocked_trade_count
    )
    compliance_finding_reduction_vs_naive = (
        naive_metrics.compliance_finding_count - portfolio_os_metrics.compliance_finding_count
    )
    target_deviation_delta_vs_naive = (
        portfolio_os_metrics.target_deviation_after - naive_metrics.target_deviation_after
    )
    target_deviation_delta_vs_cost_unaware = (
        portfolio_os_metrics.target_deviation_after - cost_unaware_metrics.target_deviation_after
    )
    cost_savings_vs_naive_bps = (
        cost_savings_vs_naive / portfolio_os_metrics.pre_trade_nav * 10000.0
        if portfolio_os_metrics.pre_trade_nav
        else 0.0
    )
    conclusion = (
        "Compared with naive rebalance, PortfolioOS reduced estimated trading cost by "
        f"{cost_savings_vs_naive_bps:.2f} bps and eliminated {blocked_trade_reduction_vs_naive} blocked trades, "
        "while accepting a measured target-fit trade-off under tradability and compliance-aware execution logic."
    )
    return {
        "portfolio_os_strategy": portfolio_os_metrics.strategy_name,
        "cost_savings_vs_naive": float(cost_savings_vs_naive),
        "cost_savings_vs_cost_unaware": float(cost_savings_vs_cost_unaware),
        "turnover_reduction_vs_naive": float(turnover_reduction_vs_naive),
        "blocked_trade_reduction_vs_naive": int(blocked_trade_reduction_vs_naive),
        "compliance_finding_reduction_vs_naive": int(compliance_finding_reduction_vs_naive),
        "target_deviation_delta_vs_naive": float(target_deviation_delta_vs_naive),
        "target_deviation_delta_vs_cost_unaware": float(target_deviation_delta_vs_cost_unaware),
        "cost_savings_vs_naive_bps": float(cost_savings_vs_naive_bps),
        "explanation": build_benchmark_explanation(
            {
                "turnover_reduction_vs_naive": float(turnover_reduction_vs_naive),
                "blocked_trade_reduction_vs_naive": int(blocked_trade_reduction_vs_naive),
                "cost_savings_vs_cost_unaware": float(cost_savings_vs_cost_unaware),
            }
        ),
        "conclusion": conclusion,
    }


def run_benchmark_suite(universe: pd.DataFrame, config: AppConfig) -> BenchmarkSuiteResult:
    """Run the three benchmark strategies on a static snapshot."""

    input_findings = collect_data_quality_findings(universe, config)
    risk_context = build_risk_model_context(universe, config)
    naive_run = naive_target_rebalance(
        universe,
        config,
        risk_context=risk_context,
        input_findings=input_findings,
    )
    cost_unaware_run = cost_unaware_rebalance(universe, config, input_findings=input_findings)
    portfolio_os_run = portfolio_os_rebalance(universe, config, input_findings=input_findings)
    ordered_runs = [naive_run, cost_unaware_run, portfolio_os_run]
    metrics = [metrics_from_rebalance_run(run, config) for run in ordered_runs]
    comparison = BenchmarkComparison(
        strategies=metrics,
        comparison_summary=build_comparison_summary(metrics),
    )
    return BenchmarkSuiteResult(
        runs={run.strategy_name: run for run in ordered_runs},
        comparison=comparison,
    )


def build_benchmark_comparison_payload(
    *,
    input_paths: dict[str, str],
    suite_result: BenchmarkSuiteResult,
) -> dict[str, Any]:
    """Build the benchmark comparison JSON payload."""

    return {
        "inputs": {name: file_metadata(path) for name, path in input_paths.items()},
        "strategies": [metric.model_dump(mode="json") for metric in suite_result.comparison.strategies],
        "comparison_summary": suite_result.comparison.comparison_summary,
    }


def render_benchmark_comparison_markdown(comparison: BenchmarkComparison) -> str:
    """Render a demo-friendly benchmark comparison report."""

    metrics_by_name = {metric.strategy_name: metric for metric in comparison.strategies}
    portfolio_os_metrics = metrics_by_name["portfolio_os_rebalance"]
    naive_metrics = metrics_by_name["naive_target_rebalance"]
    cost_unaware_metrics = metrics_by_name["cost_unaware_rebalance"]
    summary = comparison.comparison_summary
    lines = [
        "# Benchmark Comparison",
        "",
        "> Auxiliary decision-support tool only. Not investment advice.",
        "",
        "## Strategy Table",
        "",
        "| Strategy | Target Deviation After | Portfolio Variance After | Tracking Error Variance After | Estimated Total Cost | Turnover | Blocked Trades | Compliance Findings | Buy Orders | Sell Orders |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for metric in comparison.strategies:
        lines.append(
            f"| {metric.strategy_name} | {metric.target_deviation_after:.6f} | "
            f"{metric.portfolio_variance_after:.8f} | {metric.tracking_error_variance_after:.8f} | "
            f"{metric.estimated_total_cost:.2f} | {metric.turnover:.4f} | "
            f"{metric.blocked_trade_count} | {metric.compliance_finding_count} | "
            f"{metric.buy_order_count} | {metric.sell_order_count} |"
        )

    lines.extend(
        [
            "",
            "## PortfolioOS vs Naive",
            f"- Estimated cost savings: {summary['cost_savings_vs_naive']:.2f}",
            f"- Turnover reduction: {summary['turnover_reduction_vs_naive']:.4f}",
            f"- Blocked trade reduction: {summary['blocked_trade_reduction_vs_naive']}",
            f"- Compliance finding reduction: {summary['compliance_finding_reduction_vs_naive']}",
            f"- Target deviation after: {portfolio_os_metrics.target_deviation_after:.6f} vs {naive_metrics.target_deviation_after:.6f}",
            f"- Target deviation delta vs naive: {summary['target_deviation_delta_vs_naive']:+.6f}",
            "",
            "## PortfolioOS vs Cost-Unaware",
            f"- Estimated cost savings: {summary['cost_savings_vs_cost_unaware']:.2f}",
            f"- Target deviation after: {portfolio_os_metrics.target_deviation_after:.6f} vs {cost_unaware_metrics.target_deviation_after:.6f}",
            f"- Target deviation delta vs cost-unaware: {summary['target_deviation_delta_vs_cost_unaware']:+.6f}",
            f"- Blocked trades: {portfolio_os_metrics.blocked_trade_count} vs {cost_unaware_metrics.blocked_trade_count}",
            "",
            "## Why PortfolioOS Looked Better",
        ]
    )
    for bullet in summary.get("explanation", []):
        lines.append(f"- {bullet}")
    lines.extend(
        [
            "",
            "## Conclusion",
            summary["conclusion"],
            "",
        ]
    )
    return "\n".join(lines)

"""Static snapshot evaluation helpers for benchmark comparisons."""

from __future__ import annotations

import numpy as np
import pandas as pd

from portfolio_os.compliance.findings import summarize_findings
from portfolio_os.constraints.base import compute_post_trade_quantities, gross_traded_notional, target_deviation_value
from portfolio_os.cost.fee import estimate_fee_array
from portfolio_os.cost.slippage import estimate_slippage_array
from portfolio_os.domain.enums import OrderSide
from portfolio_os.domain.models import BenchmarkMetrics, ComplianceFinding, Order
from portfolio_os.risk.model import RiskModelContext, portfolio_variance, tracking_error_variance
from portfolio_os.utils.config import AppConfig


def signed_quantities_from_orders(universe: pd.DataFrame, orders: list[Order]) -> np.ndarray:
    """Convert an order list into a signed quantity vector aligned to the universe."""

    quantity_map = {
        order.ticker: float(order.quantity if order.side == OrderSide.BUY else -order.quantity)
        for order in orders
    }
    return np.array([quantity_map.get(str(ticker), 0.0) for ticker in universe["ticker"]], dtype=float)


def cash_after_orders(
    universe: pd.DataFrame,
    signed_quantities: np.ndarray,
    config: AppConfig,
) -> float:
    """Compute post-trade cash using the canonical cost formulas."""

    prices = universe["estimated_price"].to_numpy(dtype=float)
    adv_shares = universe["adv_shares"].to_numpy(dtype=float)
    fees = estimate_fee_array(signed_quantities, prices, config.fees)
    slippage = estimate_slippage_array(signed_quantities, prices, adv_shares, config.slippage)
    return float(
        config.portfolio_state.available_cash
        - np.sum(prices * signed_quantities)
        - np.sum(fees)
        - np.sum(slippage)
    )


def evaluate_strategy_metrics(
    *,
    strategy_name: str,
    universe: pd.DataFrame,
    orders: list[Order],
    findings: list[ComplianceFinding],
    config: AppConfig,
    pre_trade_nav: float,
    cash_before: float,
    cash_after: float,
    risk_context: RiskModelContext | None = None,
) -> BenchmarkMetrics:
    """Evaluate a static rebalance strategy using shared project metrics."""

    prices = universe["estimated_price"].to_numpy(dtype=float)
    target_weights = universe["target_weight"].to_numpy(dtype=float)
    current_quantities = universe["quantity"].to_numpy(dtype=float)
    signed_quantities = signed_quantities_from_orders(universe, orders)
    post_trade_quantities = compute_post_trade_quantities(universe, signed_quantities)
    fees = estimate_fee_array(signed_quantities, prices, config.fees)
    slippage = estimate_slippage_array(
        signed_quantities,
        prices,
        universe["adv_shares"].to_numpy(dtype=float),
        config.slippage,
    )
    finding_summary = summarize_findings(findings)
    buy_order_count = sum(1 for order in orders if order.side == OrderSide.BUY)
    sell_order_count = sum(1 for order in orders if order.side == OrderSide.SELL)

    target_deviation_before = target_deviation_value(current_quantities, prices, target_weights, pre_trade_nav)
    target_deviation_after = target_deviation_value(post_trade_quantities, prices, target_weights, pre_trade_nav)
    portfolio_variance_before = 0.0
    portfolio_variance_after = 0.0
    tracking_error_before = 0.0
    tracking_error_after = 0.0
    if risk_context is not None:
        portfolio_variance_before = portfolio_variance(
            current_quantities,
            prices,
            pre_trade_nav,
            risk_context.sigma,
        )
        portfolio_variance_after = portfolio_variance(
            post_trade_quantities,
            prices,
            pre_trade_nav,
            risk_context.sigma,
        )
        tracking_error_before = tracking_error_variance(
            current_quantities,
            prices,
            pre_trade_nav,
            target_weights,
            risk_context.sigma,
        )
        tracking_error_after = tracking_error_variance(
            post_trade_quantities,
            prices,
            pre_trade_nav,
            target_weights,
            risk_context.sigma,
        )
    traded_notional = gross_traded_notional(signed_quantities, prices)
    estimated_fee_total = float(np.sum(fees))
    estimated_slippage_total = float(np.sum(slippage))

    return BenchmarkMetrics(
        strategy_name=strategy_name,
        pre_trade_nav=float(pre_trade_nav),
        cash_before=float(cash_before),
        cash_after=float(cash_after),
        target_deviation_before=float(target_deviation_before),
        target_deviation_after=float(target_deviation_after),
        target_deviation_improvement=float(target_deviation_before - target_deviation_after),
        portfolio_variance_before=float(portfolio_variance_before),
        portfolio_variance_after=float(portfolio_variance_after),
        tracking_error_variance_before=float(tracking_error_before),
        tracking_error_variance_after=float(tracking_error_after),
        gross_traded_notional=float(traded_notional),
        turnover=float(traded_notional / pre_trade_nav) if pre_trade_nav else 0.0,
        estimated_fee_total=estimated_fee_total,
        estimated_slippage_total=estimated_slippage_total,
        estimated_total_cost=float(estimated_fee_total + estimated_slippage_total),
        buy_order_count=int(buy_order_count),
        sell_order_count=int(sell_order_count),
        blocked_trade_count=int(finding_summary["blocked_trade_count"]),
        compliance_finding_count=int(finding_summary["total"]),
    )


def describe_backtest_scope() -> str:
    """Describe how the simulation module is used in the MVP."""

    return "The MVP uses static snapshot evaluation helpers for benchmark comparison, not historical replay."

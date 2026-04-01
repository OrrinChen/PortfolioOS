"""High-level rebalance orchestration."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from portfolio_os.compliance.pretrade import run_pretrade_checks
from portfolio_os.domain.models import ComplianceFinding
from portfolio_os.constraints.base import compute_post_trade_quantities
from portfolio_os.execution.basket import build_basket
from portfolio_os.execution.order import build_orders
from portfolio_os.optimizer.precheck import collect_rebalance_precheck_findings
from portfolio_os.optimizer.repair import repair_instructions
from portfolio_os.optimizer.solver import solve_rebalance_problem
from portfolio_os.risk.model import RiskModelContext, build_risk_model_context
from portfolio_os.utils.config import AppConfig


@dataclass
class RebalanceRun:
    """Container for the end-to-end rebalance output."""

    strategy_name: str
    universe: pd.DataFrame
    optimization_result: object
    risk_context: RiskModelContext | None
    repaired_instructions: list
    orders: list
    basket: object
    findings: list
    post_trade_quantities: np.ndarray
    cash_before: float
    cash_after: float
    pre_trade_nav: float


def run_rebalance(
    universe: pd.DataFrame,
    config: AppConfig,
    *,
    input_findings: list[ComplianceFinding] | None = None,
) -> RebalanceRun:
    """Run optimization, repair, order building, and pre-trade checks."""

    precheck_findings = collect_rebalance_precheck_findings(universe, config)
    risk_context = build_risk_model_context(universe, config)
    optimization_result = solve_rebalance_problem(
        universe,
        config,
        risk_context=risk_context,
    )
    repaired_instructions, repair_findings = repair_instructions(
        optimization_result.instructions,
        universe,
        config,
        pre_trade_nav=optimization_result.pre_trade_nav,
    )
    orders = build_orders(repaired_instructions, universe, config)
    basket = build_basket(orders)
    findings = run_pretrade_checks(
        universe,
        orders,
        config,
        pre_trade_nav=optimization_result.pre_trade_nav,
        extra_findings=[*(input_findings or []), *precheck_findings, *repair_findings],
    )
    quantity_map = {instruction.ticker: instruction.quantity for instruction in repaired_instructions}
    signed_quantities = np.array(
        [float(quantity_map.get(ticker, 0.0)) for ticker in universe["ticker"]],
        dtype=float,
    )
    post_trade_quantities = compute_post_trade_quantities(universe, signed_quantities)
    cash_after = (
        config.portfolio_state.available_cash
        - basket.total_fee
        - basket.total_slippage
        - float(np.sum(universe["estimated_price"].to_numpy(dtype=float) * signed_quantities))
    )
    return RebalanceRun(
        strategy_name="portfolio_os_rebalance",
        universe=universe.copy(),
        optimization_result=optimization_result,
        risk_context=risk_context,
        repaired_instructions=repaired_instructions,
        orders=orders,
        basket=basket,
        findings=findings,
        post_trade_quantities=post_trade_quantities,
        cash_before=config.portfolio_state.available_cash,
        cash_after=float(cash_after),
        pre_trade_nav=optimization_result.pre_trade_nav,
    )
